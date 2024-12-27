package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
)

// Config holds the application configuration
type Config struct {
	timeout      int
	maxRetries   int
	batchSize    int
	maxWorkers   int
	outputFile   string
}

// Result represents a scraping result
type Result struct {
	url   string
	links []string
	err   error
}

// Scraper handles the web scraping operations
type Scraper struct {
	config Config
	client *http.Client
}

// NewScraper creates a new scraper instance
func NewScraper(config Config) *Scraper {
	return &Scraper{
		config: config,
		client: &http.Client{
			Timeout: time.Duration(config.timeout) * time.Second,
		},
	}
}

// scrapeURL extracts WhatsApp links from a given URL with retry logic
func (s *Scraper) scrapeURL(url string) Result {
	log.Printf("Starting to scrape URL: %s", url)
	result := Result{url: url}

	backoff := time.Second
	for attempt := 1; attempt <= s.config.maxRetries; attempt++ {
		links, err := s.attemptScrape(url)
		if err == nil {
			result.links = links
			log.Printf("Successfully scraped URL: %s", url)
			return result
		}

		if attempt < s.config.maxRetries {
			log.Printf("Attempt %d failed for %s: %v. Retrying after %s", attempt, url, err, backoff)
			time.Sleep(backoff)
			backoff *= 2 // Exponential backoff
		} else {
			result.err = fmt.Errorf("failed after %d attempts: %w", s.config.maxRetries, err)
		}
	}

	return result
}

// attemptScrape performs a single scrape attempt
func (s *Scraper) attemptScrape(targetURL string) ([]string, error) {
	resp, err := s.client.Get(targetURL)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch URL %s: %w", targetURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 429 {
		return nil, fmt.Errorf("rate limit exceeded for %s (Status: 429)", targetURL)
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("unexpected status code %d for %s", resp.StatusCode, targetURL)
	}

	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to parse HTML from %s: %w", targetURL, err)
	}

	var links []string
	doc.Find("a[href^='https://web.whatsapp.com/']").Each(func(_ int, s *goquery.Selection) {
		if link, exists := s.Attr("href"); exists {
			links = append(links, link)
		}
	})

	return links, nil
}

// processBatch processes a batch of URLs concurrently
func (s *Scraper) processBatch(urls []string) []Result {
	var results []Result
	resultChan := make(chan Result, len(urls))
	var wg sync.WaitGroup

	// Create a worker pool
	workerChan := make(chan struct{}, s.config.maxWorkers)

	for _, url := range urls {
		if !validateURL(url) {
			log.Printf("Skipping invalid URL: %s", url)
			continue
		}

		wg.Add(1)
		workerChan <- struct{}{} // Acquire worker

		go func(url string) {
			defer wg.Done()
			defer func() { <-workerChan }() // Release worker
			resultChan <- s.scrapeURL(url)
		}(url)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	for result := range resultChan {
		results = append(results, result)
	}

	return results
}

// writeResults writes the results to a CSV file
func writeResults(results []Result, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	if err := writer.Write([]string{"URL", "WhatsApp Links", "Error"}); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}

	for _, result := range results {
		var errMsg string
		if result.err != nil {
			errMsg = result.err.Error()
		}

		for _, link := range result.links {
			if err := writer.Write([]string{result.url, link, errMsg}); err != nil {
				return fmt.Errorf("failed to write CSV row: %w", err)
			}
		}

		// Write a row even if no links were found
		if len(result.links) == 0 {
			if err := writer.Write([]string{result.url, "", errMsg}); err != nil {
				return fmt.Errorf("failed to write CSV row: %w", err)
			}
		}
	}

	return nil
}

func validateURL(inputURL string) bool {
	parsedURL, err := url.ParseRequestURI(inputURL)
	if err != nil {
		return false
	}
	return parsedURL.Scheme == "http" || parsedURL.Scheme == "https"
}

func main() {
	// Parse command line flags
	config := Config{}
	flag.IntVar(&config.timeout, "timeout", 10, "HTTP request timeout in seconds")
	flag.IntVar(&config.maxRetries, "retries", 3, "Maximum number of retry attempts")
	flag.IntVar(&config.batchSize, "batch", 1000, "Batch size for processing URLs")
	flag.IntVar(&config.maxWorkers, "workers", 10, "Maximum number of concurrent workers")
	flag.StringVar(&config.outputFile, "output", "output.csv", "Output CSV file path")
	flag.Parse()

	// Create scraper instance
	scraper := NewScraper(config)

	// Example URLs (replace with actual data source)
	urls := []string{
		"https://example.com",
		"https://example2.com",
	}

	// Process URLs in batches
	var allResults []Result
	for i := 0; i < len(urls); i += config.batchSize {
		end := i + config.batchSize
		if end > len(urls) {
			end = len(urls)
		}
		
		batchResults := scraper.processBatch(urls[i:end])
		allResults = append(allResults, batchResults...)

		// Optional: Add delay between batches to avoid overwhelming servers
		if end < len(urls) {
			time.Sleep(time.Second * 2)
		}
	}

	// Write results to CSV
	if err := writeResults(allResults, config.outputFile); err != nil {
		log.Fatalf("Failed to write results: %v", err)
	}

	// Report statistics
	var failed, successful int
	for _, result := range allResults {
		if result.err != nil {
			failed++
		} else {
			successful++
		}
	}

	log.Printf("Scraping completed: %d successful, %d failed", successful, failed)
}
