package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"golang.org/x/net/html/charset"
)

// Link represents a processed link with metadata
type Link struct {
	URL           string
	Type          string // "whatsapp", "file", "relative", etc.
	SourceElement string // tag where link was found
	IsHidden      bool   // whether link was hidden via CSS
}

// Result represents the result of scraping a single URL
type Result struct {
	url        string
	links      []Link
	statusCode int
	err        error
}

// Scraper handles the web scraping operations
type Scraper struct {
	config *Config
	client *http.Client
}

// NewScraper creates a new scraper instance with proper initialization
func NewScraper(config *Config) *Scraper {
	client := CreateHTTPClient(config)
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return fmt.Errorf("stopped after 10 redirects")
		}
		return nil
	}
	return &Scraper{config: config, client: client}
}

func (s *Scraper) scrapeURL(url string) Result {
	result := Result{url: url}
	
	// Add delay between requests
	if s.config.DelayBetweenRequests > 0 {
		time.Sleep(time.Duration(s.config.DelayBetweenRequests) * time.Millisecond)
	}

	log.Printf("Scraping URL: %s", url)
	
	backoff := time.Second
	for attempt := 1; attempt <= s.config.MaxRetries; attempt++ {
		links, statusCode, err := s.attemptScrape(url)
		result.statusCode = statusCode
		
		if err == nil {
			result.links = links
			log.Printf("Successfully scraped URL: %s, found %d links", url, len(links))
			return result
		}

		// Handle specific error cases
		if statusCode == http.StatusTooManyRequests {
			backoff *= 2 // Double backoff for rate limiting
			log.Printf("Rate limited (429) for %s. Waiting %v before retry", url, backoff)
		} else if statusCode >= 400 && statusCode < 500 {
			// Don't retry client errors except rate limiting
			result.err = fmt.Errorf("client error: %d - %v", statusCode, err)
			return result
		}

		if attempt < s.config.MaxRetries {
			log.Printf("Attempt %d failed for %s: %v (Status: %d). Retrying after %s", 
				attempt, url, err, statusCode, backoff)
			time.Sleep(backoff)
			backoff *= 2
		} else {
			result.err = fmt.Errorf("failed after %d attempts: %w", s.config.MaxRetries, err)
			log.Printf("Failed to scrape URL %s: %v", url, result.err)
		}
	}

	return result
}

func (s *Scraper) attemptScrape(targetURL string) ([]Link, int, error) {
	req, err := http.NewRequest("GET", targetURL, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create request: %w", err)
	}

	// Set headers to mimic browser
	req.Header.Set("User-Agent", s.config.UserAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.5")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to fetch URL: %w", err)
	}
	defer resp.Body.Close()

	// Handle status codes
	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// Handle character encoding
	bodyReader, err := charset.NewReader(resp.Body, resp.Header.Get("Content-Type"))
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("failed to decode content: %w", err)
	}

	// Create document for parsing
	doc, err := goquery.NewDocumentFromReader(bodyReader)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("failed to parse HTML: %w", err)
	}

	baseURL, err := url.Parse(targetURL)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("failed to parse base URL: %w", err)
	}

	var links []Link
	seenURLs := make(map[string]bool)

	// Extract links from various sources
	s.extractLinksFromTags(doc, baseURL, seenURLs, &links)
	s.extractLinksFromScripts(doc, baseURL, seenURLs, &links)
	s.extractLinksFromAttributes(doc, baseURL, seenURLs, &links)

	return links, resp.StatusCode, nil
}

func (s *Scraper) extractLinksFromTags(doc *goquery.Document, baseURL *url.URL, 
	seenURLs map[string]bool, links *[]Link) {
	
	doc.Find("a, link, area").Each(func(_ int, sel *goquery.Selection) {
		href, exists := sel.Attr("href")
		if !exists {
			return
		}

		link := s.processLink(href, sel, baseURL)
		if link != nil && !seenURLs[link.URL] {
			seenURLs[link.URL] = true
			*links = append(*links, *link)
		}
	})
}

func (s *Scraper) extractLinksFromScripts(doc *goquery.Document, baseURL *url.URL, 
	seenURLs map[string]bool, links *[]Link) {
	
	doc.Find("script").Each(func(_ int, sel *goquery.Selection) {
		scriptContent := sel.Text()
		// Look for URLs in various formats within script content
		// This is a simple example; you might want to use regex for more complex patterns
		for _, word := range strings.Fields(scriptContent) {
			if strings.Contains(word, "whatsapp.com") || strings.Contains(word, "wa.me") {
				// Clean up the extracted URL
				cleanURL := strings.Trim(word, `'"`)
				if link := s.processLink(cleanURL, sel, baseURL); link != nil {
					if !seenURLs[link.URL] {
						seenURLs[link.URL] = true
						*links = append(*links, *link)
					}
				}
			}
		}
	})
}

func (s *Scraper) extractLinksFromAttributes(doc *goquery.Document, baseURL *url.URL, 
	seenURLs map[string]bool, links *[]Link) {
	
	// Look for links in data attributes and other common places
	doc.Find("*[data-href], *[data-url]").Each(func(_ int, sel *goquery.Selection) {
		for _, attr := range []string{"data-href", "data-url"} {
			if href, exists := sel.Attr(attr); exists {
				if link := s.processLink(href, sel, baseURL); link != nil {
					if !seenURLs[link.URL] {
						seenURLs[link.URL] = true
						*links = append(*links, *link)
					}
				}
			}
		}
	})
}

func (s *Scraper) processLink(href string, sel *goquery.Selection, baseURL *url.URL) *Link {
	href = strings.TrimSpace(href)
	if href == "" || href == "#" {
		return nil
	}

	// Resolve relative URLs
	resolvedURL, err := baseURL.Parse(href)
	if err != nil {
		log.Printf("Failed to resolve URL %s: %v", href, err)
		return nil
	}

	// Check if the link is hidden
	isHidden := s.isElementHidden(sel)

	// Determine link type
	linkType := determineLinkType(resolvedURL.String())
	if linkType == "" {
		return nil
	}

	return &Link{
		URL:           resolvedURL.String(),
		Type:          linkType,
		SourceElement: sel.Get(0).Data,
		IsHidden:      isHidden,
	}
}

func (s *Scraper) isElementHidden(sel *goquery.Selection) bool {
	style, exists := sel.Attr("style")
	if !exists {
		return false
	}
	
	style = strings.ToLower(style)
	return strings.Contains(style, "display:none") || 
		   strings.Contains(style, "visibility:hidden") || 
		   strings.Contains(style, "opacity:0")
}

func determineLinkType(urlStr string) string {
	urlStr = strings.ToLower(urlStr)
	
	switch {
	case strings.Contains(urlStr, "whatsapp.com") || strings.Contains(urlStr, "wa.me"):
		return "whatsapp"
	case strings.HasSuffix(urlStr, ".pdf") || 
		 strings.HasSuffix(urlStr, ".doc") || 
		 strings.HasSuffix(urlStr, ".docx"):
		return "document"
	case strings.HasSuffix(urlStr, ".jpg") || 
		 strings.HasSuffix(urlStr, ".jpeg") || 
		 strings.HasSuffix(urlStr, ".png"):
		return "image"
	default:
		return ""
	}
}

func (s *Scraper) processBatch(urls []string) []Result {
	var results []Result
	resultChan := make(chan Result, len(urls))
	var wg sync.WaitGroup

	// Create a worker pool
	workerChan := make(chan struct{}, s.config.MaxWorkers)

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
			
			// Catch panics in goroutines
			defer func() {
				if r := recover(); r != nil {
					log.Printf("Recovered from panic while processing %s: %v", url, r)
					resultChan <- Result{
						url: url,
						err: fmt.Errorf("internal error: %v", r),
					}
				}
			}()
			
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

// writeResults writes the extracted links to CSV
func writeResults(results []Result, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	if err := writer.Write([]string{
		"Source URL",
		"Link URL",
		"Link Type",
		"Source Element",
		"Is Hidden",
		"Status Code",
		"Error",
	}); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}

	for _, result := range results {
		errMsg := ""
		if result.err != nil {
			errMsg = result.err.Error()
		}

		// Write a row for each link found
		for _, link := range result.links {
			if err := writer.Write([]string{
				result.url,
				link.URL,
				link.Type,
				link.SourceElement,
				fmt.Sprintf("%v", link.IsHidden),
				fmt.Sprintf("%d", result.statusCode),
				errMsg,
			}); err != nil {
				return fmt.Errorf("failed to write CSV row: %w", err)
			}
		}

		// If no links were found, write a row with just the source URL and error
		if len(result.links) == 0 {
			if err := writer.Write([]string{
				result.url,
				"",
				"",
				"",
				"",
				fmt.Sprintf("%d", result.statusCode),
				errMsg,
			}); err != nil {
				return fmt.Errorf("failed to write CSV row: %w", err)
			}
		}
	}

	return writer.Error()
}

func main() {
	// Parse command line flags
	timeout := flag.Int("timeout", 30, "HTTP request timeout in seconds")
	maxRetries := flag.Int("retries", 3, "Maximum number of retry attempts")
	batchSize := flag.Int("batch", 1000, "Batch size for processing URLs")
	maxWorkers := flag.Int("workers", 10, "Maximum number of concurrent workers")
	inputFile := flag.String("input", "input.csv", "Input CSV file containing URLs")
	outputLinksFile := flag.String("output-links", "output_links.csv", "Output CSV file for extracted links")
	outputSummaryFile := flag.String("output-summary", "output_summary.csv", "Output CSV file for summary")
	delay := flag.Int("delay", 500, "Delay between requests in milliseconds")
	flag.Parse()

	// Create configuration
	config := &Config{
		Timeout:              *timeout,
		MaxRetries:           *maxRetries,
		BatchSize:            *batchSize,
		MaxWorkers:           *maxWorkers,
		InputFile:            *inputFile,
		OutputLinksFile:      *outputLinksFile,
		OutputSummaryFile:    *outputSummaryFile,
		UserAgent:            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
		DelayBetweenRequests: *delay,
	}

	// Validate configuration
	if err := config.ValidateConfig(); err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}

	// Read URLs from input file
	urls, err := ReadURLsFromFile(config.InputFile)
	if err != nil {
		log.Fatalf("Failed to read URLs: %v", err)
	}

	log.Printf("Starting scraping of %d URLs", len(urls))
	startTime := time.Now()

	// Create scraper instance
	scraper := NewScraper(config)

	// Process URLs in batches with progress tracking
	var allResults []Result
	totalBatches := (len(urls) + config.BatchSize - 1) / config.BatchSize
	successCount := 0
	failureCount := 0
	totalLinksFound := 0

	for i := 0; i < len(urls); i += config.BatchSize {
		batchStart := time.Now()
		end := i + config.BatchSize
		if end > len(urls) {
			end = len(urls)
		}
		
		currentBatch := (i / config.BatchSize) + 1
		log.Printf("Processing batch %d/%d (URLs %d-%d)", 
			currentBatch, totalBatches, i+1, end)

		// Process current batch
		batchResults := scraper.processBatch(urls[i:end])
		
		// Update statistics
		for _, result := range batchResults {
			if result.err != nil {
				failureCount++
			} else {
				successCount++
				totalLinksFound += len(result.links)
			}
		}

		allResults = append(allResults, batchResults...)
		
		// Log batch completion statistics
		batchDuration := time.Since(batchStart)
		log.Printf("Batch %d/%d completed in %v (Success: %d, Failed: %d, Links: %d)", 
			currentBatch, totalBatches, batchDuration, 
			successCount-failureCount, failureCount, totalLinksFound)

		// Check if we're approaching the GitHub Actions timeout (350 minutes)
		if time.Since(startTime) > 340*time.Minute {
			log.Printf("Approaching GitHub Actions timeout limit. Stopping after batch %d", currentBatch)
			break
		}

		// Add delay between batches if not the last batch
		if end < len(urls) {
			log.Printf("Waiting between batches...")
			time.Sleep(time.Second * 2)
		}
	}

	// Write results and generate report
	log.Printf("Writing results to files...")
	
	if err := writeResults(allResults, config.OutputLinksFile); err != nil {
		log.Printf("Error writing links file: %v", err)
	}

	if err := WriteSummary(config.OutputSummaryFile, allResults); err != nil {
		log.Printf("Error writing summary file: %v", err)
	}

	// Generate and print final statistics
	duration := time.Since(startTime)
	log.Printf("\n=== Scraping Summary ===")
	log.Printf("Total Duration: %v", duration)
	log.Printf("URLs Processed: %d of %d", len(allResults), len(urls))
	log.Printf("Successful: %d", successCount)
	log.Printf("Failed: %d", failureCount)
	log.Printf("Total Links Found: %d", totalLinksFound)
	log.Printf("Average Links per Page: %.2f", float64(totalLinksFound)/float64(successCount))
	log.Printf("Results written to:")
	log.Printf("- Links: %s", config.OutputLinksFile)
	log.Printf("- Summary: %s", config.OutputSummaryFile)

	// Calculate and log error distribution
	errorCounts := make(map[string]int)
	for _, result := range allResults {
		if result.err != nil {
			errType := categorizeError(result.err)
			errorCounts[errType]++
		}
	}

	if len(errorCounts) > 0 {
		log.Printf("\n=== Error Distribution ===")
		for errType, count := range errorCounts {
			log.Printf("%s: %d", errType, count)
		}
	}

	// Exit with error if failure ratio is too high
	failureRatio := float64(failureCount) / float64(len(allResults))
	if failureRatio > 0.5 {
		log.Printf("\nWarning: High failure rate (%.2f%%)", failureRatio*100)
		os.Exit(1)
	}
}

// categorizeError helps classify different types of errors for reporting
func categorizeError(err error) string {
	errStr := err.Error()
	switch {
	case strings.Contains(errStr, "timeout"):
		return "Timeout"
	case strings.Contains(errStr, "too many redirects"):
		return "Redirect Loop"
	case strings.Contains(errStr, "no such host"):
		return "DNS Error"
	case strings.Contains(errStr, "connection refused"):
		return "Connection Refused"
	case strings.Contains(errStr, "status code: 403"):
		return "Access Denied (403)"
	case strings.Contains(errStr, "status code: 404"):
		return "Not Found (404)"
	case strings.Contains(errStr, "status code: 429"):
		return "Rate Limited (429)"
	case strings.Contains(errStr, "status code: 5"):
		return "Server Error (5xx)"
	case strings.Contains(errStr, "parse"):
		return "URL Parse Error"
	case strings.Contains(errStr, "certificate"):
		return "SSL/TLS Error"
	default:
		return "Other Error"
	}
}
