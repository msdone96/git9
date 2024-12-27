package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"time"
)

// Config holds the application configuration
type Config struct {
	Timeout          int    `json:"timeout"`          // HTTP request timeout in seconds
	MaxRetries       int    `json:"maxRetries"`       // Maximum number of retry attempts
	BatchSize        int    `json:"batchSize"`        // Number of URLs to process in each batch
	MaxWorkers       int    `json:"maxWorkers"`       // Maximum number of concurrent workers
	InputFile        string `json:"inputFile"`        // Input CSV file containing URLs
	OutputLinksFile  string `json:"outputLinksFile"`  // Output CSV file for extracted links
	OutputSummaryFile string `json:"outputSummaryFile"` // Output CSV file for summary stats
	UserAgent        string `json:"userAgent"`        // User agent string for HTTP requests
	DelayBetweenRequests int `json:"delayBetweenRequests"` // Delay between requests in milliseconds
}

// DefaultConfig returns a configuration with default values
func DefaultConfig() *Config {
	return &Config{
		Timeout:          10,
		MaxRetries:       3,
		BatchSize:        1000,
		MaxWorkers:       10,
		InputFile:        "input.csv",
		OutputLinksFile:  "output_links.csv",
		OutputSummaryFile: "output_summary.csv",
		UserAgent:        "Mozilla/5.0 WebScraper/1.0",
		DelayBetweenRequests: 500,
	}
}

// ValidateConfig checks if the configuration is valid
func (c *Config) ValidateConfig() error {
	if c.Timeout <= 0 {
		return fmt.Errorf("timeout must be greater than 0")
	}
	if c.MaxRetries < 0 {
		return fmt.Errorf("maxRetries cannot be negative")
	}
	if c.BatchSize <= 0 {
		return fmt.Errorf("batchSize must be greater than 0")
	}
	if c.MaxWorkers <= 0 {
		return fmt.Errorf("maxWorkers must be greater than 0")
	}
	if c.DelayBetweenRequests < 0 {
		return fmt.Errorf("delayBetweenRequests cannot be negative")
	}
	return nil
}

// ReadURLsFromFile reads URLs from the input CSV file
func ReadURLsFromFile(filename string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open input file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV: %w", err)
	}

	var urls []string
	for i, record := range records {
		if i == 0 { // Skip header row
			continue
		}
		if len(record) > 0 && record[0] != "" {
			if validateURL(record[0]) {
				urls = append(urls, record[0])
			} else {
				log.Printf("Skipping invalid URL at line %d: %s", i+1, record[0])
			}
		}
	}

	if len(urls) == 0 {
		return nil, fmt.Errorf("no valid URLs found in input file")
	}

	return urls, nil
}

// WriteSummary writes the scraping summary to a CSV file
func WriteSummary(filename string, results []Result) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create summary file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	if err := writer.Write([]string{"URL", "Number of Links", "Status", "Error Message"}); err != nil {
		return fmt.Errorf("failed to write summary header: %w", err)
	}

	for _, result := range results {
		status := "Success"
		errorMsg := ""
		if result.err != nil {
			status = "Failed"
			errorMsg = result.err.Error()
		}

		if err := writer.Write([]string{
			result.url,
			fmt.Sprintf("%d", len(result.links)),
			status,
			errorMsg,
		}); err != nil {
			return fmt.Errorf("failed to write summary row: %w", err)
		}
	}

	return nil
}

// CreateHTTPClient creates an HTTP client with the specified configuration
func CreateHTTPClient(config *Config) *http.Client {
	return &http.Client{
		Timeout: time.Duration(config.Timeout) * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			IdleConnTimeout:     30 * time.Second,
			DisableCompression:  false,
			DisableKeepAlives:   false,
			MaxConnsPerHost:     config.MaxWorkers,
		},
	}
}

// validateURL checks if a URL is valid and has the correct scheme
func validateURL(inputURL string) bool {
	parsedURL, err := url.ParseRequestURI(inputURL)
	if err != nil {
		return false
	}
	return parsedURL.Scheme == "http" || parsedURL.Scheme == "https"
}
