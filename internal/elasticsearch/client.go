package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// Client is a simple Elasticsearch HTTP client
type Client struct {
	baseURL string
	http    *http.Client
}

// NewClient creates a new Elasticsearch client
func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: strings.TrimSuffix(baseURL, "/"),
		http:    &http.Client{},
	}
}

// IndexDocument indexes a document in Elasticsearch
func (c *Client) IndexDocument(ctx context.Context, index, id string, doc interface{}) error {
	url := fmt.Sprintf("%s/%s/_doc/%s", c.baseURL, index, id)
	body, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("marshal document: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "PUT", url, strings.NewReader(string(body)))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("elasticsearch error: %s - %s", resp.Status, string(bodyBytes))
	}

	return nil
}

// Search performs a search query
func (c *Client) Search(ctx context.Context, index string, query map[string]interface{}, limit int) (*SearchResponse, error) {
	searchQuery := map[string]interface{}{
		"size": limit,
		"query": query,
	}

	body, err := json.Marshal(searchQuery)
	if err != nil {
		return nil, fmt.Errorf("marshal query: %w", err)
	}

	url := fmt.Sprintf("%s/%s/_search", c.baseURL, index)
	req, err := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader(string(body)))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("elasticsearch error: %s - %s", resp.Status, string(bodyBytes))
	}

	var result SearchResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &result, nil
}

// EnsureIndex creates an index if it doesn't exist
func (c *Client) EnsureIndex(ctx context.Context, index string, mapping map[string]interface{}) error {
	url := fmt.Sprintf("%s/%s", c.baseURL, index)
	
	// Check if index exists
	req, err := http.NewRequestWithContext(ctx, "HEAD", url, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("check index: %w", err)
	}
	resp.Body.Close()

	if resp.StatusCode == 200 {
		return nil // Index already exists
	}

	// Create index with mapping
	if mapping == nil {
		mapping = make(map[string]interface{})
	}
	body, err := json.Marshal(mapping)
	if err != nil {
		return fmt.Errorf("marshal mapping: %w", err)
	}

	req, err = http.NewRequestWithContext(ctx, "PUT", url, strings.NewReader(string(body)))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err = c.http.Do(req)
	if err != nil {
		return fmt.Errorf("create index: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("elasticsearch error: %s - %s", resp.Status, string(bodyBytes))
	}

	return nil
}

// SearchResponse represents Elasticsearch search response
type SearchResponse struct {
	Hits struct {
		Hits []struct {
			ID     string                 `json:"_id"`
			Source map[string]interface{} `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}
