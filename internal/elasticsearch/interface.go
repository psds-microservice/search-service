package elasticsearch

import "context"

// IndexSearcher abstracts Elasticsearch search/index operations for testing and swapping implementations.
type IndexSearcher interface {
	Search(ctx context.Context, index string, query map[string]interface{}, limit, offset int) (*SearchResponse, error)
	IndexDocument(ctx context.Context, index, id string, doc interface{}) error
	EnsureIndex(ctx context.Context, index string, mapping map[string]interface{}) error
}

// Ensure *Client implements IndexSearcher at compile time.
var _ IndexSearcher = (*Client)(nil)
