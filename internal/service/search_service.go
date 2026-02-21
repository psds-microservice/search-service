package service

import (
	"context"
	"fmt"

	"github.com/psds-microservice/helpy/limit"
	"github.com/psds-microservice/search-service/internal/elasticsearch"
)

// SearchServicer — интерфейс для gRPC Deps (Dependency Inversion).
type SearchServicer interface {
	SearchTickets(ctx context.Context, filters *TicketFilters) (*TicketsSearchResult, error)
	SearchSessions(ctx context.Context, filters *SessionFilters) (*SessionsSearchResult, error)
	SearchOperators(ctx context.Context, filters *OperatorFilters) (*OperatorsSearchResult, error)
	IndexTicket(ctx context.Context, in *IndexTicketInput) error
	IndexSession(ctx context.Context, in *IndexSessionInput) error
	IndexOperator(ctx context.Context, in *IndexOperatorInput) error
}

type TicketsSearchResult struct {
	Tickets []TicketHit
	Total   int64
	HasMore bool
}

type SessionsSearchResult struct {
	Sessions []SessionHit
	Total    int64
	HasMore  bool
}

type OperatorsSearchResult struct {
	Operators []OperatorHit
	Total     int64
	HasMore   bool
}

type TicketFilters struct {
	Status     string // фильтр по status
	SessionID  string // фильтр по session_id
	ClientID   string // фильтр по client_id
	OperatorID string // фильтр по operator_id
	Limit      int    // лимит результатов (по умолчанию 20)
	Offset     int    // смещение для пагинации (по умолчанию 0)
}

type SessionFilters struct {
	Status   string // фильтр по status
	ClientID string // фильтр по client_id
	PIN      string // фильтр по pin
	Limit    int    // лимит результатов (по умолчанию 20)
	Offset   int    // смещение для пагинации (по умолчанию 0)
}

type OperatorFilters struct {
	Region      string // фильтр по region
	Role        string // фильтр по role
	DisplayName string // фильтр по display_name (точное совпадение)
	Limit       int    // лимит результатов (по умолчанию 20)
	Offset      int    // смещение для пагинации (по умолчанию 0)
}

const (
	indexTickets   = "tickets"
	indexSessions  = "sessions"
	indexOperators = "operators"
)

type SearchService struct {
	es elasticsearch.IndexSearcher
}

func NewSearchService(esURL string, skipTLSVerify bool, username, password string) (*SearchService, error) {
	es := elasticsearch.NewClient(esURL, skipTLSVerify, username, password)
	return NewSearchServiceWithIndexer(es)
}

// NewSearchServiceWithIndexer builds SearchService with a given IndexSearcher (e.g. for tests).
func NewSearchServiceWithIndexer(es elasticsearch.IndexSearcher) (*SearchService, error) {
	svc := &SearchService{es: es}

	// Ensure indices exist with mappings
	ctx := context.Background()
	if err := svc.ensureIndices(ctx); err != nil {
		return nil, fmt.Errorf("ensure indices: %w", err)
	}

	return svc, nil
}

func (s *SearchService) ensureIndices(ctx context.Context) error {
	if err := s.es.EnsureIndex(ctx, indexTickets, elasticsearch.TicketsMapping()); err != nil {
		return fmt.Errorf("ensure tickets index: %w", err)
	}
	if err := s.es.EnsureIndex(ctx, indexSessions, elasticsearch.SessionsMapping()); err != nil {
		return fmt.Errorf("ensure sessions index: %w", err)
	}
	if err := s.es.EnsureIndex(ctx, indexOperators, elasticsearch.OperatorsMapping()); err != nil {
		return fmt.Errorf("ensure operators index: %w", err)
	}

	return nil
}

type IndexTicketInput struct {
	TicketID   int64  `json:"ticket_id"`
	SessionID  string `json:"session_id"`
	ClientID   string `json:"client_id"`
	OperatorID string `json:"operator_id"`
	Subject    string `json:"subject"`
	Notes      string `json:"notes"`
	Status     string `json:"status"`
}

type IndexSessionInput struct {
	SessionID string `json:"session_id"`
	ClientID  string `json:"client_id"`
	PIN       string `json:"pin"`
	Status    string `json:"status"`
}

type IndexOperatorInput struct {
	UserID      string `json:"user_id"`
	DisplayName string `json:"display_name"`
	Region      string `json:"region"`
	Role        string `json:"role"`
}

func (s *SearchService) IndexTicket(ctx context.Context, in *IndexTicketInput) error {
	doc := map[string]interface{}{
		"ticket_id":   in.TicketID,
		"session_id":  in.SessionID,
		"client_id":   in.ClientID,
		"operator_id": in.OperatorID,
		"subject":     in.Subject,
		"notes":       in.Notes,
		"status":      in.Status,
	}
	return s.es.IndexDocument(ctx, indexTickets, fmt.Sprintf("%d", in.TicketID), doc)
}

func (s *SearchService) IndexSession(ctx context.Context, in *IndexSessionInput) error {
	doc := map[string]interface{}{
		"session_id": in.SessionID,
		"client_id":  in.ClientID,
		"pin":        in.PIN,
		"status":     in.Status,
	}
	return s.es.IndexDocument(ctx, indexSessions, in.SessionID, doc)
}

func (s *SearchService) IndexOperator(ctx context.Context, in *IndexOperatorInput) error {
	doc := map[string]interface{}{
		"user_id":      in.UserID,
		"display_name": in.DisplayName,
		"region":       in.Region,
		"role":         in.Role,
	}
	return s.es.IndexDocument(ctx, indexOperators, in.UserID, doc)
}

type TicketHit struct {
	TicketID  int64  `json:"ticket_id"`
	SessionID string `json:"session_id"`
	Subject   string `json:"subject"`
	Snippet   string `json:"snippet,omitempty"`
}

type SessionHit struct {
	SessionID string `json:"session_id"`
	PIN       string `json:"pin"`
	Status    string `json:"status"`
	Snippet   string `json:"snippet,omitempty"`
}

type OperatorHit struct {
	UserID      string `json:"user_id"`
	DisplayName string `json:"display_name"`
	Region      string `json:"region"`
	Snippet     string `json:"snippet,omitempty"`
}

// searchIndex runs ES search and maps each hit source to T. Shared by searchTickets/Sessions/Operators.
func searchIndex[T any](es elasticsearch.IndexSearcher, ctx context.Context, index string, query map[string]interface{}, limit, offset int, mapHit func(map[string]interface{}) T) ([]T, int64, bool, error) {
	resp, err := es.Search(ctx, index, query, limit, offset)
	if err != nil {
		return nil, 0, false, err
	}
	hits := make([]T, 0, len(resp.Hits.Hits))
	for _, h := range resp.Hits.Hits {
		hits = append(hits, mapHit(h.Source))
	}
	total := resp.Hits.Total.Value
	hasMore := int64(offset+len(hits)) < total
	return hits, total, hasMore, nil
}

func sourceToTicketHit(src map[string]interface{}) TicketHit {
	h := TicketHit{}
	if v, ok := src["ticket_id"].(float64); ok {
		h.TicketID = int64(v)
	}
	if v, ok := src["session_id"].(string); ok {
		h.SessionID = v
	}
	if v, ok := src["subject"].(string); ok {
		h.Subject = v
	}
	return h
}

func sourceToSessionHit(src map[string]interface{}) SessionHit {
	h := SessionHit{}
	if v, ok := src["session_id"].(string); ok {
		h.SessionID = v
	}
	if v, ok := src["pin"].(string); ok {
		h.PIN = v
	}
	if v, ok := src["status"].(string); ok {
		h.Status = v
	}
	return h
}

func sourceToOperatorHit(src map[string]interface{}) OperatorHit {
	h := OperatorHit{}
	if v, ok := src["user_id"].(string); ok {
		h.UserID = v
	}
	if v, ok := src["display_name"].(string); ok {
		h.DisplayName = v
	}
	if v, ok := src["region"].(string); ok {
		h.Region = v
	}
	return h
}

func (s *SearchService) searchTickets(ctx context.Context, query map[string]interface{}, limit int, offset int) (*TicketsSearchResult, error) {
	hits, total, hasMore, err := searchIndex(s.es, ctx, indexTickets, query, limit, offset, sourceToTicketHit)
	if err != nil {
		return nil, err
	}
	return &TicketsSearchResult{Tickets: hits, Total: total, HasMore: hasMore}, nil
}

func (s *SearchService) searchSessions(ctx context.Context, query map[string]interface{}, limit int, offset int) (*SessionsSearchResult, error) {
	hits, total, hasMore, err := searchIndex(s.es, ctx, indexSessions, query, limit, offset, sourceToSessionHit)
	if err != nil {
		return nil, err
	}
	return &SessionsSearchResult{Sessions: hits, Total: total, HasMore: hasMore}, nil
}

func (s *SearchService) searchOperators(ctx context.Context, query map[string]interface{}, limit int, offset int) (*OperatorsSearchResult, error) {
	hits, total, hasMore, err := searchIndex(s.es, ctx, indexOperators, query, limit, offset, sourceToOperatorHit)
	if err != nil {
		return nil, err
	}
	return &OperatorsSearchResult{Operators: hits, Total: total, HasMore: hasMore}, nil
}

func (s *SearchService) SearchTickets(ctx context.Context, filters *TicketFilters) (*TicketsSearchResult, error) {
	if filters == nil {
		filters = &TicketFilters{}
	}
	lim := limit.ClampLimit(filters.Limit, 20, 100)
	offset := filters.Offset
	if offset < 0 {
		offset = 0
	}
	query := s.buildTicketQuery(filters)
	return s.searchTickets(ctx, query, lim, offset)
}

func (s *SearchService) SearchSessions(ctx context.Context, filters *SessionFilters) (*SessionsSearchResult, error) {
	if filters == nil {
		filters = &SessionFilters{}
	}
	lim := limit.ClampLimit(filters.Limit, 20, 100)
	offset := filters.Offset
	if offset < 0 {
		offset = 0
	}
	query := s.buildSessionQuery(filters)
	return s.searchSessions(ctx, query, lim, offset)
}

func (s *SearchService) SearchOperators(ctx context.Context, filters *OperatorFilters) (*OperatorsSearchResult, error) {
	if filters == nil {
		filters = &OperatorFilters{}
	}
	lim := limit.ClampLimit(filters.Limit, 20, 100)
	offset := filters.Offset
	if offset < 0 {
		offset = 0
	}
	query := s.buildOperatorQuery(filters)
	return s.searchOperators(ctx, query, lim, offset)
}

// buildBoolTermQuery builds an ES bool query from field->value map; empty values are skipped.
func buildBoolTermQuery(fields map[string]string) map[string]interface{} {
	var filter []map[string]interface{}
	for field, value := range fields {
		if value == "" {
			continue
		}
		filter = append(filter, map[string]interface{}{
			"term": map[string]interface{}{field: value},
		})
	}
	if len(filter) == 0 {
		return map[string]interface{}{"match_all": map[string]interface{}{}}
	}
	return map[string]interface{}{
		"bool": map[string]interface{}{"filter": filter},
	}
}

func (s *SearchService) buildTicketQuery(filters *TicketFilters) map[string]interface{} {
	return buildBoolTermQuery(map[string]string{
		"status":       filters.Status,
		"session_id":   filters.SessionID,
		"client_id":    filters.ClientID,
		"operator_id":  filters.OperatorID,
	})
}

func (s *SearchService) buildSessionQuery(filters *SessionFilters) map[string]interface{} {
	return buildBoolTermQuery(map[string]string{
		"status":     filters.Status,
		"client_id":  filters.ClientID,
		"pin":        filters.PIN,
	})
}

func (s *SearchService) buildOperatorQuery(filters *OperatorFilters) map[string]interface{} {
	return buildBoolTermQuery(map[string]string{
		"region":        filters.Region,
		"role":          filters.Role,
		"display_name":  filters.DisplayName,
	})
}
