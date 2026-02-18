package service

import (
	"context"
	"fmt"
	"strings"

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
	es *elasticsearch.Client
}

func NewSearchService(esURL string) (*SearchService, error) {
	es := elasticsearch.NewClient(esURL)
	svc := &SearchService{es: es}

	// Ensure indices exist with mappings
	ctx := context.Background()
	if err := svc.ensureIndices(ctx); err != nil {
		return nil, fmt.Errorf("ensure indices: %w", err)
	}

	return svc, nil
}

func (s *SearchService) ensureIndices(ctx context.Context) error {
	// Tickets index mapping
	ticketsMapping := map[string]interface{}{
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{
				"ticket_id":   map[string]interface{}{"type": "long"},
				"session_id":  map[string]interface{}{"type": "keyword"},
				"client_id":   map[string]interface{}{"type": "keyword"},
				"operator_id": map[string]interface{}{"type": "keyword"},
				"subject":     map[string]interface{}{"type": "text"},
				"notes":       map[string]interface{}{"type": "text"},
				"status":      map[string]interface{}{"type": "keyword"},
			},
		},
	}

	// Sessions index mapping
	sessionsMapping := map[string]interface{}{
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{
				"session_id": map[string]interface{}{"type": "keyword"},
				"client_id":  map[string]interface{}{"type": "keyword"},
				"pin":        map[string]interface{}{"type": "keyword"},
				"status":     map[string]interface{}{"type": "keyword"},
			},
		},
	}

	// Operators index mapping
	operatorsMapping := map[string]interface{}{
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{
				"user_id":      map[string]interface{}{"type": "keyword"},
				"display_name": map[string]interface{}{"type": "text"},
				"region":       map[string]interface{}{"type": "keyword"},
				"role":         map[string]interface{}{"type": "keyword"},
			},
		},
	}

	if err := s.es.EnsureIndex(ctx, indexTickets, ticketsMapping); err != nil {
		return fmt.Errorf("ensure tickets index: %w", err)
	}
	if err := s.es.EnsureIndex(ctx, indexSessions, sessionsMapping); err != nil {
		return fmt.Errorf("ensure sessions index: %w", err)
	}
	if err := s.es.EnsureIndex(ctx, indexOperators, operatorsMapping); err != nil {
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

func (s *SearchService) searchTickets(ctx context.Context, query map[string]interface{}, limit int, offset int) (*TicketsSearchResult, error) {
	resp, err := s.es.Search(ctx, indexTickets, query, limit, offset)
	if err != nil {
		return nil, err
	}

	var hits []TicketHit
	for _, hit := range resp.Hits.Hits {
		src := hit.Source
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
		hits = append(hits, h)
	}

	total := resp.Hits.Total.Value
	hasMore := int64(offset+len(hits)) < total

	return &TicketsSearchResult{
		Tickets: hits,
		Total:   total,
		HasMore: hasMore,
	}, nil
}

func (s *SearchService) searchSessions(ctx context.Context, query map[string]interface{}, limit int, offset int) (*SessionsSearchResult, error) {
	resp, err := s.es.Search(ctx, indexSessions, query, limit, offset)
	if err != nil {
		return nil, err
	}

	var hits []SessionHit
	for _, hit := range resp.Hits.Hits {
		src := hit.Source
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
		hits = append(hits, h)
	}

	total := resp.Hits.Total.Value
	hasMore := int64(offset+len(hits)) < total

	return &SessionsSearchResult{
		Sessions: hits,
		Total:    total,
		HasMore:  hasMore,
	}, nil
}

func (s *SearchService) searchOperators(ctx context.Context, query map[string]interface{}, limit int, offset int) (*OperatorsSearchResult, error) {
	resp, err := s.es.Search(ctx, indexOperators, query, limit, offset)
	if err != nil {
		return nil, err
	}

	var hits []OperatorHit
	for _, hit := range resp.Hits.Hits {
		src := hit.Source
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
		hits = append(hits, h)
	}

	total := resp.Hits.Total.Value
	hasMore := int64(offset+len(hits)) < total

	return &OperatorsSearchResult{
		Operators: hits,
		Total:     total,
		HasMore:   hasMore,
	}, nil
}

func (s *SearchService) SearchTickets(ctx context.Context, filters *TicketFilters) (*TicketsSearchResult, error) {
	if filters == nil {
		filters = &TicketFilters{}
	}
	limit := filters.Limit
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	offset := filters.Offset
	if offset < 0 {
		offset = 0
	}

	query := s.buildTicketQuery(filters)
	return s.searchTickets(ctx, query, limit, offset)
}

func (s *SearchService) SearchSessions(ctx context.Context, filters *SessionFilters) (*SessionsSearchResult, error) {
	if filters == nil {
		filters = &SessionFilters{}
	}
	limit := filters.Limit
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	offset := filters.Offset
	if offset < 0 {
		offset = 0
	}

	query := s.buildSessionQuery(filters)
	return s.searchSessions(ctx, query, limit, offset)
}

func (s *SearchService) SearchOperators(ctx context.Context, filters *OperatorFilters) (*OperatorsSearchResult, error) {
	if filters == nil {
		filters = &OperatorFilters{}
	}
	limit := filters.Limit
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	offset := filters.Offset
	if offset < 0 {
		offset = 0
	}

	query := s.buildOperatorQuery(filters)
	return s.searchOperators(ctx, query, limit, offset)
}

func (s *SearchService) buildTicketQuery(filters *TicketFilters) map[string]interface{} {
	var filter []map[string]interface{}

	// Точные фильтры
	if filters.Status != "" {
		filter = append(filter, map[string]interface{}{
			"term": map[string]interface{}{"status": filters.Status},
		})
	}
	if filters.SessionID != "" {
		filter = append(filter, map[string]interface{}{
			"term": map[string]interface{}{"session_id": filters.SessionID},
		})
	}
	if filters.ClientID != "" {
		filter = append(filter, map[string]interface{}{
			"term": map[string]interface{}{"client_id": filters.ClientID},
		})
	}
	if filters.OperatorID != "" {
		filter = append(filter, map[string]interface{}{
			"term": map[string]interface{}{"operator_id": filters.OperatorID},
		})
	}

	if len(filter) == 0 {
		return map[string]interface{}{"match_all": map[string]interface{}{}}
	}

	return map[string]interface{}{
		"bool": map[string]interface{}{
			"filter": filter,
		},
	}
}

func (s *SearchService) buildSessionQuery(filters *SessionFilters) map[string]interface{} {
	var filter []map[string]interface{}

	// Точные фильтры
	if filters.Status != "" {
		filter = append(filter, map[string]interface{}{
			"term": map[string]interface{}{"status": filters.Status},
		})
	}
	if filters.ClientID != "" {
		filter = append(filter, map[string]interface{}{
			"term": map[string]interface{}{"client_id": filters.ClientID},
		})
	}
	if filters.PIN != "" {
		filter = append(filter, map[string]interface{}{
			"term": map[string]interface{}{"pin": filters.PIN},
		})
	}

	if len(filter) == 0 {
		return map[string]interface{}{"match_all": map[string]interface{}{}}
	}

	return map[string]interface{}{
		"bool": map[string]interface{}{
			"filter": filter,
		},
	}
}

func (s *SearchService) buildOperatorQuery(filters *OperatorFilters) map[string]interface{} {
	var filter []map[string]interface{}

	// Точные фильтры
	if filters.Region != "" {
		filter = append(filter, map[string]interface{}{
			"term": map[string]interface{}{"region": filters.Region},
		})
	}
	if filters.Role != "" {
		filter = append(filter, map[string]interface{}{
			"term": map[string]interface{}{"role": filters.Role},
		})
	}
	if filters.DisplayName != "" {
		filter = append(filter, map[string]interface{}{
			"term": map[string]interface{}{"display_name": filters.DisplayName},
		})
	}

	if len(filter) == 0 {
		return map[string]interface{}{"match_all": map[string]interface{}{}}
	}

	return map[string]interface{}{
		"bool": map[string]interface{}{
			"filter": filter,
		},
	}
}
