package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/psds-microservice/search-service/internal/elasticsearch"
)

// SearchServicer — интерфейс для gRPC Deps (Dependency Inversion).
type SearchServicer interface {
	Search(ctx context.Context, q string, typeFilter string, limit int) (*SearchResult, error)
	IndexTicket(ctx context.Context, in *IndexTicketInput) error
	IndexSession(ctx context.Context, in *IndexSessionInput) error
	IndexOperator(ctx context.Context, in *IndexOperatorInput) error
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

type SearchResult struct {
	Tickets   []TicketHit   `json:"tickets"`
	Sessions  []SessionHit  `json:"sessions"`
	Operators []OperatorHit `json:"operators"`
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

func (s *SearchService) Search(ctx context.Context, q string, typeFilter string, limit int) (*SearchResult, error) {
	if limit <= 0 {
		limit = 20
	}
	q = strings.TrimSpace(q)
	if q == "" {
		return &SearchResult{
			Tickets:   []TicketHit{},
			Sessions:  []SessionHit{},
			Operators: []OperatorHit{},
		}, nil
	}

	result := &SearchResult{
		Tickets:   []TicketHit{},
		Sessions:  []SessionHit{},
		Operators: []OperatorHit{},
	}

	// Multi-match query for text fields
	query := map[string]interface{}{
		"multi_match": map[string]interface{}{
			"query":  q,
			"fields": []string{"*"},
			"type":   "best_fields",
		},
	}

	if typeFilter == "" || typeFilter == "tickets" {
		tickets, err := s.searchTickets(ctx, query, limit)
		if err != nil {
			return nil, fmt.Errorf("tickets: %w", err)
		}
		result.Tickets = tickets
	}

	if typeFilter == "" || typeFilter == "sessions" {
		sessions, err := s.searchSessions(ctx, query, limit)
		if err != nil {
			return nil, fmt.Errorf("sessions: %w", err)
		}
		result.Sessions = sessions
	}

	if typeFilter == "" || typeFilter == "operators" {
		operators, err := s.searchOperators(ctx, query, limit)
		if err != nil {
			return nil, fmt.Errorf("operators: %w", err)
		}
		result.Operators = operators
	}

	return result, nil
}

func (s *SearchService) searchTickets(ctx context.Context, query map[string]interface{}, limit int) ([]TicketHit, error) {
	resp, err := s.es.Search(ctx, indexTickets, query, limit)
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
	return hits, nil
}

func (s *SearchService) searchSessions(ctx context.Context, query map[string]interface{}, limit int) ([]SessionHit, error) {
	resp, err := s.es.Search(ctx, indexSessions, query, limit)
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
	return hits, nil
}

func (s *SearchService) searchOperators(ctx context.Context, query map[string]interface{}, limit int) ([]OperatorHit, error) {
	resp, err := s.es.Search(ctx, indexOperators, query, limit)
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
	return hits, nil
}
