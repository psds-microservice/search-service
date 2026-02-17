package grpc

import (
	"context"
	"log"

	"github.com/psds-microservice/search-service/internal/service"
	"github.com/psds-microservice/search-service/internal/validator"
	"github.com/psds-microservice/search-service/pkg/gen/search_service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Deps — зависимости gRPC-сервера
type Deps struct {
	SearchSvc *service.SearchService
	Validator *validator.Validator
}

// Server implements search_service.SearchServiceServer
type Server struct {
	search_service.UnimplementedSearchServiceServer
	Deps
}

// NewServer создаёт gRPC-сервер с внедрёнными сервисами
func NewServer(deps Deps) *Server {
	return &Server{Deps: deps}
}

func (s *Server) mapError(err error) error {
	if err == nil {
		return nil
	}
	log.Printf("grpc: error: %v", err)
	return status.Error(codes.Internal, err.Error())
}

func toProtoSearchResponse(result *service.SearchResult) *search_service.SearchResponse {
	if result == nil {
		return &search_service.SearchResponse{}
	}
	tickets := make([]*search_service.TicketHit, len(result.Tickets))
	for i, t := range result.Tickets {
		tickets[i] = &search_service.TicketHit{
			TicketId:  t.TicketID,
			SessionId: t.SessionID,
			Subject:   t.Subject,
			Snippet:   t.Snippet,
		}
	}
	sessions := make([]*search_service.SessionHit, len(result.Sessions))
	for i, s := range result.Sessions {
		sessions[i] = &search_service.SessionHit{
			SessionId: s.SessionID,
			Pin:       s.PIN,
			Status:    s.Status,
			Snippet:   s.Snippet,
		}
	}
	operators := make([]*search_service.OperatorHit, len(result.Operators))
	for i, o := range result.Operators {
		operators[i] = &search_service.OperatorHit{
			UserId:      o.UserID,
			DisplayName: o.DisplayName,
			Region:      o.Region,
			Snippet:     o.Snippet,
		}
	}
	return &search_service.SearchResponse{
		Tickets:   tickets,
		Sessions:  sessions,
		Operators: operators,
	}
}

func (s *Server) Search(ctx context.Context, req *search_service.SearchRequest) (*search_service.SearchResponse, error) {
	q := req.GetQ()
	typeFilter := req.GetType()
	limit := int(req.GetLimit())

	if err := s.Validator.ValidateSearchQuery(q, typeFilter, limit); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	result, err := s.SearchSvc.Search(ctx, q, typeFilter, limit)
	if err != nil {
		return nil, s.mapError(err)
	}

	return toProtoSearchResponse(result), nil
}

func (s *Server) IndexTicket(ctx context.Context, req *search_service.IndexTicketRequest) (*search_service.IndexResponse, error) {
	if err := s.Validator.ValidateIndexTicketInput(req.GetTicketId(), req.GetSessionId()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	in := &service.IndexTicketInput{
		TicketID:   req.GetTicketId(),
		SessionID:  req.GetSessionId(),
		ClientID:   req.GetClientId(),
		OperatorID: req.GetOperatorId(),
		Subject:    req.GetSubject(),
		Notes:      req.GetNotes(),
		Status:     req.GetStatus(),
	}

	if err := s.SearchSvc.IndexTicket(ctx, in); err != nil {
		return nil, s.mapError(err)
	}

	return &search_service.IndexResponse{Ok: true}, nil
}

func (s *Server) IndexSession(ctx context.Context, req *search_service.IndexSessionRequest) (*search_service.IndexResponse, error) {
	if err := s.Validator.ValidateIndexSessionInput(req.GetSessionId()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	in := &service.IndexSessionInput{
		SessionID: req.GetSessionId(),
		ClientID:  req.GetClientId(),
		PIN:       req.GetPin(),
		Status:    req.GetStatus(),
	}

	if err := s.SearchSvc.IndexSession(ctx, in); err != nil {
		return nil, s.mapError(err)
	}

	return &search_service.IndexResponse{Ok: true}, nil
}

func (s *Server) IndexOperator(ctx context.Context, req *search_service.IndexOperatorRequest) (*search_service.IndexResponse, error) {
	if err := s.Validator.ValidateIndexOperatorInput(req.GetUserId()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	in := &service.IndexOperatorInput{
		UserID:      req.GetUserId(),
		DisplayName: req.GetDisplayName(),
		Region:      req.GetRegion(),
		Role:        req.GetRole(),
	}

	if err := s.SearchSvc.IndexOperator(ctx, in); err != nil {
		return nil, s.mapError(err)
	}

	return &search_service.IndexResponse{Ok: true}, nil
}
