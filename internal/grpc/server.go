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

// Deps — зависимости gRPC-сервера (D: зависимость от абстракций).
type Deps struct {
	SearchSvc service.SearchServicer
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

func (s *Server) SearchTickets(ctx context.Context, req *search_service.SearchTicketsRequest) (*search_service.SearchTicketsResponse, error) {
	limit := int(req.GetLimit())
	if err := s.Validator.ValidateSearchLimit(limit); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	offset := int(req.GetOffset())
	if err := s.Validator.ValidateSearchOffset(offset); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	filters := &service.TicketFilters{
		Status:     req.GetStatus(),
		SessionID:  req.GetSessionId(),
		ClientID:   req.GetClientId(),
		OperatorID: req.GetOperatorId(),
		Limit:      limit,
		Offset:     offset,
	}

	result, err := s.SearchSvc.SearchTickets(ctx, filters)
	if err != nil {
		return nil, s.mapError(err)
	}

	ticketHits := make([]*search_service.TicketHit, len(result.Tickets))
	for i, t := range result.Tickets {
		ticketHits[i] = &search_service.TicketHit{
			TicketId:  t.TicketID,
			SessionId: t.SessionID,
			Subject:   t.Subject,
			Snippet:   t.Snippet,
		}
	}

	return &search_service.SearchTicketsResponse{
		Tickets: ticketHits,
		Total:   result.Total,
		HasMore: result.HasMore,
	}, nil
}

func (s *Server) SearchSessions(ctx context.Context, req *search_service.SearchSessionsRequest) (*search_service.SearchSessionsResponse, error) {
	limit := int(req.GetLimit())
	if err := s.Validator.ValidateSearchLimit(limit); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	offset := int(req.GetOffset())
	if err := s.Validator.ValidateSearchOffset(offset); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	filters := &service.SessionFilters{
		Status:   req.GetStatus(),
		ClientID: req.GetClientId(),
		PIN:      req.GetPin(),
		Limit:    limit,
		Offset:   offset,
	}

	result, err := s.SearchSvc.SearchSessions(ctx, filters)
	if err != nil {
		return nil, s.mapError(err)
	}

	sessionHits := make([]*search_service.SessionHit, len(result.Sessions))
	for i, ses := range result.Sessions {
		sessionHits[i] = &search_service.SessionHit{
			SessionId: ses.SessionID,
			Pin:       ses.PIN,
			Status:    ses.Status,
			Snippet:   ses.Snippet,
		}
	}

	return &search_service.SearchSessionsResponse{
		Sessions: sessionHits,
		Total:    result.Total,
		HasMore:  result.HasMore,
	}, nil
}

func (s *Server) SearchOperators(ctx context.Context, req *search_service.SearchOperatorsRequest) (*search_service.SearchOperatorsResponse, error) {
	limit := int(req.GetLimit())
	if err := s.Validator.ValidateSearchLimit(limit); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	offset := int(req.GetOffset())
	if err := s.Validator.ValidateSearchOffset(offset); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	filters := &service.OperatorFilters{
		Region:      req.GetRegion(),
		Role:        req.GetRole(),
		DisplayName: req.GetDisplayName(),
		Limit:       limit,
		Offset:      offset,
	}

	result, err := s.SearchSvc.SearchOperators(ctx, filters)
	if err != nil {
		return nil, s.mapError(err)
	}

	operatorHits := make([]*search_service.OperatorHit, len(result.Operators))
	for i, op := range result.Operators {
		operatorHits[i] = &search_service.OperatorHit{
			UserId:      op.UserID,
			DisplayName: op.DisplayName,
			Region:      op.Region,
			Snippet:     op.Snippet,
		}
	}

	return &search_service.SearchOperatorsResponse{
		Operators: operatorHits,
		Total:     result.Total,
		HasMore:   result.HasMore,
	}, nil
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
