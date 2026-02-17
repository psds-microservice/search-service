package validator

import (
	"errors"
	"strings"

	"github.com/google/uuid"
)

// Validator validates input DTOs for search-service
type Validator struct{}

func New() *Validator {
	return &Validator{}
}

// ValidateSearchQuery validates search query parameters
func (v *Validator) ValidateSearchQuery(q string, typeFilter string, limit int) error {
	if limit < 0 {
		return errors.New("validation: limit must be non-negative")
	}
	if limit > 100 {
		return errors.New("validation: limit must not exceed 100")
	}
	if typeFilter != "" && typeFilter != "tickets" && typeFilter != "sessions" && typeFilter != "operators" {
		return errors.New("validation: type must be one of: tickets, sessions, operators, or empty for all")
	}
	return nil
}

// ValidateIndexTicketInput validates IndexTicketInput
func (v *Validator) ValidateIndexTicketInput(ticketID int64, sessionID string) error {
	var errs []string
	if ticketID <= 0 {
		errs = append(errs, "ticket_id must be positive")
	}
	if strings.TrimSpace(sessionID) == "" {
		errs = append(errs, "session_id is required")
	}
	if len(errs) > 0 {
		return errors.New("validation: " + strings.Join(errs, "; "))
	}
	return nil
}

// ValidateIndexSessionInput validates IndexSessionInput
func (v *Validator) ValidateIndexSessionInput(sessionID string) error {
	if strings.TrimSpace(sessionID) == "" {
		return errors.New("validation: session_id is required")
	}
	if _, err := uuid.Parse(sessionID); err != nil {
		return errors.New("validation: session_id must be a valid UUID")
	}
	return nil
}

// ValidateIndexOperatorInput validates IndexOperatorInput
func (v *Validator) ValidateIndexOperatorInput(userID string) error {
	if strings.TrimSpace(userID) == "" {
		return errors.New("validation: user_id is required")
	}
	if _, err := uuid.Parse(userID); err != nil {
		return errors.New("validation: user_id must be a valid UUID")
	}
	return nil
}
