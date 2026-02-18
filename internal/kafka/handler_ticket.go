package kafka

import (
	"context"
	"encoding/json"
	"log"

	"github.com/psds-microservice/search-service/internal/service"
	"github.com/segmentio/kafka-go"
)

// TicketEvent — событие тикета из топика psds.ticket.*
type TicketEvent struct {
	Event      string `json:"event"`
	TicketID   int64  `json:"ticket_id"`
	SessionID  string `json:"session_id"`
	ClientID   string `json:"client_id,omitempty"`
	OperatorID string `json:"operator_id,omitempty"`
	Subject    string `json:"subject,omitempty"`
	Notes      string `json:"notes,omitempty"`
	Status     string `json:"status,omitempty"`
}

// HandleTicket обрабатывает сообщение из топика тикетов и индексирует в ES.
func HandleTicket(ctx context.Context, msg kafka.Message, searchSvc service.SearchServicer) {
	topic := msg.Topic
	var raw map[string]interface{}
	if err := json.Unmarshal(msg.Value, &raw); err != nil {
		log.Printf("kafka: [%s] unmarshal: %v", topic, err)
		return
	}
	var ev TicketEvent
	if err := json.Unmarshal(msg.Value, &ev); err != nil {
		log.Printf("kafka: [%s] unmarshal ticket event: %v", topic, err)
		return
	}
	if ev.TicketID == 0 && raw["ticket_id"] != nil {
		if tid, ok := raw["ticket_id"].(float64); ok {
			ev.TicketID = int64(tid)
		} else if tid, ok := raw["ticket_id"].(int64); ok {
			ev.TicketID = tid
		}
	}
	if ev.TicketID == 0 || ev.SessionID == "" {
		log.Printf("kafka: [%s] missing ticket_id or session_id, skipping", topic)
		return
	}
	in := &service.IndexTicketInput{
		TicketID:   ev.TicketID,
		SessionID:  ev.SessionID,
		ClientID:   ev.ClientID,
		OperatorID: ev.OperatorID,
		Subject:    ev.Subject,
		Notes:      ev.Notes,
		Status:     ev.Status,
	}
	if err := searchSvc.IndexTicket(ctx, in); err != nil {
		log.Printf("kafka: [%s] index ticket %d: %v", topic, ev.TicketID, err)
		return
	}
	log.Printf("kafka: [%s] indexed ticket %d", topic, ev.TicketID)
}
