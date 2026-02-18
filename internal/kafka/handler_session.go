package kafka

import (
	"context"
	"encoding/json"
	"log"

	"github.com/psds-microservice/search-service/internal/service"
	"github.com/segmentio/kafka-go"
)

// SessionEvent — событие сессии из топика psds.session.*
type SessionEvent struct {
	Event      string `json:"event"`
	SessionID  string `json:"session_id"`
	ClientID   string `json:"client_id,omitempty"`
	PIN        string `json:"pin,omitempty"`
	Status     string `json:"status,omitempty"`
	UserID     string `json:"user_id,omitempty"`
	OperatorID string `json:"operator_id,omitempty"`
}

// HandleSession обрабатывает сообщение из топика сессий и индексирует в ES.
func HandleSession(ctx context.Context, msg kafka.Message, searchSvc service.SearchServicer) {
	topic := msg.Topic
	var ev SessionEvent
	if err := json.Unmarshal(msg.Value, &ev); err != nil {
		log.Printf("kafka: [%s] unmarshal session event: %v", topic, err)
		return
	}
	if ev.SessionID == "" {
		log.Printf("kafka: [%s] missing session_id, skipping", topic)
		return
	}
	status := ev.Status
	if status == "" {
		switch ev.Event {
		case "session.ended", "session.finished":
			status = "finished"
		case "operator_joined":
			status = "active"
		default:
			status = "waiting"
		}
	}
	if ev.ClientID == "" {
		log.Printf("kafka: [%s] session %s missing client_id, skipping", topic, ev.SessionID)
		return
	}
	in := &service.IndexSessionInput{
		SessionID: ev.SessionID,
		ClientID:  ev.ClientID,
		PIN:       ev.PIN,
		Status:    status,
	}
	if err := searchSvc.IndexSession(ctx, in); err != nil {
		log.Printf("kafka: [%s] index session %s: %v", topic, ev.SessionID, err)
		return
	}
	log.Printf("kafka: [%s] indexed session %s", topic, ev.SessionID)
}
