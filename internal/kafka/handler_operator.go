package kafka

import (
	"context"
	"encoding/json"
	"log"

	"github.com/psds-microservice/search-service/internal/service"
	"github.com/segmentio/kafka-go"
)

// OperatorEvent — событие оператора из топика psds.operator.*
type OperatorEvent struct {
	Event       string `json:"event"`
	UserID      string `json:"user_id"`
	DisplayName string `json:"display_name,omitempty"`
	Region      string `json:"region,omitempty"`
	Role        string `json:"role,omitempty"`
}

// HandleOperator обрабатывает сообщение из топика операторов и индексирует в ES.
func HandleOperator(ctx context.Context, msg kafka.Message, searchSvc service.SearchServicer) {
	topic := msg.Topic
	var ev OperatorEvent
	if err := json.Unmarshal(msg.Value, &ev); err != nil {
		log.Printf("kafka: [%s] unmarshal operator event: %v", topic, err)
		return
	}
	if ev.UserID == "" {
		log.Printf("kafka: [%s] missing user_id, skipping", topic)
		return
	}
	if ev.DisplayName == "" && ev.Region == "" && ev.Role == "" {
		log.Printf("kafka: [%s] operator %s missing display_name/region/role, skipping", topic, ev.UserID)
		return
	}
	in := &service.IndexOperatorInput{
		UserID:      ev.UserID,
		DisplayName: ev.DisplayName,
		Region:      ev.Region,
		Role:        ev.Role,
	}
	if err := searchSvc.IndexOperator(ctx, in); err != nil {
		log.Printf("kafka: [%s] index operator %s: %v", topic, ev.UserID, err)
		return
	}
	log.Printf("kafka: [%s] indexed operator %s", topic, ev.UserID)
}
