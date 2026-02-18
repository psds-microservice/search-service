package kafka

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/psds-microservice/search-service/internal/service"
	"github.com/segmentio/kafka-go"
)

// RunConsumer запускает Kafka consumer для индексации событий в Elasticsearch.
// Слушает события сессий и операторов, извлекает данные и вызывает соответствующие методы индексации.
func RunConsumer(ctx context.Context, brokers []string, groupID string, topics []string, searchSvc service.SearchServicer) {
	if len(brokers) == 0 || len(topics) == 0 {
		log.Println("kafka: brokers or topics empty, consumer not started")
		return
	}

	type sessionEvent struct {
		Event      string                 `json:"event"`
		SessionID  string                 `json:"session_id"`
		ClientID   string                 `json:"client_id,omitempty"`
		PIN        string                 `json:"pin,omitempty"`
		Status     string                 `json:"status,omitempty"`
		UserID     string                 `json:"user_id,omitempty"`
		OperatorID string                 `json:"operator_id,omitempty"`
		Payload    map[string]interface{} `json:"-"`
	}

	type operatorEvent struct {
		Event       string                 `json:"event"`
		UserID      string                 `json:"user_id"`
		DisplayName string                 `json:"display_name,omitempty"`
		Region      string                 `json:"region,omitempty"`
		Role        string                 `json:"role,omitempty"`
		Payload     map[string]interface{} `json:"-"`
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		GroupTopics:    topics,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        time.Second,
		StartOffset:    kafka.FirstOffset,
		CommitInterval: time.Second,
	})
	defer r.Close()

	log.Printf("kafka consumer: started, group=%s, topics=%v", groupID, topics)

	for {
		select {
		case <-ctx.Done():
			log.Println("kafka consumer: stopping")
			return
		default:
		}

		msg, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("kafka read: %v", err)
			time.Sleep(time.Second)
			continue
		}

		// Коммитим оффсет после успешного чтения, чтобы consumer group регистрировалась и отображалась в Kafka UI
		if err := r.CommitMessages(ctx, msg); err != nil {
			log.Printf("kafka: commit message: %v", err)
		}

		var rawMsg map[string]interface{}
		if err := json.Unmarshal(msg.Value, &rawMsg); err != nil {
			log.Printf("kafka: cannot unmarshal message: %v", err)
			continue
		}

		eventType, _ := rawMsg["event"].(string)
		if eventType == "" {
			log.Printf("kafka: message missing event field")
			continue
		}

		// Обработка событий сессий
		if strings.HasPrefix(eventType, "session.") || strings.Contains(eventType, "session") || rawMsg["session_id"] != nil {
			var ev sessionEvent
			if err := json.Unmarshal(msg.Value, &ev); err != nil {
				log.Printf("kafka: cannot unmarshal session event: %v", err)
				continue
			}
			if ev.SessionID == "" {
				log.Printf("kafka: session event missing session_id")
				continue
			}

			// Для событий сессий нужны client_id, pin, status
			// Если их нет, пытаемся использовать дефолтные значения на основе event типа
			status := ev.Status
			if status == "" {
				if eventType == "session.ended" || eventType == "session.finished" {
					status = "finished"
				} else if eventType == "operator_joined" {
					status = "active"
				} else {
					status = "waiting"
				}
			}
			// Если критичных полей нет, пропускаем (но логируем)
			if ev.ClientID == "" {
				log.Printf("kafka: session event %s missing client_id, skipping", ev.SessionID)
				continue
			}

			in := &service.IndexSessionInput{
				SessionID: ev.SessionID,
				ClientID:  ev.ClientID,
				PIN:       ev.PIN,
				Status:    status,
			}
			if err := searchSvc.IndexSession(ctx, in); err != nil {
				log.Printf("kafka: index session %s: %v", ev.SessionID, err)
			} else {
				log.Printf("kafka: indexed session %s (event: %s)", ev.SessionID, eventType)
			}
			continue
		}

		// Обработка событий операторов
		if strings.HasPrefix(eventType, "operator.") || rawMsg["user_id"] != nil {
			var ev operatorEvent
			if err := json.Unmarshal(msg.Value, &ev); err != nil {
				log.Printf("kafka: cannot unmarshal operator event: %v", err)
				continue
			}
			if ev.UserID == "" {
				log.Printf("kafka: operator event missing user_id")
				continue
			}

			// Для событий операторов нужны display_name, region, role
			// Если их нет, используем пустые строки (но лучше, чтобы они были в событии)
			if ev.DisplayName == "" && ev.Region == "" && ev.Role == "" {
				log.Printf("kafka: operator event %s missing all fields (display_name/region/role), skipping", ev.UserID)
				continue
			}

			in := &service.IndexOperatorInput{
				UserID:      ev.UserID,
				DisplayName: ev.DisplayName,
				Region:      ev.Region,
				Role:        ev.Role,
			}
			if err := searchSvc.IndexOperator(ctx, in); err != nil {
				log.Printf("kafka: index operator %s: %v", ev.UserID, err)
			} else {
				log.Printf("kafka: indexed operator %s (event: %s)", ev.UserID, eventType)
			}
			continue
		}

		log.Printf("kafka: unknown event type: %s", eventType)
	}
}
