package kafka

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/psds-microservice/search-service/internal/service"
	"github.com/segmentio/kafka-go"
)

// RunConsumer запускает Kafka consumer: читает сообщения, по топику выбирает обработчик (ticket/session/operator), индексирует в ES.
func RunConsumer(ctx context.Context, brokers []string, groupID string, topics []string, searchSvc service.SearchServicer) {
	if len(brokers) == 0 || len(topics) == 0 {
		log.Println("kafka: brokers or topics empty, consumer not started")
		return
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

		if err := r.CommitMessages(ctx, msg); err != nil {
			log.Printf("kafka: commit message: %v", err)
		}

		topic := msg.Topic
		switch {
		case strings.HasPrefix(topic, "psds.ticket."):
			HandleTicket(ctx, msg, searchSvc)
		case strings.HasPrefix(topic, "psds.session."):
			HandleSession(ctx, msg, searchSvc)
		case strings.HasPrefix(topic, "psds.operator."):
			HandleOperator(ctx, msg, searchSvc)
		default:
			log.Printf("kafka: unknown topic %q, skipping", topic)
		}
	}
}
