package cmd

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
	"github.com/psds-microservice/search-service/internal/config"
	"github.com/psds-microservice/search-service/internal/kafka"
	"github.com/psds-microservice/search-service/internal/service"
	"github.com/spf13/cobra"
)

var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "Run Kafka consumer (index events into Elasticsearch). Deploy separately from api.",
	RunE:  runWorker,
}

func init() {
	rootCmd.AddCommand(workerCmd)
}

func runWorker(cmd *cobra.Command, args []string) error {
	_ = godotenv.Load(".env")
	_ = godotenv.Load("../.env")
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("config: %w", err)
	}
	if len(cfg.KafkaBrokers) == 0 || len(cfg.KafkaTopics) == 0 {
		return fmt.Errorf("worker requires KAFKA_BROKERS and KAFKA_TOPICS")
	}

	searchSvc, err := service.NewSearchService(cfg.Elasticsearch.URL)
	if err != nil {
		return fmt.Errorf("search service: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("worker: starting Kafka consumer (group=%s, topics=%v)", cfg.KafkaGroupID, cfg.KafkaTopics)
	kafka.RunConsumer(ctx, cfg.KafkaBrokers, cfg.KafkaGroupID, cfg.KafkaTopics, searchSvc)
	log.Println("worker: bye")
	return nil
}
