package config

import (
	"errors"
	"os"
	"strings"
)

type Config struct {
	AppHost  string
	HTTPPort string
	GRPCPort string
	LogLevel string

	Elasticsearch struct {
		URL string
	}

	KafkaBrokers []string
	KafkaGroupID string
	KafkaTopics  []string
}

func Load() (*Config, error) {
	cfg := &Config{
		AppHost:  getEnv("APP_HOST", "0.0.0.0"),
		HTTPPort: firstEnv("APP_PORT", "HTTP_PORT", "8096"),
		GRPCPort: firstEnv("GRPC_PORT", "METRICS_PORT", "9096"),
		LogLevel: getEnv("LOG_LEVEL", "info"),
	}
	cfg.Elasticsearch.URL = getEnv("ELASTICSEARCH_URL", "http://localhost:9200")

	// Kafka config
	brokersStr := getEnv("KAFKA_BROKERS", "")
	var kafkaBrokers []string
	if brokersStr != "" {
		for _, b := range strings.Split(brokersStr, ",") {
			if b = strings.TrimSpace(b); b != "" {
				kafkaBrokers = append(kafkaBrokers, b)
			}
		}
	}
	cfg.KafkaBrokers = kafkaBrokers
	cfg.KafkaGroupID = getEnv("KAFKA_GROUP_ID", "search-service")
	topicsStr := getEnv("KAFKA_TOPICS", "psds.session.events,psds.session.created,psds.session.ended,psds.session.operator_joined,psds.operator.assigned,psds.operator.created,psds.operator.updated,psds.ticket.events")
	var kafkaTopics []string
	if topicsStr != "" {
		for _, t := range strings.Split(topicsStr, ",") {
			if t = strings.TrimSpace(t); t != "" {
				kafkaTopics = append(kafkaTopics, t)
			}
		}
	}
	cfg.KafkaTopics = kafkaTopics

	return cfg, nil
}

func (c *Config) Validate() error {
	if c.Elasticsearch.URL == "" {
		return errors.New("config: ELASTICSEARCH_URL is required")
	}
	return nil
}

func (c *Config) AppEnv() string {
	return getEnv("APP_ENV", "development")
}

func firstEnv(keysAndDef ...string) string {
	if len(keysAndDef) == 0 {
		return ""
	}
	def := keysAndDef[len(keysAndDef)-1]
	keys := keysAndDef[:len(keysAndDef)-1]
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return def
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
