package config

import (
	"errors"
	"os"
)

type Config struct {
	AppHost  string
	HTTPPort string
	LogLevel string

	Elasticsearch struct {
		URL string
	}
}

func Load() (*Config, error) {
	cfg := &Config{
		AppHost:  getEnv("APP_HOST", "0.0.0.0"),
		HTTPPort: firstEnv("APP_PORT", "HTTP_PORT", "8096"),
		LogLevel: getEnv("LOG_LEVEL", "info"),
	}
	cfg.Elasticsearch.URL = getEnv("ELASTICSEARCH_URL", "http://localhost:9200")
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
