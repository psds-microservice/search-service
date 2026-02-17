package application

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/psds-microservice/search-service/internal/config"
	"github.com/psds-microservice/search-service/internal/handler"
	"github.com/psds-microservice/search-service/internal/router"
	"github.com/psds-microservice/search-service/internal/service"
)

type API struct {
	cfg *config.Config
	srv *http.Server
}

func NewAPI(cfg *config.Config) (*API, error) {
	searchSvc, err := service.NewSearchService(cfg.Elasticsearch.URL)
	if err != nil {
		return nil, fmt.Errorf("search service: %w", err)
	}
	searchHandler := handler.NewSearchHandler(searchSvc)
	r := router.New(searchHandler)
	addr := cfg.AppHost + ":" + cfg.HTTPPort
	srv := &http.Server{
		Addr:              addr,
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	return &API{cfg: cfg, srv: srv}, nil
}

func (a *API) Run(ctx context.Context) error {
	host := a.cfg.AppHost
	if host == "0.0.0.0" {
		host = "localhost"
	}
	base := "http://" + host + ":" + a.cfg.HTTPPort
	log.Printf("HTTP server listening on %s", a.srv.Addr)
	log.Printf("  Swagger UI:    %s/swagger", base)
	log.Printf("  Swagger spec:  %s/swagger/openapi.json", base)
	log.Printf("  Health:        %s/health", base)
	log.Printf("  Ready:         %s/ready", base)
	log.Printf("  Search:        %s/search", base)
	go func() {
		if err := a.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("http: %v", err)
		}
	}()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := a.srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("http shutdown: %w", err)
	}
	return nil
}
