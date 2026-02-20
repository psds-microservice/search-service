# No replace, no go.work: inject helpy and infra into GOMODCACHE.
FROM golang:1.26-alpine AS builder
ENV GOMODCACHE=/go/pkg/mod
WORKDIR /app
COPY helpy /go/pkg/mod/github.com/psds-microservice/helpy@v0.1.0
COPY infra /go/pkg/mod/github.com/psds-microservice/infra@v0.0.3
COPY search-service ./search-service
WORKDIR /app/search-service
RUN go mod download
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /search-service ./cmd/search-service

FROM alpine:3.19
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /search-service .
EXPOSE 8099
CMD ["./search-service", "api"]
