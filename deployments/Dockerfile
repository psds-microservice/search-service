# Build from repo root: docker build -f search-service/deployments/Dockerfile .
FROM golang:1.21-alpine AS builder
COPY search-service /app
COPY helpy /helpy
WORKDIR /app
RUN go mod download
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/search-service ./cmd/search-service

FROM alpine:3.19
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /out/search-service .
EXPOSE 8096
CMD ["./search-service", "api"]
