FROM golang:1.25-alpine AS builder

WORKDIR /app

# Install git (needed for private/public modules)
RUN apk add --no-cache git

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Build binary (static)
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o artmq ./cmd/artmq/main.go

FROM alpine:3.20

WORKDIR /app

# Optional: add CA certs (for HTTPS, etc.)
RUN apk add --no-cache ca-certificates

# Copy binary
COPY --from=builder /app/artmq .

# Expose MQTT port (change if needed)
EXPOSE 1883

# Run
CMD ["./artmq"]
