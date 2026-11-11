# ==============================================================================
# Build Stage
# ==============================================================================
FROM golang:1.24-alpine AS builder

WORKDIR /build

# Install CA certificates and build tools
RUN apk add --no-cache ca-certificates git tzdata

# Cache Go modules
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build statically linked, stripped binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w -X main.version=0.1.0" \
    -o /build/audienceshift ./cmd/audienceshift

# ==============================================================================
# Final Minimal Runtime Stage
# ==============================================================================
FROM alpine:3.21

# Install CA certificates for outgoing TLS / webhook delivery
RUN apk add --no-cache ca-certificates tzdata

# Create dedicated non-root application user
RUN addgroup -S audienceshift && adduser -S audienceshift -G audienceshift

WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/audienceshift /usr/local/bin/audienceshift

# Set up certificate cache directory with correct permissions
RUN mkdir -p /certs && chown -R audienceshift:audienceshift /certs /app

USER audienceshift

# 80 = ACME HTTP-01 challenge / HTTP redirect
# 443 = HTTPS / WSS relay ingress
# 8080 = Default HTTP relay port
EXPOSE 80 443 8080

VOLUME ["/certs"]

ENTRYPOINT ["audienceshift"]
CMD ["server", "--addr", ":8080"]

