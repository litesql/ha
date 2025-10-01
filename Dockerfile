FROM golang:1.25-trixie AS builder

ARG COMMIT
ARG DATE
ARG VERSION

WORKDIR /build

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=1 go build -o ha -ldflags="-X main.commit=$COMMIT -X main.date=$DATE -X main.version=$VERSION" .

# Production image
FROM debian:trixie-slim AS production

RUN groupadd --system --gid 1000 ha && \
    useradd --system --uid 1000 --gid 1000 --home /data ha

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app
RUN mkdir -p /data && chown -R ha:ha /data

VOLUME /data

COPY --from=builder /build/ha .
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

EXPOSE 4222 5432 6222 8080

USER ha

ENV HA_NATS_STORE_DIR="/data" 
ENV HA_ARGS=

ENTRYPOINT ["/app/entrypoint.sh"]