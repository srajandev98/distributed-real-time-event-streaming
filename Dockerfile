# syntax=docker/dockerfile:1

FROM golang:1.24-alpine AS builder
WORKDIR /src

COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/rtes-broker ./cmd/broker

FROM alpine:3.21
RUN addgroup -S rtes && adduser -S rtes -G rtes
WORKDIR /app

COPY --from=builder /out/rtes-broker /usr/local/bin/rtes-broker
RUN mkdir -p /app/data && chown -R rtes:rtes /app

ENV RTES_LISTEN_ADDR=:9092
ENV RTES_DATA_DIR=/app/data
ENV RTES_NUM_PARTITIONS=3

EXPOSE 9092
VOLUME ["/app/data"]

USER rtes
ENTRYPOINT ["/usr/local/bin/rtes-broker"]
