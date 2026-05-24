# syntax=docker/dockerfile:1

FROM golang:1.24-alpine AS builder
WORKDIR /src

COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/flux-broker ./cmd/broker

FROM alpine:3.21
RUN addgroup -S flux && adduser -S flux -G flux
WORKDIR /app

COPY --from=builder /out/flux-broker /usr/local/bin/flux-broker
RUN mkdir -p /app/data && chown -R flux:flux /app

ENV FLUX_LISTEN_ADDR=:9092
ENV FLUX_DATA_DIR=/app/data
ENV FLUX_NUM_PARTITIONS=3

EXPOSE 9092
VOLUME ["/app/data"]

USER flux
ENTRYPOINT ["/usr/local/bin/flux-broker"]
