FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY . .
WORKDIR /app/gateway
RUN go build -o main .

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/gateway/main .
CMD ["./main"]
