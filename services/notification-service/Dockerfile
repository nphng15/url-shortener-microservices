FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY . .
WORKDIR /app/services/notification-service
RUN go build -o main .

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/services/notification-service/main .
CMD ["./main"]
