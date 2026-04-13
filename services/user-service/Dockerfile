FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY . .
WORKDIR /app/services/user-service
RUN go build -o main .

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/services/user-service/main .
CMD ["./main"]
