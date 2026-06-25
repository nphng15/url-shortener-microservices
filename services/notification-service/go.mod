module github.com/ikniz/url-shortener/services/notification-service

go 1.23

require (
	github.com/ikniz/url-shortener/shared/auth v0.0.0
	github.com/ikniz/url-shortener/shared/events v0.0.0
	github.com/ikniz/url-shortener/shared/logger v0.0.0
	github.com/jackc/pgx/v5 v5.7.2
	github.com/rabbitmq/amqp091-go v1.10.0
)

require (
	github.com/golang-jwt/jwt/v5 v5.3.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	golang.org/x/crypto v0.31.0 // indirect
	golang.org/x/sync v0.10.0 // indirect
	golang.org/x/text v0.21.0 // indirect
)

replace (
	github.com/ikniz/url-shortener/shared/auth => ../../shared/auth
	github.com/ikniz/url-shortener/shared/events => ../../shared/events
	github.com/ikniz/url-shortener/shared/logger => ../../shared/logger
)
