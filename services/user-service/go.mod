module github.com/ikniz/url-shortener/services/user-service

go 1.23

require (
	github.com/golang-jwt/jwt/v5 v5.3.1
	github.com/ikniz/url-shortener/shared/auth v0.0.0
	github.com/ikniz/url-shortener/shared/logger v0.0.0
	github.com/jackc/pgx/v5 v5.6.0
	golang.org/x/crypto v0.27.0
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/puddle/v2 v2.2.1 // indirect
	golang.org/x/sync v0.8.0 // indirect
	golang.org/x/text v0.18.0 // indirect
)

replace (
	github.com/ikniz/url-shortener/shared/auth => ../../shared/auth
	github.com/ikniz/url-shortener/shared/logger => ../../shared/logger
)
