module github.com/ikniz/url-shortener/gateway

go 1.23

require github.com/ikniz/url-shortener/shared/logger v0.0.0

require (
	github.com/ikniz/url-shortener/shared/auth v0.0.0
	github.com/redis/go-redis/v9 v9.6.0
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/golang-jwt/jwt/v5 v5.3.1 // indirect
)

replace (
	github.com/ikniz/url-shortener/shared/auth => ../shared/auth
	github.com/ikniz/url-shortener/shared/logger => ../shared/logger
)
