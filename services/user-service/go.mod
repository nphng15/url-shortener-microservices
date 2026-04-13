module github.com/ikniz/url-shortener/services/user-service

go 1.23

require (
	github.com/ikniz/url-shortener/shared/events v0.0.0
	github.com/ikniz/url-shortener/shared/logger v0.0.0
)

replace (
	github.com/ikniz/url-shortener/shared/events => ../../shared/events
	github.com/ikniz/url-shortener/shared/logger => ../../shared/logger
)
