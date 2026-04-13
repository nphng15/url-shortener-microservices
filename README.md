# URL Shortener Microservices

A URL shortener built with Go, using a microservices architecture.

## Services

- `url-service`: Handles URL creation and redirection.
- `user-service`: Manages users and authentication.
- `analytics-service`: Tracks click metrics and events.
- `notification-service`: Sends notifications for milestone events.
- `gateway`: The API gateway routing requests to underlying services.

## Development

Run `docker compose up --build` to start all services and databases locally.

To verify health, run `./scripts/smoke_test.sh`.

## Dev docs

Check `/dev-docs`
