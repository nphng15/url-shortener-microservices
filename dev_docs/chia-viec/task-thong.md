# Thống — Task List (Lead)

> Sở hữu: shared/_, services/user-service/, gateway/, infra, url-service outbox+publisher
> KHÔNG sửa code trong: services/url-service/ (trừ outbox.go + publisher.go), services/analytics-service/
> Owner duy nhất của: docker-compose.yml, go.work, .github/_, shared/\*

---

## Tuần 1

### Ngày 1: M1 — shared packages

```
[ ] go.work:
    go 1.23
    use (
      ./shared/events
      ./shared/logger
      ./shared/auth
      ./services/url-service
      ./services/analytics-service
      ./services/user-service
      ./services/notification-service
      ./gateway
    )

[ ] shared/events/go.mod + events.go:
    - EventType constants: "url.created", "url.clicked", "url.deleted", "milestone.reached"
    - BaseEvent: EventType, OccurredAt (UTC), CorrelationID, EventID (UUID v4)
    - URLCreatedEvent: BaseEvent + ShortCode, OriginalURL, UserID, UserEmail, ExpiresAt
    - URLClickedEvent: BaseEvent + ShortCode, IPHash, UserAgent, Referer, ClickedAt
      (URLClickedEvent KHÔNG có UserID, UserEmail)
    - URLDeletedEvent: BaseEvent + ShortCode, UserID, UserEmail
    - MilestoneReachedEvent: BaseEvent + ShortCode, UserID, UserEmail, MilestoneN (int), TotalClicks (int64)

[ ] shared/events/events_test.go — JSON marshal/unmarshal round-trip cho mỗi event type

[ ] shared/logger/go.mod + logger.go:
    - func New(serviceName string) *slog.Logger
    - slog.NewJSONHandler(os.Stdout, LevelInfo)
    - Default attribute: "service" = serviceName
```

### Ngày 2: M1 — docker-compose + gateway scaffold

```
[ ] docker-compose.yml — 11 containers:
    Infra: redis (6379), rabbitmq (5672+15672), url_db (5432), analytics_db (5433), user_db (5434), notification_db (5435)

    App (tất cả context: . + dockerfile path):
    - url-service: 8081->8080, env: DATABASE_URL, REDIS_URL, RABBITMQ_URL, JWT_SECRET, SHORT_URL_BASE, IP_HASH_SALT, PORT
    - analytics-service: 8082->8080, env: DATABASE_URL, RABBITMQ_URL, JWT_SECRET, IP_HASH_SALT, PORT
    - user-service: 8083->8080, env: DATABASE_URL, JWT_SECRET, PORT
    - notification-service: 8084->8080, env: DATABASE_URL, RABBITMQ_URL, JWT_SECRET, PORT
    - gateway: 8080->8080, env:
        URL_SERVICE_URL: http://url-service:8080
        ANALYTICS_SERVICE_URL: http://analytics-service:8080
        USER_SERVICE_URL: http://user-service:8080
        NOTIFICATION_SERVICE_URL: http://notification-service:8080
        REDIS_URL: redis://redis:6379/0
        JWT_SECRET: change-this-in-production-minimum-32-chars
        PORT: "8080"

[ ] gateway scaffold: config.go, health.go, main.go (chỉ health), Dockerfile, go.mod
[ ] scripts/smoke_test.sh — curl tất cả /health (ports 8080-8084)
[ ] README.md
```

Check M1: `docker compose up --build -d` -> 11 containers healthy < 60s

### Ngày 3-5: M2 — shared/auth + user-service

```
[ ] shared/auth/go.mod + auth.go:
    - type Claims struct { UserID, Email string; jwt.RegisteredClaims }
    - func VerifyToken(tokenString, secret) (*Claims, error)
      PHẢI check token.Method.(*jwt.SigningMethodHMAC) trước khi accept key
    - var ErrTokenInvalid, var TestClaimsKey

[ ] shared/auth/middleware.go:
    - func JWTMiddleware(secret string, next http.Handler) http.Handler
    - func ClaimsFromContext(ctx) *Claims
    Push shared/auth lên main trước khi Hào/Phong cần import.

[ ] user-service/migration.sql:
    - users table: id UUID PK DEFAULT gen_random_uuid(), email TEXT UNIQUE NOT NULL,
      password_hash TEXT NOT NULL, created_at TIMESTAMPTZ DEFAULT now()

[ ] user-service/store.go: CreateUser, FindByEmail
[ ] user-service/password.go: HashPassword (bcrypt cost 12), ComparePassword
[ ] user-service/token.go: IssueToken (HS256, 24h TTL)
[ ] user-service/handler.go:
    - POST /register: 201 {id, email}. Duplicate: 409.
    - POST /login: 200 {token, user}. Sai -> 401 CÙNG MỘT BODY (timing-safe)
    - GET /me: JWT -> 200 {id, email}
[ ] user-service/validate.go + errors.go + user_test.go
```

Check M2: register -> login -> GET /me thành công

### Ngày 6-7: M3 outbox + bắt đầu gateway

```
[ ] url-service/publisher.go (amqpPublisher):
    - Publish(ctx, routingKey, body) error
    - sync.Mutex bảo vệ amqp.Channel
    - persistent delivery mode

[ ] url-service/outbox.go (OutboxCoordinator):
    - Run(ctx): 1 coordinator goroutine + 3 worker goroutines
    - Coordinator polls mỗi 2s: FetchUnpublished(limit=50) -> buffered channel (cap=50)
    - Worker: đọc channel -> Publish -> MarkPublished. Fail -> log, KHÔNG mark (retry sau)
    - FetchUnpublished dùng FOR UPDATE SKIP LOCKED

[ ] gateway/router.go — routing table (~9 routes):
    - POST /api/auth/register -> user-service /register (no auth)
    - POST /api/auth/login -> user-service /login (no auth)
    - GET /api/me -> user-service /me (auth, strip /api)
    - POST /api/shorten -> url-service /shorten (auth, strip /api, rate limit "shorten")
    - GET /api/urls -> url-service /urls (auth, strip /api)
    - DELETE /api/urls/* -> url-service /urls/* (auth, strip /api)
    - GET /r/* -> url-service /* (no auth, strip /r, rate limit "redirect")
    - GET /api/stats/* -> analytics-service /stats/* (no auth, strip /api)
    - GET /api/notifications -> notification-service /notifications (auth, strip /api)

[ ] gateway/proxy.go — httputil.ReverseProxy per upstream, path rewriting
```

---

## Tuần 2

### Ngày 8-10: Gateway còn lại

```
[ ] gateway/circuitbreaker.go:
    - 3 states: CLOSED, OPEN (503), HALF_OPEN (1 probe)
    - sync.Mutex bảo vệ MỌI state read/write
    - maxFailures=5, openTimeout=30s, failureWindow=10s
    - CHỈ APPLY CHO url-service proxy path

[ ] gateway/ratelimit.go:
    - Redis INCR + EXPIRE token bucket, key "rl:{route}:{ip}"
    - Redis error -> fail-open, log Warn
    - shorten=10 req/min, redirect=300 req/min

[ ] gateway/middleware.go: CorrelationID injection + RequestLogger
[ ] gateway/jwtmiddleware.go: shared/auth.VerifyToken, exclude /api/auth/*
[ ] gateway/errors.go
[ ] gateway/config.go (full): upstream URLs, RedisURL, JWTSecret, CB settings, RateLimits
[ ] gateway/main.go — full wiring:
    Middleware chain: RequestLogger -> CorrelationID -> JWT -> RateLimit -> CircuitBreaker -> proxy

[ ] Mở rộng shared/logger:
    - WithCorrelationID, ContextWithCorrelationID, CorrelationIDFromContext
```

### Ngày 11-14: Tests + review

```
[ ] gateway/gateway_test.go:
    - CB: CLOSED->OPEN, OPEN->HALF_OPEN, HALF_OPEN->CLOSED, HALF_OPEN->OPEN
    - Rate limiter: allow/reject, Redis error -> fail-open
    - Router: upstream matching, path rewriting
    - JWT: valid passes, invalid rejects, /api/auth/* excluded

[ ] Review + merge PRs của Hào và Phong
```

Check M5:

- POST /api/shorten -> 201
- GET /api/urls không token -> 401
- 11 lần POST /api/shorten -> 429
- Stop url-service -> 503, restart -> hồi phục sau 30s
- X-Correlation-ID trong response

---

## Tuần 3

### Ngày 15-17: E2E

```
[ ] scripts/e2e_test.sh:
    1. POST /api/auth/register -> 201
    2. POST /api/auth/login -> 200, extract token
    3. POST /api/shorten (JWT) -> 201, extract short_code
    4. GET /r/{short_code} -> 301
    5. Lặp redirect 14 lần nữa (tổng 15)
    6. Sleep 5s (chờ outbox + consumer)
    7. GET /api/stats/{short_code} -> verify total_clicks=15
    8. Verify milestone 10 reached
    9. GET /api/notifications (JWT) -> verify có items
    10. DELETE /api/urls/{short_code} (JWT) -> 204
    11. GET /r/{short_code} -> 410
    12. Rate limit: 11 lần -> 429
    13. Check X-Correlation-ID

[ ] Cold restart: docker compose down -v && up --build -> healthy < 60s
[ ] Fix final bugs, README final, demo rehearsal
```

---

## Trách nhiệm Lead (xuyên suốt)

- docker-compose.yml: chỉ bạn sửa
- Shared package changes: thảo luận team trước, bạn implement
- Merge conflicts trên main: bạn resolve
- PR review: trong 4 tiếng
- Quyết định cuối khi team 50/50

## Thêm việc nếu thời gian cho phép

- GIỮ NGUYÊN code và kiến trúc hiện tại (nó quá tốt rồi).
- THÊM Observability: Thêm Prometheus (để show metric) và Grafana (làm vài cái dashboard đẹp mắt chiếu lúc bảo vệ đồ án: số lượng click, tỷ lệ lỗi, CPU usage). Đây là điểm "ăn tiền" nhất lúc demo.
- THÊM Dead Letter Queue (DLQ) cho RabbitMQ (thể hiện bạn tư duy rất sâu về tính vẹn toàn dữ liệu - Data Integrity).
  (Bonus) Nếu thời gian cho phép, viết script Load Test bằng k6 (bắn 10,000 req/s) và mở Grafana lên xem Circuit Breaker nhảy từ CLOSED sang OPEN như thế nào.
