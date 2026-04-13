# URL Shortener — Chia việc & Quy trình

> Team: Thống (Lead), Hào, Phong | 3 tuần max, cố gắng khổ trước sướng sau dù môn kéo dài 3-4 tháng | GitHub repo public

---

## 1. Nguyên tắc

- Mỗi người **sở hữu service của mình**, không động vào code người khác
- Giao tiếp qua **group chat**. Chỉ call khi kết thúc module hoặc bị block
- Ráng mỗi ngày post 3 dòng: done gì / hôm nay làm gì / block gì
- `docker-compose.yml` — **chỉ Thống sửa**. Cần thay đổi thì nhắn Thống
- Sửa `shared/*` thì nhắn group chat trước

---

## 2. Ai làm gì

```
Hào    -> services/url-service/  (store, cache, handler, codegen, migration, tests)

Thống  -> shared/*
          services/user-service/
          services/url-service/  <- outbox.go + publisher.go (hỗ trợ Hào)
          gateway/
          docker-compose.yml, CI, README, scripts

Phong  -> services/analytics-service/
          services/notification-service/
```

Tại sao Thống lấy outbox: Outbox pattern (coordinator + workers + AMQP publisher) là phần nặng + kiến thức infra cao nhất trong url-service. Tách ra cho Thống giảm bottleneck cho Hào, và Thống vốn đã own shared packages + RabbitMQ setup nên context sẵn rồi.

Ranh giới trong url-service: Hào sở hữu domain (store, cache, handler, codegen, validate, errors). Thống sở hữu infra (outbox.go, publisher.go). Không chồng chéo vì giao tiếp qua interface (OutboxRepository, RabbitMQPublisher).

---

## 3. Git

- Branch: `feat/m3-url-service`, `fix/m4-dedup-bug`
- Commit: viết gì cũng được, miễn hiểu. VD: `add POST /shorten handler`, `fix outbox polling`
- PR vào `main`, cần 1 approve, review trong 4 tiếng
- Squash merge. Protect `main`: require PR + 1 approval

---

## 4. Timeline — 3 tuần

### Tuần 1 — M1 + M2 + khởi động M3/M4

Ngày 1-2: M1 Foundation

| Thống | Hào | Phong |
|-------|-----|-------|
| shared/events + shared/logger + docker-compose + gateway scaffold + smoke_test.sh | url-service scaffold (config, db, redis, rabbitmq, health, main, Dockerfile) | analytics + notification + user scaffolds |

Check: `docker compose up --build` -> 11 containers healthy, /health -> 200

Ngày 3-5: M2 + bắt đầu M3/M4

| Thống | Hào | Phong |
|-------|-----|-------|
| shared/auth + user-service full (store, password, token, handler, tests) | M3: migration, base62, codegen, store, validate, cache.go | M4: migration, stores (3), haship, publisher, milestone.go |

Check: register->login->GET /me hoạt động. shared/auth merged.

Ngày 6-7: M3/M4 tiếp

| Thống | Hào | Phong |
|-------|-----|-------|
| M3: outbox.go (coordinator + 3 workers) + publisher.go. Bắt đầu gateway: router.go, proxy.go | M3: outbox_store, handler (4 endpoints) | M4: consumer.go, handler.go |

---

### Tuần 2 — M3 xong + M4 xong + M5

Ngày 8-10: Hoàn thành M3/M4

| Thống | Hào | Phong |
|-------|-----|-------|
| Gateway: circuitbreaker, ratelimit, jwt middleware, middleware | M3: errors, main.go (full wiring), tests + bench, bug fixes | M4: main.go wiring, analytics_test, bug fixes |

Check M3: shorten->redirect->delete hoạt động, outbox publishes trong 5s
Check M4: dedup hoạt động, milestone khi 10 clicks, stats đúng

Ngày 11-14: M5

| Thống | Hào | Phong |
|-------|-----|-------|
| Gateway: config full, main.go wiring, gateway_test. Extend shared/logger (correlation ID) | Optimize url-service, fix bugs, hỗ trợ integration | Notification full: migration, store, consumer, handler, tests |

Check: Gateway route đúng, JWT hoạt động, rate limit 429, circuit breaker 503, notifications được lưu

---

### Tuần 3 — Integration + Demo

Ngày 15-17: E2E

| Cả team |
|---------|
| Viết + chạy e2e_test.sh. Full flow test. Cold restart test. Fix bugs. |

Check: register -> login -> shorten -> redirect x15 -> stats -> milestone -> notifications

Ngày 18-19: Polish — code cleanup, README, demo rehearsal.
Ngày 20-21: Buffer + nộp bài.

---

## 5. Checklist

### M1
```
[ ] go.work + shared/events (BaseEvent + 4 event types + JSON round-trip test) + shared/logger
[ ] docker-compose.yml — 11 containers healthy
[ ] Mỗi service: config, db, health, main, Dockerfile, go.mod
[ ] url-service thêm: redis, rabbitmq
[ ] analytics-service thêm: rabbitmq + DeclareAnalyticsQueue (bind url.clicked)
[ ] notification-service thêm: rabbitmq + DeclareNotificationQueue (bind url.created, url.deleted, milestone.reached)
[ ] Gateway: stub /health only
[ ] smoke_test.sh pass
```

### M2
```
[ ] shared/auth — Claims, VerifyToken (check SigningMethodHMAC), JWTMiddleware, ClaimsFromContext
[ ] user-service: migration (users table, id UUID, email UNIQUE, password_hash TEXT)
[ ] store (CreateUser, FindByEmail), password (bcrypt cost 12), token (HS256, 24h)
[ ] handler: POST /register, POST /login, GET /me
[ ] tests: register->201, duplicate->409, login->200+JWT, wrong pass->401 (cùng body với unknown email), GET /me->200
[ ] Password không bao giờ xuất hiện trong logs hoặc response
```

### M3
```
[ ] migration: urls table + outbox table + indexes
[ ] base62 + codegen (crypto/rand only, ShortCodeGenerator interface)            <- Hào
[ ] store (Insert, FindByCode, FindByUserID, Deactivate)                         <- Hào
[ ] outbox_store: InsertWithURL trong cùng transaction với URL insert             <- Hào
[ ] cache: RedisCache Get/Set/Delete, computeTTL (min(expires_at-now, 1h))       <- Hào
[ ] publisher (amqp, sync.Mutex), outbox coordinator (poll 2s) + 3 workers       <- Thống
[ ]   coordinator dùng FOR UPDATE SKIP LOCKED, worker channel cap=50             <- Thống
[ ] handler: POST /shorten, GET /{code}, GET /urls, DELETE /urls/{code}          <- Hào
[ ] POST /shorten + DELETE: URL row + outbox row trong cùng 1 transaction
[ ] GET /{code}: Redis GET (50ms timeout) -> miss thì PostgreSQL -> cache Set -> 301
[ ] Deactivated/expired URL -> 410 Gone (không phải 404)
[ ] DELETE -> invalidate Redis key ngay sau khi commit SQL
[ ] Ownership check: DELETE với user_id không khớp -> 403
[ ] url-service hash IP trước khi ghi vào URLClickedEvent (analytics KHÔNG re-hash)
[ ] tests + redirect benchmark
[ ] KHÔNG có math/rand. Redis KHÔNG phải source of truth.
```

### M4
```
[ ] migration: clicks + milestones + processed_events tables + indexes
[ ] stores: ClickStore, MilestoneStore, DeduplicationStore
[ ] consumer dùng evt.IPHash trực tiếp (đã hash bởi url-service, KHÔNG re-hash)
[ ] publisher: MilestoneReachedEvent (publish trực tiếp, không qua outbox)
[ ] milestone checker: thresholds 10, 100, 1000 (idempotent qua milestones table)
[ ] CheckAndPublish nhận userID="" và userEmail="" (URLClickedEvent không mang user info)
[ ] consumer: single goroutine, prefetch=1, manual ack
[ ]   duplicate event_id -> ack + discard, KHÔNG tăng click count
[ ]   poison/malformed message -> ack (KHÔNG nack), log warning
[ ]   click insert + dedup insert + milestone insert trong 1 transaction
[ ]   milestone publish fail -> log error, KHÔNG crash, KHÔNG nack
[ ] handler: GET /stats/{code} (dùng errgroup cho concurrent queries), GET /stats/{code}/timeline
[ ] tests
```

### M5
```
Notification:
[ ] migration: notifications table + indexes
[ ] store: InsertNotification (INSERT status='pending', mockEmail, UPDATE status='sent' + sent_at)
[ ] consumer: single goroutine, prefetch=1, 3 routing keys (url.created, url.deleted, milestone.reached)
[ ] handler: GET /notifications — JWT required, cursor-based pagination
[ ] tests

Gateway:
[ ] router: Route struct + routing table (~9 routes, path rewriting /api/* -> /*)
[ ] proxy: httputil.ReverseProxy per upstream, /r/{code} rewrite thành /{code}
[ ] middleware: X-Correlation-ID injection + structured request logging
[ ] jwtmiddleware: local HMAC verify, exclude /api/auth/*
[ ] ratelimit: Redis INCR+EXPIRE token bucket, per client IP
[ ] circuitbreaker: 3-state (CLOSED/OPEN/HALF_OPEN), sync.Mutex, CHỈ APPLY CHO url-service proxy
[ ] Redis error trong rate limiter -> fail-open (cho request qua), log Warn
[ ] Gateway KHÔNG import shared/events — zero domain logic
[ ] e2e_test.sh pass
```

---

## 6. Workload

| | Thống | Hào | Phong |
|-|-------|-----|-------|
| Sở hữu | shared, user-service, gateway, infra, url-service outbox+publisher | url-service (domain) | analytics, notification |
| Giờ ước lượng | ~48h | ~32h | ~38h |
| Trung bình/tuần | ~16h | ~11h | ~13h |
