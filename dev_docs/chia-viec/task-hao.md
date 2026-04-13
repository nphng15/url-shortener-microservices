# Hào — Task List

> Sở hữu: services/url-service/ (domain layer)
> Thống làm: outbox.go + publisher.go (infra layer trong url-service)
> KHÔNG sửa code ngoài url-service/

---

## Tuần 1

### Ngày 1-2: M1 — url-service scaffold
```
[ ] config.go — loadConfig từ env vars (DATABASE_URL, REDIS_URL, RABBITMQ_URL, JWT_SECRET, SHORT_URL_BASE, IP_HASH_SALT, PORT)
[ ] db.go — NewDBPool (pgxpool, MaxConns=10, MinConns=2, ping 10s timeout, fatal khi fail)
[ ] redis.go — NewRedisClient (parse URL, ping 3s timeout, NON-FATAL khi fail, return client+bool)
[ ] rabbitmq.go — NewRabbitMQConn (exponential backoff, max 10 attempts, declare exchange "url-shortener" topic)
[ ] health.go — GET /health -> 200 {"status":"ok","service":"url-service"} (pre-encoded JSON)
[ ] main.go — wire config->db->redis->rabbitmq->mux->server, graceful shutdown SIGTERM/SIGINT
[ ] Dockerfile, go.mod
```

Check: `docker compose up --build`, url-service healthy, /health -> 200

### Ngày 3-5: Bắt đầu M3 sớm
```
[ ] migration.sql:
    - urls table: id UUID PK, short_code VARCHAR(10) UNIQUE NOT NULL, original_url TEXT NOT NULL,
      user_id UUID NOT NULL, created_at TIMESTAMPTZ DEFAULT now(), expires_at TIMESTAMPTZ NULL, is_active BOOLEAN DEFAULT true
    - outbox table: id UUID PK, event_type TEXT NOT NULL, payload JSONB NOT NULL,
      created_at TIMESTAMPTZ DEFAULT now(), published_at TIMESTAMPTZ NULL
    - idx_urls_short_code (UNIQUE, implicit từ constraint)
    - idx_urls_user_id_created (user_id, created_at DESC)
    - idx_outbox_unpublished (created_at ASC) WHERE published_at IS NULL

[ ] base62.go — alphabet "0-9A-Za-z", Encode([]byte) string
[ ] codegen.go — ShortCodeGenerator interface + cryptoRandGenerator (crypto/rand, big.Int mod 62^7)
      KHÔNG DÙNG math/rand

[ ] store.go (pgxURLStore):
    - Insert(ctx, tx, record) error
    - FindByCode(ctx, shortCode) (*URLRecord, error) — check is_active + expires_at
    - FindByUserID(ctx, userID, afterID, limit) ([]URLRecord, error) — cursor pagination, newest-first
    - Deactivate(ctx, tx, shortCode, userID) error — set is_active=false, verify ownership

[ ] validate.go — URL phải có scheme (http/https) + host. Reject empty, invalid scheme.
[ ] cache.go (RedisCache):
    - Get(ctx, code) (*CachedURL, error) — Redis GET với 50ms timeout, error -> return nil (non-fatal)
    - Set(ctx, code, cached, ttl) — TTL = min(expires_at - now, 1h). URL không có expiry -> 1h
    - Delete(ctx, code) — gọi ngay sau khi deactivate URL
    - CachedURL struct: OriginalURL, ExpiresAt, IsActive
```

### Ngày 6-7: M3 handler + outbox_store
```
[ ] outbox_store.go (pgxOutboxStore):
    - InsertWithURL(ctx, tx, url, outbox) error — TRONG CÙNG TRANSACTION với URL insert
    - InsertEvent(ctx, tx, outbox) error — cho DELETE (outbox event trong cùng tx với deactivate)
    - FetchUnpublished(ctx, limit) ([]*OutboxRecord, error) — SELECT ... FOR UPDATE SKIP LOCKED
    - MarkPublished(ctx, id) error — SET published_at = now()

[ ] handler.go — POST /shorten:
    1. Parse JSON body {url, expires_in_hours}
    2. Validate URL (scheme + host)
    3. Extract user claims từ JWT context (ClaimsFromContext)
    4. Generate short code (ShortCodeGenerator)
    5. BEGIN tx -> Insert URL + Insert outbox (URLCreatedEvent, bao gồm user_email từ JWT) -> COMMIT
    6. Collision retry: nếu short_code UNIQUE violation -> generate lại, max 3 retries
    7. Return 201 {short_code, short_url, original_url, expires_at}

[ ] handler.go — GET /{code}:
    1. Redis GET (50ms timeout) -> hit: check is_active + expires_at
    2. Miss hoặc error: PostgreSQL FindByCode
    3. URL không tồn tại -> 404
    4. URL deactivated (is_active=false) -> 410 Gone
    5. URL expired (expires_at < now) -> 410 Gone
    6. Cache Set (background, non-blocking)
    7. Hash IP tại đây: hashIP(r.RemoteAddr) = SHA-256(ip + salt)
       Analytics nhận IPHash đã sẵn hash, KHÔNG re-hash
    8. Write outbox URLClickedEvent (ip_hash, user_agent, referer)
    9. Return 301 redirect

[ ] handler.go — GET /urls:
    1. JWT required, extract user_id
    2. Query param: ?after=<uuid>&limit=20
    3. FindByUserID cursor pagination
    4. Return 200 {urls: [...], next_cursor: "uuid"}

[ ] handler.go — DELETE /urls/{code}:
    1. JWT required, extract user_id
    2. BEGIN tx -> Deactivate (check ownership, user_id không khớp -> 403) + Insert outbox (URLDeletedEvent, bao gồm user_email) -> COMMIT
    3. Redis Delete (invalidate cache ngay sau commit)
    4. Return 204
```

---

## Tuần 2

### Ngày 8-10: Hoàn thành M3
```
[ ] errors.go — sentinel errors (ErrNotFound, ErrAlreadyExists, ErrForbidden, ErrExpired, ErrDeactivated)
    writeError(w, status, message) + writeJSON(w, status, data) helpers

[ ] Mở rộng main.go — full wiring:
    - runMigrations(ctx, pool)
    - Khởi tạo stores, cache, codegen, handler
    - Wire JWT middleware từ shared/auth
    - Đăng ký routes: POST /shorten (auth), GET /{code} (no auth), GET /urls (auth), DELETE /urls/{code} (auth)
    - Start outbox coordinator (Thống làm outbox, bạn chỉ gọi NewOutboxCoordinator(...) trong main.go)

[ ] url_test.go:
    - base62: encode/decode round-trip
    - codegen: output là 7 chars, chỉ chứa base62 chars, 2 calls khác nhau
    - handler: mock store + cache, test shorten/redirect/list/delete
    - cache: mock redis, test hit/miss/error fallback

[ ] bench_test.go — redirect benchmark (cached vs uncached)
```

Check M3:
- POST /shorten -> 201, short code trong DB, outbox row được tạo
- GET /{code} -> 301 (lần 1 từ DB, lần 2 từ Redis)
- DELETE -> 204, sau đó GET -> 410
- Outbox rows published_at != NULL trong 5s
- EXPLAIN ANALYZE trên urls table -> index scan

### Ngày 11-14: Hỗ trợ
```
[ ] Optimize url-service: EXPLAIN ANALYZE, Redis cache hit rate
[ ] Fix bugs từ integration testing
[ ] Hỗ trợ Thống/Phong test integration giữa các services
```

---

## Tuần 3

```
[ ] Chạy e2e_test.sh, fix url-service bugs
[ ] Full flow test: shorten -> redirect (15 lần) -> check stats -> check milestone
[ ] Cold restart: docker compose down && up -> url-service healthy
[ ] Code cleanup, comments, demo rehearsal
```

---

## Anti-pattern guard

```bash
grep -r "math/rand" services/url-service/*.go | grep -v "_test.go" && echo "FAIL" || echo "PASS"
grep -r "analytics_db\|user_db\|notification_db" services/url-service/ && echo "FAIL" || echo "PASS"
grep -r "analytics-service\|notification-service\|user-service" services/url-service/*.go && echo "FAIL" || echo "PASS"
```
