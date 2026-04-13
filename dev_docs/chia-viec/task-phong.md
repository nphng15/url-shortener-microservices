# Phong — Task List

> Sở hữu: services/analytics-service/ + services/notification-service/
> KHÔNG sửa code ngoài 2 service này

---

## Tuần 1

### Ngày 1-2: M1 — scaffolds
```
[ ] analytics-service scaffold:
    - config.go (DATABASE_URL, RABBITMQ_URL, PORT, IP_HASH_SALT)
    - db.go (NewDBPool — cùng pattern với url-service)
    - rabbitmq.go (NewRabbitMQConn + DeclareAnalyticsQueue: queue "analytics.clicks", bind routing_key "url.clicked")
    - health.go (GET /health -> 200)
    - main.go (wire tất cả, chỉ health route)
    - Dockerfile, go.mod

[ ] notification-service scaffold:
    - config.go (DATABASE_URL, RABBITMQ_URL, JWT_SECRET, PORT)
    - db.go
    - rabbitmq.go (NewRabbitMQConn + DeclareNotificationQueue: queue "notifications.events",
      bind 3 routing keys: "url.created", "url.deleted", "milestone.reached")
    - health.go, main.go, Dockerfile, go.mod

[ ] user-service scaffold (hỗ trợ Thống):
    - config.go (DATABASE_URL, JWT_SECRET, BCRYPT_COST, PORT)
    - db.go, health.go, main.go, Dockerfile, go.mod
```

Check: docker compose up -> analytics, notification, user containers healthy

### Ngày 3-5: Bắt đầu M4 sớm
```
[ ] migration.sql:
    - clicks table: id UUID PK, short_code VARCHAR(10) NOT NULL, ip_hash VARCHAR(64) NOT NULL,
      user_agent TEXT, referer TEXT, clicked_at TIMESTAMPTZ NOT NULL, correlation_id VARCHAR(64)
    - milestones table: id UUID PK, short_code VARCHAR(10) NOT NULL, milestone INT NOT NULL,
      total_clicks BIGINT NOT NULL, reached_at TIMESTAMPTZ DEFAULT now()
      UNIQUE(short_code, milestone)
    - processed_events table: event_id VARCHAR(64) PK, processed_at TIMESTAMPTZ DEFAULT now()
    - indexes: idx_clicks_short_code_clicked (short_code, clicked_at DESC),
      idx_clicks_short_code_referer, idx_milestones_short_code

[ ] stores:
    - ClickStore: InsertClick(ctx, tx, click), CountByCode(ctx, code) int64,
      CountByCodeSince(ctx, code, since) int64, TopReferers(ctx, code, limit) []RefererCount,
      TimelineBuckets(ctx, code, granularity, since) []Bucket
    - MilestoneStore: HasMilestone(ctx, tx, code, milestone) bool, InsertMilestone(ctx, tx, milestone)
    - DeduplicationStore: Exists(ctx, tx, eventID) bool, Insert(ctx, tx, eventID)

[ ] haship.go — hashIP(remoteAddr, salt) string -> SHA-256(remoteAddr + salt), return hex string
    Hàm này tồn tại trong codebase nhưng consumer KHÔNG gọi nó cho click events.
    url-service đã hash IP trước khi gửi event. Consumer dùng evt.IPHash trực tiếp.

[ ] publisher.go (AnalyticsPublisher):
    - Publish MilestoneReachedEvent trực tiếp lên AMQP channel (KHÔNG qua outbox)
    - routing key: "milestone.reached"

[ ] milestone.go (MilestoneChecker):
    - Thresholds: []int{10, 100, 1000}
    - CheckAndPublish(ctx, tx, shortCode, userID, userEmail, corrID):
      1. Query current click count (dùng tx.QueryRow để thấy click vừa insert)
      2. Với mỗi threshold: nếu count >= threshold && !HasMilestone -> InsertMilestone + Publish
    - userID và userEmail sẽ là "" (URLClickedEvent không mang user info)
    - Milestones table đảm bảo idempotent (UNIQUE constraint)
```

### Ngày 6-7: M4 consumer + handler
```
[ ] consumer.go (ClickConsumer):
    - Single goroutine, AMQP prefetch=1, manual ack
    - Flow cho mỗi message:
      1. Parse JSON body -> URLClickedEvent. Parse fail -> ack + log warning (KHÔNG nack)
      2. Check DeduplicationStore.Exists(event_id). Duplicate -> ack + discard
      3. BEGIN transaction:
         a. DeduplicationStore.Insert(event_id)
         b. ClickStore.InsertClick (dùng evt.IPHash trực tiếp, KHÔNG re-hash)
         c. MilestoneChecker.CheckAndPublish(ctx, tx, evt.ShortCode, "", "", corrID)
      4. COMMIT
      5. Ack message
    - Milestone publish fail -> log error, KHÔNG crash, KHÔNG nack (milestone row đã commit)
    - Panic recovery: log error, KHÔNG nack (tránh infinite requeue)
    - AMQP channel đóng -> log Warn, consumer paused, /health vẫn 200

[ ] handler.go (StatsHandler):
    - GET /stats/{code}: dùng errgroup chạy concurrent queries:
      g.Go -> CountByCode (total clicks)
      g.Go -> CountByCodeSince (24h)
      g.Go -> CountByCodeSince (7d)
      g.Go -> TopReferers (limit 10)
      Return 200 {short_code, total_clicks, clicks_last_24h, clicks_last_7d, top_referers}
    - GET /stats/{code}/timeline:
      Query param: ?granularity=day|hour&since=...
      Return 200 {short_code, interval, points: [{period, count}]}
    - Cả 2 endpoint đều public (KHÔNG cần JWT)
```

---

## Tuần 2

### Ngày 8-10: Hoàn thành M4
```
[ ] Mở rộng main.go — full wiring:
    - runMigrations(ctx, pool)
    - Khởi tạo stores, publisher, milestone checker, consumer, handler
    - Start consumer goroutine (go consumer.Run(ctx))
    - Đăng ký routes: GET /health, GET /stats/{code}, GET /stats/{code}/timeline
    - Graceful shutdown: cancel context -> consumer stops -> pool close -> mq close

[ ] analytics_test.go:
    - Dedup: process cùng event_id 2 lần -> click count = 1
    - Milestone: 10 clicks -> milestones row với milestone=10
    - Poison message: invalid JSON -> ack, không crash
    - Stats handler: mock store, verify JSON response shape

[ ] Bug fixes
```

Check M4:
- 10 URLClickedEvent cùng short_code -> click count = 10, milestone=10
- Duplicate event_id -> count không tăng
- GET /stats/{code} -> 200 với đúng counts
- GET /stats/{code}/timeline?granularity=day -> 200 với buckets

### Ngày 11-14: M5 — Notification service
```
[ ] migration.sql:
    - notifications table: id UUID PK, user_id UUID NOT NULL, event_type TEXT NOT NULL,
      payload JSONB NOT NULL, status TEXT NOT NULL DEFAULT 'sent',
      created_at TIMESTAMPTZ DEFAULT now(), sent_at TIMESTAMPTZ NULL
    - idx_notifications_user_created (user_id, created_at DESC)

[ ] store.go (pgxNotificationStore):
    - InsertNotification(ctx, rec):
      1. BEGIN tx
      2. INSERT với status='pending'
      3. mockEmail — log.Info("mock email sent", "to", userEmail, "type", eventType)
      4. UPDATE SET status='sent', sent_at=now() WHERE id=$1
      5. COMMIT
    - ListByUser(ctx, userID, afterID, limit) -> ([]Notification, nextCursor)
      Cursor-based pagination, newest-first

[ ] consumer.go (NotificationConsumer):
    - Single goroutine, prefetch=1, manual ack
    - 3 routing keys: url.created, url.deleted, milestone.reached
    - Flow: parse event -> map to notification -> InsertNotification -> ack

[ ] handler.go:
    - GET /notifications — JWT required
    - Extract user_id từ JWT claims
    - Query params: ?after=<uuid>&limit=20
    - Return 200 {notifications: [...], next_cursor: "uuid" | null}
    - notifications array KHÔNG BAO GIỜ null (empty array [])

[ ] errors.go + mở rộng main.go + notification_test.go
```

Check: URLCreatedEvent -> notification row status='sent'. GET /notifications + JWT -> 200.

---

## Tuần 3

```
[ ] Chạy e2e_test.sh, fix analytics + notification bugs
[ ] Full flow: shorten -> redirect 15 lần -> stats -> milestone -> notifications
[ ] Code cleanup, comments, demo rehearsal
```

---

## Anti-pattern guard

```bash
grep -r "url_db\|user_db\|notification_db" services/analytics-service/ && echo "FAIL" || echo "PASS"
grep -r "url_db\|user_db\|analytics_db" services/notification-service/ && echo "FAIL" || echo "PASS"
grep -r "user-service\|user_service\|8083" services/notification-service/ && echo "FAIL" || echo "PASS"
```
