#!/bin/bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
EMAIL="thong-e2e-$(date +%s)@example.com"
PASSWORD="Password123!"

json_get() {
  python3 -c 'import json,sys; data=json.load(sys.stdin); print(data'"$1"')'
}

request() {
  local method="$1"
  local path="$2"
  local body="${3:-}"
  local token="${4:-}"
  local headers=(-H "Content-Type: application/json")
  if [ -n "$token" ]; then
    headers+=(-H "Authorization: Bearer $token")
  fi
  if [ -n "$body" ]; then
    curl -fsS -X "$method" "${BASE_URL}${path}" "${headers[@]}" -d "$body"
  else
    curl -fsS -X "$method" "${BASE_URL}${path}" "${headers[@]}"
  fi
}

status_code() {
  local method="$1"
  local path="$2"
  local token="${3:-}"
  local headers=(-o /dev/null -sS -w "%{http_code}")
  if [ -n "$token" ]; then
    headers+=(-H "Authorization: Bearer $token")
  fi
  curl -X "$method" "${BASE_URL}${path}" "${headers[@]}"
}

echo "1. register"
request POST /api/auth/register "{\"email\":\"${EMAIL}\",\"password\":\"${PASSWORD}\"}" >/tmp/url-shortener-register.json

echo "2. login"
request POST /api/auth/login "{\"email\":\"${EMAIL}\",\"password\":\"${PASSWORD}\"}" >/tmp/url-shortener-login.json
TOKEN=$(json_get '["token"]' </tmp/url-shortener-login.json)

echo "3. shorten"
request POST /api/shorten "{\"url\":\"https://example.com/e2e\",\"expires_in_hours\":24}" "$TOKEN" >/tmp/url-shortener-shorten.json
SHORT_CODE=$(json_get '["short_code"]' </tmp/url-shortener-shorten.json)

echo "4. redirect x15"
for i in $(seq 1 15); do
  code=$(curl -o /dev/null -sS -w "%{http_code}" "${BASE_URL}/r/${SHORT_CODE}")
  if [ "$code" != "301" ] && [ "$code" != "308" ]; then
    echo "redirect $i returned $code"
    exit 1
  fi
done

echo "5. wait for outbox and consumers"
sleep 5

echo "6. stats"
request GET "/api/stats/${SHORT_CODE}" >/tmp/url-shortener-stats.json
python3 - <<'PY'
import json
with open('/tmp/url-shortener-stats.json') as f:
    data = json.load(f)
if data.get('total_clicks', 0) < 15:
    raise SystemExit(f"expected at least 15 clicks, got {data.get('total_clicks')}")
PY

echo "7. notifications"
request GET /api/notifications "" "$TOKEN" >/tmp/url-shortener-notifications.json
python3 - <<'PY'
import json
with open('/tmp/url-shortener-notifications.json') as f:
    data = json.load(f)
items = data.get('notifications') or data.get('items') or []
if not items:
    raise SystemExit('expected at least one notification')
PY

echo "8. delete"
delete_code=$(status_code DELETE "/api/urls/${SHORT_CODE}" "$TOKEN")
if [ "$delete_code" != "204" ]; then
  echo "delete returned $delete_code"
  exit 1
fi

echo "9. deleted redirect returns 410"
gone_code=$(curl -o /dev/null -sS -w "%{http_code}" "${BASE_URL}/r/${SHORT_CODE}")
if [ "$gone_code" != "410" ]; then
  echo "deleted redirect returned $gone_code"
  exit 1
fi

echo "10. rate limit"
rate_limited=0
for i in $(seq 1 11); do
  code=$(curl -o /dev/null -sS -w "%{http_code}" -H "Authorization: Bearer ${TOKEN}" -H "Content-Type: application/json" -d '{"url":"https://example.com/rate","expires_in_hours":24}' "${BASE_URL}/api/shorten")
  if [ "$code" = "429" ]; then
    rate_limited=1
    break
  fi
done
if [ "$rate_limited" != "1" ]; then
  echo "expected shorten rate limit to return 429"
  exit 1
fi

echo "11. correlation header"
correlation=$(curl -sS -D - -o /dev/null "${BASE_URL}/health" | tr -d '\r' | awk -F': ' 'tolower($1)=="x-correlation-id" {print $2}')
if [ -z "$correlation" ]; then
  correlation=$(curl -sS -D - -o /dev/null "${BASE_URL}/api/stats/${SHORT_CODE}" | tr -d '\r' | awk -F': ' 'tolower($1)=="x-correlation-id" {print $2}')
fi
if [ -z "$correlation" ]; then
  echo "missing X-Correlation-ID header"
  exit 1
fi

echo "E2E passed"
