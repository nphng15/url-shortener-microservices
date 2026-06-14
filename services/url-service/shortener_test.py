"""
Integration test script for the URL shortener microservices.

All requests go through the gateway on http://localhost:8080:

  POST /api/auth/register  → user-service /register
  POST /api/auth/login     → user-service /login
  POST /api/shorten        → url-service  /shorten   (JWT required)
  GET  /r/{code}           → url-service  /{code}    (redirect)
  POST /api/shorten-anon   → url-service  /shorten-anon

Usage:
    python shortener_test.py
"""

import sys
import uuid
import requests

# Force UTF-8 output so Unicode symbols print correctly on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# ── Base URLs ──────────────────────────────────────────────────────────────────
GATEWAY  = "http://localhost:8080"
URL_SVC  = "http://localhost:8081"   # direct – for anon shorten (no gateway route)
USER_SVC = "http://localhost:8083"   # direct – health check

# ── Helpers ────────────────────────────────────────────────────────────────────

def check_health(base: str, label: str) -> dict:
    resp = requests.get(f"{base}/health")
    resp.raise_for_status()
    data = resp.json()
    print(f"[health] {label}: {data}")
    return data


def register(email: str, password: str) -> dict:
    """POST /api/auth/register → gateway → user-service /register"""
    resp = requests.post(
        f"{GATEWAY}/api/auth/register",
        json={"email": email, "password": password},
        headers={"Content-Type": "application/json"},
    )
    resp.raise_for_status()
    return resp.json()


def login(email: str, password: str) -> str:
    """POST /api/auth/login → gateway → user-service /login. Returns JWT token."""
    resp = requests.post(
        f"{GATEWAY}/api/auth/login",
        json={"email": email, "password": password},
        headers={"Content-Type": "application/json"},
    )
    resp.raise_for_status()
    data = resp.json()
    token = data["token"]
    print(f"[login] token obtained, expires_at={data.get('expires_at')}")
    return token


def shorten_authenticated(token: str, url: str, expires_in_hours: int) -> dict:
    """POST /api/shorten → gateway → url-service /shorten (JWT required)."""
    resp = requests.post(
        f"{GATEWAY}/api/shorten",
        json={"url": url, "expires_in_hours": expires_in_hours},
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
    resp.raise_for_status()
    return resp.json()


def shorten_anon(url: str, expires_in_hours: int) -> dict:
    """POST /shorten-anon directly to url-service (no gateway route exists)."""
    resp = requests.post(
        f"{URL_SVC}/shorten-anon",
        json={"url": url, "expires_in_hours": expires_in_hours},
        headers={"Content-Type": "application/json"},
    )
    resp.raise_for_status()
    return resp.json()


def get_redirect_via_gateway(short_code: str) -> str:
    """GET /r/{code} → gateway → url-service /{code}. Returns Location header."""
    resp = requests.get(f"{GATEWAY}/r/{short_code}", allow_redirects=False)
    resp.raise_for_status()
    return resp.headers["Location"]


def get_redirect_direct(short_code: str) -> str:
    """GET /{code} directly from url-service. Returns Location header."""
    resp = requests.get(f"{URL_SVC}/{short_code}", allow_redirects=False)
    resp.raise_for_status()
    return resp.headers["Location"]


# ── Test flows ─────────────────────────────────────────────────────────────────

def test_health():
    print("\n=== Health Checks ===")
    check_health(GATEWAY,  "gateway")
    check_health(URL_SVC,  "url-service (direct)")
    check_health(USER_SVC, "user-service (direct)")


def test_authenticated_flow():
    """Register via gateway, log in via gateway, shorten via gateway, redirect via gateway."""
    print("\n=== Authenticated Flow (via gateway :8080) ===")

    email    = f"test-{uuid.uuid4().hex[:8]}@example.com"
    password = "supersecret123"
    target   = "https://www.google.com"

    # 1. Register
    reg = register(email, password)
    print(f"[register] user_id={reg['user_id']} email={reg['email']}")

    # 2. Login → JWT
    token = login(email, password)

    # 3. Shorten (authenticated, via gateway)
    result = shorten_authenticated(token, target, expires_in_hours=24)
    short_code = result["short_code"]
    print(f"[shorten]  short_code={short_code}")
    print(f"[shorten]  short_url={result.get('short_url')}")
    print(f"[shorten]  original_url={result.get('original_url')}")
    print(f"[shorten]  expires_at={result.get('expires_at')}")

    # 4. Redirect via gateway  GET /r/{code}
    location = get_redirect_via_gateway(short_code)
    assert location == target, f"Expected redirect to {target!r}, got {location!r}"
    print(f"[redirect] OK  {GATEWAY}/r/{short_code} → {location}")


def test_anonymous_flow():
    """Shorten anonymously (direct to url-service) and verify redirect via gateway."""
    print("\n=== Anonymous Flow ===")

    target = "https://www.python.org"

    result = shorten_anon(target, expires_in_hours=1)
    short_code = result["short_code"]
    print(f"[shorten-anon] short_code={short_code}")
    print(f"[shorten-anon] short_url={result.get('short_url')}")
    print(f"[shorten-anon] original_url={result.get('original_url')}")
    print(f"[shorten-anon] expires_at={result.get('expires_at')}")

    # Verify redirect via gateway
    location = get_redirect_via_gateway(short_code)
    assert location == target, f"Expected redirect to {target!r}, got {location!r}"
    print(f"[redirect]     OK  {GATEWAY}/r/{short_code} → {location}")


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        test_health()
        test_authenticated_flow()
        test_anonymous_flow()
        print("\n[PASS] All tests passed.")
    except requests.exceptions.HTTPError as e:
        print(f"\n[FAIL] HTTP error: {e.response.status_code} {e.response.text}")
    except requests.exceptions.ConnectionError as e:
        print(f"\n[FAIL] Connection error: {e}")
    except AssertionError as e:
        print(f"\n[FAIL] Assertion failed: {e}")
    except KeyError as e:
        print(f"\n[FAIL] Missing expected field in response: {e}")
    except Exception as e:
        print(f"\n[FAIL] Unexpected error: {e}")