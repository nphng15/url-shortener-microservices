/**
 * k6 Load Test – Circuit Breaker Demo
 * =====================================
 * Target: 10,000 req/s through the Gateway
 * Endpoint: GET /r/{shortCode}  (public, no auth needed)
 *
 * Usage:
 *   # Basic run (outputs to terminal)
 *   k6 run scripts/load_test.js
 *
 *   # Override the short code
 *   k6 run -e SHORT_CODE=abc123 scripts/load_test.js
 *
 *   # To trigger CB immediately, stop url-service while running:
 *   docker compose stop url-service
 */

import http from "k6/http";
import { check, sleep } from "k6";
import { Counter, Rate, Trend } from "k6/metrics";

// ── Custom Metrics ────────────────────────────────────────────────────────────
const cbOpenResponses = new Counter("circuit_breaker_open_responses");
const errorRate = new Rate("error_rate");
const redirectDuration = new Trend("redirect_duration_ms", true);

// ── Config ────────────────────────────────────────────────────────────────────
const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";

// Short code to redirect – override with -e SHORT_CODE=<your_code>
// The setup() function will create a real URL if SHORT_CODE is not provided.
const SHORT_CODE = __ENV.SHORT_CODE || "loadtest";

// ── Load Profile ──────────────────────────────────────────────────────────────
// 1,000 VUs × ~10 iterations/s per VU ≈ 10,000 req/s
export const options = {
  scenarios: {
    circuit_breaker_stress: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: "20s", target: 200 },   // Warm-up: ramp to 200 VUs
        { duration: "20s", target: 500 },   // Build up
        { duration: "20s", target: 1000 },  // Target: ~10k req/s
        { duration: "60s", target: 1000 },  // Hold at peak – watch CB trip!
        { duration: "20s", target: 0 },     // Ramp down
      ],
      gracefulRampDown: "10s",
    },
  },
  thresholds: {
    // We expect errors during CB demo – relax thresholds
    http_req_duration: ["p(95)<2000"],   // 95% of requests under 2s
    error_rate: ["rate<0.95"],           // Allow up to 95% error during CB OPEN
  },
  // Stream summary to stdout every 10s
  summaryTrendStats: ["avg", "min", "med", "max", "p(90)", "p(95)", "p(99)"],
};

// ── Setup: Create a test short URL ───────────────────────────────────────────
export function setup() {
  // If SHORT_CODE env var was given, skip setup
  if (__ENV.SHORT_CODE) {
    console.log(`Using provided SHORT_CODE: ${__ENV.SHORT_CODE}`);
    return { shortCode: __ENV.SHORT_CODE };
  }

  // 1. Register a test user
  const email = `loadtest_${Date.now()}@example.com`;
  const password = "Loadtest123!";

  const registerRes = http.post(
    `${BASE_URL}/api/auth/register`,
    JSON.stringify({ email, password }),
    { headers: { "Content-Type": "application/json" } }
  );

  if (registerRes.status !== 201 && registerRes.status !== 200) {
    console.warn(`Registration failed (${registerRes.status}): ${registerRes.body}`);
  }

  // 2. Login to get a JWT
  const loginRes = http.post(
    `${BASE_URL}/api/auth/login`,
    JSON.stringify({ email, password }),
    { headers: { "Content-Type": "application/json" } }
  );

  if (loginRes.status !== 200) {
    console.warn(`Login failed (${loginRes.status}): ${loginRes.body}`);
    return { shortCode: SHORT_CODE };
  }

  let token = "";
  try {
    token = JSON.parse(loginRes.body).token;
  } catch (e) {
    console.warn("Could not parse login token, using default SHORT_CODE");
    return { shortCode: SHORT_CODE };
  }

  // 3. Create a short URL
  const shortenRes = http.post(
    `${BASE_URL}/api/shorten`,
    JSON.stringify({ url: "https://www.google.com" }),
    {
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${token}`,
      },
    }
  );

  if (shortenRes.status !== 201 && shortenRes.status !== 200) {
    console.warn(`Shorten failed (${shortenRes.status}): ${shortenRes.body}`);
    return { shortCode: SHORT_CODE };
  }

  let code = SHORT_CODE;
  try {
    const body = JSON.parse(shortenRes.body);
    // Try common response shapes
    code =
      body.short_code ||
      body.shortCode ||
      body.code ||
      (body.short_url && body.short_url.split("/").pop()) ||
      SHORT_CODE;
  } catch (e) {
    console.warn("Could not parse short code from response, using default");
  }

  console.log(`✅ Setup complete. Short code: "${code}"`);
  console.log(`   Load test URL: ${BASE_URL}/r/${code}`);
  console.log(`\n💡 TIP: Run this in another terminal to trigger CB immediately:`);
  console.log(`   docker compose stop url-service\n`);

  return { shortCode: code };
}

// ── Main VU Function ──────────────────────────────────────────────────────────
export default function (data) {
  const shortCode = (data && data.shortCode) || SHORT_CODE;
  const url = `${BASE_URL}/r/${shortCode}`;

  const res = http.get(url, {
    // Don't follow redirects – we're testing the gateway, not the target URL
    redirects: 0,
    timeout: "5s",
    tags: { name: "redirect" },
  });

  const start = Date.now();

  // Track CB-open responses (503 with specific message)
  if (res.status === 503) {
    cbOpenResponses.add(1);
  }

  // Track errors
  const isError =
    res.status === 0 || (res.status >= 500 && res.status !== 503) || res.status === 503;
  errorRate.add(isError ? 1 : 0);

  // Track duration
  redirectDuration.add(res.timings.duration);

  check(res, {
    "status is 2xx, 3xx, or 503 (CB open)": (r) =>
      (r.status >= 200 && r.status < 400) || r.status === 503,
  });

  // Tiny think time to be realistic (1ms)
  sleep(0.001);
}

// ── Teardown ──────────────────────────────────────────────────────────────────
export function teardown(data) {
  console.log("\n═══════════════════════════════════════════");
  console.log("  Load Test Complete!");
  console.log(`  Short code tested: /r/${(data && data.shortCode) || SHORT_CODE}`);
  console.log("  Open Grafana at: http://localhost:3000");
  console.log('  Dashboard: "Circuit Breaker Monitor"');
  console.log("═══════════════════════════════════════════\n");
}

// ── Custom Summary ────────────────────────────────────────────────────────────
export function handleSummary(data) {
  const totalReqs = data.metrics.http_reqs ? data.metrics.http_reqs.values.count : 0;
  const duration = data.state.testRunDurationMs / 1000;
  const rps = totalReqs / duration;

  const cbOpen = data.metrics.circuit_breaker_open_responses
    ? data.metrics.circuit_breaker_open_responses.values.count
    : 0;

  const p95 = data.metrics.http_req_duration
    ? data.metrics.http_req_duration.values["p(95)"]
    : 0;

  const summary = `
╔══════════════════════════════════════════════════════╗
║           k6 Load Test Summary                       ║
╠══════════════════════════════════════════════════════╣
║  Total Requests   : ${String(totalReqs).padEnd(30)}║
║  Duration         : ${String(duration.toFixed(1) + "s").padEnd(30)}║
║  Avg Throughput   : ${String(rps.toFixed(0) + " req/s").padEnd(30)}║
║  CB Open (503)    : ${String(cbOpen).padEnd(30)}║
║  P95 Latency      : ${String(p95.toFixed(0) + "ms").padEnd(30)}║
╚══════════════════════════════════════════════════════╝

→ Grafana Dashboard: http://localhost:3000
`;

  return {
    stdout: summary,
  };
}
