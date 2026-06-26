# Load Testing & Circuit Breaker Demonstration

This guide explains how to run load tests using **k6** and monitor the system behavior (especially the Circuit Breaker states) using **Grafana** and **Prometheus**.

---

## 📋 Prerequisites

Before running the load tests, ensure you have the following installed on your machine:

1. **k6**: The load testing tool.
   - **Windows (winget)**: `winget install k6 --source winget`
   - **Windows (Chocolatey)**: `choco install k6`
   - **macOS (Homebrew)**: `brew install k6`
   - **Linux (Debian/Ubuntu)**: 
     ```bash
     sudo gpg --no-default-keyring --keyring /usr/share/keyrings/k6-archive-keyring.gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys C5AD19C74E50005C
     echo "deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main" | sudo tee /etc/apt/sources.list.d/k6.list
     sudo apt-get update
     sudo apt-get install k6
     ```

2. **Docker & Docker Compose**: To run the application stack along with Prometheus and Grafana.

---

## 🚀 Step-by-Step Guide

### Step 1: Start the application stack with monitoring
Start the microservices and monitoring stack (Prometheus and Grafana) in detached mode:
```bash
docker compose up -d --build
```
Verify all services are healthy by checking status or running the smoke test:
```bash
./scripts/smoke_test.sh
```

### Step 2: Run the k6 load test script
Execute the load test script located in `scripts/load_test.js`:
```bash
k6 run scripts/load_test.js
```
*Note: The script automatically spins up virtual users (VUs) aiming for up to 10,000 requests/second. It will register a test user, shorten a URL, and load test the redirect endpoint `/r/{shortCode}`.*

---

## 📊 Monitoring via Grafana

1. Open your browser and navigate to: **[http://localhost:3000](http://localhost:3000)**
2. Log in with the default credentials:
   - **Username**: `admin`
   - **Password**: `admin`
3. Open the **"Circuit Breaker Monitor"** dashboard (it is configured as the default home dashboard).

### Key Panels to Watch:
- **Circuit Breaker State**: Shows the current state of each service's breaker (`0` = CLOSED, `1` = HALF_OPEN, `2` = OPEN).
- **Requests/sec by Status Class**: Shows requests categorized by status code (`2xx`, `5xx`, and `circuit_open`).
- **Error Rate %**: Displays the percentage of errors over time.
- **CB Trips (total)**: The number of times the circuit breaker has tripped.

---

## ⚡ Simulating & Observing Circuit Breaker Transitions

To witness the Circuit Breaker trip from **CLOSED** to **OPEN**:

1. Start running the k6 load test:
   ```bash
   k6 run scripts/load_test.js
   ```
2. While the load test is running, stop the `url-service` container in another terminal to simulate a service failure:
   ```bash
   docker compose stop url-service
   ```
3. **Observe Grafana**:
   - The gateway will start failing to connect to `url-service`.
   - After **5 consecutive failures** (within a 10s window), the Circuit Breaker transitions from **CLOSED** (0, green) to **OPEN** (2, red).
   - Once the breaker is **OPEN**, the gateway immediately sheds load by rejecting requests with **`503 Service Unavailable (circuit open)`** without calling the downstream service. You will see a massive spike in **Rejected by CB** traffic in Grafana.
4. **Observe Recovery**:
   - Start the service back up:
     ```bash
     docker compose start url-service
     ```
   - After **30 seconds** (the open timeout window), the Circuit Breaker will transition to **HALF_OPEN** (1, yellow) and send probe requests.
   - Once successful probes are confirmed, the Circuit Breaker returns to **CLOSED** (0, green) and traffic flows normally.
