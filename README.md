# BMW M2

A real-time stream processor that joins `page_view` and `ad_click` Kafka events using a 30-minute event-time attribution
window. For each page view, it finds the most recent ad click for the same user within 30 minutes prior (event time) 
and emits an attributed record to SQLite.

---

## Design Documentation

- [DESIGN.md](docs/DESIGN.md) — architecture diagram, key design decisions and rationale.
- [SCENARIOS.md](docs/SCENARIOS.md) — example cases of how the processor handles different event arrival patterns and edge cases.
- [DEVELOPMENTNOTES.md](docs/DEVELOPMENTNOTES.md) — rough notes on the development process, challenges faced, and ideas for future improvements.
---

## Prerequisites

- Docker + Colima (or OrbStack / Docker Desktop)
- Java 21 (for running outside of Docker)
- Maven (for running outside of Docker)
- Python 3.11+ with `uv` (for data generator outside of Docker)

---

## Project structure

Highlevel overview of the codebase structure:

```
bmw_m2cs/
├── src/
│   ├── main/java/.../streamprocessor/         # Spring Boot app with Kafka consumer, join engine, state store, output sink
│   └── test/java/.../streamprocessor/         # unit tests
├── datagenerator/                             # Data generator that sends the 6 test scenarios to Kafka
├── loadtesting/                               # k6 load test simulating
├── monitoring/                                # Prometheus config + Grafana dashboards
│   ├── prometheus.yml
│   └── grafana/provisioning/
│       ├── datasources/                       # Prometheus + SQLite datasources (auto-provisioned)
│       └── dashboards/                        # Attribution dashboard + Spring Boot templates
├── docs/                                      # Documentation and design artifacts
│   ├── DESIGN.md                              # Architecture diagram and key design decisions
│   ├── SCENARIOS.md                           # Visual timeline of all 6 test scenarios
├── docker-compose.yml                         # Full stack: Kafka, processor, Prometheus, Grafana
├── Dockerfile                                 # Stream processor image
├── run.sh                                     # Creates output/logs dirs and starts all services
├── test-setup.sh                              # Starts up services needed to run for development testing (i.e. Kafka)
├── docker-down.sh                             # Stops all docker compose services gracefully
└── pom.xml
```

---

## Running with Docker (recommended)

Start the full stack — Zookeeper, Kafka, Kafka UI, and the stream processor:

```bash
mkdir -p output logs
docker-compose up -d
```

The stream processor will start automatically once Kafka is healthy.

**Verify environment before starting:**
```bash
./test-setup.sh
```

**Start all services:**
```bash
./run.sh
```

**Stop all services gracefully:**
```bash
./docker-down.sh
```

**View logs:**
```bash
docker-compose logs -f bmw-m2
```

**Kafka UI:** http://localhost:8080

---

## Running on host (development)

```bash
# Terminal 1 — start Kafka
docker-compose up -d zookeeper kafka kafka-ui

# Terminal 2 — start stream processor
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
mvn spring-boot:run
```

---

## Generating test data

```bash
cd datagenerator
uv sync
uv run python data_generator.py
```

The generator sends events covering all 6 test scenarios: normal attribution, out-of-order clicks, multiple clicks, window miss, late events, and no-click.

**From Docker:**
```bash
# Build the image
docker build -t bmw-m2-generator ./datagenerator

# Run against Dockerised Kafka (full Docker stack)
docker run --network bmw_m2cs_challenge-network bmw-m2-generator

# Run against host Kafka (when running Kafka locally on Mac)
# docker run --rm -e KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:9092 bmw-m2-generator
```

---

## View Tables

You can view the tables using the `sqlite3` CLI tool or any SQLite viewer (such as in your IDE). 
The database file is at `output/attributed_page_views.db`.

Query the SQLite output database:

```bash
sqlite3 output/attributed_page_views.db "SELECT * FROM attributed_page_views;"
sqlite3 output/attributed_page_views.db "SELECT * FROM late_events;"
```

Expected results if ran against the provided data generator (data_generator.py):

| page_view_id | user | attributed_campaign | reason |
|---|---|---|---|
| pv_1 | user_1 | campaign_A | normal: click 5min before |
| pv_2 | user_2 | campaign_B | out-of-order: click arrives after page view |
| pv_3 | user_3 | campaign_D | multiple clicks: picks latest |
| pv_4 | user_4 | null | window miss: click is 35min before page view |
| pv_5 | user_5 | null | late click: arrives beyond 2min allowed lateness |
| pv_6 | user_6 | null | no click exists for user |

---

## Monitoring

| Service | URL | Credentials |
|---|---|---|
| Prometheus | http://localhost:9090 | — |
| Grafana | http://localhost:3000 | `admin` / `admin` |

Prometheus scrapes the stream processor every 15s. Grafana has Prometheus pre-configured as the default datasource — no setup required on first login.

**Pre-built dashboards** are included in `monitoring/grafana/provisioning/dashboards/` and load automatically into Grafana under the **BMW M2** folder on startup:

| File | What it shows |
|---|---|
| `attribution_dashboard.json` | Real-time attribution results read directly from SQLite |
| `spring_boot_statistics_template.json` | HTTP requests, response times, uptime |
| `spring_boot_kafka_listner_template.json` | Kafka consumer lag, listener throughput |

### Attribution Dashboard

The **Attribution Dashboard** reads live from `output/attributed_page_views.db` via the [frser-sqlite-datasource](https://grafana.com/grafana/plugins/frser-sqlite-datasource/) plugin and auto-refreshes every 5 seconds.

| Panel | Description |
|---|---|
| Total Attributed Page Views | Page views where a matching ad click was found within the 30-minute window (`attributed_campaign_id IS NOT NULL`) |
| Total Unattributed Page Views | Page views with no matching click — window miss, late event, or no click for user |
| Total Late Events Dropped | Events that arrived beyond the allowed 2-minute lateness threshold |
| Attribution Rate | Percentage of page views successfully attributed to a campaign |
| Attributions by Campaign | Breakdown of matched page views per campaign |
| Late Events by Type | Late drops split by `ad_click` vs `page_view` |
| Recent Attributions | Last 50 rows from SQLite, newest first |

SQLite WAL mode is enabled on the Java side so Grafana can read concurrently without "database is locked" errors.

**Importing additional community dashboards:**
1. Open http://localhost:3000 and log in
2. Click **Dashboards** → **New** → **Import**
3. Enter a dashboard ID and click **Load**
4. Select **Prometheus** as the datasource → **Import**

| Dashboard | ID | What it shows |
|---|---|---|
| JVM (Micrometer) | `4701` | Heap, GC pauses, threads, CPU |
| Spring Boot 3.x | `19004` | Updated version for Spring Boot 3+ |

Useful Prometheus queries:
```
events_clicks_processed_total{application="bmw-m2"}
attributions_matched_total{application="bmw-m2"}
attributions_unmatched_total{application="bmw-m2"}
events_late_dropped_total{application="bmw-m2"}
state_clicks_evicted_total{application="bmw-m2"}
```

---

## Metrics

The application exposes Spring Boot Actuator on port **8081**:

| URL | Description |
|---|---|
| `http://localhost:8081/actuator/health` | Application health |
| `http://localhost:8081/actuator/metrics` | All metric names |
| `http://localhost:8081/actuator/metrics/{name}` | Single metric value |

Custom stream processor metrics:

| Metric | Description |
|---|---|
| `events.clicks.processed` | Ad click events accepted into state |
| `events.pageviews.processed` | Page view events accepted into buffer |
| `events.late.dropped` | Events dropped for exceeding allowed lateness (tag: `type=ad_click\|page_view`) |
| `attributions.matched` | Page views successfully attributed to a campaign |
| `attributions.unmatched` | Page views emitted with no matching click |
| `state.clicks.evicted` | Clicks removed from state by the eviction scheduler |

### Prometheus examples

In the Prometheus UI at http://localhost:9090, go to Graph and run this query:

JVM CPU usage (the stream processor process):                                                                                                                                                                                    
process_cpu_usage{application="bmw-m2"}

System-wide CPU usage on the host:                                                                                                                                                                                               
system_cpu_usage{application="bmw-m2"}

Number of available CPU cores:                                                                                                                                                                                                   
system_cpu_count{application="bmw-m2"}

These come free from Micrometer/Actuator — no extra configuration needed. process_cpu_usage is the most useful one: it shows what fraction of CPU the JVM is consuming (0.0 to 1.0).

To see it as a percentage, multiply by 100:                                                                                                                                                                                      
process_cpu_usage{application="bmw-m2"} * 100

Switch to the Graph tab (not Table) to see it over time.

---

## Running tests

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
mvn test
```

---

## Configuration

Key settings in `src/main/resources/application.yml`:

| Property | Default | Description |
|---|---|---|
| `kafka.bootstrap-servers` | `localhost:9092` | Kafka broker address |
| `watermark.allowed-lateness-minutes` | `2` | How late an event can arrive |
| `watermark.attribution-window-minutes` | `30` | Attribution lookback window |
| `output.database.path` | `./output/attributed_page_views.db` | SQLite output path |

When running in Docker, `KAFKA_BOOTSTRAP_SERVERS=kafka:29092` is set automatically via `docker-compose.yml`.

---

## Load Testing

Load tests use [k6](https://k6.io/) with the [xk6-kafka](https://github.com/mostafamoradian/xk6-kafka) extension (`mostafamoradian/xk6-kafka`). Scripts live in `loadtesting/`.

The load test produces a realistic mix of events — 1 ad click followed by 2–3 page views per user — across 50 simulated users with a ramp-up, steady state, spike, and ramp-down:

| Stage | Duration | VUs |
|---|---|---|
| Ramp up | 30s | 0 → 5 |
| Steady state | 2m | 5 |
| Spike | 30s | 5 → 20 |
| Sustain spike | 1m | 20 |
| Ramp down | 20s | 20 → 0 |

Total: 4 minutes 20 seconds (plus up to 30s graceful stop, so at most ~4m50s).

k6 is excluded from the default stack and must be started explicitly:

```bash
# Start the full stack first
docker-compose up -d

# Then run the load test
docker-compose --profile loadtest up k6

# If network issue
docker-compose --profile loadtest down && docker network prune -f 2>&1
```

Watch the effect in Grafana (`http://localhost:3000`) — attribution rate, late events, JVM heap, and Kafka consumer lag 
all update in real time during the test.
