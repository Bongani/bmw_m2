# Design Decisions

## Architecture

```
Kafka (ad_clicks, page_views)
  → StreamConsumer (@KafkaListener, one thread per partition)
      → JoinEngine.processClick() or .processPageView()
          → WatermarkTracker  — drop late events, advance watermark
          → ClickStateStore   — store/query clicks per user
          → OutputSink        — write attributed result to SQLite
          → acknowledgment.acknowledge() — commit offset AFTER durable write
  JoinEngine @Scheduled(30s): evictOldClicks()
```

Key design decisions:

| Concern | Decision |
|---|---|
| Delivery guarantee | At-least-once + idempotent sink (`INSERT OR IGNORE` on `page_view_id`) |
| Offset commit | After `OutputSink.write()` only — safe to replay on crash |
| Concurrency | One thread per Kafka partition — preserves ordering, no global lock |
| State locking | Per-user `ReentrantLock` in `ClickStateStore` — fine-grained |
| Late events | Dropped and written to `late_events` table for monitoring |
| State eviction | `min(watermarks) - 30min - allowedLateness` — bounded memory |



## Component Flow

![Component Flow Diagram](diagram_drawio.png)

---

## Offset Commit Sequence (at-least-once guarantee)

```
  Kafka delivers record
        │
        ▼
  JoinEngine processes (state updated, pv buffered)
        │
        ▼
  emitSafePageViews() → OutputSink.write() ← durable to SQLite
        │
        ▼
  acknowledgment.acknowledge() ← offset committed
        │
        ▼
  CRASH HERE? → restart replays from last committed offset
               → INSERT OR IGNORE skips already-written rows ✅
```

On restart, watermark and page-view buffer are both lost (in-memory only). Kafka replays
all unacknowledged events from the last committed offset, rebuilding both from scratch.

---

## Key Design Decisions

| Concern | Decision | Reason |
|---|---|---|
| Offset commit timing | After `OutputSink.write()` returns | Guarantees at-least-once; no lost outputs on crash |
| Idempotent output | `page_view_id` PRIMARY KEY + `INSERT OR IGNORE` | Safe to replay; dedup at sink |
| Page view emit timing | Buffer until `watermark >= pv.eventTime + allowedLateness` | Prevents emitting with null attribution when the matching click hasn't arrived yet |
| Concurrency model | One thread per Kafka partition | Preserves per-partition ordering without explicit ordering locks |
| State locking | Per-user `ReentrantLock` in `ClickStateStore` | Fine-grained; avoids global lock bottleneck |
| Watermark direction | Monotonically increasing per partition | Prevents re-opening closed windows |
| Watermark contribution by event type | Clicks: `eventTime`; page views: `eventTime - allowedLateness` | See watermark design note below |
| State eviction cutoff | `min(all watermarks) - 30min - allowedLateness` | Bounded memory; safe to evict only what can never be attributed |
| Late event handling | Drop + write to `late_events` via `LateEventSink` | Explicit audit trail; beyond `allowedLateness` window |
| SQLite WAL mode | `PRAGMA journal_mode=WAL` on both sinks | Allows Grafana to read concurrently without "database is locked" errors |

---

## Watermark Design Note

### Problem

Using a single watermark that advances to `event.eventTime` for all event types creates a
conflict between two scenarios:

- **Scenario 2 (out-of-order click):** pv_2 arrives at 12:15 before its click (12:12).
  If pv_2 advances the watermark to 12:15, cutoff becomes 12:13, and click_2 at 12:12
  is rejected as late — wrong attribution.

- **Scenario 5 (late click):** click_5 has event_time 12:40 but arrives at processing
  time 12:50, after pv_5 (12:45). If page views don't advance the watermark, the watermark
  is still 12:35 (from click_4), cutoff is 12:33, and click_5 at 12:40 is accepted —
  breaking the late-event drop requirement.

### Alternative considered: per-event-type watermarks

I considered maintaining separate watermark maps keyed by `"event_type#partition"` —
one for `ad_click`, one for `page_view`. Clicks check and advance the click watermark;
page views check and advance the page_view watermark; the buffer flushes using the click
watermark only.

This correctly protects click_2 but breaks scenario 5: with no click arriving between
click_4 (12:35) and click_5 (12:40, arrives at 12:50), the click watermark never reaches
12:42, so click_5 is not rejected as late.

### Chosen approach: conservative page view watermark contribution

Page views contribute `eventTime - allowedLateness` to the single shared watermark instead
of `eventTime`. This shifts the page view's contribution back by exactly the lateness
window, which is the maximum gap a legitimate out-of-order click can arrive in.

**Watermark update formula:**

```
clicks:     watermark = max(current, click.eventTime)
page views: watermark = max(current, pv.eventTime - allowedLateness)
```

The `max(current, ...)` monotonic guarantee lives in `WatermarkTracker.updateWatermark()`.
The shift is applied at the call site in `JoinEngine.processPageView()`:

```java
// clicks
watermarkTracker.updateWatermark(partition, eventTime);

// page views
Instant conservativeWatermark = eventTime.minus(watermarkTracker.getAllowedLateness());
watermarkTracker.updateWatermark(partition, conservativeWatermark);
```

Effect on the two problem scenarios:

```
pv_2 at 12:15 → contributes max(current, 12:13) to watermark  → cutoff 12:11
click_2 at 12:12 → 12:12 > 12:11 → NOT late ✅

pv_5 at 12:45 → contributes max(current, 12:43) to watermark  → cutoff 12:41
click_5 at 12:40 → 12:40 < 12:41 → late, dropped ✅
```

No other scenario is affected. The watermark remains a single `ConcurrentHashMap<Integer, Instant>`.

---

## Spring Boot Startup

```
StreamProcessorApplication.main()
│
▼
SpringApplication.run()
│
├── reads application.yml (kafka URLs, topic names, concurrency, lateness config)
├── creates all @Component/@Configuration beans
│     OutputSink.init()      → opens SQLite connection, creates attributed_page_views table
│     LateEventSink.init()   → opens SQLite connection, creates late_events table
└── starts the Kafka listener containers automatically
```

Spring Boot auto-scans everything annotated with `@Component`, `@Configuration`, and
`@Bean` and wires them together via dependency injection (`@RequiredArgsConstructor`).

---

## Request Flow (once running)

```
Kafka broker
│
│  delivers ConsumerRecord<String, String>
▼
StreamConsumer.consumeAdClick()   or   StreamConsumer.consumePageView()
│
│  called by Spring's ConcurrentMessageListenerContainer
│  one thread per partition (concurrency=3 in yml)
│  partition ordering preserved within each thread
│
▼
objectMapper.readValue(record.value(), AdClickEvent.class)
│
▼
event.setPartition(record.partition())
event.setOffset(record.offset())
│
▼
joinEngine.processClick(event)   or   joinEngine.processPageView(event)
│
▼
acknowledgment.acknowledge()
  tells Kafka: "I have durably processed up to this offset"
  Spring sends the actual commit on the next poll cycle
```

---

## Manual Acknowledgment — Why It Matters

`KafkaConsumerConfig` sets `AckMode.MANUAL` and `ENABLE_AUTO_COMMIT=false`:

```
Default (auto-commit):          This project:
  receive record                  receive record
  auto-commit offset ← too early  process it
  process it                       write to SQLite
  crash → record LOST ❌           acknowledge() ← safe
                                   crash → replay from last ack ✅
```

---

## Scheduled Eviction

`@EnableScheduling` activates Spring's task scheduler. `JoinEngine` runs eviction every
30 seconds in a background thread, independent of Kafka processing:

```java
@Scheduled(fixedRate = 30000)
public void evictOldClicks()
```

Eviction cutoff: `min(all partition watermarks) - attributionWindow - allowedLateness`.
This ensures no click that could still be attributed to a buffered page view is removed.