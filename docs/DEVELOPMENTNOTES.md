## Other design decisions to think about:

Technology choices:
Springboot + Kafka client + SQLite is a simple stack that gets the job done for a demo. In production, you might choose:
- Kafka Streams, Flink, Apache Streams (version 4.1) for built-in state management, windowing, and fault tolerance
  A lot of issues regarding scalability, reliability, and operational complexity can be offloaded to the framework.
  But in this case we wanted to implement something quick. If I were to go ahead put this into production,
  here are some of the things I would change:

#### Scalability
- Replace SQLite with a distributed sink (Cassandra, PostgreSQL, or a data warehouse) to handle higher throughput and larger data volumes
    - SQLite is single-writer and won't scale horizontally
    - Have already experienced issues with SQLite needing WAL to be enabled to avoid "database is locked"
- Replace in-memory ClickStateStore with an external state store (Redis, RocksDB) so multiple processor instances can share state
- Partition ad_clicks and page_views by user_id (or some kind of distributable hash)
    - this guarantees all events for a user land on the same partition, eliminating cross-partition state lookups entirely
    - though might have consquences for load balancing if some users are much more active than others

#### Fault tolerance
- Having one application instance means no failover if it crashes. In production, you'd run multiple instances behind a
  load balancer, and use an external state store so they can share state and pick up where each other left off
  (something similar to YARN or Zookeeper for leader election and partition assignment)
- On restart, the processor replays Kafka from the last committed offset but the in-memory click state is gone.
  Clicks between the last eviction and the crash are lost, causing missed attributions. Fix: snapshot state to a  
  durable store periodically, or rebuild from a compacted Kafka topic on startup

#### Out-of-order handling
- The 2-minute allowed lateness is a fixed tradeoff — too small means dropping valid events, too large means holding
  state longer. In production this would be tuned based on actual observed event delay distributions
- Consider a separate dead-letter topic for late events rather than only writing to SQLite — lets you reprocess them if the lateness window is adjusted


#### Operational
- A dead-letter topic for any events that fail processing due to transient errors
    - Spring Kafka automatically registers a DefaultErrorHandler with a FixedBackOff(0, 9) — 0ms interval, 9 retries (10 total   
      attempts) — when no custom error handler is configured
- Kafka consumer lag is the key SLA metric — if lag grows, the processor is falling behind and watermarks stop advancing, which freezes state eviction and grows memory unboundedly
- Add alerting on kafka_reader_lag and events.late.dropped rate — a spike in late drops usually means a producer is slow or a partition is skewed
- The eviction scheduler runs every 30 seconds on a fixed interval — in a high-throughput system this could cause GC pressure spikes; consider evicting incrementally on each processPageView call instead

#### DDoS and botting
- If a user is generating an extremely high volume of clicks (e.g. a bot), the ClickStateStore for that user could grow
  unbounded until eviction. In production, you might want to add a per-user click count cap to prevent memory exhaustion
  from malicious actors.
- Implement rate limiting or anomaly detection on the producer side to prevent bots from overwhelming the system with
  fake events.
- Implement authentication and authorization for Kafka and the database to prevent unauthorized access and data tampering
- Implement monitoring and alerting for unusual spikes in traffic or error rates that could indicate a DDoS attack or bot activity
- Could be feed fake events into the system to throw off attribution (e.g. a seller trying to make it look like their
  ads are getting more clicks than they really are) — monitoring for unusual patterns of clicks and page views could help detect this

#### Security
- Kafka SASL/TLS authentication — the current config uses plaintext
- Database credentials and bootstrap server addresses should come from a secrets manager (Vault, AWS Secrets Manager), not environment variables in docker-compose

#### Multi-tenancy / correctness edge cases
- What if a user has thousands of clicks in the window? The TreeSet per user is unbounded until eviction — add a per-user click count cap
- Clock skew between producers can cause events from one producer to appear late relative to another — a small allowedLateness buffer helps but doesn't fully solve it


## checkpointing
Do we have or need checkpointing. When the application comes backup after crashing how does it know where to pickup from?

#### What is already protected

- Kafka offsets are committed only after OutputSink.write() succeeds (AckMode.MANUAL). On restart, the consumer resumes 
from the last committed offset and replays any unwritten events.
- INSERT OR IGNORE on page_view_id means replayed events that were already written are silently skipped — no duplicates.
#### What is lost on crash
- ClickStateStore — the in-memory map of clicks per user is gone.
- WatermarkTracker — watermark resets to Instant.MIN.
- pageViewBuffer — any buffered page views waiting for watermark to advance are lost.

#### Consequence
If the app crashes after committing click offsets but before the corresponding page views arrive, those clicks won't be 
in the store when the page views are replayed. Attribution would return null instead of the correct campaign.

For a 30-minute attribution window, a crash mid-stream is a real correctness risk. The standard fix is state rebuild on 
startup: before starting consumers, replay the ad_clicks topic from the beginning up to the committed    
offset to repopulate ClickStateStore. Since clicks are keyed by user_id, this is a single sequential scan. The eviction 
logic then prunes anything older than the window.

## Watermarking

What is the watermark rule used for this application?

From WatermarkTracker the rule is:

An event is late if: eventTime < watermark - allowedLateness

Where:
- Watermark — the maximum event time seen so far on a given partition (monotonically increasing, never goes backward)
- Allowed lateness — 2 minutes (configurable via watermark.allowed-lateness-minutes)

So if the watermark is 12:30:00, any event with eventTime < 12:28:00 is dropped as late.

State eviction cutoff uses a stricter rule:                                                                                                                                                                                      
min(all partition watermarks) - 30min - allowedLateness

This ensures clicks are only evicted from memory once no future page view from any partition could possibly still attribute to them.



---
## JUNK NOTES

docker-compose up -d --build bmw-m2
- Running from docker need to persist data in a volume, otherwise we lose all data when the container stops.
- make note kafka ui on README
- make note of metrics ui README

Document to keep notes and questions while implementing

Command to rebuild with docker-compose:
```bash
docker-compose up -d --build bmw-m2
```

#TODO: events with null timstamps should automatically go to the dead letter topic, and not cause the processor to crash
with a NullPointerException but for some reason JSONException is not being thrown

## Known bug: integer event_time bypasses DLT routing

**Observed:** `bad_click_1` (sent with `event_time: 99999` — an integer instead of a string)
lands in `late_events` instead of `ad_clicks.DLT`.

**Root cause:** `JavaTimeModule`'s `InstantDeserializer` silently accepts integers and treats
them as epoch seconds. So `99999` is parsed as `1970-01-02T03:46:39Z` — no
`JsonProcessingException` is thrown, the event passes deserialization, and is later
dropped as late (watermark is far past 1970).

**Expected behaviour:** the integer token should cause a `MismatchedInputException`
(subclass of `JsonProcessingException`), which the `DefaultErrorHandler` would route
straight to `ad_clicks.DLT` without retrying.

**Fix (deferred):** configure coercion on the `ObjectMapper` in `KafkaConsumerConfig`:
```java
mapper.coercionConfigFor(LogicalType.DateTime)
      .setCoercion(CoercionInputShape.Integer, CoercionAction.Fail);
```
Imports needed: `com.fasterxml.jackson.databind.cfg.CoercionAction`,
`CoercionInputShape`, `com.fasterxml.jackson.databind.type.LogicalType`.
This makes Jackson throw on any integer-to-DateTime coercion, routing the event to DLT
as intended. A unit test should verify `objectMapper().readValue(...)` throws
`JsonProcessingException` for `{"event_time": 99999, ...}`.

- to kafka data will be sent randomly unless we specify the partition key
  -> what happens if there is no events in a partion, then we don't get watermrks. Howeversince we are going randm
  or round robbin we should be getting watermarks from all the partitions. If we are not getting watermarks from a partition then it means there is no data in that partition.

- what happens if no events come in after awhile? Should we flush
  -correctness of data (can assume data is always correct in terms of format and values)
    -
- what happens repartitioning happens
- What happen if database fails?
  -scalability, availability, reliability,partitioning, replication
  Does hashmap need to be initiated
  -github action
  -containirization
  -load testing
---
Flushing for stale events stuck in buffer:

Since pv_6 is the last event the generator sends, no subsequent event ever arrives to push the watermark past 13:22:00. The buffer flush never fires. pv_6 sits in memory until the app stops — and is never written to SQLite.

This also explains the order you see: pv_4 is emitted by pv_6's arrival, not its own. When pv_6 is processed, the watermark jumps to 13:20:00, and emitSafePageViews finds pv_4 (safe-emit = 13:12:00) is now unblocked and      
flushes it.
                                                                                                                                                                                                                                   
---                                                                                                                                                                                                                              
The fix: idle flush in the scheduled task.

The @Scheduled eviction task already runs every 30s. We can extend it to also flush buffered page views using wall-clock time as a stand-in watermark when no real events are arriving:

The rule would be: if Instant.now() >= pv.eventTime + allowedLateness, it's safe to emit — the real world has moved past the lateness window even if no Kafka event has arrived.

----


                                                                                     
                                                                                                                                        