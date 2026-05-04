# Scenarios

These scenarios illustrate how the system handles various e cases around event timing, watermarking, and attribution logic. 
Each scenario is designed to test a specific aspect of the implementation, such as late arrivals, multiple clicks, and window boundaries.

The majority of the cases are covered in the datagenertor[data_generator.py](../datagenerator/data_generator.py) implementation.

## Event Time Timeline (minutes from base_time 12:00:00)

```
         0    5   10   12   15   16   20   25   30   35   40   45   50   70   80
         │    │    │    │    │    │    │    │    │    │    │    │    │    │    │

USER 1   │  [C1]──────▶[PV1]                                                  ✅ ATTRIBUTED
(normal) │  click_1   page_view                                                campaign_A
         │  +5min      +10min
         │  └──5 min gap──┘ ← within 30-min window

USER 2   │                  [PV2]  [C2]                                        ✅ ATTRIBUTED
(late    │                  +15m   +12m (event_time)                           campaign_B
 click)  │                         but arrives at +16m (processing time)
         │                  PV arrives first ↑  click arrives second
         │                  pv_2 buffered until watermark ≥ 12:17 (12:15+2min)
         │                  click_2 stored at +16m → pv_2 emits correctly

USER 3   │                         [C3a]     [C3b]     [PV3]                  ✅ ATTRIBUTED
(multi   │                         +20m      +25m      +30m                   campaign_D
 click)  │                          └────────────────────┘ both in window     (latest click)
         │                                    ↑ pick this one (more recent)

USER 4   │                                   [C4]                   [PV4]     ❌ NO ATTRIBUTION
(window  │                                   +35m                   +70m      click too old
 miss)   │                                    └──────35 min gap──────┘
         │                                          > 30-min window

USER 5   │                                         [C5]──event_time──[PV5]    ❌ DROPPED
(too     │                                         +40m              +45m     click arrives
 late)   │                                              arrives at +50m ──▶   at +50m, watermark
         │                                              watermark already      already at 12:45
         │                                              past +47m (45+2)       cutoff = 12:43

USER 6   │                                                                [PV6] ❌ NO ATTRIBUTION
(no      │                                                                +80m  no click ever
 click)  │                                                                      existed
```

---

## Processing Time Order (what actually arrives at the Kafka consumer)

```
  +5:01  ad_click    user_1  click_1   campaign_A   ← normal click
  +10:02 page_view   user_1  pv_1                   ← normal page view
  +15:01 page_view   user_2  pv_2                   ← page view arrives BEFORE its click
  +16:00 ad_click    user_2  click_2   campaign_B   ← click arrives AFTER page view ⚠️
  +20:01 ad_click    user_3  click_3a  campaign_C
  +25:01 ad_click    user_3  click_3b  campaign_D
  +30:02 page_view   user_3  pv_3
  +35:01 ad_click    user_4  click_4   campaign_E
  +45:02 page_view   user_5  pv_5                   ← page view before late click
  +50:00 ad_click    user_5  click_5   campaign_F   ← arrives 10 min late, beyond lateness ⚠️
  +70:02 page_view   user_4  pv_4                   ← 35min after its click, too old
  +80:01 page_view   user_6  pv_6                   ← no click ever
```

---

## Watermark Behaviour (partition 0, allowedLateness = 2 min)

```
  After processing pv_5  → watermark advances to 12:45
  click_5 arrives        → isTooLate? event_time=12:40, cutoff=12:43 → YES, DROPPED ❌
                                                      (watermark - lateness = 12:45 - 2min)
```

---

## Page View Buffer (emit-on-watermark)

A page view is buffered on arrival and only emitted once the watermark has advanced far
enough that no late click can still arrive and change its attribution.

**Safe-emit rule:** emit pv when `watermark >= pv.eventTime + allowedLateness`

This is why pv_2 gets correct attribution despite click_2 arriving a minute after it:

```
  +15:01  processPageView(pv_2 @ 12:15)
            buffer pv_2 (safe-emit time = 12:15 + 2min = 12:17)
            updateWatermark(12:15)
            flushBuffer: watermark 12:15 < 12:17 → pv_2 stays in buffer

  +16:00  processClick(click_2 @ 12:12)
            addClick to store
            updateWatermark: 12:12 < 12:15 → watermark stays at 12:15 (monotonic)
            flushBuffer: watermark 12:15 < 12:17 → pv_2 still in buffer

  +30:02  processPageView(pv_3 @ 12:30)
            buffer pv_3
            updateWatermark(12:30)
            flushBuffer: watermark 12:30 >= 12:17 → emit pv_2 now ✅
                         (click_2 is now in store → attributed to campaign_B)
                         watermark 12:30 < 12:32 → pv_3 stays in buffer
```

The buffer is a `ConcurrentHashMap<partition, List<PageViewEvent>>`. Every call to
`processClick` and `processPageView` triggers `emitSafePageViews(partition)` after
updating the watermark.

---

## Expected Output (emitted to SQLite)

### attributed_page_views

```
  page_view_id │ user_id │ event_time           │ url                              │ attributed_campaign_id │ attributed_click_id
  ─────────────┼─────────┼──────────────────────┼──────────────────────────────────┼────────────────────────┼────────────────────
  pv_1         │ user_1  │ 2024-01-01T12:10:00Z │ https://example.com/product1     │ campaign_A             │ click_1
  pv_2         │ user_2  │ 2024-01-01T12:15:00Z │ https://example.com/product2     │ campaign_B             │ click_2
  pv_3         │ user_3  │ 2024-01-01T12:30:00Z │ https://example.com/product3     │ campaign_D             │ click_3b  ← not 3a!
  pv_4         │ user_4  │ 2024-01-01T13:10:00Z │ https://example.com/product4     │ NULL                   │ NULL
  pv_5         │ user_5  │ 2024-01-01T12:45:00Z │ https://example.com/product5     │ NULL                   │ NULL
  pv_6         │ user_6  │ 2024-01-01T13:20:00Z │ https://example.com/product6     │ NULL                   │ NULL
```

### late_events

```
  event_id │ event_type │ event_time           │ watermark_at_detection
  ─────────┼────────────┼──────────────────────┼───────────────────────
  click_5  │ ad_click   │ 2024-01-01T12:40:00Z │ 2024-01-01T12:45:00Z
```

`processed_time`, `partition`, and `offset` are filled at runtime and vary per run.

---

## Dead Letter Topic (DLT) Scenarios

Events that cannot be deserialized are never retried — `DefaultErrorHandler` routes them straight to `ad_clicks.DLT` or
`page_views.DLT`. Events that fail after retries (e.g. transient DB error) also land there. The offset is committed in 
both cases so the consumer does not stall.

### DLT scenario 1 — malformed JSON

A producer sends a click with a syntax error in the payload.

```
raw Kafka value: {"user_id":"user_7","click_id":"bad_click_2","campaign_id":"campaign_G"
                   ← missing closing brace
```

`objectMapper.readValue()` throws `JsonProcessingException` (unclosed JSON object).  
`DefaultErrorHandler` catches it, publishes the raw bytes to `ad_clicks.DLT`, commits the offset. No retry.

### DLT scenario 2 — missing required field

A page view arrives without `event_time`.

```json
{"user_id": "user_8", "event_id": "bad_pv_1", "url": "https://example.com/product8"}
```

Jackson throws `MismatchedInputException` (subclass of `JsonProcessingException`) because `event_time` is non-nullable.  
Routed to `page_views.DLT` immediately, no retry.

### DLT scenario 3 — wrong field type

A click arrives with `event_time` as an integer instead of an ISO-8601 string.

```json
{"user_id": "user_9", "click_id": "bad_click_3", "campaign_id": "campaign_H", "event_time": 99999}
```

**Known limitation:** `JavaTimeModule`'s `InstantDeserializer` silently coerces integers as epoch seconds, so this event 
is parsed as `1970-01-02T03:46:39Z` and later dropped as late rather than routed to DLT. (TODO: fix this issue)

### DLT scenario 4 — transient DB failure after retries

A page view deserializes correctly but `OutputSink.write()` throws `SQLException` on every attempt (e.g. disk full).

`DefaultErrorHandler` retries 3 times with `FixedBackOff(0ms)`. After the third failure it publishes the original 
record to `page_views.DLT` and commits the offset, preventing the consumer from stalling indefinitely.

### DLT expected output

```
  ad_clicks.DLT   ← bad_click_2 (malformed JSON)
                  ← bad_click_3 (wrong type — only after coercion fix)
  page_views.DLT  ← bad_pv_1    (missing event_time)
                  ← any pv that exhausted DB retries
```

DLT records carry the original raw bytes plus exception metadata headers added by 
`DeadLetterPublishingRecoverer` (`kafka_dlt-exception-message`, `kafka_dlt-original-topic`, etc.).