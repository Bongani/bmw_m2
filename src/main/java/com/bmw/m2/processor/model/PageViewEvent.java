package com.bmw.m2.processor.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Represents a page view event from the stream.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PageViewEvent {

    // could be anonymous user, what do we do then?
    @JsonProperty(value = "user_id")
    private String userId;

    @JsonProperty(value = "event_time", required = true)
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss", timezone = "UTC")
    private Instant eventTime;

    @JsonProperty(value = "url", required = true)
    private String url;

    @JsonProperty(value = "event_id", required = true)
    private String eventId;

    // Metadata fields for processing
    private transient int partition;
    private transient long offset;
}
