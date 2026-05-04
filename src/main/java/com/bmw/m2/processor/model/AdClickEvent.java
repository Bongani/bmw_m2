package com.bmw.m2.processor.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Represents an ad click event from the stream.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AdClickEvent {

    // could be anonymous user, what do we do then?
    @JsonProperty("user_id")
    private String userId;

    @JsonProperty(value = "event_time", required = true)
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss", timezone = "UTC")
    private Instant eventTime;

    // unsure if business case requires this to be true
    @JsonProperty("campaign_id")
    private String campaignId;

    @JsonProperty(value = "click_id", required = true)
    private String clickId;

    // Metadata fields for processing
    private transient int partition;
    private transient long offset;
}
