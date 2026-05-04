package com.bmw.m2.processor.consumer;

import com.bmw.m2.processor.engine.JoinEngine;
import com.bmw.m2.processor.model.AdClickEvent;
import com.bmw.m2.processor.model.PageViewEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class StreamConsumerTest {

    @Mock
    private JoinEngine joinEngine;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private Acknowledgment acknowledgment;

    private StreamConsumer streamConsumer;

    @BeforeEach
    void setUp() {
        streamConsumer = new StreamConsumer(joinEngine, objectMapper);
    }

    private ConsumerRecord<String, String> clickRecord(String json) {
        return new ConsumerRecord<>("ad_clicks", 0, 100L, "key", json);
    }

    private ConsumerRecord<String, String> pageViewRecord(String json) {
        return new ConsumerRecord<>("page_views", 1, 200L, "key", json);
    }

    // --- Ad Click Tests ---

    @Test
    void adClickIsProcessedAndAcknowledged() throws Exception {
        AdClickEvent click = AdClickEvent.builder().userId("user_1").clickId("click_1").build();
        when(objectMapper.readValue(any(String.class), eq(AdClickEvent.class))).thenReturn(click);

        streamConsumer.consumeAdClick(clickRecord("{...}"), acknowledgment);

        verify(joinEngine).processClick(click);
        verify(acknowledgment).acknowledge();
    }

    @Test
    void adClickParseFailureDoesNotAcknowledge() throws Exception {
        when(objectMapper.readValue(any(String.class), eq(AdClickEvent.class)))
                .thenThrow(new JsonProcessingException("bad json") {});

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> streamConsumer.consumeAdClick(clickRecord("bad json"), acknowledgment));

        // Cause must be JsonProcessingException so the error handler routes to DLT without retrying
        assertInstanceOf(JsonProcessingException.class, ex.getCause());
        verify(joinEngine, never()).processClick(any());
        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    void adClickJoinEngineFailureDoesNotAcknowledge() throws Exception {
        AdClickEvent click = AdClickEvent.builder().userId("user_1").clickId("click_1").build();
        when(objectMapper.readValue(any(String.class), eq(AdClickEvent.class))).thenReturn(click);
        doThrow(new SQLException("db error")).when(joinEngine).processClick(any());

        assertThrows(RuntimeException.class,
                () -> streamConsumer.consumeAdClick(clickRecord("{...}"), acknowledgment));

        verify(acknowledgment, never()).acknowledge();
    }

    // --- Page View Tests ---

    @Test
    void pageViewIsProcessedAndAcknowledged() throws Exception {
        PageViewEvent pageView = PageViewEvent.builder().userId("user_1").eventId("pv_1").build();
        when(objectMapper.readValue(any(String.class), eq(PageViewEvent.class))).thenReturn(pageView);

        streamConsumer.consumePageView(pageViewRecord("{...}"), acknowledgment);

        verify(joinEngine).processPageView(pageView);
        verify(acknowledgment).acknowledge();
    }

    @Test
    void pageViewParseFailureDoesNotAcknowledge() throws Exception {
        when(objectMapper.readValue(any(String.class), eq(PageViewEvent.class)))
                .thenThrow(new JsonProcessingException("bad json") {});

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> streamConsumer.consumePageView(pageViewRecord("bad json"), acknowledgment));

        // Cause must be JsonProcessingException so the error handler routes to DLT without retrying
        assertInstanceOf(JsonProcessingException.class, ex.getCause());
        verify(joinEngine, never()).processPageView(any());
        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    void pageViewJoinEngineFailureDoesNotAcknowledge() throws Exception {
        PageViewEvent pageView = PageViewEvent.builder().userId("user_1").eventId("pv_1").build();
        when(objectMapper.readValue(any(String.class), eq(PageViewEvent.class))).thenReturn(pageView);
        doThrow(new SQLException("db error")).when(joinEngine).processPageView(any());

        assertThrows(RuntimeException.class,
                () -> streamConsumer.consumePageView(pageViewRecord("{...}"), acknowledgment));

        verify(acknowledgment, never()).acknowledge();
    }
}
