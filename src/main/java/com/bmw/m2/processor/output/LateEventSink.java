package com.bmw.m2.processor.output;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;

/**
 * Sink for late events that were dropped due to exceeding allowed lateness.
 *
 * Writes to the same SQLite file as OutputSink for easy cross-table analysis.
 * Useful for monitoring lateness spikes by partition, calculating exact lateness gaps,
 * and identifying misconfigured allowedLateness settings.
 */
@Slf4j
@Component
public class LateEventSink {

    @Value("${output.database.path:./output/attributed_page_views.db}")
    private String databasePath;

    private Connection connection;

    private static final String CREATE_TABLE_SQL = """
            CREATE TABLE IF NOT EXISTS late_events (
                event_id               TEXT PRIMARY KEY,
                event_type             TEXT NOT NULL,
                event_time             TEXT NOT NULL,
                processed_time         TEXT NOT NULL,
                partition              INTEGER NOT NULL,
                offset                 INTEGER NOT NULL,
                watermark_at_detection TEXT NOT NULL
            )
            """;

    private static final String INSERT_SQL = """
            INSERT OR IGNORE INTO late_events
                (event_id, event_type, event_time, processed_time, partition, offset, watermark_at_detection)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """;

    public void initWithConnection(Connection conn) throws SQLException {
        this.connection = conn;
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(CREATE_TABLE_SQL);
        }
    }

    @PostConstruct
    public void init() throws SQLException {
        String jdbcUrl = "jdbc:sqlite:" + databasePath;
        connection = DriverManager.getConnection(jdbcUrl);
        log.info("LateEventSink connected to SQLite database at {}", databasePath);

        // WAL allows Grafana to read the DB concurrently while the processor is writing.
        // Without it, SQLite's exclusive write lock causes "database is locked" errors in Grafana.
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("PRAGMA journal_mode=WAL");
            stmt.execute("PRAGMA busy_timeout=5000");
            stmt.execute(CREATE_TABLE_SQL);
            log.info("Ensured late_events table exists");
        }
    }

    /**
     * Record a late event that was dropped from processing.
     *
     * @param eventId    the unique event ID (click_id or page_view event_id)
     * @param eventType  "ad_click" or "page_view"
     * @param eventTime  the event's own timestamp
     * @param partition  the Kafka partition it arrived on
     * @param offset     the Kafka offset of the record
     * @param watermark  the current watermark when the event was detected as late
     */
    public void write(String eventId, String eventType, Instant eventTime,
                      int partition, long offset, Instant watermark) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement(INSERT_SQL)) {
            stmt.setString(1, eventId);
            stmt.setString(2, eventType);
            stmt.setString(3, eventTime.toString());
            stmt.setString(4, Instant.now().toString());
            stmt.setInt(5, partition);
            stmt.setLong(6, offset);
            stmt.setString(7, watermark.toString());

            int rowsInserted = stmt.executeUpdate();
            if (rowsInserted == 0) {
                log.debug("Duplicate late event skipped for event_id {}", eventId);
            } else {
                log.warn("Late event recorded: {} {} at {} on partition {} offset {} (watermark: {})",
                        eventType, eventId, eventTime, partition, offset, watermark);
            }
        }
    }

    @PreDestroy
    public void close() {
        if (connection != null) {
            try {
                connection.close();
                log.info("LateEventSink SQLite connection closed");
            } catch (SQLException e) {
                log.error("Error closing LateEventSink SQLite connection", e);
            }
        }
    }
}
