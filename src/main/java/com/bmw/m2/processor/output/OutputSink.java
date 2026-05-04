package com.bmw.m2.processor.output;

import com.bmw.m2.processor.model.AttributedPageView;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Durable output sink that writes attributed page views to a SQLite database.
 *
 * Uses INSERT OR IGNORE on page_view_id PRIMARY KEY for idempotent writes —
 * safe to replay the same record on crash/restart without producing duplicates.
 */
@Slf4j
@Component
public class OutputSink {

    @Value("${output.database.path:./output/attributed_page_views.db}")
    private String databasePath;

    private Connection connection;

    private static final String CREATE_TABLE_SQL = """
            CREATE TABLE IF NOT EXISTS attributed_page_views (
                page_view_id            TEXT PRIMARY KEY,
                user_id                 TEXT NOT NULL,
                event_time              TEXT NOT NULL,
                url                     TEXT NOT NULL,
                attributed_campaign_id  TEXT,
                attributed_click_id     TEXT
            )
            """;

    private static final String INSERT_SQL = """
            INSERT OR IGNORE INTO attributed_page_views
                (page_view_id, user_id, event_time, url, attributed_campaign_id, attributed_click_id)
            VALUES (?, ?, ?, ?, ?, ?)
            """;

    public void initWithConnection(Connection conn) throws SQLException {
        this.connection = conn;
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(CREATE_TABLE_SQL);
        }
    }

    @PostConstruct
    public void init() throws SQLException {
        // Create output directory if it doesn't exist
        File dbFile = new File(databasePath);
        File parentDir = dbFile.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
            log.info("Created output directory: {}", parentDir.getAbsolutePath());
        }

        // Open SQLite connection
        String jdbcUrl = "jdbc:sqlite:" + databasePath;
        connection = DriverManager.getConnection(jdbcUrl);
        log.info("Connected to SQLite database at {}", databasePath);

        // WAL allows Grafana to read the DB concurrently while the processor is writing.
        // Without it, SQLite's exclusive write lock causes "database is locked" errors in Grafana.
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("PRAGMA journal_mode=WAL");
            stmt.execute("PRAGMA busy_timeout=5000");
            stmt.execute(CREATE_TABLE_SQL);
            log.info("Ensured attributed_page_views table exists");
        }
    }

    /**
     * Write an attributed page view to the database.
     * Uses INSERT OR IGNORE so duplicate page_view_id records are silently skipped —
     * this makes restarts safe under at-least-once delivery.
     *
     * @param record the attributed page view to persist
     */
    public void write(AttributedPageView record) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement(INSERT_SQL)) {
            stmt.setString(1, record.getPageViewId());
            stmt.setString(2, record.getUserId());
            stmt.setString(3, record.getEventTime().toString());
            stmt.setString(4, record.getUrl());
            stmt.setString(5, record.getAttributedCampaignId());
            stmt.setString(6, record.getAttributedClickId());

            int rowsInserted = stmt.executeUpdate();
            if (rowsInserted == 0) {
                log.debug("Duplicate write skipped for page_view_id {}", record.getPageViewId());
            } else {
                log.debug("Wrote attributed page view {} for user {}", record.getPageViewId(), record.getUserId());
            }
        }
    }

    @PreDestroy
    public void close() {
        if (connection != null) {
            try {
                connection.close();
                log.info("SQLite connection closed");
            } catch (SQLException e) {
                log.error("Error closing SQLite connection", e);
            }
        }
    }
}