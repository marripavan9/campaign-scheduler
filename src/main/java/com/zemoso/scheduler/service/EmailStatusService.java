package com.zemoso.scheduler.service;

import com.zemoso.scheduler.email.SMTPEmailService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class EmailStatusService {

    private static final Logger logger = LoggerFactory.getLogger(EmailStatusService.class);

    private static final String CHECK_EMAIL_STATUS_QUERY = "SELECT COUNT(*) FROM email_status WHERE campaign_run_id = ? AND email_address = ? AND status = 'SENT'";
    private static final String INSERT_EMAIL_STATUS_QUERY = "INSERT INTO email_status (campaign_run_id, email_address, status) VALUES (?, ?, ?)";

    public static boolean shouldTriggerEmail(Connection conn, int runId, String email) throws SQLException {
        try (PreparedStatement checkStatusStmt = conn.prepareStatement(CHECK_EMAIL_STATUS_QUERY)) {
            checkStatusStmt.setInt(1, runId);
            checkStatusStmt.setString(2, email);

            try (ResultSet resultSet = checkStatusStmt.executeQuery()) {
                return !resultSet.next() || resultSet.getInt(1) == 0;
            }
        } catch (SQLException e) {
            handleSQLException("Error checking email status", e);
            throw new SQLException("Error checking email status", e);
        }
    }

    public static boolean triggerEmailAndUpdateStatus(Connection conn, int runId, String email, String body) throws SQLException {
        return SMTPEmailService.sendEmailWithRetry(email, body, runId, conn);
    }

    private static void handleSQLException(String message, SQLException e) {
        logger.error("{}: {}", message, e.getMessage(), e);
    }
}
