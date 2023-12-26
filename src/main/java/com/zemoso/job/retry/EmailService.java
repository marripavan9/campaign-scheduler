package com.zemoso.job.retry;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class EmailService {

    public static boolean shouldTriggerEmail(Connection conn, int runId) throws SQLException {
        String checkEmailStatusQuery = "SELECT COUNT(*) FROM email_status WHERE campaign_run_id = ? AND status = 'SENT'";
        try (PreparedStatement checkStatusStmt = conn.prepareStatement(checkEmailStatusQuery)) {
            checkStatusStmt.setInt(1, runId);
            try (ResultSet resultSet = checkStatusStmt.executeQuery()) {
                return !resultSet.next() || resultSet.getInt(1) == 0;
            }
        }
    }

    public static void triggerEmails(Connection conn, int runId, String emailIdsString, int successCount, int failureCount) throws SQLException {
        String[] emailIds = emailIdsString.split(",");

        // Check if any email has 'SENT' status in email_status table
        boolean shouldTriggerEmail = shouldTriggerEmail(conn, runId);

        if (shouldTriggerEmail) {
            // Trigger emails
            for (String email : emailIds) {
                boolean emailSent = sendEmail(email);
                if(emailSent) successCount = successCount+1;
                else failureCount = failureCount+1;
                // Update email_status table
                updateEmailStatus(conn, runId, email, emailSent ? "SENT" : "FAILED");
            }
        }
    }

    private static void updateEmailStatus(Connection conn, int runId, String email, String status) throws SQLException {
        String updateStatusQuery = "INSERT INTO email_status (campaign_run_id, email_address, status) VALUES (?, ?, ?)";
        try (PreparedStatement updateStmt = conn.prepareStatement(updateStatusQuery)) {
            updateStmt.setInt(1, runId);
            updateStmt.setString(2, email);
            updateStmt.setString(3, status);
            updateStmt.executeUpdate();
        }
    }

    private static boolean sendEmail(String email) {
        // Replace this with your actual email sending logic
        // Return true if email is sent successfully, false otherwise
        return true;
    }
}