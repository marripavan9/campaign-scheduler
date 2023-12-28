package com.zemoso.scheduler.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class EmailStatusService {

    public static boolean shouldTriggerEmail(Connection conn, int runId, String email) throws SQLException {
        String checkEmailStatusQuery = "SELECT COUNT(*) FROM email_status WHERE campaign_run_id = ? AND email_address = ? AND status = 'SENT'";
        try (PreparedStatement checkStatusStmt = conn.prepareStatement(checkEmailStatusQuery)) {
            checkStatusStmt.setInt(1, runId);
            checkStatusStmt.setString(2, email);
            try (ResultSet resultSet = checkStatusStmt.executeQuery()) {
                return !resultSet.next() || resultSet.getInt(1) == 0;
            }
        }
    }

    public static boolean triggerEmailAndUpdateStatus(Connection conn, int runId, String email) throws SQLException {
        boolean emailSent = sendEmail(email);
        // Create a batch insert statement for email_status
        String insertEmailStatusQuery = "INSERT INTO email_status (campaign_run_id, email_address, status) VALUES (?, ?, ?)";
        try (PreparedStatement pstmtEmailStatus = conn.prepareStatement(insertEmailStatusQuery)) {
            // Batch insert email_status records
            pstmtEmailStatus.setInt(1, runId);
            pstmtEmailStatus.setString(2, email);
            pstmtEmailStatus.setString(3, emailSent ? "SENT" : "FAILED");
            pstmtEmailStatus.executeUpdate();
        }
        return emailSent;
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