package com.zemoso.scheduler.operation;

import com.zemoso.scheduler.email.SMTPEmailService;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class EmailProcessingOperation {

    public static void triggerEmailsAndRecordStatus(Connection conn, int campaignId, List<String> emailIds, String body) throws SQLException {
        // Create a record in the campaign_run table
        int campaignRunId = CampaignOperation.createCampaignRunRecord(conn, campaignId);

        int successCount = 0;
        int failureCount = 0;

        // Batch insert for email_status records
        String insertEmailStatusQuery = "INSERT INTO email_status (campaign_run_id, email_address, status) VALUES (?, ?, ?)";
        try (PreparedStatement pstmtEmailStatus = conn.prepareStatement(insertEmailStatusQuery)) {
            for (String email : emailIds) {
                // Trigger email sending logic here
                boolean emailSent = SMTPEmailService.sendEmail(email, body);
                // Batch insert email_status records
                pstmtEmailStatus.setInt(1, campaignRunId);
                pstmtEmailStatus.setString(2, email);
                pstmtEmailStatus.setString(3, emailSent ? "SENT" : "FAILED");
                pstmtEmailStatus.addBatch();
                // Update success or failure count
                if (emailSent) {
                    successCount++;
                } else {
                    failureCount++;
                }
            }
            // Execute batch insert
            pstmtEmailStatus.executeBatch();
        }
        // Update the campaign_run table with end_time, success_count, and failure_count
        CampaignOperation.updateCampaignRunRecord(conn, campaignRunId, successCount, failureCount);
    }
}
