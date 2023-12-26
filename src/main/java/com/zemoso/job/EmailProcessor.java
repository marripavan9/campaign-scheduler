package com.zemoso.job;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class EmailProcessor {

    public static void triggerEmailsAndRecordStatus(Connection conn, int campaignId, List<String> emailIds) throws SQLException {
        // Set campaign status to 'running'
        CampaignOps.updateCampaignStatus(conn, campaignId, "running");

        // Create a record in the campaign_run table
        int campaignRunId = CampaignOps.createCampaignRunRecord(conn, campaignId);

        int successCount = 0;
        int failureCount = 0;

        // Batch insert for email_status records
        String insertEmailStatusQuery = "INSERT INTO email_status (campaign_run_id, email_address, status) VALUES (?, ?, ?)";
        try (PreparedStatement pstmtEmailStatus = conn.prepareStatement(insertEmailStatusQuery)) {
            for (String email : emailIds) {
                // Trigger email sending logic here
                boolean emailSent = sendEmail(email);

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
        CampaignOps.updateCampaignRunRecord(conn, campaignRunId, successCount, failureCount);

        // Set campaign status back to 'success'
        CampaignOps.updateCampaignStatus(conn, campaignId, "success");
        System.out.println("Done");
    }

    private static boolean sendEmail(String email) {
        return true;
    }
}
