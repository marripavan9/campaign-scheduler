package com.zemoso.job.retry;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class EmailTriggerJob {

    public static void triggerEmailsAndUpdateStatus(Connection conn, int campaignId, int runId) throws SQLException {
        // Fetch email_ids from the campaign
        String emailIds = CampaignService.getEmailIds(campaignId);

        int successCount = 0;
        int failureCount = 0;

        // Check if any email has 'SENT' status in email_status table
        boolean shouldTriggerEmail = EmailService.shouldTriggerEmail(conn, runId);

        if (shouldTriggerEmail) {
            // Trigger emails
            EmailService.triggerEmails(conn, runId, emailIds, successCount, failureCount);
            // Update campaign_run table
            CampaignService.updateCampaignRunRecord(conn, runId, successCount, failureCount);
            // Update campaign table
            CampaignService.updateCampaignRecord(conn, campaignId);
        }
    }
}