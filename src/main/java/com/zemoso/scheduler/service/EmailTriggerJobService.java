package com.zemoso.scheduler.service;

import java.sql.Connection;
import java.sql.SQLException;

public class EmailTriggerJobService {

    public static void triggerEmailsAndUpdateStatus(Connection conn, int campaignId, int runId) throws SQLException {
        String emailIds = EmailCampaignService.getEmailIds(campaignId);
        String[] emailArray;
        boolean shouldTriggerEmail;
        int successCount = 0;
        int failureCount = 0;

        if(emailIds != null) {
            emailArray = emailIds.split(",");
            for (String email : emailArray) {
                shouldTriggerEmail = EmailStatusService.shouldTriggerEmail(conn, runId, email.trim());
                if (shouldTriggerEmail) {
                    boolean status = EmailStatusService.triggerEmailAndUpdateStatus(conn, runId, email);
                    if(status) successCount++;
                    else failureCount++;
                }
            }
        }
        EmailCampaignService.updateCampaignRunRecord(conn, runId, successCount, failureCount);
    }
}