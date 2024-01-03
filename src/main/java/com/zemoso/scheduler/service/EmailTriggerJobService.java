package com.zemoso.scheduler.service;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public class EmailTriggerJobService {

    public static void triggerEmailsAndUpdateStatus(Connection conn, int campaignId, int runId) throws SQLException {
        Map<String, String> emailIdsAndContent = EmailCampaignService.getEmailIdsAndContent(campaignId);
        String emailIds = emailIdsAndContent.get("email_ids");
        String content = emailIdsAndContent.get("content");
        String[] emailArray;
        boolean shouldTriggerEmail;
        int successCount = 0;
        int failureCount = 0;

        if(emailIds != null) {
            emailArray = emailIds.split(",");
            for (String email : emailArray) {
                shouldTriggerEmail = EmailStatusService.shouldTriggerEmail(conn, runId, email.trim());
                if (shouldTriggerEmail) {
                    boolean status = EmailStatusService.triggerEmailAndUpdateStatus(conn, runId, email, content);
                    if(status) successCount++;
                    else failureCount++;
                }
            }
        }
        EmailCampaignService.updateCampaignRunRecord(conn, runId, successCount, failureCount);
    }
}