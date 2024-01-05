package com.zemoso.scheduler.service;

import com.zemoso.scheduler.constants.FieldNames;
import com.zemoso.scheduler.operation.CampaignOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public class EmailTriggerJobService {

    private static final Logger logger = LoggerFactory.getLogger(EmailTriggerJobService.class);

    public static void triggerEmailsAndUpdateStatus(Connection conn, int campaignId, int runId) throws SQLException {
        Map<String, String> emailIdsAndContent = getEmailIdsAndContent(campaignId);
        String emailIds = emailIdsAndContent.get(FieldNames.CAMPAIGN_EMAIL_IDS);
        String content = emailIdsAndContent.get(FieldNames.CAMPAIGN_CONTENT);

        if (emailIds != null) {
            String[] emailArray = emailIds.split(FieldNames.COMMA);
            int successCount = 0;
            int failureCount = 0;

            for (String email : emailArray) {
                email = email.trim();
                if (shouldTriggerEmail(conn, runId, email)) {
                    boolean emailStatus = triggerEmailAndUpdateStatus(conn, runId, email, content);
                    if (emailStatus) {
                        successCount++;
                    } else {
                        failureCount++;
                    }
                }
            }

            updateCampaignRunRecord(conn, runId, successCount, failureCount);
            insertAuditLogRecord(conn, runId, successCount, failureCount);
        }
    }

    private static void insertAuditLogRecord(Connection conn, int runId, int successCount, int failureCount) throws SQLException {
        try {
            EmailCampaignService.insertAuditLogRecord(conn, runId, successCount, failureCount);
        } catch (SQLException e) {
            handleSQLException("Error updating campaign run record", e);
            throw new SQLException("Error updating campaign run record", e);
        }
    }

    private static Map<String, String> getEmailIdsAndContent(int campaignId) throws SQLException {
        try {
            return EmailCampaignService.getEmailIdsAndContent(campaignId);
        } catch (SQLException e) {
            handleSQLException("Error getting email ids and content", e);
            throw new SQLException("Error getting email ids and content", e);
        }
    }

    private static boolean shouldTriggerEmail(Connection conn, int runId, String email) throws SQLException {
        try {
            return EmailStatusService.shouldTriggerEmail(conn, runId, email);
        } catch (SQLException e) {
            handleSQLException("Error checking if email should be triggered", e);
            throw new SQLException("Error checking if email should be triggered", e);
        }
    }

    private static boolean triggerEmailAndUpdateStatus(Connection conn, int runId, String email, String content) throws SQLException {
        try {
            return EmailStatusService.triggerEmailAndUpdateStatus(conn, runId, email, content);
        } catch (SQLException e) {
            handleSQLException("Error triggering email and updating status", e);
            throw new SQLException("Error triggering email and updating status", e);
        }
    }

    private static void updateCampaignRunRecord(Connection conn, int runId, int successCount, int failureCount) throws SQLException {
        try {
            EmailCampaignService.updateCampaignRunRecord(conn, runId, successCount, failureCount);
        } catch (SQLException e) {
            handleSQLException("Error updating campaign run record", e);
            throw new SQLException("Error updating campaign run record", e);
        }
    }

    private static void handleSQLException(String message, SQLException e) {
        logger.error("{}: {}", message, e.getMessage(), e);
    }
}
