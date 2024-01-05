package com.zemoso.scheduler.operation;

import com.zemoso.scheduler.constants.FieldNames;
import com.zemoso.scheduler.email.SMTPEmailService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

public class EmailProcessingOperation {

    private static final Logger logger = LoggerFactory.getLogger(EmailProcessingOperation.class);

    public static void triggerEmailsAndRecordStatus(Connection conn, int campaignId, String emailIds, String body) {
        int campaignRunId;
        int successCount = 0;
        int failureCount = 0;
        try {
            campaignRunId = CampaignOperation.createCampaignRunRecord(conn, campaignId);
            String[] emails = emailIds.split(FieldNames.COMMA);
            for (String email : emails) {
                boolean emailSent = SMTPEmailService.sendEmailWithRetry(email, body, campaignRunId, conn);
                if (emailSent) {
                    successCount++;
                } else {
                    failureCount++;
                }
            }
            CampaignOperation.updateCampaignRunRecord(conn, campaignRunId, successCount, failureCount);
            CampaignOperation.insertAuditLogRecord(conn, campaignRunId, successCount, failureCount);
        } catch (SQLException e) {
            handleSQLException(conn, e);
        } finally {
            closeConnection(conn);
        }
    }

    private static void handleSQLException(Connection conn, SQLException e) {
        e.printStackTrace();
        logger.error("Error processing emails: {}", e.getMessage(), e);
        try {
            if (conn != null) {
                conn.rollback();
            }
        } catch (SQLException rollbackException) {
            logger.error("Error rolling back transaction: {}", rollbackException.getMessage(), rollbackException);
        }
    }

    private static void closeConnection(Connection conn) {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException closeException) {
            logger.error("Error closing connection: {}", closeException.getMessage(), closeException);
        }
    }
}


