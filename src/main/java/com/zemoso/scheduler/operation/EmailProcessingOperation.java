package com.zemoso.scheduler.operation;

import com.zemoso.scheduler.constants.FieldNames;
import com.zemoso.scheduler.email.SMTPEmailService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class EmailProcessingOperation {

    private static final Logger logger = LoggerFactory.getLogger(EmailProcessingOperation.class);

    public static void triggerEmailsAndRecordStatus(Connection conn, int campaignId, List<String> emailIds, String body) {
        int campaignRunId;
        int successCount = 0;
        int failureCount = 0;

        try {
            conn.setAutoCommit(false);
            campaignRunId = CampaignOperation.createCampaignRunRecord(conn, campaignId);
            String insertEmailStatusQuery = "INSERT INTO email_status (campaign_run_id, email_address, status) VALUES (?, ?, ?)";

            try (PreparedStatement pstmtEmailStatus = conn.prepareStatement(insertEmailStatusQuery)) {
                for (String email : emailIds) {
                    boolean emailSent = SMTPEmailService.sendEmail(email, body);
                    pstmtEmailStatus.setInt(1, campaignRunId);
                    pstmtEmailStatus.setString(2, email);
                    pstmtEmailStatus.setString(3, emailSent ? FieldNames.EMAIL_SENT : FieldNames.EMAIL_FAILED);
                    pstmtEmailStatus.addBatch();
                    if (emailSent) {
                        successCount++;
                    } else {
                        failureCount++;
                    }
                }
                pstmtEmailStatus.executeBatch();
            }
            conn.commit();
            CampaignOperation.updateCampaignRunRecord(conn, campaignRunId, successCount, failureCount);
        } catch (SQLException e) {
            handleSQLException(conn, e);
        } finally {
            closeConnection(conn);
        }
    }

    private static void handleSQLException(Connection conn, SQLException e) {
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
                conn.setAutoCommit(true);
                conn.close();
            }
        } catch (SQLException closeException) {
            logger.error("Error closing connection: {}", closeException.getMessage(), closeException);
        }
    }
}


