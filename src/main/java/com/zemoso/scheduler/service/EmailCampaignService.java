package com.zemoso.scheduler.service;

import com.zemoso.scheduler.constants.FieldNames;
import com.zemoso.scheduler.operation.DatabaseConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class EmailCampaignService {

    private static final Logger logger = LoggerFactory.getLogger(EmailCampaignService.class);

    private static final String SELECT_EMAIL_CONTENT_QUERY = "SELECT email_ids, content FROM campaign WHERE id = ?";
    private static final String SELECT_CAMPAIGN_RUN_QUERY = "SELECT success_count, failure_count, retry_count FROM campaign_run WHERE id = ?";
    private static final String UPDATE_CAMPAIGN_RUN_QUERY = "UPDATE campaign_run SET end_time = NOW(), success_count = ?, failure_count = ?, status = ?, retry_count =? WHERE id = ?";

    public static Map<String, String> getEmailIdsAndContent(int campaignId) throws SQLException {
        try (Connection conn = DatabaseConnector.getConnection();
             PreparedStatement emailIdsStmt = conn.prepareStatement(SELECT_EMAIL_CONTENT_QUERY)) {
            emailIdsStmt.setInt(1, campaignId);

            try (ResultSet emailIdsResultSet = emailIdsStmt.executeQuery()) {
                if (emailIdsResultSet.next()) {
                    Map<String, String> result = new HashMap<>();
                    result.put(FieldNames.CAMPAIGN_EMAIL_IDS, emailIdsResultSet.getString(FieldNames.CAMPAIGN_EMAIL_IDS));
                    result.put(FieldNames.CAMPAIGN_CONTENT, emailIdsResultSet.getString(FieldNames.CAMPAIGN_CONTENT));
                    return result;
                }
                return null;
            }
        } catch (SQLException e) {
            handleSQLException("Error fetching email content", e);
            throw new SQLException("Error fetching email content", e);
        }
    }

    public static void updateCampaignRunRecord(Connection conn, int campaignRunId, int successCount, int failureCount) throws SQLException {
        int existingSuccessCount = 0;
        int existingFailureCount = 0;
        int retryCount = 0;

        try (PreparedStatement selectStmt = conn.prepareStatement(SELECT_CAMPAIGN_RUN_QUERY)) {
            selectStmt.setInt(1, campaignRunId);

            try (ResultSet resultSet = selectStmt.executeQuery()) {
                if (resultSet.next()) {
                    existingSuccessCount = resultSet.getInt(FieldNames.CAMPAIGN_RUN_SUCCESS_COUNT);
                    existingFailureCount = resultSet.getInt(FieldNames.CAMPAIGN_RUN_FAILURE_COUNT);
                    retryCount = resultSet.getInt(FieldNames.CAMPAIGN_RUN_RETRY_COUNT);
                }
            }
        } catch (SQLException e) {
            handleSQLException("Error fetching existing counts", e);
            throw new SQLException("Error fetching existing counts", e);
        }

        successCount += existingSuccessCount;
        failureCount += existingFailureCount;

        String status = FieldNames.SUCCESS;

        if(successCount > 0 && failureCount > 0) {
            status = FieldNames.PARTIALLY_SUCCESS;
        } else if(successCount == 0 && failureCount > 0) {
            status = FieldNames.FAILED;
        }

        try (PreparedStatement updateStmt = conn.prepareStatement(UPDATE_CAMPAIGN_RUN_QUERY)) {
            updateStmt.setInt(1, successCount);
            updateStmt.setInt(2, failureCount);
            updateStmt.setString(3, status);
            updateStmt.setInt(4, retryCount+1);
            updateStmt.setInt(5, campaignRunId);
            updateStmt.executeUpdate();
        } catch (SQLException e) {
            handleSQLException("Error updating campaign run record", e);
            throw new SQLException("Error updating campaign run record", e);
        }
    }

    public static void insertAuditLogRecord(Connection conn, int campaignRunId, int successCount, int failureCount) throws SQLException {
        String insertQuery = "INSERT INTO campaign_run_audit_log (campaign_run_id, start_time, status, success_count, failure_count) " +
                "SELECT id, start_time, status, ?, ? FROM campaign_run WHERE id = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(insertQuery)) {
            pstmt.setInt(1, successCount);
            pstmt.setInt(2, failureCount);
            pstmt.setInt(3, campaignRunId);
            pstmt.executeUpdate();
        }
    }

    private static void handleSQLException(String message, SQLException e) {
        logger.error("{}: {}", message, e.getMessage(), e);
    }
}
