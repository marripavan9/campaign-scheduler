package com.zemoso.scheduler.service;

import com.zemoso.scheduler.operation.DatabaseOperation;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class EmailCampaignService {

    public static Map<String, String> getEmailIdsAndContent(int campaignId) throws SQLException {
        String emailIdsQuery = "SELECT email_ids, content FROM campaign WHERE id = ?";
        try (Connection conn = DatabaseOperation.getConnection();
             PreparedStatement emailIdsStmt = conn.prepareStatement(emailIdsQuery)) {
            emailIdsStmt.setInt(1, campaignId);
            try (ResultSet emailIdsResultSet = emailIdsStmt.executeQuery()) {
                if (emailIdsResultSet.next()) {
                    Map<String, String> result = new HashMap<>();
                    result.put("email_ids", emailIdsResultSet.getString("email_ids"));
                    result.put("content", emailIdsResultSet.getString("content"));
                    return result;
                }
                return null;
            }
        }
    }

    public static void updateCampaignRunRecord(Connection conn, int campaignRunId, int successCount, int failureCount) throws SQLException {
        String selectQuery = "SELECT success_count, failure_count FROM campaign_run WHERE id = ?";
        String updateQuery = "UPDATE campaign_run SET end_time = NOW(), success_count = ?, failure_count = ?, status = ? WHERE id = ?";

        try (PreparedStatement selectStmt = conn.prepareStatement(selectQuery)) {
            selectStmt.setInt(1, campaignRunId);
            ResultSet resultSet = selectStmt.executeQuery();

            int existingSuccessCount = 0;
            int existingFailureCount = 0;

            if (resultSet.next()) {
                existingSuccessCount = resultSet.getInt("success_count");
                existingFailureCount = resultSet.getInt("failure_count");
            }

            // Update counts based on existing counts
            successCount += existingSuccessCount;
            failureCount += existingFailureCount;
        }

        try (PreparedStatement updateStmt = conn.prepareStatement(updateQuery)) {
            updateStmt.setInt(1, successCount);
            updateStmt.setInt(2, failureCount);
            updateStmt.setString(3, "SUCCESS");
            updateStmt.setInt(4, campaignRunId);
            updateStmt.executeUpdate();
        }
    }
}