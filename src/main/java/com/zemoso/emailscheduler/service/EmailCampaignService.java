package com.zemoso.emailscheduler.service;

import com.zemoso.emailscheduler.operation.DatabaseOperation;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class EmailCampaignService {

    public static String getEmailIds(int campaignId) throws SQLException {
        String emailIdsQuery = "SELECT email_ids FROM campaign WHERE id = ?";
        try (Connection conn = DatabaseOperation.getConnection();
             PreparedStatement emailIdsStmt = conn.prepareStatement(emailIdsQuery)) {
            emailIdsStmt.setInt(1, campaignId);
            try (ResultSet emailIdsResultSet = emailIdsStmt.executeQuery()) {
                if (emailIdsResultSet.next()) {
                    return emailIdsResultSet.getString("email_ids");
                }
                return null;
            }
        }
    }

    public static void updateCampaignRunRecord(Connection conn, int campaignRunId, int successCount, int failureCount) throws SQLException {
        String selectQuery = "SELECT success_count, failure_count FROM campaign_run WHERE id = ?";
        String updateQuery = "UPDATE campaign_run SET end_time = NOW(), success_count = ?, failure_count = ? WHERE id = ?";

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
            updateStmt.setInt(3, campaignRunId);
            updateStmt.executeUpdate();
        }
    }

    public static void updateCampaignRecord(Connection conn, int campaignId) throws SQLException {
        // Update status in campaign table
        String updateStatusQuery = "UPDATE campaign SET status = 'success' WHERE id = ?";
        try (PreparedStatement updateStmt = conn.prepareStatement(updateStatusQuery)) {
            updateStmt.setInt(1, campaignId);
            updateStmt.executeUpdate();
        }
    }
}