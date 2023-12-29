package com.zemoso.scheduler.operation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class CampaignOperation {

    private static final Logger logger = LoggerFactory.getLogger(CampaignOperation.class);

    public static Map<Integer, Map<String, Object>> fetchCampaignRecords(Connection conn) throws SQLException {
        Map<Integer, Map<String, Object>> resultMap = new HashMap<>();
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT * FROM campaign WHERE (status = 'READY' OR status = 'RESUME') AND CURDATE() BETWEEN start_date AND end_date;");
            while (rs.next()) {
                try {
                    int id = rs.getInt("id");
                    String content = rs.getString("content");
                    String emailIdsStr = rs.getString("email_ids");
                    String[] emails = emailIdsStr.split(",");
                    Map<String, Object> entryMap = new HashMap<>();
                    entryMap.put("content", content);
                    entryMap.put("emailIds", Arrays.asList(emails));
                    resultMap.put(id, entryMap);
                } catch (SQLException e) {
                    logger.error("Exception while processing a campaign record: {}", e.getMessage());
                    logger.debug("Exception details:", e);
                }
            }
        }
        return resultMap;
    }

    public static int createCampaignRunRecord(Connection conn, int campaignId) throws SQLException {
        String insertQuery = "INSERT INTO campaign_run (campaign_id, start_time, status) VALUES (?, NOW(), ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(insertQuery, Statement.RETURN_GENERATED_KEYS)) {
            pstmt.setInt(1, campaignId);
            pstmt.setString(2, "RUNNING");
            pstmt.executeUpdate();

            // Get the generated campaign_run_id
            ResultSet generatedKeys = pstmt.getGeneratedKeys();
            if (generatedKeys.next()) {
                return generatedKeys.getInt(1);
            } else {
                throw new SQLException("Failed to retrieve campaign_run_id.");
            }
        }
    }

    public static void updateCampaignRunRecord(Connection conn, int campaignRunId, int successCount, int failureCount) throws SQLException {
        String updateQuery = "UPDATE campaign_run SET end_time = NOW(), success_count = ?, failure_count = ?, status = ? WHERE id = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(updateQuery)) {
            pstmt.setInt(1, successCount);
            pstmt.setInt(2, failureCount);
            pstmt.setString(3, "SUCCESS");
            pstmt.setInt(4, campaignRunId);
            pstmt.executeUpdate();
        }
    }
}
