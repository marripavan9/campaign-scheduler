package com.zemoso.scheduler.operation;

import com.zemoso.scheduler.constants.FieldNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class CampaignOperation {

    private static final Logger logger = LoggerFactory.getLogger(CampaignOperation.class);

    public static Map<Integer, Map<String, Object>> fetchCampaignRecords(Connection conn) {
        Map<Integer, Map<String, Object>> resultMap = new HashMap<>();

        String query = "SELECT * FROM campaign WHERE (status = ? OR status = ?) AND CURDATE() BETWEEN start_date AND end_date;";
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, FieldNames.READY);
            pstmt.setString(2, FieldNames.RESUME);

            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    processCampaignRecord(rs, resultMap);
                }
            }
        } catch (SQLException e) {
            logError("Exception while fetching campaign records", e);
        }

        return resultMap;
    }

    private static void processCampaignRecord(ResultSet rs, Map<Integer, Map<String, Object>> resultMap) {
        try {
            int id = rs.getInt(FieldNames.CAMPAIGN_ID);
            String content = rs.getString(FieldNames.CAMPAIGN_CONTENT);
            String emailIdsStr = rs.getString(FieldNames.CAMPAIGN_EMAIL_IDS);
            String[] emails = emailIdsStr.split(FieldNames.COMMA);
            Map<String, Object> entryMap = new HashMap<>();
            entryMap.put(FieldNames.CAMPAIGN_CONTENT, content);
            entryMap.put(FieldNames.CAMPAIGN_EMAIL_IDS, Arrays.asList(emails));
            resultMap.put(id, entryMap);
        } catch (SQLException e) {
            logError("Exception while processing a campaign record", e);
        }
    }

    public static int createCampaignRunRecord(Connection conn, int campaignId) {
        String insertQuery = "INSERT INTO campaign_run (campaign_id, start_time, status) VALUES (?, NOW(), ?)";

        try (PreparedStatement pstmt = conn.prepareStatement(insertQuery, Statement.RETURN_GENERATED_KEYS)) {
            pstmt.setInt(1, campaignId);
            pstmt.setString(2, FieldNames.RUNNING);
            pstmt.executeUpdate();

            return getGeneratedKey(pstmt);
        } catch (SQLException e) {
            logError("Exception while creating campaign run record", e);
            return -1;
        }
    }

    private static int getGeneratedKey(PreparedStatement pstmt) throws SQLException {
        try (ResultSet generatedKeys = pstmt.getGeneratedKeys()) {
            if (generatedKeys.next()) {
                return generatedKeys.getInt(1);
            } else {
                throw new SQLException("Failed to retrieve campaign_run_id.");
            }
        }
    }

    public static void updateCampaignRunRecord(Connection conn, int campaignRunId, int successCount, int failureCount) {
        String updateQuery = "UPDATE campaign_run SET end_time = NOW(), success_count = ?, failure_count = ?, status = ? WHERE id = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(updateQuery)) {
            pstmt.setInt(1, successCount);
            pstmt.setInt(2, failureCount);
            pstmt.setString(3, FieldNames.SUCCESS);
            pstmt.setInt(4, campaignRunId);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            logError("Exception while updating campaign run record", e);
        }
    }

    private static void logError(String message, SQLException e) {
        logger.error("{}: {}", message, e.getMessage(), e);
        e.printStackTrace();
    }
}

