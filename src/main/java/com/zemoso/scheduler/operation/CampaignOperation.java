package com.zemoso.scheduler.operation;

import com.zemoso.scheduler.constants.FieldNames;
import com.zemoso.scheduler.model.Campaign;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

public class CampaignOperation {

    private static final Logger logger = LoggerFactory.getLogger(CampaignOperation.class);

    public static List<Campaign> fetchCampaignRecords(Connection conn) {
        List<Campaign> campaignList = new ArrayList<>();

        String query = "SELECT * FROM campaign WHERE (status = ? OR status = ?) AND CURDATE() BETWEEN start_date AND end_date;";
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, FieldNames.READY);
            pstmt.setString(2, FieldNames.RESUME);

            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    Campaign campaign = processCampaignRecord(rs);
                    campaignList.add(campaign);
                }
            }
        } catch (SQLException e) {
            logError("Exception while fetching campaign records", e);
        }

        return campaignList;
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

        String status = FieldNames.SUCCESS;

        if(successCount > 0 && failureCount > 0) {
            status = FieldNames.PARTIALLY_SUCCESS;
        } else if(successCount == 0 && failureCount > 0) {
            status = FieldNames.FAILED;
        }

        try (PreparedStatement pstmt = conn.prepareStatement(updateQuery)) {
            pstmt.setInt(1, successCount);
            pstmt.setInt(2, failureCount);
            pstmt.setString(3, status);
            pstmt.setInt(4, campaignRunId);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            logError("Exception while updating campaign run record", e);
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

    public static int getRetryLimit(Connection conn, int campaignRunId) {
        String selectQuery = "SELECT retry_limit FROM campaign_run WHERE id = ?";
        try (PreparedStatement selectStmt = conn.prepareStatement(selectQuery)) {
            selectStmt.setInt(1, campaignRunId);
            try (ResultSet resultSet = selectStmt.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getInt("retry_limit");
                }
            }
        } catch (SQLException e) {
            logError("Error retrieving retry limit", e);
        }
        return 3;
    }

    private static void logError(String message, SQLException e) {
        logger.error("{}: {}", message, e.getMessage(), e);
        e.printStackTrace();
    }

    public static Campaign fetchUpdatedCampaignByIdFromDatabase(Connection conn, int id, LocalDateTime lastRunTimestamp) {
        String query = "SELECT * FROM campaign WHERE id = ? AND status = ? AND last_updated > ? AND CURDATE() BETWEEN start_date AND end_date;";
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setInt(1, id);
            pstmt.setString(2, FieldNames.READY);
            pstmt.setTimestamp(3, Timestamp.valueOf(lastRunTimestamp));

            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return processCampaignRecord(rs);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static Campaign processCampaignRecord(ResultSet rs) throws SQLException {
        Campaign campaign = new Campaign();
        campaign.setId(rs.getInt("id"));
        campaign.setContent(rs.getString("content"));
        campaign.setEmailIds(rs.getString("email_ids"));
        campaign.setStartDate(rs.getTimestamp("start_date").toLocalDateTime());
        campaign.setEndDate(rs.getTimestamp("end_date") != null ? rs.getTimestamp("end_date").toLocalDateTime() : null);
        campaign.setFrequency(rs.getString("frequency"));
        campaign.setStatus(rs.getString("status"));
        campaign.setLastUpdated(rs.getTimestamp("last_updated").toLocalDateTime());
        return campaign;
    }
}

