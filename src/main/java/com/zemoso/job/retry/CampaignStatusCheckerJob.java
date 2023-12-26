package com.zemoso.job.retry;

import com.zemoso.job.DBOps;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class CampaignStatusCheckerJob implements Job {

    @Override
    public void execute(JobExecutionContext jobExecutionContext) {

        try (Connection conn = DBOps.getConnection()) {
            String selectQuery = "SELECT c.*, cr.id AS run_id FROM campaign c " +
                    "INNER JOIN campaign_run cr ON c.id = cr.campaign_id " +
                    "WHERE c.status = 'running' AND cr.end_time IS NULL";

            try (PreparedStatement selectStmt = conn.prepareStatement(selectQuery)) {
                try (ResultSet resultSet = selectStmt.executeQuery()) {
                    while (resultSet.next()) {
                        int campaignId = resultSet.getInt("id");
                        int runId = resultSet.getInt("run_id");
                        EmailTriggerJob.triggerEmailsAndUpdateStatus(conn, campaignId, runId);
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Done");
    }

}