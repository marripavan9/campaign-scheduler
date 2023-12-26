package com.zemoso.emailscheduler.job;

import com.zemoso.emailscheduler.operation.DatabaseOperation;
import com.zemoso.emailscheduler.service.EmailTriggerJobService;
import org.quartz.Job;
import org.quartz.JobExecutionContext;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ReRunEmailCampaignJob implements Job {

    @Override
    public void execute(JobExecutionContext jobExecutionContext) {

        try (Connection conn = DatabaseOperation.getConnection()) {
            String selectQuery = "SELECT c.*, cr.id AS run_id FROM campaign c " +
                    "INNER JOIN campaign_run cr ON c.id = cr.campaign_id " +
                    "WHERE c.status = 'running' AND cr.end_time IS NULL";

            try (PreparedStatement selectStmt = conn.prepareStatement(selectQuery)) {
                try (ResultSet resultSet = selectStmt.executeQuery()) {
                    while (resultSet.next()) {
                        int campaignId = resultSet.getInt("id");
                        int runId = resultSet.getInt("run_id");
                        EmailTriggerJobService.triggerEmailsAndUpdateStatus(conn, campaignId, runId);
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Done");
    }

}