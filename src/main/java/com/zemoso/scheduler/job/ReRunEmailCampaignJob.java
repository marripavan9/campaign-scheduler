package com.zemoso.scheduler.job;

import com.zemoso.scheduler.operation.DatabaseOperation;
import com.zemoso.scheduler.service.EmailTriggerJobService;
import org.quartz.Job;
import org.quartz.JobExecutionContext;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

public class ReRunEmailCampaignJob implements Job {


    @Override
    public void execute(JobExecutionContext jobExecutionContext) {
        Date startDate = new Date();
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
            Date endDate = new Date();
            long executionTime = endDate.getTime() - startDate.getTime();
            double executionTimeInSeconds = executionTime / 1000.0;
            System.out.println("Execution time: " + executionTimeInSeconds + " seconds");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Processing Done");
    }

}