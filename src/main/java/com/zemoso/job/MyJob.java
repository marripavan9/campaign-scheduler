package com.zemoso.job;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class MyJob implements Job {

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        try (Connection conn = DBOps.getConnection()) {
            Map<Integer, Map<String, Object>> resultMap = CampaignOps.fetchCampaignRecords(conn);

            // Trigger emails and create records in the email_status table
            for (Map.Entry<Integer, Map<String, Object>> entry : resultMap.entrySet()) {
                int campaignId = entry.getKey();
                Map<String, Object> campaignData = entry.getValue();
                List<String> emailIds = (List<String>) campaignData.get("emailIds");

                // Call the EmailProcessor to handle email processing logic
                EmailProcessor.triggerEmailsAndRecordStatus(conn, campaignId, emailIds);
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            throw new JobExecutionException(e);
        }
    }
}

