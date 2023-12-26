package com.zemoso.emailscheduler.job;

import com.zemoso.emailscheduler.operation.CampaignOperation;
import com.zemoso.emailscheduler.operation.DatabaseOperation;
import com.zemoso.emailscheduler.operation.EmailProcessingOperation;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class EmailCampaignJob implements Job {

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        try (Connection conn = DatabaseOperation.getConnection()) {
            Map<Integer, Map<String, Object>> resultMap = CampaignOperation.fetchCampaignRecords(conn);

            // Trigger emails and create records in the email_status table
            for (Map.Entry<Integer, Map<String, Object>> entry : resultMap.entrySet()) {
                int campaignId = entry.getKey();
                Map<String, Object> campaignData = entry.getValue();
                List<String> emailIds = (List<String>) campaignData.get("emailIds");

                // Call the EmailProcessor to handle email processing logic
                EmailProcessingOperation.triggerEmailsAndRecordStatus(conn, campaignId, emailIds);
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            throw new JobExecutionException(e);
        }
        System.out.println("Done");
    }
}

