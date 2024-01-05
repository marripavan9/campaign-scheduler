package com.zemoso.scheduler.job;

import com.zemoso.scheduler.model.Campaign;
import com.zemoso.scheduler.operation.CampaignOperation;
import com.zemoso.scheduler.operation.DatabaseConnector;
import com.zemoso.scheduler.operation.EmailProcessingOperation;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Date;

@PersistJobDataAfterExecution
public class EmailCampaignJob implements Job {

    private static final Logger logger = LoggerFactory.getLogger(EmailCampaignJob.class);
    private static final String CAMPAIGN_KEY = "campaign";
    private static final String FIRSTRUN_KEY = "firstRun";
    private static final String RUNTIME_KEY = "runTime";

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        try {
            JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
            Campaign campaign = (Campaign) jobDataMap.get(CAMPAIGN_KEY);
            boolean firstRun = jobDataMap.getBoolean(FIRSTRUN_KEY);
            Date startDate = new Date();

            executeJobLogic(jobDataMap, campaign, firstRun, startDate);
        } catch (SQLException e) {
            handleSQLException(e);
        }
    }

    private void executeJobLogic(JobDataMap jobDataMap, Campaign campaign, boolean firstRun, Date startDate) throws SQLException {
        try (Connection conn = DatabaseConnector.getConnection()) {
            if (firstRun) {
                jobDataMap.put(FIRSTRUN_KEY, false);
            } else {
                LocalDateTime runTime = (LocalDateTime) jobDataMap.get(RUNTIME_KEY);
                Campaign updatedCampaign = CampaignOperation.fetchUpdatedCampaignByIdFromDatabase(conn, campaign.getId(), runTime);
                if (updatedCampaign != null) {
                    campaign = updatedCampaign;
                }
            }

            int campaignId = campaign.getId();
            processCampaign(campaignId, campaign, conn);

            Date endDate = new Date();
            long executionTime = endDate.getTime() - startDate.getTime();
            double executionTimeInSeconds = executionTime / 1000.0;
            logger.info("Execution time: {} seconds", executionTimeInSeconds);

            jobDataMap.put(RUNTIME_KEY, LocalDateTime.now());
        }
    }

    private void processCampaign(int campaignId, Campaign campaignData, Connection conn) {
        String emailIds = campaignData.getEmailIds();
        String content = campaignData.getContent();
        EmailProcessingOperation.triggerEmailsAndRecordStatus(conn, campaignId, emailIds, content);
    }


    private void handleSQLException(SQLException e) throws JobExecutionException {
        logger.error("Error executing job: {}", e.getMessage(), e);
        throw new JobExecutionException("Error executing job", e);
    }
}
