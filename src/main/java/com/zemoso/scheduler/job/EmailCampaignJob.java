package com.zemoso.scheduler.job;

import com.zemoso.scheduler.operation.CampaignOperation;
import com.zemoso.scheduler.operation.DatabaseConnector;
import com.zemoso.scheduler.operation.EmailProcessingOperation;
import com.zemoso.scheduler.operation.PropertiesLoader;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;

public class EmailCampaignJob implements Job {

    private static final Logger logger = LoggerFactory.getLogger(EmailCampaignJob.class);

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        Date startDate = new Date();
        ExecutorService executorService = Executors.newFixedThreadPool(PropertiesLoader.getThreadPoolSize());
        try (Connection conn = DatabaseConnector.getConnection()) {
            Map<Integer, Map<String, Object>> resultMap = CampaignOperation.fetchCampaignRecords(conn);
            List<Future<?>> futures = new ArrayList<>();
            for (Map.Entry<Integer, Map<String, Object>> entry : resultMap.entrySet()) {
                int campaignId = entry.getKey();
                Map<String, Object> campaignData = entry.getValue();
                processCampaign(campaignId, campaignData, executorService, conn, futures);
            }
            for (Future<?> future : futures) {
                future.get();
            }
            Date endDate = new Date();
            long executionTime = endDate.getTime() - startDate.getTime();
            double executionTimeInSeconds = executionTime / 1000.0;
            logger.info("Execution time: {} seconds", executionTimeInSeconds);
        } catch (SQLException e) {
            logger.error("SQLException: {}", e.getMessage(), e);
            throw new JobExecutionException(e);
        } catch (ExecutionException | InterruptedException e) {
            logger.error("Exception during execution: {}", e.getMessage(), e);
            Thread.currentThread().interrupt();
        } finally {
            executorService.shutdown();
        }
    }

    private void processCampaign(int campaignId, Map<String, Object> campaignData, ExecutorService executorService, Connection conn, List<Future<?>> futures) {
        List<String> emailIds = (List<String>) campaignData.getOrDefault("email_ids", Collections.emptyList());
        String content = (String) campaignData.get("content");
        Runnable task = () -> {
            try {
                EmailProcessingOperation.triggerEmailsAndRecordStatus(conn, campaignId, emailIds, content);
            } catch (Exception e) {
                logger.error("Exception while processing campaign with ID {}: {}", campaignId, e.getMessage(), e);
            }
        };
        futures.add(executorService.submit(task));
    }
}