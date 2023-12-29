package com.zemoso.scheduler.job;

import com.zemoso.scheduler.operation.CampaignOperation;
import com.zemoso.scheduler.operation.DatabaseOperation;
import com.zemoso.scheduler.operation.EmailProcessingOperation;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class EmailCampaignJob implements Job {

    private static final Logger logger = LoggerFactory.getLogger(EmailCampaignJob.class);
    private static final int THREAD_POOL_SIZE = 10;

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        Date startDate = new Date();
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        try (Connection conn = DatabaseOperation.getConnection()) {
            Map<Integer, Map<String, Object>> resultMap = CampaignOperation.fetchCampaignRecords(conn);
            List<Future<Void>> futures = new ArrayList<>();

            for (Map.Entry<Integer, Map<String, Object>> entry : resultMap.entrySet()) {
                int campaignId = entry.getKey();
                Map<String, Object> campaignData = entry.getValue();
                List<String> emailIds = (List<String>) campaignData.get("emailIds");
                String content = (String) campaignData.get("content");
                Callable<Void> task = () -> {
                    try {
                        // Call the EmailProcessor to handle email processing logic
                        EmailProcessingOperation.triggerEmailsAndRecordStatus(conn, campaignId, emailIds, content);
                    } catch (Exception e) {
                        // Log the exception
                        logger.error("Exception while processing campaign with ID {}: {}", campaignId, e.getMessage());
                        logger.debug("Exception details:", e);
                    }
                    return null;
                };
                futures.add(executorService.submit(task));
            }
            for (Future<Void> future : futures) {
                future.get();
            }
            Date endDate = new Date();
            long executionTime = endDate.getTime() - startDate.getTime();
            double executionTimeInSeconds = executionTime / 1000.0;
            logger.info("Execution time: " + executionTimeInSeconds + " seconds");
            System.out.println("Execution time: " + executionTimeInSeconds + " seconds");
        } catch (SQLException e) {
            logger.info(e.getMessage());
            e.printStackTrace();
            throw new JobExecutionException(e);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            executorService.shutdown();
        }
        System.out.println("Processing Done");
    }
}