package com.zemoso.scheduler.job;

import com.zemoso.scheduler.operation.DatabaseConnector;
import com.zemoso.scheduler.operation.PropertiesLoader;
import com.zemoso.scheduler.service.EmailTriggerJobService;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ReRunEmailCampaignJob implements Job {

    private static final Logger logger = LoggerFactory.getLogger(ReRunEmailCampaignJob.class);

    @Override
    public void execute(JobExecutionContext jobExecutionContext) {
        Date startDate = new Date();
        ExecutorService executorService = Executors.newFixedThreadPool(PropertiesLoader.getThreadPoolSize());

        try (Connection conn = DatabaseConnector.getConnection()) {
            List<Future<Void>> futures = processRunningCampaigns(conn, executorService);
            awaitCompletion(futures);

        } catch (SQLException | InterruptedException | ExecutionException e) {
            logger.error("Error during execution: {}", e.getMessage(), e);
        } finally {
            executorService.shutdown();
        }

        logExecutionTime(startDate);
    }

    private List<Future<Void>> processRunningCampaigns(Connection conn, ExecutorService executorService) throws SQLException {
        List<Future<Void>> futures = new ArrayList<>();
        String selectQuery = "SELECT c.*, cr.id AS run_id FROM campaign c INNER JOIN campaign_run cr ON c.id = cr.campaign_id " +
                "WHERE cr.status NOT IN ('SUCCESS', 'RUNNING') AND cr.start_time < NOW() - INTERVAL 1 MINUTE AND NOW() < c.end_date AND cr.retry_count < cr.retry_limit;";

        try (PreparedStatement selectStmt = conn.prepareStatement(selectQuery);
             ResultSet resultSet = selectStmt.executeQuery()) {

            while (resultSet.next()) {
                int campaignId = resultSet.getInt("id");
                int runId = resultSet.getInt("run_id");

                Future<Void> future = executorService.submit(() -> {
                    EmailTriggerJobService.triggerEmailsAndUpdateStatus(conn, campaignId, runId);
                    return null;
                });

                futures.add(future);
            }
        }

        return futures;
    }

    private void awaitCompletion(List<Future<Void>> futures) throws InterruptedException, ExecutionException {
        for (Future<Void> future : futures) {
            future.get();
        }
    }

    private void logExecutionTime(Date startDate) {
        Date endDate = new Date();
        long executionTime = endDate.getTime() - startDate.getTime();
        double executionTimeInSeconds = executionTime / 1000.0;
        logger.info("Execution time: {} seconds", executionTimeInSeconds);
        System.out.println("Processing Done");
    }
}
