package com.zemoso.scheduler.job;

import com.zemoso.scheduler.operation.DatabaseOperation;
import com.zemoso.scheduler.service.EmailTriggerJobService;
import org.quartz.Job;
import org.quartz.JobExecutionContext;

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

    private static final int THREAD_POOL_SIZE = 10;

    @Override
    public void execute(JobExecutionContext jobExecutionContext) {
        Date startDate = new Date();
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        List<Future<Void>> futures = new ArrayList<>();

        try (Connection conn = DatabaseOperation.getConnection()) {
            String selectQuery = "SELECT c.*, cr.id AS run_id FROM campaign c " +
                    "INNER JOIN campaign_run cr ON c.id = cr.campaign_id " +
                    "WHERE c.status = 'running' AND cr.end_time IS NULL";

            try (PreparedStatement selectStmt = conn.prepareStatement(selectQuery)) {
                try (ResultSet resultSet = selectStmt.executeQuery()) {
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
            }

            for (Future<Void> future : futures) {
                future.get();
            }

        } catch (SQLException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            executorService.shutdown();
        }

        Date endDate = new Date();
        long executionTime = endDate.getTime() - startDate.getTime();
        double executionTimeInSeconds = executionTime / 1000.0;
        System.out.println("Execution time: " + executionTimeInSeconds + " seconds");
        System.out.println("Processing Done");
    }

}