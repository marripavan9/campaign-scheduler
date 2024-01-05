package com.zemoso.scheduler.scheduler;

import com.zemoso.scheduler.job.EmailCampaignJob;
import com.zemoso.scheduler.job.ReRunEmailCampaignJob;
import com.zemoso.scheduler.model.Campaign;
import com.zemoso.scheduler.operation.CampaignOperation;
import com.zemoso.scheduler.operation.DatabaseConnector;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;

public class EmailCampaignScheduler {

    public static void main(String[] args) {
        try {
            scheduleRegularEmailJob();
            scheduleReRunEmailJob();
        } catch (SchedulerException | SQLException e) {
            e.printStackTrace();
        }
    }

    private static void scheduleRegularEmailJob() throws SchedulerException, SQLException {
        List<Campaign> campaigns = CampaignOperation.fetchCampaignRecords(DatabaseConnector.getConnection());
        for (Campaign campaign : campaigns) {
            String cronExpression = campaign.getFrequency();
            JobDetail emailJob = createJobDetail(EmailCampaignJob.class, "emailJob_" + campaign.getId(), "emailJobGroup");
            emailJob.getJobDataMap().put("campaign", campaign);
            emailJob.getJobDataMap().put("firstRun", true);
            emailJob.getJobDataMap().put("runTime", LocalDateTime.now());
            Trigger emailTrigger = createCronTrigger(cronExpression, "emailTrigger_" + campaign.getId(), "emailTriggerGroup");
            scheduleJob(emailJob, emailTrigger);
        }
    }

    private static void scheduleReRunEmailJob() throws SchedulerException {
        JobDetail reRunEmailJob = createJobDetail(ReRunEmailCampaignJob.class, "emailTriggerJob", "group2");
        Trigger reRunEmailTrigger = createCronTrigger("0 0/10 * * * ?", "trigger2", "group2");
        scheduleJob(reRunEmailJob, reRunEmailTrigger);
    }

    private static JobDetail createJobDetail(Class<? extends Job> jobClass, String jobName, String jobGroup) {
        return JobBuilder.newJob(jobClass)
                .withIdentity(jobName, jobGroup)
                .build();
    }

    private static Trigger createCronTrigger(String cronExpression, String triggerName, String triggerGroup) {
        return TriggerBuilder.newTrigger()
                .withIdentity(triggerName, triggerGroup)
                .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression))
                .build();
    }


    private static void scheduleJob(JobDetail jobDetail, Trigger trigger) throws SchedulerException {
        Scheduler scheduler = createScheduler();
        scheduler.scheduleJob(jobDetail, trigger);
        scheduler.start();
    }

    private static Scheduler createScheduler() throws SchedulerException {
        SchedulerFactory sf = new StdSchedulerFactory();
        return sf.getScheduler();
    }
}



