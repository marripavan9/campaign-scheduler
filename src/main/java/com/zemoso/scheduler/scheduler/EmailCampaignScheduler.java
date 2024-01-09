package com.zemoso.scheduler.scheduler;

import com.zemoso.scheduler.constants.FieldNames;
import com.zemoso.scheduler.job.EmailCampaignJob;
import com.zemoso.scheduler.job.ReRunEmailCampaignJob;
import com.zemoso.scheduler.model.Campaign;
import com.zemoso.scheduler.operation.CampaignOperation;
import com.zemoso.scheduler.operation.DatabaseConnector;
import com.zemoso.scheduler.operation.PropertiesLoader;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;

public class EmailCampaignScheduler {

    private static final Logger logger = LoggerFactory.getLogger(EmailCampaignScheduler.class);


    public static void main(String[] args) {
        try {
            scheduleRegularEmailJob();
            scheduleReRunEmailJob();
        } catch (SchedulerException se) {
            logger.error("Error scheduling job: {}", se.getMessage(), se);
        } catch (SQLException sqlException) {
            logger.error("Error in database operation: {}", sqlException.getMessage(), sqlException);
        }
    }

    private static void scheduleRegularEmailJob() throws SchedulerException, SQLException {
        List<Campaign> campaigns = CampaignOperation.fetchCampaignRecords(DatabaseConnector.getConnection());
        for (Campaign campaign : campaigns) {
            scheduleEmailJobForCampaign(campaign);
        }
    }

    private static void scheduleEmailJobForCampaign(Campaign campaign) throws SchedulerException {
        String cronExpression = campaign.getFrequency();
        JobDetail emailJob = createEmailJobDetail(campaign);
        Trigger emailTrigger = createEmailCronTrigger(cronExpression, campaign);
        scheduleJob(emailJob, emailTrigger);
    }

    private static JobDetail createEmailJobDetail(Campaign campaign) {
        JobDetail emailJob = createJobDetail(EmailCampaignJob.class, "emailJob_" + campaign.getId(), "emailJobGroup");
        emailJob.getJobDataMap().put(FieldNames.CAMPAIGN, campaign);
        emailJob.getJobDataMap().put(FieldNames.FIRST_RUN, true);
        emailJob.getJobDataMap().put(FieldNames.RUN_TIME, LocalDateTime.now());
        return emailJob;
    }

    private static Trigger createEmailCronTrigger(String cronExpression, Campaign campaign) {
        return createCronTrigger(cronExpression, "emailTrigger_" + campaign.getId(), "emailTriggerGroup");
    }

    private static void scheduleReRunEmailJob() throws SchedulerException {
        Properties properties = loadReRunEmailProperties();
        JobDetail reRunEmailJob = createReRunEmailJobDetail();
        Trigger reRunEmailTrigger = createReRunEmailCronTrigger(properties);
        scheduleJob(reRunEmailJob, reRunEmailTrigger);
    }

    private static Properties loadReRunEmailProperties() {
        return PropertiesLoader.loadProperties();
    }

    private static JobDetail createReRunEmailJobDetail() {
        return createJobDetail(ReRunEmailCampaignJob.class, "emailTriggerJob", "group2");
    }

    private static Trigger createReRunEmailCronTrigger(Properties properties) {
        return createCronTrigger(properties.getProperty(FieldNames.RERUN_CRON), "trigger2", "group2");
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



