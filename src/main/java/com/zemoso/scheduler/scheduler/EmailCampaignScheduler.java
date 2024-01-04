package com.zemoso.scheduler.scheduler;

import com.zemoso.scheduler.job.EmailCampaignJob;
import com.zemoso.scheduler.job.ReRunEmailCampaignJob;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

public class EmailCampaignScheduler {

    public static void main(String[] args) {
        try {
            scheduleRegularEmailJob();
            scheduleReRunEmailJob();
        } catch (SchedulerException e) {
            handleSchedulerException(e);
        }
    }

    private static void scheduleRegularEmailJob() throws SchedulerException {
        JobDetail regularEmailJob = createJobDetail(EmailCampaignJob.class, "myJob", "myJobGroup");
        Trigger regularEmailTrigger = createCronTrigger("0 0/1 * * * ?", "myTrigger", "myTriggerGroup");
        scheduleJob(regularEmailJob, regularEmailTrigger);
    }

    private static void scheduleReRunEmailJob() throws SchedulerException {
        JobDetail reRunEmailJob = createJobDetail(ReRunEmailCampaignJob.class, "emailTriggerJob", "group2");
        Trigger reRunEmailTrigger = createCronTrigger("0 0/1 * * * ?", "trigger2", "group2");
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

    private static void handleSchedulerException(SchedulerException e) {
        e.printStackTrace();
    }
}



