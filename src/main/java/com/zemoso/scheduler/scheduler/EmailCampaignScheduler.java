package com.zemoso.scheduler.scheduler;

import com.zemoso.scheduler.job.EmailCampaignJob;
import com.zemoso.scheduler.job.ReRunEmailCampaignJob;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

public class EmailCampaignScheduler {

    public static void main(String[] args) throws SchedulerException {
        // Create job and trigger
        JobDetail job = JobBuilder.newJob(EmailCampaignJob.class)
                .withIdentity("myJob", "myJobGroup")
                .build();

        String regularCronExpression = "0 0/1 * * * ?"; // Every 15 minutes
        CronTrigger trigger = TriggerBuilder.newTrigger()
                .withIdentity("myTrigger", "myTriggerGroup")
                .withSchedule(CronScheduleBuilder.cronSchedule(regularCronExpression))
                .build();

        // Get scheduler instance and start
        SchedulerFactory sf = new StdSchedulerFactory();
        org.quartz.Scheduler scheduler = sf.getScheduler();
        scheduler.start();

        // Schedule the job
        scheduler.scheduleJob(job, trigger);

        // Define Job Details for the second job
        JobDetail jobDetail2 = JobBuilder.newJob(ReRunEmailCampaignJob.class)
                .withIdentity("emailTriggerJob", "group2")
                .build();

      //  String retryCronExpression = "0 */4 * * *"; // for every 4 hours
        String retryCronExpression = "0 0/1 * * * ?"; // Every 15 minutes

        Trigger trigger2 = TriggerBuilder.newTrigger()
                .withIdentity("trigger2", "group2")
                .withSchedule(CronScheduleBuilder.cronSchedule(retryCronExpression))
                .build();

        // Schedule the second job with the trigger
       // scheduler.scheduleJob(jobDetail2, trigger2);

        // Start the scheduler
        scheduler.start();
    }



}


