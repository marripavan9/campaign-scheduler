package com.zemoso.job;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

public class Scheduler {

    public static void main(String[] args) throws SchedulerException {
        // Create job and trigger
        JobDetail job = JobBuilder.newJob(MyJob.class)
                .withIdentity("myJob", "myJobGroup")
                .build();

        String cronExpression = "0 0/1 * * * ?"; // Every 15 minutes
        CronTrigger trigger = TriggerBuilder.newTrigger()
                .withIdentity("myTrigger", "myTriggerGroup")
                .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression))
                .build();

        // Get scheduler instance and start
        SchedulerFactory sf = new StdSchedulerFactory();
        org.quartz.Scheduler scheduler = sf.getScheduler();
        scheduler.start();

        // Schedule the job
        scheduler.scheduleJob(job, trigger);
    }
}


