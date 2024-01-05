package com.zemoso.scheduler.constants;

public class FieldNames {

    // Campaign Table
    public static final String CAMPAIGN_ID = "id";
    public static final String CAMPAIGN_CONTENT = "content";
    public static final String CAMPAIGN_EMAIL_IDS = "email_ids";
    public static final String CAMPAIGN_STATUS = "status";
    public static final String CAMPAIGN_START_DATE = "start_date";
    public static final String CAMPAIGN_END_DATE = "end_date";

    // CampaignRun Table
    public static final String CAMPAIGN_RUN_ID = "id";
    public static final String CAMPAIGN_RUN_CAMPAIGN_ID = "campaign_id";
    public static final String CAMPAIGN_RUN_START_TIME = "start_time";
    public static final String CAMPAIGN_RUN_END_TIME = "end_time";
    public static final String CAMPAIGN_RUN_SUCCESS_COUNT = "success_count";
    public static final String CAMPAIGN_RUN_FAILURE_COUNT = "failure_count";
    public static final String CAMPAIGN_RUN_RETRY_COUNT = "retry_count";
    public static final String CAMPAIGN_RUN_STATUS = "status";

    public static final String EMAIL_USERNAME_KEY = "email.username";
    public static final String EMAIL_PASSWORD_KEY = "email.password";
    public static final String SMTP_HOST_KEY = "smtp.host";
    public static final String SMTP_PORT_KEY = "smtp.port";
    public static final String SMTP_AUTH_KEY = "smtp.auth";
    public static final String SMTP_SECURE_KEY = "smtp.secure";
    public static final String EMAIL_SUBJECT_KEY = "email.subject";

    public static final String COMMA = ",";
    public static final String RUNNING = "RUNNING";
    public static final String SUCCESS = "SUCCESS";
    public static final String PARTIALLY_SUCCESS = "PARTIALLY_SUCCESS";
    public static final String FAILED = "FAILED";
    public static final String READY = "READY";
    public static final String RESUME = "RESUME";


    public static final String DB_URL = "db.url";
    public static final String DB_USERNAME = "db.username";
    public static final String DB_PASSWORD = "db.password";
    public static final String POOL_SIZE = "pool.size";
    public static final String DEFAULT_POOL_SIZE = "10";
    public static final String CONFIG_FILE_NAME = "application.properties";
    public static final String EMAIL_SENT = "SENT";
    public static final String EMAIL_FAILED = "FAILED";
}
