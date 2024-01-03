# Email Campaign Scheduler

The Email Campaign Scheduler is a cron-based scheduler designed to automate the execution of email campaigns based on specified criteria. It performs the following tasks:

## Functionality Record Selection:

Picks up records from the campaign_run table with a status of 'RUNNING' and end_time as null. 

## Data Extraction:

The campaignId will be extracted from the campaign_run table, which will be helpful in extracting the email addresses and content from the selected campaign records. Stores extracted data in a HashMap.

## Job Execution:

Updates the status of the selected campaign_run records to **'RUNNING'**.

## Email Triggering:

Triggers emails using a batch process. Updates the email_status table with the status of each email (success/failure).

## Job Completion:

Updates the campaign_run table with the end_date, successful, status and failed email counts.

## How to Use Configuration:

Configure the scheduler using the provided `application.properties` file.
Set the database connection details and other necessary parameters.

## Run Scheduler:

Start the scheduler by configuring the desired cron expression. To run this scheduler, there should be campaign records in the campaign table with status as READY or START; else, the scheduler will not pick the campaign records. The following file will help you run the `campaign-connect` repository, which will be useful to create the data in the campaign table:

- [Campaign Connect Repository](https://github.com/marripavan9/campaign-connect)
- [Campaign Connect README](https://github.com/marripavan9/campaign-connect/blob/master/README.md)

## Monitor Logs:

Monitor the application logs for information on job execution and email triggering.

## Dependencies

- Java
- MySQL Database
- Quartz Scheduler

## Notes

- This scheduler is designed to automate email campaigns efficiently.
- Make sure to configure the database and scheduler properties accordingly.
- Logging is provided for tracking the execution and status of email campaigns.
