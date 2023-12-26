**Email Campaign Scheduler:**

The Email Campaign Scheduler is a cron-based scheduler designed to automate the execution of email campaigns based on specified criteria. It performs the following tasks:

**Functionality Record Selection:**

Picks up records from the campaign table with a status of 'success'. Filters records based on the start_date and end_date.

**Data Extraction:**

Extracts email addresses and content from the selected campaign records. Stores extracted data in a HashMap.

**Job Execution:**

Updates the status of the selected campaign records to 'running'. Creates a record in the campaign_run table with default values and the start_date.

**Email Triggering:**

Triggers emails using a batch process. Updates the email_status table with the status of each email (success/failure).

**Job Completion:**

Updates the campaign_run table with the end_date, successful, and failed email counts.
Updates the status of the campaign records in the campaign table to 'success'.

**How to Use Configuration:**

Configure the scheduler using the provided application.properties file.
Set the database connection details and other necessary parameters.

**Run Scheduler:**

Start the scheduler by configuring the desired cron expression.

**Monitor Logs:**

Monitor the application logs for information on job execution and email triggering.

**Dependencies**

Java,
MySQL Database,
Quartz Scheduler.

**Notes**

This scheduler is designed to automate email campaigns efficiently.
Make sure to configure the database and scheduler properties accordingly.
Logging is provided for tracking the execution and status of email campaigns.
