package com.zemoso.job.email;


import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;

public class EmailSender {
    public static String listId = "f5efbdca5e"; // Replace with your Audience ID
   // public static String apiKey = "e784eac9e4b10a8880794d46ea7eea32";
    public static String apiKey = "ec10a5c3748008a6e78f5bebf88fd380-us21";

    public static String senderEmail = "pavan.marri@zemosolabs.com";
    public static String recipientEmail = "marripavan11@gmai.com";

    public static void main(String[] args) {
        // Mailchimp Transactional API endpoint
        String apiUrl = "https://mandrillapp.com/api/1.0/messages/send.json";
        // JSON payload for the API request
        String payload = String.format(
                "{" +
                        "    \"key\": \"%s\"," +
                        "    \"message\": {" +
                        "        \"from_email\": \"%s\"," +
                        "        \"to\": [{" +
                        "            \"email\": \"%s\"" +
                        "        }]," +
                        "        \"subject\": \"Your Subject\"," +
                        "        \"text\": \"Your email body text\"" +
                        "    }" +
                        "}",
                apiKey, senderEmail, recipientEmail
        );

        // Send the HTTP POST request
        try {
            HttpClient httpClient = HttpClients.createDefault();
            HttpPost httpPost = new HttpPost(apiUrl);
            httpPost.setEntity(new StringEntity(payload));

            HttpResponse response = httpClient.execute(httpPost);

            // Check the response
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200) {
                System.out.println("Email sent successfully!");
            } else {
                System.out.println("Failed to send email. Status code: " + statusCode);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
