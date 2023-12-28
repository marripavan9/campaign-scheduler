package com.zemoso.scheduler.email;

import com.zemoso.scheduler.operation.DatabaseOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

public class SMTPEmailService {

    private static final Logger logger = LoggerFactory.getLogger(SMTPEmailService.class);

    public static boolean sendEmail(String email, String body) {
        Properties prop = DatabaseOperation.loadProperties();
        String username = prop.getProperty("email.username");
        String password = prop.getProperty("email.password");

        prop.put("mail.smtp.host", "smtp.gmail.com");
        prop.put("mail.smtp.port", "587");
        prop.put("mail.smtp.auth", "true");
        prop.put("mail.smtp.starttls.enable", "true");

        Session session = Session.getInstance(prop,
                new javax.mail.Authenticator() {
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(username, password);
                    }
                });

        try {
            Message message = new MimeMessage(session);
            message.setFrom(new InternetAddress(username));
            message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(email));
            message.setSubject("Campaign");
            message.setText(body);
            Transport.send(message);
            logger.info(String.format("Email sent successfully to %s", email));
            return true;
        } catch (MessagingException e) {
            logger.error(String.format("Exception raised while sending an email to %s %s", email, e.getMessage()));
            e.printStackTrace();
        }
        return false;
    }
}
