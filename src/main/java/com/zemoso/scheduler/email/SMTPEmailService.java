package com.zemoso.scheduler.email;

import com.zemoso.scheduler.operation.PropertiesLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

import static com.zemoso.scheduler.constants.FieldNames.*;

public class SMTPEmailService {

    private static final Logger logger = LoggerFactory.getLogger(SMTPEmailService.class);


    public static boolean sendEmail(String email, String body) {
        try {
            Properties prop = loadEmailProperties();
            String[] propertyKeys = {EMAIL_USERNAME_KEY, EMAIL_PASSWORD_KEY, SMTP_HOST_KEY, SMTP_PORT_KEY, SMTP_AUTH_KEY, SMTP_SECURE_KEY, EMAIL_SUBJECT_KEY};

            for (String key : propertyKeys) {
                String value = prop.getProperty(key);
                validateProperty(value, key);
            }

            prop.put("mail.smtp.host", prop.getProperty(SMTP_HOST_KEY));
            prop.put("mail.smtp.port", prop.getProperty(SMTP_PORT_KEY));
            prop.put("mail.smtp.auth", prop.getProperty(SMTP_AUTH_KEY));
            prop.put("mail.smtp.starttls.enable", prop.getProperty(SMTP_SECURE_KEY));

            Session session = createSession(prop.getProperty(EMAIL_USERNAME_KEY), prop.getProperty(EMAIL_PASSWORD_KEY), prop);
            Message message = createEmailMessage(session, prop.getProperty(EMAIL_USERNAME_KEY), email, prop.getProperty(EMAIL_SUBJECT_KEY), body);
            Transport.send(message);
            return true;
        } catch (MessagingException | IllegalArgumentException e) {
            logger.error("Error sending email to {}: {}", email, e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Unexpected error sending email to {}: {}", email, e.getMessage(), e);
        }
        return false;
    }

    private static Properties loadEmailProperties() {
        return PropertiesLoader.loadProperties();
    }

    private static Session createSession(String username, String password, Properties prop) {
        return Session.getInstance(prop, new javax.mail.Authenticator() {
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(username, password);
            }
        });
    }

    private static Message createEmailMessage(Session session, String from, String to, String subject, String body)
            throws MessagingException {
        Message message = new MimeMessage(session);
        message.setFrom(new InternetAddress(from));
        message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to));
        message.setSubject(subject);
        message.setText(body);
        return message;
    }

    private static void validateProperty(String property, String propertyName) {
        if (property == null || property.trim().isEmpty()) {
            throw new IllegalArgumentException("Invalid or missing value for property: " + propertyName);
        }
    }
}
