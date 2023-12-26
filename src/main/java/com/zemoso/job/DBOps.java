package com.zemoso.job;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DBOps {

    public static Connection getConnection() throws SQLException {
        Properties props = loadProperties();
        return DriverManager.getConnection(props.getProperty("db.url"), props.getProperty("db.username"), props.getProperty("db.password"));
    }

    private static Properties loadProperties() {
        Properties props = new Properties();
        try (InputStream input = DBOps.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input != null) {
                props.load(input);
            } else {
                System.out.println("Sorry, unable to find application.properties");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return props;
    }
}
