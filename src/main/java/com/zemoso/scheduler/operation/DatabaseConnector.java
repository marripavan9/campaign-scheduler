package com.zemoso.scheduler.operation;

import com.zemoso.scheduler.constants.FieldNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DatabaseConnector {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseConnector.class);

    public static Connection getConnection() throws SQLException {
        Properties props = PropertiesLoader.loadProperties();

        try (Connection connection = DriverManager.getConnection(
                props.getProperty(FieldNames.DB_URL),
                props.getProperty(FieldNames.DB_USERNAME),
                props.getProperty(FieldNames.DB_PASSWORD))) {
            return connection;
        } catch (SQLException e) {
            logger.error("Error connecting to the database: {}", e.getMessage(), e);
            throw e;
        }
    }
}


