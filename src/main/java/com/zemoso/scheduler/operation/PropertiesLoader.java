package com.zemoso.scheduler.operation;

import com.zemoso.scheduler.constants.FieldNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesLoader {

    private static final Logger logger = LoggerFactory.getLogger(PropertiesLoader.class);

    public static Properties loadProperties() {
        Properties props = new Properties();
        try (InputStream input = PropertiesLoader.class.getClassLoader().getResourceAsStream(FieldNames.CONFIG_FILE_NAME)) {
            if (input != null) {
                props.load(input);
            } else {
                logger.info("Sorry, unable to find {}", FieldNames.CONFIG_FILE_NAME);
            }
        } catch (IOException e) {
            logger.error("Error loading properties: {}", e.getMessage(), e);
        }
        return props;
    }

    public static int getThreadPoolSize() {
        Properties properties = loadProperties();
        return Integer.parseInt(properties.getProperty(FieldNames.POOL_SIZE, FieldNames.DEFAULT_POOL_SIZE));
    }
}


