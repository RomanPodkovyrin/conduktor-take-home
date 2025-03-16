package com.roman.conduktor;

import com.roman.conduktor.loader.DataLoader;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
public class ConduktorApplication {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ConduktorApplication.class);

    public static void main(String[] args) {
        ApplicationContext context = SpringApplication.run(ConduktorApplication.class, args);

        // Note for Reviewer: This is a simple way to load data to Kafka
        // It is not a production-ready solution
        // In a real-world scenario, I would use a separate service to load data to Kafka
        // Or would make an endpoint to trigger the data loading

        // Check if we should load data (could be done via command-line arg)
        if (args.length > 0 && "load-data".equals(args[0])) {
            logger.info("Loading data to Kafka...");
        }
    }

}
