package io.comhub.loadgen;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Spring Boot entry point for the Comhub load generator.
 *
 * @author Roman Hadiuchko
 */
@SpringBootApplication
public class LoadgenApplication {

    public static void main(String[] args) {
        SpringApplication.run(LoadgenApplication.class, args);
    }
}
