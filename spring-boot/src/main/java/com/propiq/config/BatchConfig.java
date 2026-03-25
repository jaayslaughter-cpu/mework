package com.propiq.config;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;
import org.springframework.boot.web.client.RestTemplateBuilder;

import java.time.Duration;

/**
 * Spring Batch configuration.
 * Jobs are disabled at startup (propiq.batch.job.enabled=false).
 * All 7 tasklets run on @Scheduled timers, not on job launch.
 */
@Configuration
@EnableBatchProcessing
public class BatchConfig {

    /**
     * Shared RestTemplate with sensible timeouts for external API calls.
     * Connection: 5s   |   Read: 10s
     */
    @Bean
    public RestTemplate restTemplate(RestTemplateBuilder builder) {
        return builder
                .connectTimeout(Duration.ofSeconds(5))
                .readTimeout(Duration.ofSeconds(10))
                .build();
    }
}
