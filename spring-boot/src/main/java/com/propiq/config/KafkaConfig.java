package com.propiq.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Kafka topic definitions for the PropIQ bet pipeline.
 *
 * bet_queue     — All approved bets from the 10-agent army
 * abort_queue   — Emergency cancellations (late scratches, roof changes)
 * dfs_alerts    — Formatted DFS picks for PrizePicks / Underdog Fantasy
 */
@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic betQueueTopic() {
        return TopicBuilder.name("bet_queue")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic abortQueueTopic() {
        return TopicBuilder.name("abort_queue")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic dfsAlertsTopic() {
        return TopicBuilder.name("dfs_alerts")
                .partitions(1)
                .replicas(1)
                .build();
    }
}
