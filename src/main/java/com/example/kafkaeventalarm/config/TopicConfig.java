package com.example.kafkaeventalarm.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class TopicConfig {
    @Value(value = "${kafka.bootstrapAddress}")
    private String bootstrapAddress;

    @Value("${order.topic.name}")
    private String inputTopic;

    @Value("${return.topic.name}")
    private String returnTopic;

    @Bean
    public NewTopic orderTopic() {
        return TopicBuilder.name(inputTopic)
                .partitions(1)
                .replicas(1)
                //.config("retention.ms", "60000")
                //config("segment.ms", "60000")
                .build();
    }

    @Bean
    public NewTopic returnTopic() {
        return TopicBuilder.name(returnTopic)
                .partitions(1)
                .replicas(1)
                //.config("retention.ms", "60000")
                //config("segment.ms", "60000")
                .build();
    }

    //If not using spring boot
    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        return new KafkaAdmin(configs);
    }
}
