package com.example.kafkaeventalarm;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class KafkaKSQLApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaKSQLApplication.class, args);
    }

}
