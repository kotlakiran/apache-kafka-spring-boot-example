package com.kiran.springboot.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication
@EnableAutoConfiguration
public class ApacheKafkaExample {

	public static void main(String[] args) {
		SpringApplication.run(ApacheKafkaExample.class, args);
	}
}
