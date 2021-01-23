package com.kiran.springboot.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.retry.annotation.EnableRetry;

@EnableKafka
@SpringBootApplication
@EnableAutoConfiguration
@EnableRetry
@EnableAspectJAutoProxy
public class ApacheKafkaExample {

	public static void main(String[] args) {
		SpringApplication.run(ApacheKafkaExample.class, args);
	}
}
