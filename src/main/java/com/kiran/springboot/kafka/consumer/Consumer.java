package com.kiran.springboot.kafka.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import com.kiran.springboot.kafka.constants.ApplicationConstants;

@Component
public class Consumer {
	
	
	
	/**
	 * Consume message without headers
	 * 
	 * @param message
	 */
	@KafkaListener(topics = ApplicationConstants.TOPIC_NAME)
	public void listen(@Payload String payload,Acknowledgment ack) {
		System.out.println(
				String.format("Message Consumed with  payload=%s",payload));
		ack.acknowledge();
	}
}
