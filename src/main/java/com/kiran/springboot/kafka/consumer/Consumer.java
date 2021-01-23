package com.kiran.springboot.kafka.consumer;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import com.kiran.springboot.kafka.constants.ApplicationConstants;
import com.kiran.springboot.kafka.consumer.exception.TypeTwoException;
import com.kiran.springboot.kafka.consumer.executor.KafkaTestExecutorService;
import com.kiran.springboot.kafka.consumer.executor.TestWorker;
import com.kiran.springboot.kafka.consumer.service.Consumerservice;

@Component
public class Consumer {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTestExecutorService.class);

	@Autowired
	private Consumerservice consumerservice;
	
	@Autowired
	private KafkaTestExecutorService kafkaTestExecutorService;

	/**
	 * Consume message without headers
	 * 
	 * @param message
	 * @throws Exception
	 */
	@KafkaListener(topics = ApplicationConstants.TOPIC_NAME)
	public void listen(@Payload List<ConsumerRecord<String, String>> records) throws Exception {

		/*
		 * CompletableFuture.runAsync(() -> { LOGGER.
		 * info("Message Consumed with   record.partition ={} ,Thread.currentThread().getName()= {}"
		 * , record.partition(), Thread.currentThread().getName()); try {
		 * consumerservice.testSpringRetry(record); } catch (TypeTwoException e) {
		 * e.printStackTrace(); } });
		 */
		
		records.forEach(record->{
			LOGGER.info("Message Consumed with   record.partition ={} ,Thread.currentThread().getName()= {}", record.partition(), Thread.currentThread().getName());
			TestWorker testWorkeTask=new com.kiran.springboot.kafka.consumer.executor.TestWorker(consumerservice, record);
			kafkaTestExecutorService.submit(testWorkeTask);		
		});


	}
}
