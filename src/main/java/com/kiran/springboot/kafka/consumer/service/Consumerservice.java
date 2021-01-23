package com.kiran.springboot.kafka.consumer.service ;

import java.util.Random;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import com.kiran.springboot.kafka.consumer.exception.TypeOneException;
import com.kiran.springboot.kafka.consumer.exception.TypeTwoException;
import com.kiran.springboot.kafka.consumer.executor.KafkaTestExecutorService;



@Service
public class Consumerservice {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTestExecutorService.class);

	
	@Retryable(label = "test", exclude = { TypeOneException.class}, include = {
			TypeTwoException.class }, maxAttempts = 3, backoff = @Backoff(delay = 10000, multiplier = 2, maxDelay = 20000))
	public void testSpringRetry(ConsumerRecord<String,String> record) throws TypeTwoException {
		
		Random rn = new Random();
		int  randamValue=rn.nextInt(10 - 1 + 1) + 1;
		
		if(randamValue  > 8) {
			LOGGER.info("TypeTwoException  sending to retry ");
			throw new TypeTwoException();
		}
		
	}
	
	@Recover
	public void recover(final Exception e, ConsumerRecord<String,String> record) throws Throwable {

		String  topicType=new String(record.headers().lastHeader("jms_queuetype").value());		
		LOGGER.info("fall back method for recovery");
		if("KAFKA_DLT".equalsIgnoreCase(topicType)) {
			//
			LOGGER.info("reply frame work ");

			return ;
		}
		LOGGER.info("publish to retry DLT ");
	}
}
