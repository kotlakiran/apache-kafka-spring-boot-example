package com.kiran.springboot.kafka.consumer.executor;

import java.util.concurrent.Callable;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kiran.springboot.kafka.consumer.service.Consumerservice;

public class TestWorker implements Callable<String> {

	public static final Logger LOGGER = LoggerFactory.getLogger(TestWorker.class);



	private com.kiran.springboot.kafka.consumer.service.Consumerservice consumerservice;
	ConsumerRecord<String,String> record;


	public TestWorker(Consumerservice consumerservice, ConsumerRecord<String,String> record) {
		this.consumerservice = consumerservice;
		this.record = record;
	}


	@Override
	public String call() throws Exception {
		


		String currentThread = Thread.currentThread().getName();
		
		try {

			consumerservice.testSpringRetry(record);
		}
		catch (Exception ex) {
			String errorMessage = String.format("Exception while processing jms message");
			LOGGER.error(errorMessage, ex);
			//throw 
		}

		finally {
			LOGGER.info("done");
		}
		return "";
	}

}
