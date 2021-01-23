package com.kiran.springboot.kafka.producer;

import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.kiran.springboot.kafka.constants.ApplicationConstants;
import com.kiran.springboot.kafka.consumer.executor.TestWorker;

@Component
public class Producer {

	private static final String TOPIC_NAME = "local.topic";

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;
	


	/**
	 * Sends a plain string message to the topic
	 * 
	 * @param msg
	 */
	public void sendMessage(String msg) {
		
		
		try {
			//System.out.println(Thread.currentThread().getName());
			for (int i = 0; i < 10000; i++) {			
			CompletableFuture.runAsync(() -> {	
				//System.out.println(Thread.currentThread().getName());
		ListenableFuture<SendResult<String, String>> sendResult = kafkaTemplate.send(ApplicationConstants.TOPIC_NAME, msg);
		
			  }); 
			}
		}
		
		catch (Exception e) {
			// TODO: handle exception
		}
		//System.out.println(String.format("Message sent successfully sendResult=%s", msg));
	}

	

	public void sendAsyncMessage(String message) {

		ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(TOPIC_NAME, message);

		future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

			@Override
			public void onSuccess(SendResult<String, String> result) {
				System.out.println(
						"Sent message=[" + message + "] with offset=[" + result.getRecordMetadata().offset() + "]");
			}

			@Override
			public void onFailure(Throwable ex) {
				System.out.println("Unable to send message=[" + message + "] due to : " + ex.getMessage());
			}
		});
	}

}
