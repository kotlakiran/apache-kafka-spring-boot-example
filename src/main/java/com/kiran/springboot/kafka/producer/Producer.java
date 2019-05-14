package com.kiran.springboot.kafka.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

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
		ListenableFuture<SendResult<String, String>> sendResult = kafkaTemplate.send(TOPIC_NAME, msg);
		System.out.println(String.format("Message sent successfully sendResult=%s", sendResult.toString()));
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
