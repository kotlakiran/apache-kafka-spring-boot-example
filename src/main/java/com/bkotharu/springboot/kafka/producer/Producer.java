package com.bkotharu.springboot.kafka.producer;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.head;

import java.util.UUID;

import org.apache.kafka.common.header.Header;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.bkotharu.springboot.kafka.dto.MessageHeaders;

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

	/**
	 * Sends a plain string message with headers
	 * 
	 * @param msg
	 */
	public void sendMessageWithHeaders(String data, MessageHeaders headers) {
		Message<String> message = MessageBuilder.withPayload(data).setHeader(KafkaHeaders.TOPIC, TOPIC_NAME)
				.setHeader(KafkaHeaders.MESSAGE_KEY, "999").setHeader(KafkaHeaders.PARTITION_ID, 0)
				.setHeader(KafkaHeaders.CORRELATION_ID, headers.getCorrelationId())
				.setHeader("transactionId", headers.getTransactionId()                                                           ).build();

		System.out.println(String.format("Sending Message=%s to topic=%s", data, TOPIC_NAME));
		kafkaTemplate.send(message);
		System.out.println("Message sent successfully");
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
