package com.bkotharu.springboot.kafka.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import com.bkotharu.springboot.kafka.constants.ApplicationConstants;
import com.bkotharu.springboot.kafka.producer.Producer;

@Component
public class Consumer {

	/**
	 * Consume message without headers
	 * 
	 * @param message
	 */
	@KafkaListener(topics = ApplicationConstants.TOPIC_NAME, groupId = ApplicationConstants.GROUP_NAME)
	public void listen(String message) {
		System.out.println(
				String.format("Message Consumed with Group=%s, Message=%s", ApplicationConstants.GROUP_NAME, message));
	}

	/**
	 * Consume message with Headers
	 * 
	 * @param message
	 * @param partition
	 */
	@KafkaListener(topics = ApplicationConstants.TOPIC_WITH_HEADERS_NAME)
	public void listenWithHeaders(@Payload String message, @Header(KafkaHeaders.CORRELATION_ID) String correlationId) {
		System.out.println("Received Message: " + message + " with correlationId: " + correlationId);
	}

	/**
	 * Consuming messages from a Partition
	 * 
	 * @param message
	 * @param partition
	 */
	@KafkaListener(topicPartitions = @TopicPartition(topic = "topicName", partitionOffsets = {
			@PartitionOffset(partition = "0", initialOffset = "0"),
			@PartitionOffset(partition = "3", initialOffset = "0") }))
	public void listenToParition(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
		System.out.println("Received Messasge: " + message + "from partition: " + partition);
	}

	/**
	 * Listeners listening to filtered messages
	 * 
	 * @param message
	 */
	@KafkaListener(topics = "topicName", containerFactory = "filterKafkaListenerContainerFactory")
	public void listenToFilteredMessages(String message) {
		// handle message
	}

}
