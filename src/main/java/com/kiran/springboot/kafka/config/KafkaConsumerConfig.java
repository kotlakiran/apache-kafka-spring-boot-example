package com.kiran.springboot.kafka.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

@Configuration
public class KafkaConsumerConfig {

	@Value(value = "${kafka.bootstrapAddress}")
	private String bootstrapAddress;

	@Value(value = "${kafka.consumer.name}")
	private String groupName;

	@Value(value = "${kafka.polltimeout}")
	private String pollTimeout;

	@Value(value = "${kafka.concurrency}")
	private int concurrency;

	@Bean
	public ConsumerFactory<String, String> consumerFactory() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupName);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

		return new DefaultKafkaConsumerFactory<>(props);
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory());
		factory.setConcurrency(concurrency);
		// to consume multiple mesage at a time
		factory.setBatchListener(true);
		// factory.getContainerProperties().setAckMode(AckMode.);

		factory.setRecordFilterStrategy(new RecordFilterStrategy<String, String>() {

			@Override
			public boolean filter(ConsumerRecord<String, String> consumerRecord) {
				JsonObject jsonObject = new JsonParser().parse((String) (consumerRecord.value())).getAsJsonObject();
				if (null != jsonObject.get("test") && jsonObject.get("test").getAsString().equalsIgnoreCase("test")) {
					return true;
				} else {
					return false;
				}
			}
		});

		return factory;
	}
}
