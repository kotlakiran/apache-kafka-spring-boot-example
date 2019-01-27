package com.bkotharu.springboot.kafka.rest;

import org.apache.kafka.common.header.Headers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.bkotharu.springboot.kafka.dto.MessageHeaders;
import com.bkotharu.springboot.kafka.producer.Producer;

@RestController
@RequestMapping("/publish")
public class MessageController {

	@Autowired
	private Producer producer;

	@GetMapping("/message/{message}")
	public String publishMessage(@PathVariable final String message) {
		producer.sendMessage(message);
		return "Message Published Successfully";
	}

	@GetMapping("/messageWithHeaders/{message}")
	public String publishMessageWithHeaders(@PathVariable final String message,
			@RequestHeader(name = "correlationId") String correlationId,
			@RequestHeader(name = "transactionId") String transactionId) {
		MessageHeaders messageHeaders = new MessageHeaders(correlationId, transactionId);
		producer.sendMessageWithHeaders(message, messageHeaders);
		return "Message Published Successfully";
	}
}
