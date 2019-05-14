package com.kiran.springboot.kafka.rest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kiran.springboot.kafka.producer.Producer;

@RestController
@RequestMapping("/publish")
public class MessageController {

	@Autowired
	private Producer producer;

	@PostMapping("/message")
	public String publishMessage(@RequestBody String payload) {
		producer.sendMessage(payload);
		return "Message Published Successfully";
	}
}
