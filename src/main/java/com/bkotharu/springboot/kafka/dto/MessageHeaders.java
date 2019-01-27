package com.bkotharu.springboot.kafka.dto;

public class MessageHeaders {

	private String correlationId;

	private String transactionId;

	public MessageHeaders(String correlationId, String transactionId) {
		super();
		this.correlationId = correlationId;
		this.transactionId = transactionId;
	}

	public String getCorrelationId() {
		return correlationId;
	}

	public void setCorrelationId(String correlationId) {
		this.correlationId = correlationId;
	}

	public String getTransactionId() {
		return transactionId;
	}

	public void setTransactionId(String transactionId) {
		this.transactionId = transactionId;
	}

}
