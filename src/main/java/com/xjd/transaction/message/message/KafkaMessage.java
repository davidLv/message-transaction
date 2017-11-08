package com.xjd.transaction.message.message;

import lombok.Getter;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.xjd.transaction.message.Message;
import com.xjd.utils.basic.AssertUtils;
import com.xjd.utils.basic.JsonUtils;

/**
 * @author elvis.xu
 * @since 2017-11-07 11:19
 */
@Getter
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"timestamp", "key", "value"})
@JsonIgnoreProperties({"topic", "partitionKey", "partitionOrdered", "orderTimeoutInMillis", "ordered", "orderKey"})
public class KafkaMessage implements Message.OrderedMessage<String> {
	protected String key;
	protected String value;
	protected long timestamp;
	protected String topic;
	protected String partitionKey;
	protected boolean partitionOrdered = false;
	protected long orderTimeoutInMillis = -1;

	public KafkaMessage(String key, String value, Long timestamp) {
		AssertUtils.assertArgumentNonBlank(key, "key cannot be blank");
		this.key = key;
		this.value = value;
		this.timestamp = timestamp == null ? System.currentTimeMillis() : timestamp;
	}

	protected KafkaMessage() {
	}

	public KafkaMessage setTopic(String topic) {
		this.topic = topic;
		return this;
	}

	public KafkaMessage setPartition(String partitionKey) {
		return setPartition(partitionKey, false);
	}

	public KafkaMessage setPartition(String partitionKey, boolean partitionOrdered) {
		if (partitionOrdered) {
			AssertUtils.assertArgumentNonBlank(partitionKey, "partitionKey cannot be blank when partitionOrdered is true");
		}
		this.partitionKey = partitionKey;
		this.partitionOrdered = partitionOrdered;
		return this;
	}


	@Override
	public boolean isOrdered() {
		return partitionOrdered;
	}

	@Override
	public String getOrderKey() {
		return getTopic() + "-" + getPartitionKey();
	}

	@Override
	public long getOrderTimeoutInMillis() {
		return orderTimeoutInMillis;
	}

	public void setOrderTimeoutInMillis(long orderTimeoutInMillis) {
		this.orderTimeoutInMillis = orderTimeoutInMillis;
	}

	@Override
	public String toString() {
		return JsonUtils.toJson(this);
	}

	public static KafkaMessage fromString(String text) {
		return JsonUtils.fromJson(text, KafkaMessage.class);
	}
}
