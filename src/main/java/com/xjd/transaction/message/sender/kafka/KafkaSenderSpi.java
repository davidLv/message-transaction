package com.xjd.transaction.message.sender.kafka;

import java.util.concurrent.Future;

import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.xjd.transaction.message.message.KafkaMessage;
import com.xjd.transaction.message.sender.Context;
import com.xjd.transaction.message.sender.spi.SenderSpi;
import com.xjd.utils.basic.AssertUtils;

/**
 * @author elvis.xu
 * @since 2017-11-07 16:50
 */
@Slf4j
public class KafkaSenderSpi implements SenderSpi<KafkaMessage> {
	protected Producer<String, String> kafkaProducer;

	public KafkaSenderSpi(Producer<String, String> kafkaProducer) {
		AssertUtils.assertArgumentNonNull(kafkaProducer, "kafkaProducer cannot be null.");
		this.kafkaProducer = kafkaProducer;
	}

	@Override
	public void send(Context.OneContext context, KafkaMessage message) throws Throwable {
		Future<RecordMetadata> future = kafkaProducer.send(new ProducerRecord<String, String>(message.getTopic(), message.getPartitionKey(), message.toString()));
		kafkaProducer.flush();
		RecordMetadata metadata = future.get();
		if (log.isTraceEnabled()) {
			log.trace("kafka message sent success: id={}, key={}", metadata, message.getKey());
		}
	}
}
