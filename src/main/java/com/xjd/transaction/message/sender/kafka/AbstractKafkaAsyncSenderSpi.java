package com.xjd.transaction.message.sender.kafka;

import java.util.concurrent.Future;

import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.xjd.transaction.message.message.KafkaMessage;
import com.xjd.transaction.message.sender.Context;
import com.xjd.transaction.message.sender.spi.SuggestedSenderSpi;
import com.xjd.utils.basic.AssertUtils;

/**
 * @author elvis.xu
 * @since 2017-11-07 17:41
 */
@Slf4j
public abstract class AbstractKafkaAsyncSenderSpi implements SuggestedSenderSpi<KafkaMessage> {
	protected Producer<String, String> kafkaProducer;

	public AbstractKafkaAsyncSenderSpi(Producer<String, String> kafkaProducer) {
		AssertUtils.assertArgumentNonNull(kafkaProducer, "kafkaProducer cannot be null.");
		this.kafkaProducer = kafkaProducer;
	}

	@Override
	public void doSend(Context.OneContext context, KafkaMessage message) throws Throwable {
		Future<RecordMetadata> future = kafkaProducer.send(new ProducerRecord<String, String>(message.getTopic(), message.getPartitionKey(), message.toString()));
		kafkaProducer.flush();
		RecordMetadata metadata = future.get();
		if (log.isTraceEnabled()) {
			log.trace("kafka message sent success: id={}, key={}", metadata, message.getKey());
		}
	}

}
