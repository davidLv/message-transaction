package com.xjd.transaction.message.receiver.kafka;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import com.xjd.transaction.message.MessageReceiver;
import com.xjd.transaction.message.message.KafkaMessage;
import com.xjd.transaction.message.receiver.spi.ReceiverSpi;

/**
 * @author elvis.xu
 * @since 2017-11-08 17:03
 */
@Slf4j
public abstract class AbstractKafkaReceiver implements ReceiverSpi<KafkaMessage>, AutoCloseable {
	protected Consumer<String, String> consumer;
	protected Executor executor;
	protected MessageReceiver<KafkaMessage> receiver;
	protected volatile boolean closed = false;

	protected Map<KafkaMessage, ConsumerRecord> recordMap = new ConcurrentHashMap<>();

	public AbstractKafkaReceiver(Consumer<String, String> consumer, Executor executor, MessageReceiver<KafkaMessage> receiver) {
		this.consumer = consumer;
		this.executor = executor;
		this.receiver = receiver;
		init();
	}

	protected void init() {
		Thread bossThread = new Thread(() -> {
			while (!closed) {
				try {
					consumeMessage();
				} catch (Throwable t) {
					handleError(t);
				}
			}
		}, "KFK_RCV_BOSS");
		bossThread.setDaemon(true);
		bossThread.start();
	}

	protected void consumeMessage() {
		ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
		Set<TopicPartition> partitions = records.partitions();
		int size = partitions.size();
		List<CountDownLatch> latches = new ArrayList<>(size - 1);
		for (TopicPartition partition : partitions) {
			size --;
			if (size == 0) {
				consumePartitionRecords(partition, records.records(partition), false);
			} else {
				consumePartitionRecords(partition, records.records(partition), true);
			}
		}
		for (CountDownLatch latch : latches) {
			while (true) {
				try {
					latch.await();
					break;
				} catch (InterruptedException e) {
					// nothing
				}
			}
		}
	}

	protected CountDownLatch consumePartitionRecords(TopicPartition partition, List<ConsumerRecord<String, String>> records, boolean async) {
		CountDownLatch latch = async ? new CountDownLatch(1) : null;
		Runnable task = () -> {
			try {
				for (ConsumerRecord<String, String> record : records) {
					KafkaMessage message = null;
					try {
						String value = record.value();
						message = KafkaMessage.fromString(value);
					} catch (Throwable t) {
						log.warn("message parse error, skipped!: id={}, value={}", id(record), record.value(), t);
						commitMessage(partition, new OffsetAndMetadata(record.offset() + 1));
						continue;
					}
					try {
						recordMap.put(message, record);
						try {
							process(message);
						} finally {
							recordMap.remove(message);
						}
					} catch (Throwable t) {
						handleError(t);
						break;
					}
				}
			} catch (Throwable t) {
				// impossible in normal
				log.error("", t);
			} finally {
				if (latch != null) latch.countDown();
			}
		};
		if (async) {
			executor.execute(task);
		} else {
			task.run();
		}
		return latch;
	}

	protected void commitMessage(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
		synchronized (consumer) { // consumer is not ThreadSafe
			consumer.commitSync(Collections.singletonMap(partition, offsetAndMetadata));
		}
	}

	protected String id(ConsumerRecord<String, String> record) {
		return record.topic() + "-" + record.partition() + "@" + record.offset();
	}

	public void handleError(Throwable t) {
		log.error("", t);
	}

	@Override
	public void close() {
		closed = true;
	}

	@Override
	public MessageReceiver<KafkaMessage> getReceiver() {
		return receiver;
	}


	@Override
	public boolean doAfterProcess(KafkaMessage message, Throwable t) {
		return true;
	}

	@Override
	public void finishMessage(KafkaMessage message) {
		ConsumerRecord record = recordMap.get(message);
		commitMessage(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
	}

	@Override
	public boolean lockMessage(KafkaMessage message) {
		return true;
	}

	@Override
	public void unlockMessage(KafkaMessage message) {

	}
}
