package com.xjd.transaction.message.sender;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Test;

import com.xjd.transaction.message.message.KafkaMessage;
import com.xjd.transaction.message.sender.kafka.AbstractKafkaAsyncSenderSpi;
import com.xjd.transaction.message.sender.kafka.KafkaSenderSpi;

/**
 * @author elvis.xu
 * @since 2017-11-08 08:27
 */
public class DefaultSenderTest {
	protected static Producer<String, String> producer;
	static {
		Properties props = new Properties();
		props.put("bootstrap.servers", "service4:9191,server4:9192");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 0);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<>(props);
	}


	@Test
	public void send() throws Exception {
		DefaultSender<KafkaMessage> sender = new DefaultSender<KafkaMessage>(new KafkaSenderSpi(producer));

		sender.send(new KafkaMessage("test", "test", null).setTopic("test"));

		producer.close();

	}

	@Test
	public void asyncSend() throws Exception {
		ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(5);
		DefaultSender<KafkaMessage> sender = new DefaultSender<KafkaMessage>(new AbstractKafkaAsyncSenderSpi(producer) {
			@Override
			public boolean lockMessage(Context.OneContext context, KafkaMessage message) {
				System.out.println("lockMessage");
				return true;
			}

			@Override
			public void unlockMessage(Context.OneContext context, KafkaMessage message) {
				System.out.println("unlockMessage");
			}

			@Override
			public boolean messageSendable(Context.OneContext context, KafkaMessage message) {
				System.out.println("messageSendable");
				return true;
			}

			@Override
			public void doAfterSend(Context.OneContext context, KafkaMessage message, Throwable t) {
				System.out.println("doAfterSend");
			}

			@Override
			public void save(KafkaMessage message) {
				System.out.println("save");
			}

			@Override
			public Iterable<KafkaMessage> read(boolean newBatch) {
				System.out.println("read");
				if (newBatch) {
					return Arrays.asList(new KafkaMessage("xxx", "xxx", null).setTopic("test").setPartition("xxx", true),
							new KafkaMessage("yyy", "yyy", null).setTopic("test").setPartition("xxx", true));
				}
				return null;
			}
		}, scheduledExecutorService, 4000L);

		sender.send(new KafkaMessage("111", "111", null).setTopic("test").setPartition("xxx", true));
		sender.send(new KafkaMessage("222", "222", null).setTopic("test").setPartition("xxx", false));
		sender.send(new KafkaMessage("333", "333", null).setTopic("test").setPartition("xxx", false));

		Thread.sleep(20000L);

		producer.close();
		scheduledExecutorService.shutdown();
	}

}