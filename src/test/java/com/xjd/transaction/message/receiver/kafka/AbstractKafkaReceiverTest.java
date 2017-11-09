package com.xjd.transaction.message.receiver.kafka;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Before;
import org.junit.Test;

import com.xjd.transaction.message.message.KafkaMessage;

/**
 * @author elvis.xu
 * @since 2017-11-09 10:56
 */
public class AbstractKafkaReceiverTest {
	protected static Consumer<String, String> consumer;
	protected static Executor executor;

	@Before
	public void setUp() throws Exception {
		Properties props = new Properties();
		props.put("bootstrap.servers", "service4:9191");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "false");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Arrays.asList("test", "foo"));
		executor = Executors.newCachedThreadPool();
	}

	@Test
	public void test() throws Exception {
		AbstractKafkaReceiver receiver = new AbstractKafkaReceiver(consumer, executor, message -> {
			System.out.println("receive message: " + message);
		}) {
			@Override
			public int messageProcessable(KafkaMessage message) {
				return 0;
			}
		};

		Thread.sleep(60000L);
	}
}