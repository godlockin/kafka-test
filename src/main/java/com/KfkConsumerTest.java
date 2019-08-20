package com;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.TimeZone;

public class KfkConsumerTest {

	public static void main(String[] args) throws Exception {
		TimeZone.setDefault(TimeZone.getTimeZone("CTT"));

		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.0.227:9092");
		props.put("group.id", "group1");
		props.put("enable.auto.commit", "true");
		props.put("auto.offset.reset", "earliest");
		props.put("auto.commit.interval.ms", "1000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList("topic-st"));
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(200);
			records.forEach(x -> {
				System.out.println(String.format("[%s] message:[%s] on offset:[%s]"
						, x.key()
						, JSON.parseObject(x.value())
						, x.offset()));
			});
		}
	}
}
