package com;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;

public class KfkProducerTest {

	public static void main(String[] args) throws Exception {
		TimeZone.setDefault(TimeZone.getTimeZone("CTT"));
		Random random = new Random();

		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.0.227:9092");
		props.put("acks", "all");
		props.put("bath.size", 16382);
		props.put("linger.ms", 0);
		props.put("buffer.memory", 33554432);

		props.put("retries", 0);
		props.put("max.in.flight.requests.per.connections", 1);

		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		KafkaProducer<String, String> producer = new KafkaProducer<>(props);

		int i = 0;
		while (++i < 100) {
			Map<String, Object> data = new HashMap<>();
			data.put("articleId", Integer.valueOf(random.nextInt(10)).longValue());
			data.put("userId", Integer.valueOf(random.nextInt(10)).longValue());
			data.put("action", random.nextInt(10));
			String msg = JSON.toJSONString(data);

			String timestamp = Long.valueOf(System.currentTimeMillis()).toString();
			producer.send(new ProducerRecord<>("topic-st", 0, timestamp, msg));
			System.out.println(String.format("Published: [%s] at: [%s]", msg, timestamp));
		}
		producer.close();
	}
}
