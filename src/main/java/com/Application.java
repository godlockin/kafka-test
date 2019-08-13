package com;

import com.alibaba.fastjson.JSON;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeoutException;

public class Application {

	public static void main(String[] args) throws Exception {
		TimeZone.setDefault(TimeZone.getTimeZone("CTT"));
//		ConfigurableApplicationContext context = SpringApplication.run(Application.class, args);
//
//		KafkaProducer kafkaProducer = context.getBean(KafkaProducer.class);
//
//		try {
//			int i = 0;
//			while (++i < 1000) {
//				kafkaProducer.send();
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}

		Random random = new Random();

		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.0.227:9092");
		props.put("acks", "all");
		props.put("bath.size", 16382);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
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
			producer.send(new ProducerRecord<>("topic-st", timestamp, msg));
		}
		producer.close();

//		ConnectionFactory connectionFactory = new ConnectionFactory();
//		connectionFactory.setHost("localhost");
//		connectionFactory.setPort(5672);
//		connectionFactory.setUsername("user");
//		connectionFactory.setPassword("bitnami");
//
//		Connection connection = connectionFactory.newConnection();
//		try (Channel channel = connection.createChannel()) {
//			int i = 0;
//			while (++i < 100) {
//				Map<String, Object> data = new HashMap<>();
//				data.put("articleId", Integer.valueOf(random.nextInt(10)).longValue());
//				data.put("userId", Integer.valueOf(random.nextInt(10)).longValue());
//				data.put("action", random.nextInt(10));
//				String msg = JSON.toJSONString(data);
//
//				channel.basicPublish("", "test-q", MessageProperties.PERSISTENT_TEXT_PLAIN, msg.getBytes());
//			}
//		} catch (IOException | TimeoutException e) {
//			e.printStackTrace();
//		}
	}
}
