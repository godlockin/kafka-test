import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class kfkTest {
    public static void main(String[] args) {

        Random random = new Random();

        Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
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
			producer.send(new ProducerRecord<>("test-topic", timestamp, msg));
		}
		producer.close();
    }
}
