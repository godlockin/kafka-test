import com.alibaba.fastjson.JSON;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeoutException;

public class test {

    public static void main(String[] args) throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setPort(5672);
        connectionFactory.setUsername("user");
        connectionFactory.setPassword("bitnami");

        Random random = new Random();

        Connection connection = connectionFactory.newConnection();
        try (Channel channel = connection.createChannel()) {
            int i = 0;
            while (i < 100) {
                Map<String, Object> data = new HashMap<>();
                data.put("articleId", Integer.valueOf(random.nextInt(10)).longValue());
                data.put("userId", Integer.valueOf(random.nextInt(10)).longValue());
                data.put("action", random.nextInt(10));
                String msg = JSON.toJSONString(data);

                channel.basicPublish("", "test-q", null, msg.getBytes());
            }
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
        System.out.println("done");
    }
}
