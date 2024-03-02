package rabbit;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

public class rabbitConsumer {
    public static void main(String[] args) throws URISyntaxException, NoSuchAlgorithmException, KeyManagementException, IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri("amqp://guest:guest@localhost:5672");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare("test", "fanout", true);
        channel.queueDeclare("prices", false, false, false, null);

        channel.basicConsume("prices", true, (ct, message) -> {
            String m = new String(message.getBody(), "UTF-8");
            System.out.println("received message - " + m);
        }, ct -> { System.err.println("Error occurred while consuming messages: " + ct);});

    }
}
