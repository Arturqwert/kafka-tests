package rabbit;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

public class SendMessageToMQ {
    public static void main(String[] args) throws URISyntaxException, NoSuchAlgorithmException, KeyManagementException, IOException, TimeoutException, InterruptedException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri("amqp://guest:guest@localhost:5672");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare("test", "fanout", true);
        channel.queueDeclare("prices", false, false, false, null);
        System.out.println("connected!");

        String currency = "USD";
        String price = "100.2";
        String finalMessage = "{\"currency\":\"" + currency + "\",\"price\": " + price + "}";

        channel.basicPublish("", "prices", null, finalMessage.getBytes());


        System.out.println("type exit to disconnect");
        Scanner scanner;
        scanner = new Scanner(System.in);
        String command = scanner.nextLine();


        //consumer

        if (command.equalsIgnoreCase("exit") || command.equalsIgnoreCase("quit")){
            channel.close();
            connection.close();
            System.exit(0);
        }
    }
}
