package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CurrencyRateProducerServiceTest {

    private static KafkaConsumer<String, String> consumer;

    @BeforeAll
    public static void connect() throws UnknownHostException {
        Properties config = new Properties();
        config.put("client.id", InetAddress.getLocalHost().getHostName());
        config.put("bootstrap.servers", "localhost:29092");
        config.put("group.id", "autotest-java");
        config.put("enable.auto.commit", "true");
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(config);
    }

    @AfterAll
    public static void disconnect(){
        consumer.close();
    }

    @Test
    public void test() throws Exception {
        String rate = "0.997";

        given().get("http://localhost:8081/rate/" + rate);

        String rateAsIs = getRateFromKafka(
                List.of("prices"),
                Duration.ofSeconds(10),
                r -> r.value().contains(rate),
                "rate" + rate + " is not founded."
        );

        assertTrue(rateAsIs.contains(rate));
    }


    private String getRateFromKafka(
            Collection<String> topics,
            Duration duration,
            Predicate<ConsumerRecord<String, String>> predicate) throws Exception {
        return getRateFromKafka(topics, duration, predicate, "not found");
    }
    private String getRateFromKafka(
            Collection<String> topics,
            Duration duration,
            Predicate<ConsumerRecord<String, String>> predicate,
            String message)
            throws Exception {

        consumer.subscribe(topics);
        long startTime = System.currentTimeMillis();
        ConsumerRecords<String, String> poll;

        while (System.currentTimeMillis() - startTime < duration.toMillis()) {
            poll = consumer.poll(Duration.ofSeconds(10));
            for (ConsumerRecord<String, String> record : poll) {
                if (predicate.test(record)) {
                    System.out.println(record);
                    return record.value();
                }
            }
        }

        throw new TimeoutException(message);
    }
}