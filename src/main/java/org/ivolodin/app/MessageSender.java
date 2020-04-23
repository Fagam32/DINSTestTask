package org.ivolodin.app;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class MessageSender {
    public static final Logger logger = LogManager.getLogger(MessageSender.class);


    private static final Producer<Long, String> producer;

    static {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_SERVER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(props);
    }

    public static void send(boolean isMinimal, long actualSize, long expectedSize) {

        String message = getMessageText(isMinimal, actualSize, expectedSize);

        Random random = new Random();
        try {
            producer.send(new ProducerRecord<>("alerts", random.nextLong(), message)).get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Troubles with sending", e);
        }

        logger.info("Message: \"" + message + "\" is sent");
    }

    private static String getMessageText(boolean isMinimal, long actualSize, long expectedSize) {
        StringBuilder sb = new StringBuilder();
        sb.append("Traffic size is");

        if (isMinimal)
            sb.append(" less than ");
        else
            sb.append(" more than ");

        sb.append(expectedSize).append(" (Actual size is ").append(actualSize).append(")");
        return sb.toString();
    }
}
