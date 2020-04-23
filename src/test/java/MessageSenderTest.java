import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.*;
import org.ivolodin.app.Constants;
import org.ivolodin.app.MessageSender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MessageSenderTest {
    Consumer<Long, String> consumer;

    @BeforeEach
    void getConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_SERVER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("alert"));
    }

    @Test
    public void baseTest() {
        boolean isSend = false;
        while (true) {
            ConsumerRecords<Long, String> records = consumer.poll(Duration.ofSeconds(1));
            if (!records.isEmpty()) {
                String expected = createMessage(true, 1000, 2000);
                assertEquals(1, records.count());
                assertEquals(expected, records.iterator().next().value());
                break;
            }
            if (!isSend) {
                MessageSender.send(true, 1000, 2000);
                isSend = true;
            }
        }
    }

    @Test
    public void severalMessagesTest() throws InterruptedException {

        Set<String> expectedSet = new HashSet<>();
        expectedSet.add(createMessage(true, 2000, 3000));
        expectedSet.add(createMessage(true, 2000, 1000));
        expectedSet.add(createMessage(false, 2000, 3000));
        expectedSet.add(createMessage(false, 2000, 2000));

        Thread.sleep(100);

        boolean isSend = false;
        int readMessages = 0;
        while (true) {
            ConsumerRecords<Long, String> records = consumer.poll(Duration.ofSeconds(1));
            if (readMessages >= 4) {
                break;
            }

            for (ConsumerRecord<Long, String> record : records) {
                assertTrue(expectedSet.contains(record.value()));
                readMessages++;
            }
            if (!isSend) {
                MessageSender.send(true, 2000, 3000);
                MessageSender.send(true, 2000, 1000);
                MessageSender.send(false, 2000, 3000);
                MessageSender.send(false, 2000, 2000);
                isSend = true;
            }
        }
    }

    public String createMessage(boolean isMinimal, long actualSize, long expectedSize) {
        StringBuilder message = new StringBuilder();
        message.append("Traffic size")
                .append(" is");
        if (isMinimal)
            message.append(" less than ");
        else
            message.append(" more than ");
        message.append(expectedSize);
        message.append(" (Actual size is ").append(actualSize).append(")");
        return message.toString();
    }

    @AfterEach
    public void clearKafkaTopics() {
        consumer.unsubscribe();
        consumer.close();
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_SERVER);
        AdminClient client = KafkaAdminClient.create(props);
        client.deleteTopics(Collections.singletonList("alert"));
        client.close();
    }
}
