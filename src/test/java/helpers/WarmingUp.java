package helpers;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.ivolodin.app.Application;
import org.ivolodin.app.Constants;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class WarmingUp {

    ExecutorService serverExecutor;
    ExecutorService appExecutor;
    Consumer<Long, String> consumer;
    TestClient client;
    TestServer testServer;

    public void warmUp() {
        startServer();
        runClient();
        runApplication();
        getConsumer();
        waitForResponse();
        stopClient();
        clearKafkaTopics();
        clearDB();
        stopServer();
    }

    void startServer() {
        testServer = new TestServer();
        serverExecutor = Executors.newSingleThreadExecutor();
        serverExecutor.submit(() -> testServer.start());
        try {
            Thread.sleep(300);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void runClient() {
        client = new TestClient();
        client.startConnection();
    }

    public void runApplication() {
        String[] args = new String[1];
        args[0] = "to=localhost:4445";
        Application app = Application.getInstance();
        appExecutor = Executors.newSingleThreadExecutor();
        appExecutor.submit(() -> app.start(args));
    }

    void getConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_SERVER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("alerts"));
    }

    private void waitForResponse() {
        String expected = "Traffic size is less than 1024";
        boolean isRead = false;
        while (!isRead) {
            ConsumerRecords<Long, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Long, String> record : records) {
                String actual = record.value();
                assertTrue(actual.startsWith(expected));
                isRead = true;
            }
        }
    }

    public void stopClient() {
        client.stop();
        Application.getInstance().stop();
        appExecutor.shutdown();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void clearKafkaTopics() {
        consumer.close();
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_SERVER);
        AdminClient client = KafkaAdminClient.create(props);
        client.deleteTopics(Collections.singletonList("alerts"));
        client.close();
    }

    public void clearDB() {
        String url = "jdbc:mysql://localhost:3306/traffic_limits?serverTimezone=UTC";
        String user = "test";
        String password = "123456";
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection connection = DriverManager.getConnection(url, user, password);
            Statement statement = connection.createStatement();
            statement.execute("TRUNCATE limits_per_hour");
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    void stopServer() {
        testServer.stop();
        serverExecutor.shutdownNow();
    }
}
