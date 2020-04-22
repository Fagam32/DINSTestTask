import helpers.TestClient;
import helpers.TestServer;
import helpers.WarmingUp;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.ivolodin.app.Application;
import org.ivolodin.app.Constants;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ComplexTest {

    TestClient client;
    Consumer<Long, String> consumer;
    ExecutorService appExecutor;
    static TestServer testServer;
    static ExecutorService serverExecutor;

    @BeforeAll
    static void prepareEnvironment() {
        warmUp();
        startServer();
    }

    static void warmUp() {
        System.out.println("Starting warming up");
        WarmingUp warmingUp = new WarmingUp();
        warmingUp.warmUp();
        System.out.println("Warming up ended");
    }

    static void startServer() {
        serverExecutor = Executors.newSingleThreadExecutor();
        testServer = new TestServer();
        serverExecutor.submit(() -> testServer.start());
    }

    @BeforeEach
    void getConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_SERVER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("alert"));
    }

    @BeforeEach
    public void runClient() {
        client = new TestClient();
        client.startConnection();
    }

    @BeforeEach
    public void runApplication() throws InterruptedException {
        String[] args = new String[1];
        args[0] = "to=localhost:4445";
        Application app = Application.getInstance();
        appExecutor = Executors.newSingleThreadExecutor();
        appExecutor.submit(() -> app.start(args));
    }

    @Test
    public void baseTest() {
        String expected = "Traffic size is less than 1024";
        boolean isRead = false;
        while (!isRead) {
            ConsumerRecords<Long, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<Long, String> record : records) {
                String actual = record.value();
                assertTrue(actual.startsWith(expected));
                isRead = true;
            }
        }
    }

    @Test
    public void updateLimits() throws InterruptedException {
        insertNewLimits(100, 300);

        Thread.sleep(1000); //waiting for limits to update

        for (int i = 0; i < 5; i++) {
            client.sendMessage();
            Thread.sleep(300);
        }

        String expected = "Traffic size is more than 300";

        boolean isRead = false;
        while (!isRead) {
            ConsumerRecords<Long, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<Long, String> record : records) {
                String actual = record.value();
                assertTrue(actual.startsWith(expected), actual);
                isRead = true;
            }
        }
    }

    @Test
    public void insertNewLimits_and_trafficsAreSwapped() throws InterruptedException {
        insertNewLimits(500, 100);
        Thread.sleep(1000); //waiting for limits to update

        client.sendMessage();

        String expectedLess = "Traffic size is more than 100";
        String expectedMore = "Traffic size is less than 500";

        int countOfReadRecords = 0;
        do {
            ConsumerRecords<Long, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<Long, String> record : records) {
                String actual = record.value();
                boolean contains = (actual.startsWith(expectedLess)) || (actual.startsWith(expectedMore));
                if (contains)
                    countOfReadRecords++;
            }

        } while (countOfReadRecords < 2);
        assertEquals(2, countOfReadRecords);
    }

    @AfterEach
    public void stopClient() throws InterruptedException {
        client.stop();
        Application.getInstance().stop();
        appExecutor.shutdown();
        Thread.sleep(1000);
    }

    @AfterEach
    public void clearKafkaTopics() {
        consumer.close();
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_SERVER);
        AdminClient client = KafkaAdminClient.create(props);
        client.deleteTopics(Collections.singletonList("alert"));
        client.close();
    }

    @AfterEach
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

    private void insertNewLimits(int min, int max) {
        String url = "jdbc:mysql://localhost:3306/traffic_limits?serverTimezone=UTC";
        String user = "test";
        String password = "123456";
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection connection = DriverManager.getConnection(url, user, password);
            PreparedStatement preparedStatement = connection.prepareStatement(
                    "INSERT INTO limits_per_hour (limit_name, limit_value) VALUES "
                            + "('min', ?)"
                            + ",('max', ?)");
            preparedStatement.setInt(1, min);
            preparedStatement.setInt(2, max);
            preparedStatement.executeUpdate();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    @AfterAll
    static void stopServer() {
        testServer.stop();
        serverExecutor.shutdownNow();
    }
}
