import org.ivolodin.app.Limits;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LimitsTest {

    @Test
    public void baseTest() {
        try (Limits limits = Limits.getInstance()) {
            Thread.sleep(1500); //waiting for initialization
            assertEquals(1024, limits.getMinSize());
            assertEquals(1073741824, limits.getMaxSize());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void insertNewLimitsTest() {
        try (Limits limits = Limits.getInstance()) {
            Thread.sleep(500); //waiting for initialization
            assertEquals(1024, limits.getMinSize());
            assertEquals(1073741824, limits.getMaxSize());

            insertNewLimits(1000, 2000);
            Thread.sleep(1001); // waiting for updating sizes

            assertEquals(1000, limits.getMinSize());
            assertEquals(2000, limits.getMaxSize());

            insertNewLimits(20000,100);
            Thread.sleep(1001); // waiting for updating sizes

            assertEquals(20000, limits.getMinSize());
            assertEquals(100, limits.getMaxSize());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void swappedMinMaxLimits(){
        try (Limits limits = Limits.getInstance()){
            Thread.sleep(500); //waiting for initialization
            assertEquals(1024, limits.getMinSize());
            assertEquals(1073741824, limits.getMaxSize());

            insertNewLimits(500, 100);

            Thread.sleep(1001);

            assertEquals(500, limits.getMinSize());
            assertEquals(100, limits.getMaxSize());
        } catch (InterruptedException e) {
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
            preparedStatement.setInt(1,min);
            preparedStatement.setInt(2,max);
            preparedStatement.executeUpdate();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    @AfterEach
    public void clear(){
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
}
