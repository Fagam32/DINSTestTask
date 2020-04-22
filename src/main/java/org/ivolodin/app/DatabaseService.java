package org.ivolodin.app;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseService {
    public static final Logger logger = LogManager.getLogger(DatabaseService.class);

    private static Connection connection = null;
    private static final String CREATE_DB_URL = "jdbc:mysql://localhost:3306/?serverTimezone=UTC";
    private static final String DB_URL = "jdbc:mysql://localhost:3306/traffic_limits?serverTimezone=UTC";

    private static void establishConnectionAndPrepareDatabase() {
        establishConnection();
        prepareDatabase();

    }

    private static void establishConnection() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection(CREATE_DB_URL, Constants.DB_USER, Constants.DB_PASSWORD);
            if (connection == null){
                logger.error("No connection established");
                System.exit(1);
            }
        } catch (ClassNotFoundException | SQLException e) {
            logger.error("Can't establish connection", e);
            System.exit(1);
        }
    }

    private static void prepareDatabase() {
        try {
            createSchema();
            createTable();
            insertInitialValues();
        } catch (SQLException e) {
            logger.error("Can't prepare database", e);
            System.exit(1);
        }
    }

    private static void createSchema() throws SQLException {
        Statement statement = connection.createStatement();

        if (statement.execute("CREATE SCHEMA IF NOT EXISTS traffic_limits"))
            throw new SQLException("Cannot CREATE schema");
    }

    private static void createTable() throws SQLException {
        Statement statement;

        connection = DriverManager.getConnection(DB_URL, Constants.DB_USER, Constants.DB_PASSWORD);
        statement = connection.createStatement();

        statement.execute(
                "CREATE TABLE IF NOT EXISTS limits_per_hour"
                        + "("
                        + "id int NOT NULL AUTO_INCREMENT,"
                        + "limit_name CHAR(3) NOT NULL,"
                        + "limit_value INT NOT NULL,"
                        + "effective_date TIMESTAMP DEFAULT NOW() NOT NULL,"
                        + "PRIMARY KEY(id)"
                        + ")"
        );
    }

    private static void insertInitialValues() throws SQLException {
        Statement statement;

        statement = connection.createStatement();
        statement.execute(
                "INSERT INTO limits_per_hour (limit_name, limit_value) VALUES "
                        + "('min', 1024)"
                        + ",('max', 1073741824)"
        );
    }

    public static Connection getConnection() {
        try {
            if (connection == null || connection.isClosed()) {
                establishConnectionAndPrepareDatabase();
            }
        } catch (SQLException e) {
            logger.error("Problems with establishing connection ", e);
            System.exit(1);
        }
        return connection;
    }

    public static void close() {
        if (connection != null) {
            try {
                connection.close();
                connection = null;
            } catch (SQLException e) {
                logger.error("Problems with closing connection ", e);
            }
        }
    }

}
