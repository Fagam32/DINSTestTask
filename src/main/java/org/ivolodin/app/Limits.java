package org.ivolodin.app;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Limits implements AutoCloseable {
    public static final Logger logger = LogManager.getLogger(Limits.class);

    private static Limits instance = null;
    private static Updater updater;
    private int minSize;
    private int maxSize;

    private Limits() {
        updateLimits();
    }

    private void updateLimits() {
        updater = new Updater("Limits updater");
        updater.start();
    }

    public int getMinSize() {
        return minSize;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public static Limits getInstance() {
        if (instance == null) {
            instance = new Limits();
        }
        return instance;
    }

    @Override
    public void close() {
        try {
            if (updater != null) {
                updater.stopUpdating();
                updater.join();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        instance = null;
    }


    private class Updater extends Thread {
        private boolean isStopped = false;
        int prevMin = -1;
        int prevMax = -1;

        public Updater() {
            super();
        }

        public Updater(String name) {
            super(name);
        }

        public void stopUpdating() {
            isStopped = true;
        }

        @Override
        public void run() {
            Connection connection = DatabaseService.getConnection();
            try {
                while (true) {
                    update(connection);
                    if (isStopped) break;

                    Thread.sleep(1000);
                }
            } catch (SQLException e) {
                logger.error("Caught Exception in infinite updater loop", e);
            } catch (InterruptedException e) {
                logger.info("Interrupted infinite updater loop", e);
            }

            DatabaseService.close();
        }

        private void update(Connection connection) throws SQLException {
            Statement statement = connection.createStatement();
            statement.execute(
                    "SELECT limit_name, limit_value FROM limits_per_hour "
                            + "WHERE effective_date=(SELECT max(effective_date) FROM limits_per_hour)"
            );
            ResultSet resultSet = statement.getResultSet();
            while (resultSet.next()) {
                if (resultSet.getString("limit_name").equals("min")
                        && prevMin != resultSet.getInt("limit_value")) {
                    prevMin = minSize;
                    minSize = resultSet.getInt("limit_value");
                }
                if (resultSet.getString("limit_name").equals("max")
                        && prevMax != resultSet.getInt("limit_value")) {
                    prevMax = maxSize;
                    maxSize = resultSet.getInt("limit_value");
                }
            }
        }
    }
}
