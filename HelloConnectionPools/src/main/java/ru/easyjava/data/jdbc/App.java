package ru.easyjava.data.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

/**
 * Simple example of JDBC usage.
 */
public final class App {
    /**
     * Number of parallel database readers.
     */
    private static final Integer NO_THREADS = 16;
    /**
     * Query that create table.
     */
    private static final String CREATE_QUERY =
            "CREATE TABLE EXAMPLE (GREETING VARCHAR(6), TARGET VARCHAR(6))";
    /**
     * Query that populates table with data.
     */
    private static final String DATA_QUERY =
            "INSERT INTO EXAMPLE VALUES('Hello','World')";

    /**
     * Do not construct me.
     */
    private App() {
    }

    /**
     * Entry point.
     *
     * @param args Command line args. Not used.
     * @throws InterruptedException when synchronization fails.
     */
    public static void main(final String[] args) throws InterruptedException {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(
                "jdbc:postgresql://127.0.0.1/test?user=test&password=test");
        HikariDataSource ds = new HikariDataSource(config);

        try (Connection db = ds.getConnection()) {
            try (Statement dataQuery = db.createStatement()) {
                dataQuery.execute(CREATE_QUERY);
                dataQuery.execute(DATA_QUERY);
            }

        } catch (SQLException ex) {
            System.out.println("Database connection failure: "
                    + ex.getMessage());
            return;
        }

        CountDownLatch startLatch = new CountDownLatch(NO_THREADS);
        CountDownLatch finishLatch = new CountDownLatch(NO_THREADS);

        Runnable readingThread = () -> {
            startLatch.countDown();
            try {
                startLatch.await();
            } catch (InterruptedException ex) {
                System.out.println("Synchronization failure: "
                        + ex.getMessage());
                return;
            }
            try (Connection db = ds.getConnection()) {
                try (PreparedStatement query =
                             db.prepareStatement("SELECT * FROM EXAMPLE")) {
                    ResultSet rs = query.executeQuery();
                    while (rs.next()) {
                        System.out.println(String.format("%s, %s!",
                                rs.getString(1),
                                rs.getString("TARGET")));
                    }
                    rs.close();
                }
            } catch (SQLException ex) {
                System.out.println("Database connection failure: "
                        + ex.getMessage());
            }
            finishLatch.countDown();
        };

        IntStream.range(0, NO_THREADS).forEach(
                (index) -> new Thread(readingThread).start()
        );

        finishLatch.await();
        System.out.println("All reading thread complete.");

    }
}
