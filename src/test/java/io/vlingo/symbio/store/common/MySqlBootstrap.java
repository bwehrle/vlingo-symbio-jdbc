package io.vlingo.symbio.store.common;

import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.common.jdbc.ConnectionProvider;
import io.vlingo.symbio.store.common.jdbc.DatabaseType;

import java.sql.Connection;
import java.sql.Statement;

public class MySqlBootstrap implements BootstrapProvider {

    public static DbBootstrap.BootstrapAdapter bootstrapAdapter = new DbBootstrap.BootstrapAdapter() {

        public void startService(final Configuration ignored) {
        }

        public void stopService() {
        }

        public void createDatabase(final Connection connection, final String databaseName) throws Exception {
            try (final Statement statement = connection.createStatement()) {
                connection.setAutoCommit(true);
                statement.executeUpdate("CREATE DATABASE IF NOT EXISTS " + databaseName);
            } catch (Exception ex) {
                System.out.println("MySQL database " + databaseName + " could not be created because: " + ex.getMessage());
                throw ex;
            }
        }

        @Override
        public void dropDatabase(final Connection connection, final String databaseName) throws Exception {
            try (final Statement statement = connection.createStatement()) {
                connection.setAutoCommit(true);
                statement.executeUpdate("DROP DATABASE " + databaseName);
            } catch (Exception ex) {
                System.out.println("MySQL database " + databaseName + " could not be dropped because: " + ex.getMessage());
                throw ex;
            }
        }
    };

    public DbBootstrap getBootstrap(final DataFormat format) {
        final TestConfiguration configuration = new TestConfiguration(
                DatabaseType.MySQL,
                "com.mysql.cj.jdbc.Driver",
                format,
                "jdbc:mysql://localhost/",
                "vlingo_test",  // database name
                "vlingo_test",  // username
                "vlingo123",    // password
                false,          // useSSL
                "TEST",         // originatorId
                true);          // create tables

        return new DbBootstrap(configuration, new ConnectionProvider(configuration), bootstrapAdapter);
    }
}
