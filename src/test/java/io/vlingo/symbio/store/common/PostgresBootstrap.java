package io.vlingo.symbio.store.common;

import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.common.jdbc.ConnectionProvider;
import io.vlingo.symbio.store.common.jdbc.DatabaseType;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

public class PostgresBootstrap implements BootstrapProvider {

    public static final DbBootstrap.BootstrapAdapter adapter = new DbBootstrap.BootstrapAdapter() {
        private Configuration configuration;

        public void startService(final Configuration configuration) {
            this.configuration = configuration;
        }

        public void stopService() {
        }

        @Override
        public void createDatabase(final Connection connection, final String databaseName) throws Exception {
            try (final Statement statement = connection.createStatement()) {
                connection.setAutoCommit(true);
                statement.executeUpdate("CREATE DATABASE " + databaseName + " WITH OWNER = " + configuration.username);
                connection.setAutoCommit(false);
            } catch (Exception e) {
                final List<String> message = Arrays.asList(e.getMessage().split(" "));
                if (message.contains("database") && message.contains("already") && message.contains("exists")) return;
                System.out.println("Postgres database " + databaseName + " could not be created because: " + e.getMessage());

                throw e;
            }
        }

        @Override
        public void dropDatabase(final Connection connection, final String databaseName) throws Exception {
            try (final Statement statement = connection.createStatement()) {
                connection.setAutoCommit(true);
                statement.executeUpdate("DROP DATABASE " + databaseName);
                connection.setAutoCommit(false);
            } catch (Exception e) {
                System.out.println("Postgres database " + databaseName + " could not be dropped because: " + e.getMessage());
            }
        }
    };

    public DbBootstrap getBootstrap(final DataFormat format) {
        final TestConfiguration configuration = new TestConfiguration(
                DatabaseType.Postgres,
                "org.postgresql.Driver",
                format,
                "jdbc:postgresql://localhost/",
                "vlingo_test",  // database name
                "vlingo_test",  // username
                "vlingo123",    // password
                false,          // useSSL
                "TEST",         // originatorId
                true);          // create tables

        return new DbBootstrap(configuration, new ConnectionProvider(configuration), adapter);
    }
}
