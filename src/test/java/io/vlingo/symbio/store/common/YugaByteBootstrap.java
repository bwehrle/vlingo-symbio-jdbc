package io.vlingo.symbio.store.common;

import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.common.jdbc.ConnectionProvider;
import io.vlingo.symbio.store.common.jdbc.DatabaseType;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

public class YugaByteBootstrap implements BootstrapProvider {

    public static final DbBootstrap.BootstrapAdapter bootstrapAdapter = new DbBootstrap.BootstrapAdapter(){
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
                System.out.println("YugaByte database " + databaseName + " could not be created because: " + e.getMessage());

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
                System.out.println("YugaByte database " + databaseName + " could not be dropped because: " + e.getMessage());
            }
        }
    };
    @Override
    public DbBootstrap getBootstrap(DataFormat dataFormat) {
        final Configuration testConfiguration = new Configuration(
                    DatabaseType.Postgres,
                    "org.postgresql.Driver",
                    dataFormat,
                    "jdbc:postgresql://localhost:5433/",
                    "vlingo_test",  // database name
                    "postgres",  // username
                    "postgres",    // password
                    false,          // useSSL
                    "TEST",         // originatorId
                    true);          // create tables

        return new DbBootstrap(testConfiguration, new ConnectionProvider(testConfiguration), bootstrapAdapter);
    }
}
