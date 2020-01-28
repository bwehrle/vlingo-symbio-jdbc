package io.vlingo.symbio.store.common;

import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.common.jdbc.ConnectionProvider;
import io.vlingo.symbio.store.common.jdbc.DatabaseType;

import java.sql.Connection;

public class HSQLBootstrapProvider implements BootstrapProvider {

    public static final DbBootstrap.BootstrapAdapter adapter = new DbBootstrap.BootstrapAdapter() {
        @Override
        public void startService(Configuration configuration) throws Exception {
        }

        @Override
        public void stopService() throws Exception {
        }

        @Override public void createDatabase(final Connection connection, final String databaseName) {
        }

        @Override public void dropDatabase(final Connection connection, final String databaseName) {
        }
    };

    @Override
    public DbBootstrap getBootstrap(DataFormat dataFormat) {
        TestConfiguration testConfiguration = testConfiguration(dataFormat);
        return new DbBootstrap(testConfiguration, new ConnectionProvider(testConfiguration), adapter);
    }

    public static TestConfiguration testConfiguration(final DataFormat format) {
        return new TestConfiguration(
                DatabaseType.HSQLDB,
                "org.hsqldb.jdbc.JDBCDriver",
                format,
                "jdbc:hsqldb:mem:",
                "testdb",       // database name
                "SA",           // username
                "",             // password
                false,          // useSSL
                "TEST",         // originatorId
                true);          // create tables
    }
}
