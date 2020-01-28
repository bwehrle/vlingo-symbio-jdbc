package io.vlingo.symbio.store.common;

import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.common.jdbc.ConnectionProvider;
import io.vlingo.symbio.store.common.jdbc.DatabaseType;
import io.vlingo.symbio.store.common.jdbc.mysql.MySQLConfigurationProvider;

import java.util.concurrent.atomic.AtomicInteger;

public class TestConfiguration extends Configuration {
    static private final AtomicInteger uniqueNumber = new AtomicInteger(0);

    public TestConfiguration(
            final DatabaseType databaseType,
            final String driverClassname,
            final DataFormat format,
            final String url,
            final String databaseName,
            final String username,
            final String password,
            final boolean useSSL,
            final String originatorId,
            final boolean createTables) {
        super(databaseType, driverClassname, format, url, databaseName, username, password, useSSL, originatorId, createTables);
    }

    public static String generateDatabaseName(final String baseDatabaseName, final DataFormat format) {
        return baseDatabaseName +
                "_" +
                uniqueNumber.incrementAndGet() +
                (format.isBinary() ? "b" : "t");
    }
}