// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.common.jdbc;

import io.vlingo.symbio.store.DataFormat;

/**
 * A standard configuration for JDBC connections used by
 * {@code StateStore}, {@code Journal}, and {@code ObjectStore}.
 */
public class Configuration {

  /**
   * A default timeout for transactions if not specified by the client.
   * Note that this is not used as a database timeout, but rather the
   * timeout between reading some entity/entities from storage and writing
   * and committing it/them back to storage. This means that user think-time
   * is built in to the timeout value. The current default is 5 minutes
   * but can be overridden by using the constructor that accepts the
   * {@code transactionTimeoutMillis} value. For a practical use of this
   * see the following implementations, where this timeout represents the
   * time between creation of the {@code UnitOfWork} and its expiration:
   * <p>
   * {@code io.vlingo.symbio.store.object.jdbc.jdbi.JdbiObjectStoreDelegate}
   * {@code io.vlingo.symbio.store.object.jdbc.jdbi.UnitOfWork}
   * </p>
   */
  public static final long DefaultTransactionTimeout = 5 * 60 * 1000L; // 5 minutes

  public final String driverClassname;
  public final String url;
  public final String username;
  public final String password;
  public final boolean useSSL;
  public final String databaseName;
  public final DatabaseType databaseType;
  public final DataFormat format;
  public final String originatorId;
  public final boolean createTables;
  public final long transactionTimeoutMillis;

    public static Configuration cloneOf(final Configuration other) {
      return new Configuration(other.databaseType,
              other.driverClassname,
              other.format,
              other.url,
              other.databaseName,
              other.username,
              other.password,
              other.useSSL,
              other.originatorId,
              other.createTables,
              other.transactionTimeoutMillis);
    }


    public Configuration forNewDatabase(final String newDatabaseName) {
      return new Configuration(
              databaseType,
              driverClassname,
              format,
              url,
              newDatabaseName,
              username,
              password,
              useSSL,
              originatorId,
              createTables,
              transactionTimeoutMillis);
  }

  public Configuration(
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
    this(databaseType,
            driverClassname,
            format,
            url,
            databaseName,
            username,
            password,
            useSSL,
            originatorId,
            createTables,
            DefaultTransactionTimeout);
  }

  public Configuration(
          final DatabaseType databaseType,
          final String driverClassname,
          final DataFormat format,
          final String url,
          final String databaseName,
          final String username,
          final String password,
          final boolean useSSL,
          final String originatorId,
          final boolean createTables,
          final long transactionTimeoutMillis) {

    this.databaseType = databaseType;
    this.driverClassname = driverClassname;
    this.url = url;
    this.databaseName = databaseName;
    this.username = username;
    this.password = password;
    this.useSSL = useSSL;
    this.format = format;
    this.originatorId = originatorId;
    this.createTables = createTables;
    this.transactionTimeoutMillis = transactionTimeoutMillis;
  }

  public Configuration withFormat(DataFormat format) {
    return new Configuration(this.databaseType,
            this.driverClassname,
            format,
            this.url,
            this.databaseName,
            this.username,
            this.password,
            this.useSSL,
            this.originatorId,
            this.createTables,
            this.transactionTimeoutMillis);
  }
}
