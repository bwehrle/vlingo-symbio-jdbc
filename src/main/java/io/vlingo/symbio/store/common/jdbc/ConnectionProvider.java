// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.common.jdbc;

import org.jdbi.v3.core.statement.SqlStatement;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Provider of {@code Connection} instances.
 */
public class ConnectionProvider {

  public final Configuration configuration;

  public ConnectionProvider(Configuration configuration) {
    this.configuration = configuration;
  }

  /**
   * Answer a new instance of a {@code Connection}.
   * @return Connection
   */
  public Connection connection() {
    return openConnection();
  }

  private Connection openConnection() {
    try {
      Class.forName(configuration.driverClassname);
      final Properties properties = new Properties();
      properties.setProperty("user", configuration.username);
      properties.setProperty("password", configuration.password);
      properties.setProperty("ssl", Boolean.toString(configuration.useSSL));
      final Connection connection = DriverManager.getConnection(configuration.url + configuration.databaseName, properties);
      connection.setAutoCommit(false);
      return connection;
    }  catch (Exception e) {
      throw new IllegalStateException(getClass().getSimpleName() + ": Cannot connect because database unavailable or wrong credentials.");
    }
  }
}
