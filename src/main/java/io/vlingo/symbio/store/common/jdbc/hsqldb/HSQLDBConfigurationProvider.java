// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.common.jdbc.hsqldb;

import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.common.jdbc.DatabaseType;

public class HSQLDBConfigurationProvider {

  public static Configuration configuration(
          final DataFormat format,
          final String url,
          final String databaseName,
          final String username,
          final String password,
          final String originatorId,
          final boolean createTables) {
    return new Configuration(
            DatabaseType.HSQLDB,
            "org.hsqldb.jdbc.JDBCDriver",
            format,
            url,
            databaseName,
            username,
            password,
            false,          // useSSL
            originatorId,
            createTables);
  }

}
