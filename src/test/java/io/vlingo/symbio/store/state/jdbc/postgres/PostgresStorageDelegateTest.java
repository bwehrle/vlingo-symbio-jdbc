// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.jdbc.postgres;

import io.vlingo.actors.Logger;
import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.common.DbBootstrap;
import io.vlingo.symbio.store.common.PostgresBootstrap;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.common.jdbc.postgres.PostgresConfigurationProvider;
import io.vlingo.symbio.store.state.jdbc.JDBCStorageDelegate;
import io.vlingo.symbio.store.state.jdbc.JDBCStorageDelegateTest;

import java.sql.Connection;

public class PostgresStorageDelegateTest extends JDBCStorageDelegateTest {

    @Override
    protected DbBootstrap getBootstrap(DataFormat format) {
        return new PostgresBootstrap().getBootstrap(format);
    }

    @Override
    protected JDBCStorageDelegate<Object> storageDelegate(final Connection connection,
                                                          final Configuration configuration,
                                                          final Logger logger) {
        return new PostgresStorageDelegate(connection, configuration, logger);
    }
}
