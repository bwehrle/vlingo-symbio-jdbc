// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.jdbc.mysql;

import io.vlingo.actors.Logger;
import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.common.DbBootstrap;
import io.vlingo.symbio.store.common.MySqlBootstrap;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.state.StateStore;
import io.vlingo.symbio.store.state.jdbc.JDBCTextStateStoreEntryTest;

import java.sql.Connection;

public class MySQLJDBCTextStateStoreEntryTest extends JDBCTextStateStoreEntryTest {

    @Override
    protected StateStore.StorageDelegate storageDelegate(final Connection connection, final Configuration configuration, final Logger logger) {
        return new MySQLStorageDelegate(connection, configuration, logger);
    }

    @Override
    protected DbBootstrap getBootstrap(DataFormat dataFormat) {
        return new MySqlBootstrap().getBootstrap(dataFormat);
    }

}
