// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.jdbc.yugabyte;

import io.vlingo.actors.Logger;
import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.common.DbBootstrap;
import io.vlingo.symbio.store.common.YugaByteBootstrap;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.state.StateStore;
import io.vlingo.symbio.store.state.jdbc.JDBCTextStateStoreEntryTest;
import org.junit.Ignore;

import java.sql.Connection;

@Ignore
public class YugaByteJDBCTextStateStoreEntryTest extends JDBCTextStateStoreEntryTest {
    @Override
    protected StateStore.StorageDelegate storageDelegate(Connection connection, Configuration configuration, Logger logger) {
        return new YugaByteStorageDelegate(connection, configuration, logger);
    }

    @Override
    protected DbBootstrap getBootstrap(DataFormat dataFormat) {
        return new YugaByteBootstrap().getBootstrap(dataFormat);
    }
}
