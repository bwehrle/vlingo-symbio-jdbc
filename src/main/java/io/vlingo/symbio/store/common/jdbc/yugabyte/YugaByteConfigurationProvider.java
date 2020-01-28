// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.common.jdbc.yugabyte;

import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.common.jdbc.DatabaseType;

public class YugaByteConfigurationProvider {

    public static Configuration configuration(
            final DataFormat format,
            final String url,
            final String databaseName,
            final String username,
            final String password,
            final String originatorId,
            final boolean createTables) throws Exception {
        return new Configuration(
                DatabaseType.Postgres,
                "org.postgresql.Driver",
                format,
                url,
                databaseName,
                username,
                password,
                false,
                originatorId,
                createTables);
    }
}
