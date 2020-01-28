// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.object.jdbc.jpa.postgres;

import io.vlingo.actors.Logger;
import io.vlingo.symbio.StateAdapterProvider;
import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.common.DbBootstrap;
import io.vlingo.symbio.store.common.PostgresBootstrap;
import io.vlingo.symbio.store.object.jdbc.JDBCObjectStoreEntryJournalQueries;
import io.vlingo.symbio.store.object.jdbc.PostgresObjectStoreEntryJournalQueries;
import io.vlingo.symbio.store.object.jdbc.jpa.JDBCObjectStoreEntryReaderTest;
import io.vlingo.symbio.store.object.jdbc.jpa.JPAObjectStoreDelegate;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

public class PostgresJPAObjectStoreIntegrationTest extends JDBCObjectStoreEntryReaderTest {


    @Override
    protected DbBootstrap createBootstrap() {
        return new PostgresBootstrap().getBootstrap(DataFormat.Text);
    }

    @Override
    protected JPAObjectStoreDelegate createDelegate(Map<String, Object> properties, String originatorId,
                                                    StateAdapterProvider stateAdapterProvider, Logger logger) {
        return new JPAObjectStoreDelegate(JPAObjectStoreDelegate.JPA_POSTGRES_PERSISTENCE_UNIT, properties, "TEST",
                stateAdapterProvider, logger);
    }

    @Override
    protected JDBCObjectStoreEntryJournalQueries createQueries(Connection connection) {
        return new PostgresObjectStoreEntryJournalQueries(connection);
    }

    @Override
    protected Map<String, Object> getDatabaseSpecificProperties(String databaseNamePostfix) {
        Map<String, Object> properties = new HashMap<>();

        properties.put("javax.persistence.jdbc.driver", "org.postgresql.Driver");
        properties.put("javax.persistence.jdbc.url", "jdbc:postgresql://localhost/" + databaseNamePostfix);
        properties.put("javax.persistence.jdbc.user", "vlingo_test");
        properties.put("javax.persistence.jdbc.password", "vlingo123");

        return properties;
    }
}
