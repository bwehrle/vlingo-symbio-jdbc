// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import io.vlingo.symbio.store.common.DbBootstrap;
import io.vlingo.symbio.store.common.jdbc.ConnectionProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.vlingo.actors.Logger;
import io.vlingo.actors.World;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.state.Entity1;
import io.vlingo.symbio.store.state.StateTypeStateStoreMap;

public abstract class JDBCStorageDelegateTest {
    private Configuration configuration;
    private JDBCStorageDelegate<Object> delegate;
    private String entity1StoreName;
    private World world;
    private DbBootstrap dbBootstrap;
    private Connection connection;

    @Test
    public void testThatDatabaseOpensTablesCreated() throws Exception {
        delegate = storageDelegate(connection, configuration, world.defaultLogger());

        assertNotNull(delegate);
    }

    @Test
    public void testThatTextWritesRead() throws Exception {
        delegate = storageDelegate(connection, configuration, world.defaultLogger());

        assertNotNull(delegate);

        final State.TextState writeState = new State.TextState("123", Entity1.class, 1, "{ \"data\" : \"data1\" }", 1, Metadata.with("metadata", "op"));

        delegate.beginWrite();
        final PreparedStatement writeStatement = delegate.writeExpressionFor(entity1StoreName, writeState);
        writeStatement.executeUpdate();
        delegate.complete();

        delegate.beginRead();
        final PreparedStatement readStatement = delegate.readExpressionFor(entity1StoreName, "123");
        final ResultSet result = readStatement.executeQuery();
        final State.TextState readState = delegate.stateFrom(result, "123");
        delegate.complete();

        assertEquals(writeState, readState);
    }

    @Test
    public void testThatTextStatesUpdate() throws Exception {
        delegate = storageDelegate(connection, configuration, world.defaultLogger());

        assertNotNull(delegate);

        final State.TextState writeState1 = new State.TextState("123", Entity1.class, 1, "{ \"data\" : \"data1\" }", 1, Metadata.with("metadata1", "op1"));

        delegate.beginWrite();
        final PreparedStatement writeStatement1 = delegate.writeExpressionFor(entity1StoreName, writeState1);
        writeStatement1.executeUpdate();
        delegate.complete();

        delegate.beginRead();
        final PreparedStatement readStatement1 = delegate.readExpressionFor(entity1StoreName, "123");
        final ResultSet result1 = readStatement1.executeQuery();
        final State.TextState readState1 = delegate.stateFrom(result1, "123");
        delegate.complete();

        assertEquals(writeState1, readState1);

        final State.TextState writeState2 = new State.TextState("123", Entity1.class, 1, "{ \"data\" : \"data1\" }", 1, Metadata.with("metadata2", "op2"));

        delegate.beginWrite();
        final PreparedStatement writeStatement2 = delegate.writeExpressionFor(entity1StoreName, writeState2);
        writeStatement2.executeUpdate();
        delegate.complete();

        delegate.beginRead();
        final PreparedStatement readStatement2 = delegate.readExpressionFor(entity1StoreName, "123");
        final ResultSet result2 = readStatement2.executeQuery();
        final State.TextState readState2 = delegate.stateFrom(result2, "123");
        delegate.complete();

        assertEquals(writeState2, readState2);
        assertNotEquals(0, writeState1.compareTo(readState2));
        assertNotEquals(0, writeState2.compareTo(readState1));
    }

    @Test
    public void testThatBinaryWritesRead() throws Exception {
        Configuration binaryConfiguration = configuration.withFormat(DataFormat.Binary);
        delegate = storageDelegate(connection, binaryConfiguration, world.defaultLogger());

        assertNotNull(delegate);

        final State.BinaryState writeState = new State.BinaryState("123", Entity1.class, 1, "{ \"data\" : \"data1\" }".getBytes(), 1, Metadata.with("metadata", "op"));

        delegate.beginWrite();
        final PreparedStatement writeStatement = delegate.writeExpressionFor(entity1StoreName, writeState);
        writeStatement.executeUpdate();
        delegate.complete();

        delegate.beginRead();
        final PreparedStatement readStatement = delegate.readExpressionFor(entity1StoreName, "123");
        final ResultSet result = readStatement.executeQuery();
        final State.BinaryState readState = delegate.stateFrom(result, "123");
        delegate.complete();

        assertEquals(writeState, readState);
    }

    @Before
    public void setUp() {
        world = World.startWithDefaults("test-store");
        dbBootstrap = getBootstrap(DataFormat.Text);
        dbBootstrap.startService();
        configuration = dbBootstrap.createRandomDatabase();
        connection = new ConnectionProvider(configuration).connection();
        entity1StoreName = Entity1.class.getSimpleName();
        StateTypeStateStoreMap.stateTypeToStoreName(Entity1.class, entity1StoreName);
    }

    protected abstract DbBootstrap getBootstrap(DataFormat format);

    @After
    public void tearDown() throws Exception {
        delegate.close();
        connection.close();
        Thread.sleep(1000);
        dbBootstrap.dropDatabase(configuration.databaseName);
        dbBootstrap.stopService();
        Thread.sleep(1000);
        world.terminate();
    }

    /**
     * Create specific storage delegate.
     * @param configuration
     * @param logger
     * @return
     */
    protected abstract JDBCStorageDelegate<Object> storageDelegate(final Connection connection,
                                                                   final Configuration configuration,
                                                                   final Logger logger);

    /**
     * Create specific test configuration.
     * @param format
     * @return
     * @throws Exception
     */
}
