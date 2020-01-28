// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.jdbc;

import io.vlingo.actors.Definition;
import io.vlingo.actors.Logger;
import io.vlingo.actors.World;
import io.vlingo.actors.testkit.AccessSafely;
import io.vlingo.symbio.BaseEntry;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.EntryAdapterProvider;
import io.vlingo.symbio.StateAdapterProvider;
import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.TestEvents;
import io.vlingo.symbio.store.common.DbBootstrap;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.common.jdbc.ConnectionProvider;
import io.vlingo.symbio.store.state.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public abstract class JDBCTextStateStoreEntryTest {
    private Configuration configuration;
    private StateStore.StorageDelegate delegate;
    private MockTextDispatcher dispatcher;
    private EntryAdapterProvider entryAdapterProvider;
    private String entity1StoreName;
    private MockResultInterest interest;
    private StateStore store;
    private World world;
    protected Connection connection;
    private DbBootstrap dbBootstrap;

    @Test
    public void testThatSourcesAppendAsEntries() {
        final AccessSafely accessInterest1 = interest.afterCompleting(3);
        dispatcher.afterCompleting(0);

        final Entity1 entity1 = new Entity1("123", 1);
        store.write(entity1.id, entity1, 1, Arrays.asList(new TestEvents.Event1()), interest);

        final Entity1 entity2 = new Entity1("234", 2);
        store.write(entity2.id, entity2, 1, Arrays.asList(new TestEvents.Event2()), interest);

        final Entity1 entity3 = new Entity1("345", 3);
        store.write(entity3.id, entity3, 1, Arrays.asList(new TestEvents.Event3()), interest);

        assertEquals(3, (int) accessInterest1.readFrom("textWriteAccumulatedSourcesCount"));

        final List<BaseEntry.TextEntry> readEntries = new ArrayList<>();
        final AccessSafely accessReadEntries = AccessSafely.afterCompleting(1);
        accessReadEntries
                .writingWith("all", (List<BaseEntry.TextEntry> all) -> readEntries.addAll(all))
                .readingWith("all", () -> readEntries)
                .readingWith("allCount", () -> readEntries.size());

        store.entryReader("test")
                .andThenTo(reader -> reader.readNext(3))
                .andThenConsume((List<Entry<?>> all) -> {
                    accessReadEntries.writeUsing("all", all);
                });

        assertEquals(3, (int) accessReadEntries.readFrom("allCount"));

        assertEquals(new TestEvents.Event1(), entryAdapterProvider.asSource(readEntries.get(0)));
        assertEquals(new TestEvents.Event2(), entryAdapterProvider.asSource(readEntries.get(1)));
        assertEquals(new TestEvents.Event3(), entryAdapterProvider.asSource(readEntries.get(2)));
    }

    @Before
    public void setUp() throws Exception {
        world = World.startWithDefaults("test-store");

        entity1StoreName = Entity1.class.getSimpleName();
        StateTypeStateStoreMap.stateTypeToStoreName(Entity1.class, entity1StoreName);

        dbBootstrap = getBootstrap(DataFormat.Text);
        final Configuration testDbConfiguration = dbBootstrap.createRandomDatabase();
        connection = new ConnectionProvider(testDbConfiguration).connection();
        delegate = storageDelegate(connection, testDbConfiguration, world.defaultLogger());
        interest = new MockResultInterest(false);
        dispatcher = new MockTextDispatcher(0, interest);

        final StateAdapterProvider stateAdapterProvider = new StateAdapterProvider(world);
        entryAdapterProvider = new EntryAdapterProvider(world);

        stateAdapterProvider.registerAdapter(Entity1.class, new Entity1.Entity1StateAdapter());
        // NOTE: No adapter registered for Entity2.class because it will use the default

        store = world.actorFor(
                StateStore.class,
                Definition.has(JDBCStateStoreActor.class, Definition.parameters(dispatcher, delegate)));
    }

    @After
    public void tearDown() throws Exception {
        if (configuration == null) return;
        world.terminate();
        delegate.close();
        connection.close();
        dbBootstrap.dropDatabase(configuration.databaseName);
        dbBootstrap.stopService();
    }

    /**
     * Create specific storage delegate.
     * @param configuration
     * @param logger
     * @return
     */
    protected abstract StateStore.StorageDelegate storageDelegate(final Connection connection,
                                                                  final Configuration configuration,
                                                                  final Logger logger);

    /**
     * Create specific test configuration.
     * @param format
     * @return
     * @throws Exception
     */
    protected abstract DbBootstrap getBootstrap(DataFormat dataFormat);
}
