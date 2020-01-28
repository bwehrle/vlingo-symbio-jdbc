// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import io.vlingo.symbio.store.common.BootstrapProvider;
import io.vlingo.symbio.store.common.DbBootstrap;
import io.vlingo.symbio.store.common.TestConfiguration;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.common.jdbc.ConnectionProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.vlingo.actors.Definition;
import io.vlingo.actors.World;
import io.vlingo.actors.testkit.AccessSafely;
import io.vlingo.symbio.EntryAdapterProvider;
import io.vlingo.symbio.State;
import io.vlingo.symbio.StateAdapterProvider;
import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.common.event.TestEvent;
import io.vlingo.symbio.store.common.event.TestEventAdapter;
import io.vlingo.symbio.store.state.Entity1;
import io.vlingo.symbio.store.state.Entity1.Entity1StateAdapter;
import io.vlingo.symbio.store.state.MockResultInterest;
import io.vlingo.symbio.store.state.MockTextDispatcher;
import io.vlingo.symbio.store.state.StateStore;
import io.vlingo.symbio.store.state.StateStore.StorageDelegate;
import io.vlingo.symbio.store.state.StateTypeStateStoreMap;

public abstract class JDBCStateStoreActorTest {
  protected Configuration configuration;
  protected StorageDelegate delegate;
  protected MockTextDispatcher dispatcher;
  protected String entity1StoreName;
  protected MockResultInterest interest;
  protected StateStore store;
  protected World world;
  private DbBootstrap dbBootstrap;
  protected Connection connection;

  @Test
  public void testThatStateStoreDispatches() throws Exception {
    final AccessSafely accessInterest1 = interest.afterCompleting(6);
    final AccessSafely accessDispatcher1 = dispatcher.afterCompleting(6);

    final Entity1 entity1 = new Entity1("123", 1);
    store.write(entity1.id, entity1, 1, interest);
    final Entity1 entity2 = new Entity1("234", 2);
    store.write(entity2.id, entity2, 1, interest);
    final Entity1 entity3 = new Entity1("345", 3);
    store.write(entity3.id, entity3, 1, interest);

    assertEquals(3, (int) accessDispatcher1.readFrom("dispatchedStateCount"));
    assertEquals(3, (int) accessInterest1.readFrom("confirmDispatchedResultedIn"));
    final State<?> state123 = accessDispatcher1.readFrom("dispatchedState", dispatchId("123"));
    assertEquals("123", state123.id);
    final State<?> state234 = accessDispatcher1.readFrom("dispatchedState", dispatchId("234"));
    assertEquals("234", state234.id);
    final State<?> state345 = accessDispatcher1.readFrom("dispatchedState", dispatchId("345"));
    assertEquals("345", state345.id);

    final AccessSafely accessInterest2 = interest.afterCompleting(4);
    final AccessSafely accessDispatcher2 = dispatcher.afterCompleting(2);

    final Entity1 entity4 = new Entity1("456", 4);
    store.write(entity4.id, entity4, 1, interest);
    final Entity1 entity5 = new Entity1("567", 5);
    store.write(entity5.id, entity5, 1, interest);

    assertTrue(4 <= (int) accessDispatcher2.readFrom("dispatchedStateCount"));
    assertEquals(5, (int) accessInterest2.readFrom("confirmDispatchedResultedIn"));
    final State<?> state456 = accessDispatcher2.readFrom("dispatchedState", dispatchId("456"));
    assertEquals("456", state456.id);
    final State<?> state567 = accessDispatcher2.readFrom("dispatchedState", dispatchId("567"));
    assertEquals("567", state567.id);
  }

  @Test
  public void testThatReadErrorIsReported() {
    final AccessSafely accessInterest1 = interest.afterCompleting(3);
    dispatcher.afterCompleting(2);

    final Entity1 entity = new Entity1("123", 1);
    store.write(entity.id, entity, 1, interest);
    store.read(null, Entity1.class, interest);

    assertEquals(1, (int) accessInterest1.readFrom("errorCausesCount"));
    final Exception cause1 = accessInterest1.readFrom("errorCauses");
    assertEquals("The id is null.", cause1.getMessage());
    Result result1 = accessInterest1.readFrom("textReadResult");
    assertTrue(result1.isError());
    assertNull(accessInterest1.readFrom("stateHolder"));

    interest = new MockResultInterest();
    final AccessSafely accessInterest2 = interest.afterCompleting(1);
    dispatcher.afterCompleting(1);

    store.read(entity.id, null, interest);  // includes read

    assertEquals(1, (int) accessInterest2.readFrom("errorCausesCount"));
    final Exception cause2 = accessInterest2.readFrom("errorCauses");
    assertEquals("The type is null.", cause2.getMessage());
    Result result2 = accessInterest2.readFrom("textReadResult");
    assertTrue(result2.isError());
    final Object objectState = accessInterest2.readFrom("stateHolder");
    assertNull(objectState);
  }

  @Test
  public void testThatWriteErrorIsReported() {
    final AccessSafely accessInterest1 = interest.afterCompleting(1);
    dispatcher.afterCompleting(1);

    store.write(null, null, 0, interest);

    assertEquals(1, (int) accessInterest1.readFrom("errorCausesCount"));
    final Exception cause1 = accessInterest1.readFrom("errorCauses");
    assertEquals("The state is null.", cause1.getMessage());
    final Result result1 = accessInterest1.readFrom("textWriteAccumulatedResults");
    assertTrue(result1.isError());
    final Object objectState = accessInterest1.readFrom("stateHolder");
    assertNull(objectState);
  }

  @Test
  public void testRedispatch() {
    final AccessSafely accessInterest = interest.afterCompleting(6);
    final AccessSafely accessDispatcher = dispatcher.afterCompleting(6);

    final Entity1 entity1 = new Entity1(UUID.randomUUID().toString(), 1);
    final TestEvent testEvent1 = new TestEvent(UUID.randomUUID().toString(), 30);
    store.write(entity1.id, entity1, 1, Collections.singletonList(testEvent1), interest);

    final Entity1 entity2 = new Entity1(UUID.randomUUID().toString(), 2);
    final TestEvent testEvent2 = new TestEvent(UUID.randomUUID().toString(), 45);
    store.write(entity2.id, entity2, 1, Collections.singletonList(testEvent2), interest);


    final Entity1 entity3 = new Entity1(UUID.randomUUID().toString(), 3);
    final TestEvent testEvent3 = new TestEvent(UUID.randomUUID().toString(), 12);
    store.write(entity3.id, entity3, 1, Arrays.asList(testEvent1, testEvent2, testEvent3), interest);

    final int confirmDispatchedResultedIn = accessInterest.readFrom("confirmDispatchedResultedIn");
    assertEquals("confirmDispatchedResultedIn", 3, confirmDispatchedResultedIn);

    final int writeTextResultedIn = accessInterest.readFrom("writeTextResultedIn");
    assertEquals("writeTextResultedIn", 3, writeTextResultedIn);

    final int dispatchedStateCount = accessDispatcher.readFrom("dispatchedStateCount");
    assertEquals("dispatchedStateCount", 3, dispatchedStateCount);

    final int dispatchAttemptCount = accessDispatcher.readFrom("dispatchAttemptCount");
    assertEquals("dispatchAttemptCount", 3, dispatchAttemptCount);
  }

  @Before
  public void setUp() throws Exception {
    world = World.startWithDefaults("test-store");

    dbBootstrap = getBootstrap(DataFormat.Text);
    dbBootstrap.startService();
    configuration = dbBootstrap.createRandomDatabase();
    connection = new ConnectionProvider(configuration).connection();

    entity1StoreName = Entity1.class.getSimpleName();
    StateTypeStateStoreMap.stateTypeToStoreName(Entity1.class, entity1StoreName);

    delegate = delegate();
    interest = new MockResultInterest();
    dispatcher = new MockTextDispatcher(0, interest);

    EntryAdapterProvider.instance(world).registerAdapter(TestEvent.class, new TestEventAdapter());
    StateAdapterProvider.instance(world).registerAdapter(Entity1.class, new Entity1StateAdapter());
    // NOTE: No adapter registered for Entity2.class because it will use the default

    store = world.actorFor(
            StateStore.class,
            Definition.has(JDBCStateStoreActor.class,
                    Definition.parameters(dispatcher, delegate, new ConnectionProvider(configuration))));
  }

  @After
  public void tearDown() throws Exception {
    if (configuration == null) return;
    world.terminate();
    delegate.close();
    dbBootstrap.dropDatabase(configuration.databaseName);
    dbBootstrap.stopService();
  }

  protected abstract StorageDelegate delegate() throws Exception;

  protected abstract DbBootstrap getBootstrap(DataFormat dataFormat);

  private String dispatchId(final String entityId) {
    return entity1StoreName + ":" + entityId;
  }
}
