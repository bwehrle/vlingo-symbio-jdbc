// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.vlingo.actors.Actor;
import io.vlingo.actors.Definition;
import io.vlingo.common.Completes;
import io.vlingo.common.Failure;
import io.vlingo.common.Success;
import io.vlingo.symbio.BaseEntry;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.EntryAdapterProvider;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.Source;
import io.vlingo.symbio.State;
import io.vlingo.symbio.State.TextState;
import io.vlingo.symbio.StateAdapterProvider;
import io.vlingo.symbio.store.EntryReader;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.dispatch.Dispatchable;
import io.vlingo.symbio.store.dispatch.Dispatcher;
import io.vlingo.symbio.store.dispatch.DispatcherControl;
import io.vlingo.symbio.store.dispatch.control.DispatcherControlActor;
import io.vlingo.symbio.store.state.StateStore;
import io.vlingo.symbio.store.state.StateStoreEntryReader;
import io.vlingo.symbio.store.state.StateTypeStateStoreMap;

public class JDBCStateStoreActor extends Actor implements StateStore {

  private final JDBCStorageDelegate<TextState> delegate;
  private final Dispatcher<Dispatchable<Entry<?>, State<String>>> dispatcher;
  private final DispatcherControl dispatcherControl;
  private final Map<String,StateStoreEntryReader<?>> entryReaders;
  private final EntryAdapterProvider entryAdapterProvider;
  private final StateAdapterProvider stateAdapterProvider;

  public JDBCStateStoreActor(final JDBCStorageDelegate<TextState> delegate) {
    this(null, delegate, 0L, 0L);
  }

  public JDBCStateStoreActor(final Dispatcher<Dispatchable<Entry<?>, State<String>>> dispatcher,
                             final JDBCStorageDelegate<TextState> delegate) {
    this(dispatcher, delegate, 1000L, 1000L);
  }

  public JDBCStateStoreActor(final Dispatcher<Dispatchable<Entry<?>, State<String>>> dispatcher,
                             final JDBCStorageDelegate<TextState> delegate,
                             final long checkConfirmationExpirationInterval,
                             final long confirmationExpiration) {
    this.delegate = delegate;
    this.entryReaders = new HashMap<>();
    this.entryAdapterProvider = EntryAdapterProvider.instance(stage().world());
    this.stateAdapterProvider = StateAdapterProvider.instance(stage().world());

    if (dispatcher!=null){
      this.dispatcher = dispatcher;
      this.dispatcherControl = stage().actorFor(
        DispatcherControl.class,
        Definition.has(
          DispatcherControlActor.class,
          Definition.parameters(dispatcher, delegate.copy(), checkConfirmationExpirationInterval, confirmationExpiration))
      );
    } else {
      this.dispatcher = null;
      this.dispatcherControl = null;
    }
  }

  @Override
  public void stop() {
    for (final StateStoreEntryReader<?> reader : entryReaders.values()) {
      reader.close();
    }
    delegate.close();
    if (dispatcherControl != null) {
      dispatcherControl.stop();
    }
    super.stop();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <ET extends Entry<?>> Completes<StateStoreEntryReader<ET>> entryReader(final String name) {
    StateStoreEntryReader<?> reader = entryReaders.get(name);
    if (reader == null) {
      final EntryReader.Advice advice = delegate.entryReaderAdvice();
      reader = childActorFor(StateStoreEntryReader.class, Definition.has(advice.entryReaderClass, Definition.parameters(advice, name)));
      entryReaders.put(name, reader);
    }
    return completes().with((StateStoreEntryReader<ET>) reader);
  }

  @Override
  public void read(final String id, final Class<?> type, final ReadResultInterest interest, final Object object) {
    if (interest != null) {
      if (id == null || type == null) {
        interest.readResultedIn(Failure.of(new StorageException(Result.Error, id == null ? "The id is null." : "The type is null.")), id, null, -1, null, object);
        return;
      }

      final String storeName = StateTypeStateStoreMap.storeNameFrom(type);

      if (storeName == null) {
        interest.readResultedIn(Failure.of(new StorageException(Result.NoTypeStore, "No type store.")), id, null, -1, null, object);
        return;
      }

      try {
        delegate.beginRead();
        final PreparedStatement readStatement = delegate.readExpressionFor(storeName, id);
        try (final ResultSet result = readStatement.executeQuery()) {
          if (result.first()) {
            final TextState raw = delegate.stateFrom(result, id);
            final Object state = stateAdapterProvider.fromRaw(raw);
            interest.readResultedIn(Success.of(Result.Success), id, state, raw.dataVersion, raw.metadata, object);
          } else {
            interest.readResultedIn(Failure.of(new StorageException(Result.NotFound, "Not found for: " + id)), id, null, -1, null, object);
          }
        }
        delegate.complete();
      } catch (final Exception e) {
        delegate.fail();
        interest.readResultedIn(Failure.of(new StorageException(Result.Failure, e.getMessage(), e)), id, null, -1, null, object);
        logger().error(
                getClass().getSimpleName() +
                " readText() failed because: " + e.getMessage() +
                " for: " + (id == null ? "unknown id" : id),
                e);
      }
    } else {
      logger().warn(
              getClass().getSimpleName() +
              " readText() missing ResultInterest for: " +
              (id == null ? "unknown id" : id));
    }
  }

  @Override
  public <S,C> void write(final String id, final S state, final int stateVersion, final List<Source<C>> sources, final Metadata metadata,
          final WriteResultInterest interest, final Object object) {
    if (interest != null) {
      if (state == null) {
        interest.writeResultedIn(Failure.of(new StorageException(Result.Error, "The state is null.")), id,null, stateVersion, sources, object);
      } else {
        try {
          final String storeName = StateTypeStateStoreMap.storeNameFrom(state.getClass());

          if (storeName == null) {
            interest.writeResultedIn(Failure.of(new StorageException(Result.NoTypeStore, "No type store.")), id, state, stateVersion, sources, object);
            return;
          }

          final TextState raw = metadata == null ?
                  stateAdapterProvider.asRaw(id, state, stateVersion) :
                  stateAdapterProvider.asRaw(id, state, stateVersion, metadata);

          delegate.beginWrite();
          final PreparedStatement writeStatement = delegate.writeExpressionFor(storeName, raw);
          writeStatement.execute();
          final String dispatchId = storeName + ":" + id;
          final List<Entry<?>> entries = appendEntries(sources, stateVersion, metadata);

          final Dispatchable<Entry<?>, State<String>> dispatchable = buildDispatchable(dispatchId, raw, entries);
          final PreparedStatement dispatchableStatement = delegate.dispatchableWriteExpressionFor(dispatchable);
          dispatchableStatement.execute();

          delegate.complete();

          dispatch(dispatchable);

          interest.writeResultedIn(Success.of(Result.Success), id, state, stateVersion, sources, object);
        } catch (final Exception e) {
          logger().error(getClass().getSimpleName() + " writeText() error because: " + e.getMessage(), e);
          delegate.fail();
          interest.writeResultedIn(Failure.of(new StorageException(Result.Error, e.getMessage(), e)), id, state, stateVersion, sources, object);
        }
      }
    } else {
      logger().warn(
              getClass().getSimpleName() +
              " writeText() missing ResultInterest for: " +
              (state == null ? "unknown id" : id));
    }
  }

  @SuppressWarnings("rawtypes")
  private <C> List<Entry<?>> appendEntries(final List<Source<C>> sources, final int stateVersion, final Metadata metadata) {
    if (sources.isEmpty()) return Collections.emptyList();
    Connection connection = null;

    try {
      connection = createOrReuse(delegate.connection);
      // final Delegate delegate = getOrCreateDelegate(connection)
      final List<Entry<?>> adapted = entryAdapterProvider.asEntries(sources, stateVersion, metadata);
      for (final Entry<?> entry : adapted) {
        long id = -1L;
        final PreparedStatement appendStatement = delegate.appendExpressionFor(entry, connection);
        final int count = appendStatement.executeUpdate();
        if (count == 1) {
          final PreparedStatement queryLastIdentityStatement = delegate.appendIdentityExpression();
          try (final ResultSet result = queryLastIdentityStatement.executeQuery()) {
            if (result.next()) {
              id = result.getLong(1);
              ((BaseEntry) entry).__internal__setId(Long.toString(id));
            }
          }
        }
        if (id == -1L) {
          final String message = "Could not retrieve entry id.";
          logger().error(message);
          throw new IllegalStateException(message);
        }
      }
      return adapted;
    } catch (final Exception e) {
      final String message = "Failed to append entry because: " + e.getMessage();
      logger().error(message, e);
      closeConnection(connection);
      throw new IllegalStateException(message, e);
    }
  }

  private void closeConnection(Connection connection) {
    try {
      if (connection != null) {
        connection.close();
      }
    } catch (SQLException ignored) {}
  }

  /// Move this off to a ConnectionProvider/DataSource actor for isolation from the actor concern
  private Connection createOrReuse(final Connection connection) {
    try {
      if (connection == null || connection.isClosed()) {
        // Use connectionProvider here to get a new connection, yo!
        return delegate.connection();
      }
      return connection;
    } catch (SQLException sqlEx) {
      throw new IllegalStateException(sqlEx.getMessage(), sqlEx);
    }
  }

  private void dispatch(final Dispatchable<Entry<?>, State<String>> dispatchable) {
    if (this.dispatcher != null) {
      dispatcher.dispatch(dispatchable);
    }
  }

  private Dispatchable<Entry<?>, State<String>> buildDispatchable(final String dispatchId, final State<String> state, final List<Entry<?>> entries) {
    return new Dispatchable<>(dispatchId, LocalDateTime.now(), state.asTextState(), entries);
  }
}
