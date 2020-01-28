// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.journal.jdbc;

import com.google.gson.Gson;
import io.vlingo.actors.Actor;
import io.vlingo.common.Completes;
import io.vlingo.common.Tuple2;
import io.vlingo.symbio.BaseEntry;
import io.vlingo.symbio.BaseEntry.TextEntry;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.common.jdbc.ConnectionProvider;
import io.vlingo.symbio.store.common.jdbc.DatabaseType;
import io.vlingo.symbio.store.journal.JournalReader;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class JDBCJournalReaderActor extends Actor implements JournalReader<TextEntry> {
    private final ConnectionProvider connectionProvider;
    private Connection connection;
    private final DatabaseType databaseType;
    private final Gson gson;
    private final String name;
    private JDBCQueries queries;
    private long offset;

    public JDBCJournalReaderActor(final Configuration configuration,
                                  final ConnectionProvider connectionProvider,
                                  final String name) throws SQLException {
        this.connectionProvider = connectionProvider;
        this.databaseType = configuration.databaseType;
        this.name = name;
        this.gson = new Gson();

        if (!reuseOrOpenConnection() || !retrieveCurrentOffset()) {
            logger().error("vlingo-symbio-jdbc:journal-reader-" + databaseType + ": " + "Unable to initialize");
            throw new IllegalStateException("Unable to initialize");
        }
    }

    private boolean reuseOrOpenConnection() {
        try {
            if (connection == null || connection.isClosed()) {
                connection = connectionProvider.connection();
                this.queries = JDBCQueries.queriesFor(this.connection);
            }
            return true;
        }catch (Exception e) {
            logger().error("vlingo-symbio-jdbc:journal-reader-" + databaseType + ": " + e.getMessage(), e);
            return false;
        }
    }

    @Override
    public void close() {
      try {
        queries.close();
        connection.close();
      } catch (SQLException e) {
        // ignore
      }
    }

    @Override
    public Completes<String> name() {
        return completes().with(name);
    }

    @Override
    public Completes<TextEntry> readNext() {
        if (!reuseOrOpenConnection()) {
            return completes().with(null);
        }

        try (final ResultSet resultSet = queries.prepareSelectEntryQuery(offset).executeQuery()) {
            if (resultSet.next()) {
                final Tuple2<TextEntry,Long> entry = entryFromResultSet(resultSet);
                offset = entry._2 + 1;
                updateCurrentOffset();
                return completes().with(entry._1);
            }
        } catch (Exception e) {
            logger().error("vlingo-symbio-jdbc:journal-reader-" + databaseType + ": " + e.getMessage(), e);
        }

        return completes().with(null);
    }

    @Override
    public Completes<TextEntry> readNext(final String fromId) {
        reuseOrOpenConnection();
        seekTo(fromId);
        return readNext();
    }

    @Override
    public Completes<List<TextEntry>> readNext(final int maximumEvents) {
        final List<TextEntry> events = new ArrayList<>(maximumEvents);

        if (!reuseOrOpenConnection()) {
            return completes().with(null);
        }

        try (final ResultSet resultSet = queries.prepareSelectEntryBatchQuery(offset, maximumEvents).executeQuery()) {
            while (resultSet.next()) {
                final Tuple2<TextEntry,Long> entry = entryFromResultSet(resultSet);
                offset = entry._2 + 1;
                events.add(entry._1);
            }

            updateCurrentOffset();
            return completes().with(events);

        } catch (Exception e) {
            logger().error("vlingo-symbio-jdbc:journal-reader-" + databaseType + ": " + e.getMessage(), e);
        }

        return completes().with(null);
    }

    @Override
    public Completes<List<TextEntry>> readNext(final String fromId, final int maximumEntries) {
      seekTo(fromId);
      reuseOrOpenConnection();
      return readNext(maximumEntries);
    }

    @Override
    public void rewind() {
        this.offset = 1;
        reuseOrOpenConnection();
        updateCurrentOffset();
    }

    @Override
    public Completes<String> seekTo(final String id) {
        if (!reuseOrOpenConnection()) {
            return completes().with(String.valueOf(offset));
        }

        switch (id) {
            case Beginning:
                this.offset = 1;
                updateCurrentOffset();
                break;
            case End:
                this.offset = retrieveLastOffset() + 1;
                updateCurrentOffset();
                break;
            case Query:
                break;
            default:
                this.offset = Integer.parseInt(id);
                updateCurrentOffset();
                break;
        }

        return completes().with(String.valueOf(offset));
    }

    @Override
    public Completes<Long> size() {
        if (!reuseOrOpenConnection()) {
            return completes().with(-1L);
        }

        try (final ResultSet resultSet = queries.prepareSelectJournalCount().executeQuery()) {
          if (resultSet.next()) {
              final long count = resultSet.getLong(1);
              connection.commit();
              return completes().with(count);
          }
        } catch (Exception e) {
          logger().error("vlingo-symbio-jdbc:journal-reader-" + databaseType + ": " + e.getMessage(), e);
          logger().error("vlingo-symbio-jdbc:journal-reader-" + databaseType + ": Rewinding the offset");
        }

        return completes().with(-1L);
    }

    private Tuple2<TextEntry,Long> entryFromResultSet(final ResultSet resultSet) throws SQLException, ClassNotFoundException {
        final long id = resultSet.getLong(1);
        final String entryData = resultSet.getString(2);
        final String entryType = resultSet.getString(3);
        final int eventTypeVersion = resultSet.getInt(4);
        final String entryMetadata = resultSet.getString(5);

        final Class<?> classOfEvent = Class.forName(entryType);

        final Metadata eventMetadataDeserialized = gson.fromJson(entryMetadata, Metadata.class);
        return Tuple2.from(new BaseEntry.TextEntry(String.valueOf(id), classOfEvent, eventTypeVersion, entryData, eventMetadataDeserialized), id);
    }

    private boolean retrieveCurrentOffset() {
        this.offset = 1;
        try (final ResultSet resultSet = queries.prepareSelectCurrentOffsetQuery(name).executeQuery()) {
            if (resultSet.next()) {
                this.offset = resultSet.getLong(1);
                connection.commit();
            }
            return true;
        } catch (Exception e) {
            logger().error("vlingo-symbio-jdbc:journal-reader-" + databaseType + ": " + e.getMessage(), e);
            logger().error("vlingo-symbio-jdbc:journal-reader-" + databaseType + ": Rewinding the offset");
            return false;
        }
    }

    private void updateCurrentOffset() {
        try {
            queries.prepareUpsertOffsetQuery(name, offset).executeUpdate();
            connection.commit();
        } catch (Exception e) {
            logger().error("vlingo-symbio-jdbc:journal-reader-" + databaseType + ": Could not persist the offset. Will retry on next read.");
            logger().error("vlingo-symbio-jdbc:journal-reader-" + databaseType + ": " + e.getMessage(), e);
        }
    }

    private long retrieveLastOffset() {
        try (final ResultSet resultSet = queries.prepareSelectLastOffsetQuery().executeQuery()) {
            if (resultSet.next()) {
                final long lastOffset = resultSet.getLong(1);
                connection.commit();
                return lastOffset;
            }
        } catch (Exception e) {
            logger().error("vlingo-symbio-jdbc:journal-reader-" + databaseType + ": Could not retrieve latest offset, using current.");
        }

        return offset;
    }
}
