// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.object.jdbc.jdbi;

import io.vlingo.actors.Actor;
import io.vlingo.common.Completes;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.store.object.ObjectStoreEntryReader;
import io.vlingo.symbio.store.object.QueryExpression;
import io.vlingo.symbio.store.object.StateObjectMapper;
import org.jdbi.v3.core.mapper.RowMapper;

import java.util.Collection;
import java.util.List;

/**
 * An {@code ObjectStoreEntryReader} for Jdbi.
 */
public class JdbiObjectStoreEntryReaderActor extends Actor implements ObjectStoreEntryReader<Entry<String>> {
  private final JdbiPersistMapper currentEntryOffsetMapper;
  private final JdbiOnDatabase jdbi;
  private final String name;
  private final QueryExpression queryLastEntryId;
  private final QueryExpression querySize;

  private long offset;

  public JdbiObjectStoreEntryReaderActor(final JdbiOnDatabase jdbi,
                                         final Collection<StateObjectMapper> mappers,
                                         final String name) {
    this.jdbi = jdbi;
    this.name = name;
    this.offset = 1L;
    this.queryLastEntryId = jdbi.queryLastEntryId();
    this.currentEntryOffsetMapper = jdbi.currentEntryOffsetMapper(new String[] {":name", ":offset"});
    this.querySize = jdbi.querySize();

    mappers.forEach(mapper -> jdbi.handle.registerRowMapper((RowMapper<?>) mapper.queryMapper()));

    restoreCurrentOffset();
  }

  @Override
  public void close() {
  }

  @Override
  public Completes<String> name() {
    return completes().with(name);
  }

  @Override
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public Completes<Entry<String>> readNext() {
    try {
      final QueryExpression expression = jdbi.queryEntry(offset);
      final Entry entry = jdbi.handle().createQuery(expression.query).mapTo(Entry.class).one();
      ++offset;
      updateCurrentOffset();
      return completes().with(entry);
    } catch (Exception e) {
      logger().info("vlingo/symbio-jdbc: " + getClass().getSimpleName() + " Could not read next entry because: " + e.getMessage(), e);
      return completes().with(null);
    }
  }

  @Override
  public Completes<Entry<String>> readNext(final String fromId) {
    seekTo(fromId);
    return readNext();
  }

  @Override
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public Completes<List<Entry<String>>> readNext(final int maximumEntries) {
    try {
      final QueryExpression expression = jdbi.queryEntries(offset, maximumEntries);
      final List<Entry<String>> entries = (List) jdbi.handle().createQuery(expression.query).mapTo(expression.type).list();
      offset += entries.size();
      updateCurrentOffset();
      return completes().with(entries);
    } catch (Exception e) {
      logger().info("vlingo/symbio-jdbc: " + getClass().getSimpleName() + " Could not read next entry because: " + e.getMessage(), e);
      return completes().with(null);
    }
  }

  @Override
  public Completes<List<Entry<String>>> readNext(final String fromId, final int maximumEntries) {
    seekTo(fromId);
    return readNext(maximumEntries);
  }

  @Override
  public void rewind() {
    this.offset = 1;
    updateCurrentOffset();
  }

  @Override
  public Completes<String> seekTo(final String id) {
    switch (id) {
    case Beginning:
        this.offset = 1;
        updateCurrentOffset();
        break;
    case End:
        this.offset = retrieveLatestOffset() + 1;
        updateCurrentOffset();
        break;
    case Query:
        break;
    default:
        this.offset = Long.parseLong(id);
        updateCurrentOffset();
        break;
    }

    return completes().with(String.valueOf(offset));
  }

  @Override
  public Completes<Long> size() {
    try {
      return completes().with(jdbi.handle().createQuery(querySize.query).mapTo(Long.class).one());
    } catch (Exception e) {
      logger().info("vlingo/symbio-jdbc: " + getClass().getSimpleName() + " Could not retrieve size, using -1L.");
      return completes().with(-1L);
    }
  }

  private void restoreCurrentOffset() {
    this.offset = retrieveLatestOffset();
  }

  private long retrieveLatestOffset() {
    try {
      return jdbi.handle().createQuery(queryLastEntryId.query).mapTo(Long.class).one();
    } catch (Exception e) {
      logger().info("vlingo/symbio-jdbc: " + getClass().getSimpleName() + " Could not retrieve latest offset, using current.");
      return offset;
    }
  }

  private void updateCurrentOffset() {
    jdbi.handle().createUpdate(currentEntryOffsetMapper.insertStatement).bind("name", name).bind("offset", offset).execute();
  }
}
