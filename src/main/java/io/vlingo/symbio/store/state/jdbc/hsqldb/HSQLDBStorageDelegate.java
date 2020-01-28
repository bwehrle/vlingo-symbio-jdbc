// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.jdbc.hsqldb;

import io.vlingo.actors.Logger;
import io.vlingo.common.Tuple2;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.EntryReader;
import io.vlingo.symbio.store.common.jdbc.CachedStatement;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.state.StateStore.StorageDelegate;
import io.vlingo.symbio.store.state.jdbc.JDBCDispatchableCachedStatements;
import io.vlingo.symbio.store.state.jdbc.JDBCStorageDelegate;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;

public class HSQLDBStorageDelegate extends JDBCStorageDelegate<Blob> implements StorageDelegate, HSQLDBQueries {
  private final Configuration configuration;

  public HSQLDBStorageDelegate(final Configuration configuration, final Logger logger) {

    super(configuration.format,
          configuration.originatorId,
          configuration.createTables,
          logger);

    this.configuration = configuration;
  }

  @Override
  public StorageDelegate copy() {
    try {
      return new HSQLDBStorageDelegate(Configuration.cloneOf(configuration), logger);
    } catch (Exception e) {
      final String message = "Copy of PostgresStorageDelegate failed because: " + e.getMessage();
      logger.error(message, e);
      throw new IllegalStateException(message, e);
    }
  }

  @Override
  public EntryReader.Advice entryReaderAdvice() {
    try {
      return new EntryReader.Advice(
              Tuple2.from(Configuration.cloneOf(configuration), connection),
              HSQLDBStateStoreEntryReaderActor.class,
              namedEntry(SQL_QUERY_ENTRY_BATCH),
              namedEntry(SQL_QUERY_ENTRY),
              namedEntry(QUERY_COUNT),
              namedEntryOffsets(QUERY_LATEST_OFFSET),
              namedEntryOffsets(UPDATE_CURRENT_OFFSET));
    } catch (Exception e) {
      throw new IllegalStateException("Cannot create EntryReader.Advice because: " + e.getMessage(), e);
    }
  }

  @Override
  protected byte[] binaryDataFrom(final ResultSet resultSet, final int columnIndex) throws Exception {
    final Blob blob = resultSet.getBlob(columnIndex);
    final byte[] data = blob.getBytes(1, (int) blob.length());
    return data;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected <D> D binaryDataTypeObject() throws Exception {
    return (D) connection.createBlob();
  }

  @Override
  protected JDBCDispatchableCachedStatements<Blob> dispatchableCachedStatements() {
    return new HSQLDBDispatchableCachedStatements(originatorId, connection, format, logger);
  }

  @Override
  protected String dispatchableIdIndexCreateExpression() {
    return namedDispatchable(SQL_DISPATCH_ID_INDEX);
  }

  @Override
  protected String dispatchableOriginatorIdIndexCreateExpression() {
    return namedDispatchable(SQL_ORIGINATOR_ID_INDEX);
  }

  @Override
  protected String dispatchableTableCreateExpression() {
    return MessageFormat.format(SQL_CREATE_DISPATCHABLES_STORE, dispatchableTableName(),
            format.isBinary() ? SQL_FORMAT_BINARY : SQL_FORMAT_TEXT);
  }

  @Override
  protected String dispatchableTableName() {
    return TBL_VLINGO_SYMBIO_DISPATCHABLES;
  }

  @Override
  protected String entryTableCreateExpression() {
    return MessageFormat.format(SQL_CREATE_ENTRY_STORE, entryTableName(),
            format.isBinary() ? SQL_FORMAT_BINARY : SQL_FORMAT_TEXT);
  }

  @Override
  protected String entryOffsetsTableCreateExpression() {
    return MessageFormat.format(SQL_CREATE_ENTRY_STORE_OFFSETS, entryOffsetsTableName(),
            format.isBinary() ? SQL_FORMAT_BINARY : SQL_FORMAT_TEXT);
  }

  @Override
  protected String entryTableName() {
    return TBL_VLINGO_SYMBIO_STATE_ENTRY;
  }

  @Override
  protected String entryOffsetsTableName() {
    return TBL_VLINGO_SYMBIO_STATE_ENTRY_OFFSETS;
  }

  @Override
  protected String readExpression(final String storeName, final String id) {
    return MessageFormat.format(SQL_STATE_READ, storeName.toUpperCase());
  }

  @Override
  protected <S> void setBinaryObject(final CachedStatement<Blob> cached, int columnIndex, State<S> state) throws Exception {
    final byte[] data = (byte[]) state.data;
    cached.data.setBytes(1, data);
    cached.preparedStatement.setBlob(columnIndex, cached.data);
  }

  @Override
  protected <E> void setBinaryObject(final CachedStatement<Blob> cached, int columnIndex, Entry<E> entry) throws Exception {
    final byte[] data = (byte[]) entry.entryData();
    cached.data.setBytes(1, data);
    cached.preparedStatement.setBlob(columnIndex, cached.data);
  }

  @Override
  protected <S> void setTextObject(final CachedStatement<Blob> cached, int columnIndex, State<S> state) throws Exception {
    cached.preparedStatement.setString(columnIndex, (String) state.data);
  }

  @Override
  protected <E> void setTextObject(final CachedStatement<Blob> cached, int columnIndex, Entry<E> entry) throws Exception {
    cached.preparedStatement.setString(columnIndex, (String) entry.entryData());
  }

  @Override
  protected String stateStoreTableCreateExpression(final String tableName) {
    return MessageFormat.format(SQL_CREATE_STATE_STORE, tableName,
            format.isBinary() ? SQL_FORMAT_BINARY : SQL_FORMAT_TEXT);
  }

  @Override
  protected String tableNameFor(String storeName) {
    return "TBL_" + storeName.toUpperCase();
  }

  @Override
  protected String textDataFrom(final ResultSet resultSet, final int columnIndex) throws Exception {
    final String data = resultSet.getString(columnIndex);
    return data;
  }

  @Override
  protected String writeExpression(String storeName) {
    return MessageFormat.format(SQL_STATE_WRITE, storeName.toUpperCase(),
            format.isBinary() ? SQL_FORMAT_BINARY_CAST : SQL_FORMAT_TEXT_CAST);
  }

  private String namedDispatchable(final String sql) {
    return MessageFormat.format(sql, dispatchableTableName());
  }

  private String namedEntry(final String sql) {
    return MessageFormat.format(sql, entryTableName());
  }

  private String namedEntryOffsets(final String sql) {
    final String formatted = MessageFormat.format(sql, entryOffsetsTableName());
    return formatted;
  }

  private static Blob blobIfBinary(final Connection connection, DataFormat format, final Logger logger) {
    try {
      return format.isBinary() ? connection.createBlob() : null;
    } catch (SQLException e) {
      final String message =
              HSQLDBDispatchableCachedStatements.class.getSimpleName() + ": Failed to create blob because: " + e.getMessage();
      logger.error(message, e);
      throw new IllegalStateException(message);
    }
  }

  class HSQLDBDispatchableCachedStatements extends JDBCDispatchableCachedStatements<Blob> {
    HSQLDBDispatchableCachedStatements(
            final String originatorId,
            final Connection connection,
            final DataFormat format,
            final Logger logger) {

      super(originatorId, connection, format, blobIfBinary(connection, format, logger), logger);
    }

    @Override
    protected String appendDispatchableExpression() {
      return namedDispatchable(SQL_DISPATCHABLE_APPEND);
    }

    @Override
    protected String appendEntryExpression() {
      return namedEntry(SQL_APPEND_ENTRY);
    }

    @Override
    protected String queryEntryExpression() {
      return namedEntry(SQL_QUERY_ENTRY);
    }

    @Override
    protected String appendEntryIdentityExpression() {
      return SQL_APPEND_ENTRY_IDENTITY;
    }

    @Override
    protected String deleteDispatchableExpression() {
      return namedDispatchable(SQL_DISPATCHABLE_DELETE);
    }

    @Override
    protected String selectDispatchableExpression() {
      return namedDispatchable(SQL_DISPATCHABLE_SELECT);
    }
  }
}
