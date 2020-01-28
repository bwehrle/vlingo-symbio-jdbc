// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import io.vlingo.actors.Logger;
import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.common.jdbc.CachedStatement;

public abstract class JDBCDispatchableCachedStatements<T>  {
  private  CachedStatement<T> appendDispatchable;
  private  CachedStatement<T> queryEntry;
  private  CachedStatement<T> appendEntry;
  private  CachedStatement<T> appendEntryIdentity;
  private  CachedStatement<T> deleteDispatchable;
  private  CachedStatement<T> queryAllDispatchables;

  private final String originatorId;
  private final T appendDataObject;
  private final Logger logger;

  protected JDBCDispatchableCachedStatements(
          final String originatorId,
          final Connection connection,
          final DataFormat format,
          final T appendDataObject,
          final Logger logger) {

    this.originatorId = originatorId;
    this.appendDataObject = appendDataObject;
    this.logger = logger;

    createStatementsOnNewConnection(connection);
  }

  private void createStatementsOnNewConnection(Connection connection) {
      this.queryEntry = createStatement(queryEntryExpression(), appendDataObject, connection, logger);
      this.appendEntry = createStatement(appendEntryExpression(), appendDataObject, connection, logger);
      this.appendEntryIdentity = createStatement(appendEntryIdentityExpression(), null, connection, logger);
      this.appendDispatchable = createStatement(appendDispatchableExpression(), appendDataObject, connection, logger);
      this.deleteDispatchable = createStatement(deleteDispatchableExpression(), null, connection, logger);
      this.queryAllDispatchables = prepareQuery(createStatement(selectDispatchableExpression(), null, connection, logger), originatorId, logger);
  }

  public void closeAll() {
    closeStatement(queryEntry);
    closeStatement(appendEntry);
    closeStatement(appendEntryIdentity);
    closeStatement(appendDispatchable);
    closeStatement(deleteDispatchable);
    closeStatement(queryAllDispatchables);
  }

  private void closeStatement(CachedStatement<T> statement) {
      if (statement != null) {
        statement.close();
      }
  }

  public final CachedStatement<T> appendDispatchableStatement() {
    return cleanPreparedStatement(appendDispatchable);
  }

  public final CachedStatement<T> appendEntryStatement() {
    return cleanPreparedStatement(appendEntry);
  }

  public final CachedStatement<T> appendEntryIdentityStatement() {
    return cleanPreparedStatement(appendEntryIdentity);
  }

  public final CachedStatement<T> deleteStatement() {
    return cleanPreparedStatement(deleteDispatchable);
  }

  public final CachedStatement<T> queryAllStatement() {
    return cleanPreparedStatement(queryAllDispatchables);
  }
  
  public CachedStatement<T> getQueryEntry() {
    return cleanPreparedStatement(queryEntry);

  }

  protected abstract String appendEntryExpression();
  protected abstract String queryEntryExpression();

  protected abstract String appendDispatchableExpression();
  protected abstract String appendEntryIdentityExpression();
  protected abstract String deleteDispatchableExpression();
  protected abstract String selectDispatchableExpression();

  private CachedStatement<T> cleanPreparedStatement(CachedStatement<T> cachedStatement) {
    try {
      cachedStatement.preparedStatement.clearParameters();
      return cachedStatement;
    } catch (SQLException sqlEx) {
      throw new IllegalStateException(sqlEx.getMessage(), sqlEx);
    }
  }

  private CachedStatement<T> createStatement(
          final String sql,
          final T data,
          final Connection connection,
          final Logger logger) {

    try {
      final PreparedStatement preparedStatement = connection.prepareStatement(sql);
      return new CachedStatement<T>(preparedStatement, data);
    } catch (Exception e) {
      final String message =
              getClass().getSimpleName() + ": Failed to create dispatchable statement: \n" +
              sql +
              "\nbecause: " + e.getMessage();
      logger.error(message, e);
      throw new IllegalStateException(message);
    }
  }

  private CachedStatement<T> prepareQuery(final CachedStatement<T> cached, String originatorId, final Logger logger) {
    try {
      cached.preparedStatement.setString(1, originatorId);
      return cached;
    } catch (Exception e) {
      final String message =
              getClass().getSimpleName() + ": Failed to prepare query=all because: " + e.getMessage();
      logger.error(message, e);
      throw new IllegalStateException(message);
    }
  }
}
