package io.vlingo.symbio.store.state.jdbc;

import io.vlingo.symbio.store.common.jdbc.CachedStatement;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class JDBCStoreStatementCache<T> implements Closeable {

  protected Map<String, CachedStatement<T>> statementsForStore;
  protected Connection lastConnection;

  JDBCStoreStatementCache() {
    lastConnection = null;
    statementsForStore = new HashMap<>();
  }

  @Override
  public void close() {
    statementsForStore.forEach( (k, cs) -> {
      try {
        cs.preparedStatement.close();
      } catch (SQLException ignored) {}
    });
    statementsForStore.clear();
  }

  private void clearCacheOnConnection(Connection connection) {
    if (!connection.equals(lastConnection)) {
      close();
      lastConnection = connection;
    }
  }

  public CachedStatement<T> getOrCreateStatement(final String storeId,
                                                     final Connection connection,
                                                     final Supplier<CachedStatement<T>> supplier) {
    clearCacheOnConnection(connection);
    CachedStatement<T> cachedStatement =  statementsForStore.computeIfAbsent(storeId, (s -> supplier.get()));
    return cleanStatementParameters(cachedStatement);
  }

  private CachedStatement<T> cleanStatementParameters(final CachedStatement<T> statement) {
    try {
      statement.preparedStatement.clearParameters();
      return statement;
    } catch(SQLException sqlEx) {
      throw new IllegalStateException(sqlEx.getMessage(), sqlEx);
    }
  }

}
