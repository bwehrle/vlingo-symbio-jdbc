package io.vlingo.symbio.store.state.jdbc;

import io.vlingo.symbio.store.common.jdbc.CachedStatement;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class JDBCStoreStatementCache<T>  {

  protected Map<String, CachedStatement<T>> statementsForStore;

  JDBCStoreStatementCache() {
    statementsForStore = new HashMap<>();
  }

  public void closeAndRemoveAll() {
    statementsForStore.forEach( (k, cs) -> {
      try {
        cs.preparedStatement.close();
      } catch (SQLException ignored) {}
    });
    statementsForStore.clear();
  }

  public CachedStatement<T> getOrCreateStatement(final String storeId,
                                                 final Connection connection,
                                                 final Function<Connection, CachedStatement<T>> supplier) {

    CachedStatement<T> cachedStatement = statementsForStore.computeIfAbsent(storeId,
            s -> supplier.apply(connection));
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
