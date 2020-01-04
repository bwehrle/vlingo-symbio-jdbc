package io.vlingo.symbio.store.common.jdbc;

import io.vlingo.common.Completes;

import java.sql.Connection;

public interface Datasource {

    Completes<Connection> getConnection();

    void releaseConnection(Connection connection);
}
