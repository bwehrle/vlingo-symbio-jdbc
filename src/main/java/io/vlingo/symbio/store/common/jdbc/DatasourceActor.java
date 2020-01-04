package io.vlingo.symbio.store.common.jdbc;

import io.vlingo.actors.Actor;
import io.vlingo.common.Completes;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

public class DatasourceActor extends Actor implements Datasource {

    private final int connectionLimit;
    private final ConnectionProvider connectionProvider;
    private final Queue<Connection> unusedConnections;
    private final Map<Integer, Connection> busyConnections;
    private static int connectionAliveTimeoutMs = 100;

    public DatasourceActor(ConnectionProvider connectionProvider) {
        this.connectionProvider = connectionProvider;
        this.connectionLimit = 10;
        this.unusedConnections = new PriorityQueue<>(connectionLimit);
        this.busyConnections = new HashMap<>(connectionLimit);
    }

    ///
    // This should be an async actor that provides connections.  Since opening connections
    // can block indefinitely due to DNS, or during long periods of time (due to communication failures and lack
    // of timeouts in the JDBC API) this should be done in an async way
    //
    @Override
    public Completes<Connection> getConnection() {
        if (unusedConnections.size() > 0) {
            Connection connection = unusedConnections.remove();
           return completes().with(aliveOrReopen(connection));
        }
        else if (busyConnections.size() < connectionLimit) {
            final Connection connection = connectionProvider.connection();
            busyConnections.put(connection.hashCode(), connection);
            return Completes.withSuccess(connection);
        } else {
            throw new IllegalStateException("No connections are currently available");
        }
    }

    private Connection aliveOrReopen(Connection connection) {
        boolean isValid = false;
        try {
            isValid = connection.isValid(connectionAliveTimeoutMs);
        } catch (SQLException sqlEx) {
            isValid = false;
        }
        finally {
            if (isValid) {
                return connection;
            } else {
                return connectionProvider.connection();
            }
        }
    }

    @Override
    public void releaseConnection(Connection connection) {
        if (connection != null && busyConnections.containsKey(connection.hashCode())) {
            busyConnections.remove(connection.hashCode());
            unusedConnections.offer(connection);
        }
    }


}
