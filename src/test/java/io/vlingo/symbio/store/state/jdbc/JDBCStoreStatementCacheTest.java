package io.vlingo.symbio.store.state.jdbc;

import io.vlingo.symbio.store.common.jdbc.CachedStatement;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.function.Supplier;

import static org.junit.Assert.*;

public class JDBCStoreStatementCacheTest {

    public static final String TEST_ID = "foo";
    private PreparedStatement mockPreparedStatement;
    private Connection mockConnection;
    private JDBCStoreStatementCache<String> cache;

    @Before
    public void setUp() {
        mockPreparedStatement = createPreparedStatement();
        mockConnection = createConnection();
        cache = new JDBCStoreStatementCache<>();
    }

    @Test
    public void cache_statements_for_same_connection() {

        final Supplier<CachedStatement<String>> supplier = () -> new CachedStatement<>(mockPreparedStatement, null);
        final CachedStatement<String> statement1 = cache.getOrCreateStatement(TEST_ID, mockConnection, supplier);
        final CachedStatement<String> statement2 = cache.getOrCreateStatement(TEST_ID, mockConnection, supplier);

        assertSame(statement1, statement2);
    }

    @Test
    public void invoke_new_statement_after_connection_change() {
        final Connection mockConnectionChanged = createConnection();

        final Supplier<CachedStatement<String>> supplier = () -> new CachedStatement<>(mockPreparedStatement, null);
        final CachedStatement<String> statement1 = cache.getOrCreateStatement(TEST_ID, mockConnection, supplier);
        final CachedStatement<String> statement2 = cache.getOrCreateStatement(TEST_ID, mockConnectionChanged, supplier);

        assertNotSame(statement1, statement2);
    }

    private Connection createConnection() {
        return (Connection) Proxy.newProxyInstance(MockObjectHandler.class.getClassLoader(),
                new Class[]{Connection.class},
                new MockObjectHandler());
    }

    private PreparedStatement createPreparedStatement() {
        return (PreparedStatement) Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(),
                new Class[]{PreparedStatement.class},
                new MockObjectHandler());
    }

    static class MockObjectHandler implements InvocationHandler {

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.getName().equals("equals")) {
                return proxy == args[0];
            } else {
                return null;
            }
        }
    }
}