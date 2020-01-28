package io.vlingo.symbio.store.common;

import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.common.jdbc.ConnectionProvider;

import java.sql.Connection;
import java.util.HashSet;
import java.util.Set;

public class DbBootstrap {

  private final Configuration configuration;
  private final ConnectionProvider connectionProvider;
  private final BootstrapAdapter bootstrapAdapter;
  private final Set<String> createdDatabases;
  private boolean serviceStarted;

  public DbBootstrap(final Configuration configuration,
                     final ConnectionProvider connectionProvider,
                     final BootstrapAdapter bootstrapAdapter) {

    this.configuration = configuration;
    this.connectionProvider = connectionProvider;
    this.bootstrapAdapter = bootstrapAdapter;
    this.createdDatabases = new HashSet<>();
    this.serviceStarted = false;
  }

  protected void registerNewDatabase(final String databaseName) {
    this.createdDatabases.add(databaseName);
  }

  protected void deRegisterDroppedDatabase(final String databaseName) {
    this.createdDatabases.remove(databaseName);
  }

  protected boolean validateAllDatabasesDropped() {
    return this.createdDatabases.isEmpty();
  }

  public boolean startService() {
    try {
      if (!serviceStarted) {
        bootstrapAdapter.startService(configuration);
        serviceStarted = true;
        return true;
      } else {
        return false;
      }
    } catch (Exception ex) {
      throw new IllegalStateException(ex.getMessage(), ex);
    }
  }

  public boolean stopService() {
    try {
      if (serviceStarted) {
        bootstrapAdapter.stopService();
        serviceStarted = false;
      }
      return validateAllDatabasesDropped();
    } catch (Exception ex) {
      throw new IllegalStateException(ex.getMessage(), ex);
    }
  }

  public void dropDatabase(final String databaseName) {
    try( final Connection connection = connectionProvider.connection()) {
      bootstrapAdapter.dropDatabase(connection, databaseName);
      deRegisterDroppedDatabase(databaseName);
    } catch (Exception ex) {
      throw new IllegalStateException(ex.getMessage(), ex);
    }
  }

  public Configuration createRandomDatabase() {
    final String databaseName = TestConfiguration.generateDatabaseName(configuration.databaseName, configuration.format);
    return createDatabase(databaseName);

  }

  public Configuration createDatabase(final String databaseName) {
    try( final Connection connection = connectionProvider.connection()) {
      bootstrapAdapter.createDatabase(connection, databaseName);
      registerNewDatabase(databaseName);
      return configuration.forNewDatabase(databaseName);
    } catch (Exception ex) {
      throw new IllegalStateException(ex.getMessage(), ex);
    }
  }

  public interface BootstrapAdapter {
    void startService(final Configuration configuration) throws Exception;
    void stopService() throws Exception;
    void createDatabase(final Connection connection, final String databaseName) throws Exception;
    void dropDatabase(final Connection connection, final String databaseName) throws Exception;
  }
}
