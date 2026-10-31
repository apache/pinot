/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.integration.tests;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.broker.broker.helix.BaseBrokerStarter;
import org.apache.pinot.client.admin.PinotAdminClient;
import org.apache.pinot.server.starter.helix.BaseServerStarter;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.ITestResult;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;


/// Shares one default offline cluster between sequential segment-upload tests.
/// The owner has independent process state; each class retains its own data directories and configuration hooks.
/// Direct class selection uses the same suite callbacks to start and close its own cluster.
/// Parallel classes are rejected.
public abstract class SharedOfflineUploadIntegrationTestSuite extends BaseClusterIntegrationTest {
  private static final Set<SharedOfflineUploadIntegrationTestSuite> PARTICIPANTS = new LinkedHashSet<>();
  private static OfflineUploadClusterOwner _owner;
  private static SharedOfflineUploadIntegrationTestSuite _activeClass;
  private static Throwable _cleanupFailure;

  private final Set<String> _tables = new LinkedHashSet<>();
  private final Set<String> _schemas = new LinkedHashSet<>();

  @BeforeSuite(alwaysRun = true)
  public final void setUpSharedOfflineUploadSuite()
      throws Throwable {
    synchronized (SharedOfflineUploadIntegrationTestSuite.class) {
      checkCleanupFailure();
      if (_owner == null) {
        _owner = new OfflineUploadClusterOwner();
        try {
          _owner.startCluster();
        } catch (Throwable t) {
          _cleanupFailure = t;
          Throwable cleanupFailure = _owner.stopCluster(null);
          if (cleanupFailure != null) {
            t.addSuppressed(cleanupFailure);
          }
          throw t;
        }
      }
    }
  }

  @BeforeClass(alwaysRun = true)
  public final void bindSharedOfflineUploadCluster() {
    synchronized (SharedOfflineUploadIntegrationTestSuite.class) {
      checkCleanupFailure();
      if (_activeClass != null) {
        throw new IllegalStateException("Another offline upload class still owns the shared cluster");
      }
      _activeClass = this;
      PARTICIPANTS.add(this);
      // These inherited fields are read directly by metadata-push tests and external-view helpers.
      _controllerConfig = owner().getControllerConfig();
      _helixResourceManager = owner().getHelixResourceManager();
    }
  }

  @BeforeMethod(alwaysRun = true)
  public final void checkSharedOfflineUploadCleanup() {
    checkCleanupFailure();
  }

  @Override
  public final void addSchema(Schema schema)
      throws IOException {
    _schemas.add(schema.getSchemaName());
    super.addSchema(schema);
  }

  @Override
  public final void addTableConfig(TableConfig tableConfig)
      throws IOException {
    if (tableConfig.getTableType() != TableType.OFFLINE) {
      throw new IllegalArgumentException("The shared offline upload suite only supports OFFLINE tables");
    }
    _tables.add(tableConfig.getTableName());
    super.addTableConfig(tableConfig);
  }

  @AfterMethod(alwaysRun = true)
  public final void cleanUpSharedOfflineUploadMethod(ITestResult result)
      throws Throwable {
    Throwable failure = cleanUpTables(null);
    if (failure != null) {
      recordCleanupFailure(failure);
      if (result.getThrowable() != null && result.getThrowable() != failure) {
        result.getThrowable().addSuppressed(failure);
      }
      throw failure;
    }
  }

  @AfterClass(alwaysRun = true)
  public final void cleanUpSharedOfflineUploadClass()
      throws Throwable {
    Throwable failure = cleanUpClass(null);
    synchronized (SharedOfflineUploadIntegrationTestSuite.class) {
      if (_activeClass == this) {
        _activeClass = null;
      }
    }
    if (failure != null) {
      recordCleanupFailure(failure);
      throw failure;
    }
  }

  private Throwable cleanUpTables(Throwable failure) {
    for (String table : List.copyOf(_tables)) {
      try {
        owner().removeTable(table);
        _tables.remove(table);
      } catch (Throwable t) {
        failure = appendFailure(failure, t);
      }
    }
    return failure;
  }

  private Throwable cleanUpClass(Throwable failure) {
    failure = cleanUpTables(failure);
    // Preserve the original schema lifetime across upload methods, then isolate the next class.
    if (_tables.isEmpty()) {
      for (String schema : List.copyOf(_schemas)) {
        try {
          if (owner().getHelixResourceManager().getSchema(schema) != null) {
            owner().deleteSchema(schema);
          }
          _schemas.remove(schema);
        } catch (Throwable t) {
          failure = appendFailure(failure, t);
        }
      }
    }
    if (_pinotConnection != null) {
      failure = cleanUp(failure, () -> {
        _pinotConnection.close();
        _pinotConnection = null;
      });
    }
    if (_pinotConnectionV2 != null) {
      failure = cleanUp(failure, () -> {
        _pinotConnectionV2.close();
        _pinotConnectionV2 = null;
      });
    }
    if (_tables.isEmpty() && _schemas.isEmpty()) {
      failure = cleanUp(failure, () -> FileUtils.deleteDirectory(_tempDir));
    }
    return failure;
  }

  @AfterSuite(alwaysRun = true)
  public final void tearDownSharedOfflineUploadSuite()
      throws Throwable {
    synchronized (SharedOfflineUploadIntegrationTestSuite.class) {
      if (_owner == null) {
        return;
      }
      Throwable failure = null;
      for (SharedOfflineUploadIntegrationTestSuite participant : PARTICIPANTS) {
        failure = participant.cleanUpClass(failure);
      }
      failure = _owner.stopCluster(failure);
      _owner = null;
      _activeClass = null;
      PARTICIPANTS.clear();
      if (failure != null) {
        recordCleanupFailure(failure);
        throw failure;
      }
    }
  }

  private static OfflineUploadClusterOwner owner() {
    if (_owner == null) {
      throw new IllegalStateException("The shared offline upload cluster has not started");
    }
    return _owner;
  }

  private static void checkCleanupFailure() {
    if (_cleanupFailure != null) {
      throw new IllegalStateException("The shared offline upload cluster did not clean up successfully",
          _cleanupFailure);
    }
  }

  private static synchronized void recordCleanupFailure(Throwable failure) {
    _cleanupFailure = appendFailure(_cleanupFailure, failure);
  }

  private static Throwable appendFailure(Throwable failure, Throwable next) {
    if (failure == null) {
      return next;
    }
    if (failure != next) {
      failure.addSuppressed(next);
    }
    return failure;
  }

  private static Throwable cleanUp(Throwable failure, CleanupAction action) {
    try {
      action.run();
    } catch (Throwable t) {
      return appendFailure(failure, t);
    }
    return failure;
  }

  /// A cleanup step whose failure must not prevent the remaining steps.
  @FunctionalInterface
  private interface CleanupAction {
    void run()
        throws Exception;
  }

  @Override
  public final PinotAdminClient getOrCreateAdminClient()
      throws IOException {
    return owner().getOrCreateAdminClient();
  }

  @Override
  public final String getControllerBaseApiUrl() {
    return owner().getControllerBaseApiUrl();
  }

  @Override
  public final String getZkUrl() {
    return owner().getZkUrl();
  }

  @Override
  public final String getHelixClusterName() {
    return owner().getHelixClusterName();
  }

  @Override
  protected final String getBrokerBaseApiUrl() {
    return owner().getBrokerBaseApiUrl();
  }

  @Override
  protected final String getBrokerGrpcEndpoint() {
    return owner().getBrokerGrpcEndpoint();
  }

  /// Owns only the default ZK/controller/broker/server processes; no test class owns these resources.
  private static final class OfflineUploadClusterOwner extends BaseClusterIntegrationTest {
    private BaseBrokerStarter _allocatedBroker;
    private BaseServerStarter _allocatedServer;

    private void startCluster()
        throws Exception {
      startZk();
      startController();
      startBroker();
      startServer();
    }

    @Override
    protected BaseBrokerStarter createBrokerStarter() {
      _allocatedBroker = super.createBrokerStarter();
      return _allocatedBroker;
    }

    @Override
    protected BaseServerStarter createServerStarter() {
      _allocatedServer = super.createServerStarter();
      return _allocatedServer;
    }

    private Throwable stopCluster(Throwable failure) {
      if (_allocatedServer != null) {
        // startServers registers a starter only after start succeeds; retain partially initialized starters too.
        if (!_serverStarters.contains(_allocatedServer)) {
          _serverStarters.add(_allocatedServer);
        }
        failure = cleanUp(failure, () -> {
          stopServer();
          _allocatedServer = null;
        });
      }
      if (_allocatedBroker != null) {
        if (!_brokerStarters.contains(_allocatedBroker)) {
          _brokerStarters.add(_allocatedBroker);
        }
        failure = cleanUp(failure, () -> {
          stopBroker();
          _allocatedBroker = null;
        });
      }
      if (_controllerStarter != null) {
        failure = cleanUp(failure, this::stopController);
      }
      return cleanUp(failure, this::stopZk);
    }

    private void removeTable(String tableNameWithType)
        throws IOException {
      if (_serverStarters.size() != 1) {
        throw new IllegalStateException("Expected one server while cleaning up an offline upload table");
      }
      if (_helixResourceManager.getOfflineTableConfig(tableNameWithType) != null) {
        dropOfflineTable(tableNameWithType);
      }
      waitForTableDataManagerRemoved(tableNameWithType);
      waitForEVToDisappear(tableNameWithType);
      String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
      TestUtils.waitForCondition(ignored -> {
        try {
          getDebugInfo("debug/routingTable/" + rawTableName);
          return false;
        } catch (Exception e) {
          String message = e.getMessage();
          return message != null && message.contains("Got error status code: 404");
        }
      }, 60_000L, "Routing table still exists after dropping offline upload table " + rawTableName);
    }
  }
}
