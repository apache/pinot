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

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.client.admin.PinotAdminClient;
import org.apache.pinot.integration.tests.custom.CustomDataQueryClusterIntegrationTest;
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


/// Runs segment upload scenarios against the custom suite cluster.
/// Each class owns its table and schema resources; parallel classes are rejected.
public abstract class SharedOfflineUploadIntegrationTestSuite extends CustomDataQueryClusterIntegrationTest {
  private static final Set<SharedOfflineUploadIntegrationTestSuite> PARTICIPANTS = new LinkedHashSet<>();
  private static SharedOfflineUploadIntegrationTestSuite _activeClass;

  private final Set<String> _tables = new LinkedHashSet<>();
  private final Set<String> _schemas = new LinkedHashSet<>();

  @BeforeClass(alwaysRun = true)
  public final void bindSharedOfflineUploadCluster() {
    synchronized (SharedOfflineUploadIntegrationTestSuite.class) {
      assertCustomSuiteHealthy();
      if (_activeClass != null) {
        throw new IllegalStateException("Another offline upload class still owns the shared cluster");
      }
      _activeClass = this;
      PARTICIPANTS.add(this);
      // These inherited fields are read directly by metadata-push tests and external-view helpers.
      _controllerConfig = getSharedControllerStarter().getConfig();
      _controllerBaseApiUrl = _controllerConfig.generateVipUrl();
      _helixResourceManager = getSharedHelixResourceManager();
      initControllerRequestURLBuilder();
    }
  }

  // These tests create their own empty table at the point required by each scenario.
  @Override
  @BeforeClass
  public void setUp()
      throws Exception {
  }

  @Override
  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws IOException {
  }

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public Schema createSchema()
      throws IOException {
    return createDefaultSchema();
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    return getAllAvroFiles();
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    return createDefaultOfflineTableConfig();
  }

  @Override
  public String getTimeColumnName() {
    return DEFAULT_TIME_COLUMN_NAME;
  }

  @Override
  protected String getSortedColumn() {
    return getDefaultSortedColumn();
  }

  @Override
  protected List<String> getInvertedIndexColumns() {
    return getDefaultInvertedIndexColumns();
  }

  @Override
  protected List<String> getNoDictionaryColumns() {
    return getDefaultNoDictionaryColumns();
  }

  @Override
  protected List<String> getRangeIndexColumns() {
    return getDefaultRangeIndexColumns();
  }

  @Override
  protected List<String> getBloomFilterColumns() {
    return getDefaultBloomFilterColumns();
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
        removeTable(table);
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
          if (getSharedHelixResourceManager().getSchema(schema) != null) {
            deleteSchema(schema);
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
    if (_tables.isEmpty() && _schemas.isEmpty() && this != _sharedClusterTestSuite) {
      // The suite owner retains its directory until the Custom suite teardown.
      failure = cleanUp(failure, () -> FileUtils.deleteDirectory(_tempDir));
    }
    return failure;
  }

  @AfterSuite(alwaysRun = true)
  public final void tearDownSharedOfflineUploadSuite()
      throws Throwable {
    Throwable failure = null;
    for (SharedOfflineUploadIntegrationTestSuite participant : PARTICIPANTS) {
      failure = participant.cleanUpClass(failure);
    }
    PARTICIPANTS.clear();
    _activeClass = null;
    if (failure != null) {
      recordCleanupFailure(failure);
      throw failure;
    }
  }

  private static void recordCleanupFailure(Throwable failure) {
    recordCustomSuiteCleanupFailure(failure);
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
    if (_sharedClusterTestSuite == this) {
      return super.getOrCreateAdminClient();
    }
    return _sharedClusterTestSuite.getOrCreateAdminClient();
  }

  private void removeTable(String tableNameWithType)
      throws IOException {
    List<BaseServerStarter> servers = getSharedServerStarters();
    if (servers.size() != 2) {
      throw new IllegalStateException("Expected two custom suite servers while cleaning up an offline table");
    }
    if (getSharedHelixResourceManager().getOfflineTableConfig(tableNameWithType) != null) {
      dropOfflineTable(tableNameWithType);
    }
    TestUtils.waitForCondition(ignored -> {
      for (BaseServerStarter server : servers) {
        if (server.getServerInstance().getInstanceDataManager().getTableDataManager(tableNameWithType) != null) {
          return false;
        }
      }
      return true;
    }, 60_000L, "Failed to remove table data manager for table: " + tableNameWithType);
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
