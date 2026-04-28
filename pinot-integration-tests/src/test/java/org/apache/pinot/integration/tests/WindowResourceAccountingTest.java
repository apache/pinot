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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.accounting.PerQueryCPUMemAccountantFactory.PerQueryCPUMemResourceUsageAccountant;
import org.apache.pinot.core.accounting.QueryResourceTrackerImpl;
import org.apache.pinot.integration.tests.window.utils.WindowFunnelUtils;
import org.apache.pinot.server.starter.helix.BaseServerStarter;
import org.apache.pinot.spi.accounting.QueryResourceTracker;
import org.apache.pinot.spi.accounting.ThreadAccountant;
import org.apache.pinot.spi.accounting.ThreadAccountantFactory;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants.Accounting;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class WindowResourceAccountingTest extends SharedRichClusterIntegrationTest {
  private static final String SHARED_TABLE_NAME = "window_resource_accounting";

  private File _classTempDir;
  private File _classSegmentDir;
  private File _classTarDir;

  /// [PerQueryCPUMemResourceUsageAccountant] clears state at the end of a query. It cannot be used in tests to check if
  /// resources are being accounted. This class is a simple extension of [PerQueryCPUMemResourceUsageAccountant] that
  /// aggregates memory usage by query id.
  /// Note that this is useful only in simple scenarios when one query is running.
  public static class TestAccountantFactory implements ThreadAccountantFactory {

    @Override
    public ThreadAccountant init(PinotConfiguration config, String instanceId, InstanceType instanceType) {
      return new TestAccountant(config, instanceId, instanceType);
    }

    public static class TestAccountant extends PerQueryCPUMemResourceUsageAccountant {
      Map<String, QueryResourceTrackerImpl> _queryResourceUsages = new ConcurrentHashMap<>();

      public TestAccountant(PinotConfiguration config, String instanceId, InstanceType instanceType) {
        super(config, instanceId, instanceType);
      }

      @Override
      public void sampleUsage() {
        super.sampleUsage();
        QueryExecutionContext executionContext = QueryThreadContext.get().getExecutionContext();
        _queryResourceUsages.computeIfAbsent(executionContext.getCid(),
            s -> new QueryResourceTrackerImpl(executionContext, 0, 0)).merge(0, getThreadEntry().getAllocatedBytes());
      }

      @Override
      public Map<String, QueryResourceTrackerImpl> getQueryResources() {
        return _queryResourceUsages;
      }
    }
  }

  @Override
  protected long getCountStarResult() {
    return WindowFunnelUtils._countStarResult;
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    brokerConf.setProperty(Broker.CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT, true);

    String prefix = Accounting.BROKER_PREFIX + ".";
    brokerConf.setProperty(prefix + Accounting.Keys.FACTORY_NAME, TestAccountantFactory.class.getName());
    brokerConf.setProperty(prefix + Accounting.Keys.ENABLE_THREAD_MEMORY_SAMPLING, true);
    brokerConf.setProperty(prefix + Accounting.Keys.OOM_PROTECTION_KILLING_QUERY, true);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    serverConf.setProperty(Server.CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT, true);

    String prefix = Accounting.SERVER_PREFIX + ".";
    serverConf.setProperty(prefix + Accounting.Keys.FACTORY_NAME, TestAccountantFactory.class.getName());
    serverConf.setProperty(prefix + Accounting.Keys.ENABLE_THREAD_MEMORY_SAMPLING, true);
    serverConf.setProperty(prefix + Accounting.Keys.ENABLE_THREAD_CPU_SAMPLING, false);
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    _classTempDir = getClassTempDir();
    _classSegmentDir = new File(_classTempDir, "segmentDir");
    _classTarDir = new File(_classTempDir, "tarDir");
    TestUtils.ensureDirectoriesExistAndEmpty(_classTempDir, _classSegmentDir, _classTarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    cleanTableAndSchema();

    // create & upload schema AND table config
    Schema schema = createSchema();
    addSchema(schema);

    List<File> avroFiles = WindowFunnelUtils.createAvroFiles(_classTempDir);
    // create offline table
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    // create & upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _classSegmentDir,
        _classTarDir);
    uploadSegments(getTableName(), _classTarDir);
    waitForAllDocsLoaded(60_000L);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    Exception exception = null;
    exception = runCleanup(exception, this::cleanTableAndSchema);
    exception = runCleanup(exception, this::closePinotConnections);
    exception = runCleanup(exception, this::stopServer);
    exception = runCleanup(exception, this::stopBroker);
    exception = runCleanup(exception, this::stopControllerIfStarted);
    exception = runCleanup(exception, this::stopZk);
    exception = runCleanup(exception, this::deleteClassTempDir);
    if (exception != null) {
      throw exception;
    }
  }

  @Test
  public void testFunnel()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    String query = String.format(
        "SELECT "
            + "funnelMaxStep(timestampCol, '1000', 4, "
            + "url = '/product/search', "
            + "url = '/cart/add', "
            + "url = '/checkout/start', "
            + "url = '/checkout/confirmation' "
            + ") "
            + "FROM %s LIMIT %d",
        getTableName(), getCountStarResult());

    JsonNode response = postQuery(query);
    String requestId = response.get("requestId").asText();
    List<BaseServerStarter> serverStartersWithTable = getServerStartersWithTable();
    assertFalse(serverStartersWithTable.isEmpty(), "Expected at least one server to serve table: " + getTableName());

    boolean foundRequestId = false;
    long allocatedBytes = 0;
    for (BaseServerStarter serverStarter : serverStartersWithTable) {
      ThreadAccountant threadAccountant = serverStarter.getServerInstance().getThreadAccountant();
      assertTrue(threadAccountant instanceof TestAccountantFactory.TestAccountant,
          "Unexpected thread accountant for server: " + serverStarter.getInstanceId());
      Map<String, ? extends QueryResourceTracker> queryMemUsage = threadAccountant.getQueryResources();
      for (Map.Entry<String, ? extends QueryResourceTracker> entry : queryMemUsage.entrySet()) {
        if (entry.getKey().contains(requestId)) {
          foundRequestId = true;
          allocatedBytes += entry.getValue().getAllocatedBytes();
        }
      }
    }
    assertTrue(foundRequestId, "Expected resource accounting for request id: " + requestId);
    assertTrue(allocatedBytes > 0, "Expected tracked allocated bytes for request id: " + requestId);
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName()).build();
  }

  @Override
  public String getTableName() {
    return isSharedRichClusterEnabled() ? SHARED_TABLE_NAME : DEFAULT_TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return WindowFunnelUtils.createSchema(getTableName());
  }

  private File getClassTempDir() {
    return isSharedRichClusterEnabled() ? new File(_tempDir, "testData") : _tempDir;
  }

  private void cleanTableAndSchema()
      throws Exception {
    if (_helixResourceManager == null) {
      return;
    }

    String tableName = getTableName();
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    if (_helixResourceManager.getAllTables().contains(offlineTableName) || _helixResourceManager.hasOfflineTable(
        tableName)) {
      dropOfflineTable(tableName);
      waitForTableDataManagerRemoved(offlineTableName);
      waitForEVToDisappear(offlineTableName);
    }
    if (_helixResourceManager.getSchema(tableName) != null) {
      deleteSchema(tableName);
    }
  }

  private List<BaseServerStarter> getServerStartersWithTable() {
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
    List<BaseServerStarter> serverStartersWithTable = new ArrayList<>();
    for (BaseServerStarter serverStarter : _serverStarters) {
      if (serverStarter.getServerInstance().getInstanceDataManager().getTableDataManager(offlineTableName) != null) {
        serverStartersWithTable.add(serverStarter);
      }
    }
    return serverStartersWithTable;
  }

  private void closePinotConnections() {
    if (_pinotConnection != null) {
      _pinotConnection.close();
      _pinotConnection = null;
    }
    if (_pinotConnectionV2 != null) {
      _pinotConnectionV2.close();
      _pinotConnectionV2 = null;
    }
  }

  private void stopControllerIfStarted() {
    if (_controllerStarter != null) {
      stopController();
    }
  }

  private void deleteClassTempDir()
      throws IOException {
    if (_classTempDir != null) {
      FileUtils.deleteDirectory(_classTempDir);
    }
  }

  private Exception runCleanup(Exception firstException, Cleanup cleanup) {
    try {
      cleanup.run();
    } catch (Exception e) {
      if (firstException == null) {
        return e;
      }
      firstException.addSuppressed(e);
    }
    return firstException;
  }

  private interface Cleanup {
    void run()
        throws Exception;
  }
}
