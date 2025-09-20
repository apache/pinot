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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.accounting.PerQueryCPUMemAccountantFactory.PerQueryCPUMemResourceUsageAccountant;
import org.apache.pinot.core.accounting.QueryResourceTrackerImpl;
import org.apache.pinot.integration.tests.window.utils.WindowFunnelUtils;
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
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Accounting;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.builder.ControllerRequestURLBuilder;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class WindowResourceAccountingTest extends BaseClusterIntegrationTest {

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

    String prefix = CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + ".";
    brokerConf.setProperty(prefix + Accounting.CONFIG_OF_FACTORY_NAME, TestAccountantFactory.class.getName());
    brokerConf.setProperty(prefix + Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING, true);
    brokerConf.setProperty(prefix + Accounting.CONFIG_OF_OOM_PROTECTION_KILLING_QUERY, true);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    serverConf.setProperty(Server.CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT, true);

    String prefix = CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + ".";
    serverConf.setProperty(prefix + Accounting.CONFIG_OF_FACTORY_NAME, TestAccountantFactory.class.getName());
    serverConf.setProperty(prefix + Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING, true);
    serverConf.setProperty(prefix + Accounting.CONFIG_OF_ENABLE_THREAD_CPU_SAMPLING, false);
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    if (_controllerRequestURLBuilder == null) {
      _controllerRequestURLBuilder =
          ControllerRequestURLBuilder.baseUrl("http://localhost:" + getControllerPort());
    }
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    // create & upload schema AND table config
    Schema schema = createSchema();
    addSchema(schema);

    List<File> avroFiles = WindowFunnelUtils.createAvroFiles(_tempDir);
      // create offline table
      TableConfig tableConfig = createOfflineTableConfig();
      addTableConfig(tableConfig);

      // create & upload segments
      int segmentIndex = 0;
      for (File avroFile : avroFiles) {
        ClusterIntegrationTestUtils.buildSegmentFromAvro(avroFile, tableConfig, schema, segmentIndex++, _segmentDir,
            _tarDir);
        uploadSegments(getTableName(), _tarDir);
      }
    }

  @AfterClass
  public void tearDown()
      throws IOException {
    // Shutdown the Pinot cluster
    stopServer();
    stopBroker();
    stopController();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
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
    ThreadAccountant threadAccountant = _serverStarters.get(0).getServerInstance().getThreadAccountant();
    assertTrue(threadAccountant instanceof TestAccountantFactory.TestAccountant);
    Map<String, ? extends QueryResourceTracker> queryMemUsage = threadAccountant.getQueryResources();
    assertFalse(queryMemUsage.isEmpty());
    boolean foundRequestId = false;
    String queryIdKey = null;
    for (String key : queryMemUsage.keySet()) {
      if (key.contains(response.get("requestId").asText())) {
        foundRequestId = true;
        queryIdKey = key;
        break;
      }
    }
    assertTrue(foundRequestId);
    assertTrue(queryMemUsage.get(queryIdKey).getAllocatedBytes() > 0);
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName()).build();
  }

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return WindowFunnelUtils.createSchema(getTableName());
  }
}
