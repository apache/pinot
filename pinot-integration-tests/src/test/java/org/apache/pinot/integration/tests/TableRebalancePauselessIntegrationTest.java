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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.restlet.resources.RebalanceConfig;
import org.apache.pinot.common.restlet.resources.RebalanceResult;
import org.apache.pinot.common.restlet.resources.RebalanceSummaryResult;
import org.apache.pinot.controller.BaseControllerStarter;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.integration.tests.realtime.utils.FailureInjectingControllerStarter;
import org.apache.pinot.integration.tests.realtime.utils.FailureInjectingPinotLLCRealtimeSegmentManager;
import org.apache.pinot.server.starter.helix.BaseServerStarter;
import org.apache.pinot.server.starter.helix.HelixInstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/// Tests rebalance and force-commit behavior against a pauseless real-time table.
///
/// Sets up a single pauseless table over the standard test data. This used to extend
/// [BasePauselessRealtimeIngestionTest], which also builds a second, non-pauseless table for the
/// failure-injection tests to compare segment ZK metadata against. Nothing here compares metadata
/// across tables, so that second 48-segment table was pure setup cost and is no longer built.
public class TableRebalancePauselessIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableRebalancePauselessIntegrationTest.class);

  private final static int FORCE_COMMIT_REBALANCE_TIMEOUT_MS = 600_000;
  private static final long MAX_SEGMENT_COMPLETION_TIME_MILLIS = 10_000L;
  // startOneServer() does not register the starter in _serverStarters, so its size doesn't advance across parameterized
  // invocations. Track our own counter so each invocation gets a unique serverId range, and therefore unique on-disk
  // data dirs (TEMP_SERVER_DIR/dataDir-<serverId>); otherwise leftover state from a prior invocation pushes segment
  // transitions into ERROR and rebalance hangs.
  private int _nextExtraServerId = 1;

  @Override
  public BaseControllerStarter createControllerStarter() {
    return new FailureInjectingControllerStarter();
  }

  @Override
  protected void overrideControllerConf(Map<String, Object> properties) {
    properties.put(ControllerConf.CLUSTER_TENANT_ISOLATION_ENABLE, false);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    try {
      LOGGER.info("Set segment.store.uri: {} for server with scheme: {}", _controllerConfig.getDataDir(),
          new URI(_controllerConfig.getDataDir()).getScheme());
      serverConf.setProperty("pinot.server.instance.segment.store.uri", "file:" + _controllerConfig.getDataDir());
      serverConf.setProperty("pinot.server.instance." + HelixInstanceDataManagerConfig.UPLOAD_SEGMENT_TO_DEEP_STORE,
          "true");
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    startZk();
    startKafka();
    startController();
    startBroker();
    startServer();
    createServerTenant(getServerTenant(), 0, 1);
    createBrokerTenant(getBrokerTenant(), 1);
    setMaxSegmentCompletionTimeMillis();
    setUpPauselessTable();
    waitForAllDocsLoaded(600_000L);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    dropRealtimeTable(getTableName());
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  private void setUpPauselessTable()
      throws Exception {
    List<File> avroFiles = unpackAvroData(_tempDir);
    pushAvroIntoKafka(avroFiles);

    addSchema(createSchema());

    TableConfig tableConfig = createRealtimeTableConfig(avroFiles.get(0));
    tableConfig.getValidationConfig().setRetentionTimeUnit("DAYS");
    tableConfig.getValidationConfig().setRetentionTimeValue("100000");

    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setStreamIngestionConfig(
        new StreamIngestionConfig(List.of(tableConfig.getIndexingConfig().getStreamConfigs())));
    ingestionConfig.getStreamIngestionConfig().setPauselessConsumptionEnabled(true);
    tableConfig.getIndexingConfig().setStreamConfigs(null);
    tableConfig.setIngestionConfig(ingestionConfig);

    addTableConfig(tableConfig);
  }

  private void setMaxSegmentCompletionTimeMillis() {
    PinotLLCRealtimeSegmentManager realtimeSegmentManager = _helixResourceManager.getRealtimeSegmentManager();
    if (realtimeSegmentManager instanceof FailureInjectingPinotLLCRealtimeSegmentManager) {
      ((FailureInjectingPinotLLCRealtimeSegmentManager) realtimeSegmentManager).setMaxSegmentCompletionTimeoutMs(
          MAX_SEGMENT_COMPLETION_TIME_MILLIS);
    }
  }

  @DataProvider(name = "forceCommitTableConfigProvider")
  public Object[][] forceCommitTableConfigProvider() {
    String originalTenant = "tenantA";
    String originalTenantStrictReplicaGroup = "tenantA_strictRG";

    TableConfig tableConfig = new TableConfig(getRealtimeTableConfig());
    TableConfig tableConfigStrictReplicaGroup = new TableConfig(getRealtimeTableConfig());
    tableConfig.setTenantConfig(new TenantConfig(getBrokerTenant(), originalTenant, null));
    tableConfig.getValidationConfig().setReplication("2");
    tableConfig.getValidationConfig().setPeerSegmentDownloadScheme("http");

    tableConfigStrictReplicaGroup.setRoutingConfig(
        new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE,
            false));
    tableConfigStrictReplicaGroup.setTenantConfig(
        new TenantConfig(getBrokerTenant(), originalTenantStrictReplicaGroup, null));
    tableConfigStrictReplicaGroup.getValidationConfig().setReplication("2");
    tableConfigStrictReplicaGroup.getValidationConfig().setPeerSegmentDownloadScheme("http");

    return new Object[][]{
        {tableConfigStrictReplicaGroup},
        {tableConfig}
    };
  }

  @Test(dataProvider = "forceCommitTableConfigProvider")
  public void testForceCommit(TableConfig tableConfig)
      throws Exception {
    final String tenantA = tableConfig.getTenantConfig().getServer();
    final String tenantB = tenantA + "_new";

    final int baseServerIndex = _nextExtraServerId;
    _nextExtraServerId += 4;
    BaseServerStarter serverStarter0 = startOneServer(baseServerIndex);
    BaseServerStarter serverStarter1 = startOneServer(baseServerIndex + 1);
    createServerTenant(tenantA, 0, 2);

    BaseServerStarter serverStarter2 = startOneServer(baseServerIndex + 2);
    BaseServerStarter serverStarter3 = startOneServer(baseServerIndex + 3);
    createServerTenant(tenantB, 0, 2);
    RebalanceConfig rebalanceConfig = new RebalanceConfig();
    try {
      // Prepare the table to replicate segments across two servers on tenantA
      updateTableConfig(tableConfig);
      rebalanceConfig.setDryRun(false);
      rebalanceConfig.setIncludeConsuming(true);
      rebalanceConfig.setMinAvailableReplicas(0);

      RebalanceResult rebalanceResult = triggerTableRebalance(rebalanceConfig, TableType.REALTIME);
      RebalanceSummaryResult summary = rebalanceResult.getRebalanceSummaryResult();
      assertEquals(
          summary.getServerInfo().getNumServers().getExpectedValueAfterRebalance(),
          2);
      assertEquals(summary.getSegmentInfo().getConsumingSegmentToBeMovedSummary().getNumConsumingSegmentsToBeMoved(),
          4);

      waitForRebalanceToComplete(rebalanceResult.getJobId(), FORCE_COMMIT_REBALANCE_TIMEOUT_MS);
      // Wait for table's ideal state and external view to converge after rebalance
      waitForTableEVISConverge(getTableName(), FORCE_COMMIT_REBALANCE_TIMEOUT_MS);
      // Add additional buffer to ensure consuming segments are fully stabilized
      Thread.sleep(5000);

      if (tableConfig.getRoutingConfig() != null
          && RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE.equalsIgnoreCase(
          tableConfig.getRoutingConfig().getInstanceSelectorType())) {
        // test: move segments from tenantA to tenantB
        performForceCommitSegmentMovingTest(rebalanceConfig, tableConfig, tenantB, true,
            FORCE_COMMIT_REBALANCE_TIMEOUT_MS);
        waitForTableEVISConverge(getTableName(), FORCE_COMMIT_REBALANCE_TIMEOUT_MS);

        // test: move segment from tenantB to tenantA with batch size
        rebalanceConfig.setBatchSizePerServer(1);
        performForceCommitSegmentMovingTest(rebalanceConfig, tableConfig, tenantA, true,
            FORCE_COMMIT_REBALANCE_TIMEOUT_MS);
        waitForTableEVISConverge(getTableName(), FORCE_COMMIT_REBALANCE_TIMEOUT_MS);

        // test: move segment from tenantA to tenantB with includeConsuming = false, consuming segment should not be
        // committed
        rebalanceConfig.setIncludeConsuming(false);
        performForceCommitSegmentMovingTest(rebalanceConfig, tableConfig, tenantB, false,
            FORCE_COMMIT_REBALANCE_TIMEOUT_MS);
        waitForTableEVISConverge(getTableName(), FORCE_COMMIT_REBALANCE_TIMEOUT_MS);
      } else {
        // test: move segments from tenantA to tenantB
        performForceCommitSegmentMovingTest(rebalanceConfig, tableConfig, tenantB, true,
            FORCE_COMMIT_REBALANCE_TIMEOUT_MS);
        waitForTableEVISConverge(getTableName(), FORCE_COMMIT_REBALANCE_TIMEOUT_MS);

        // test: move segment from tenantB to tenantA with includeConsuming = false, consuming segment should not be
        // committed
        rebalanceConfig.setIncludeConsuming(false);
        performForceCommitSegmentMovingTest(rebalanceConfig, tableConfig, tenantA, false,
            FORCE_COMMIT_REBALANCE_TIMEOUT_MS);
        waitForTableEVISConverge(getTableName(), FORCE_COMMIT_REBALANCE_TIMEOUT_MS);
      }
    } catch (Exception e) {
      Assert.fail("Caught exception during force commit test", e);
    } finally {
      // Resume the table
      tableConfig.setTenantConfig(new TenantConfig(getBrokerTenant(), getServerTenant(), null));
      tableConfig.getValidationConfig().setReplication("1");
      tableConfig.getValidationConfig().setPeerSegmentDownloadScheme(null);
      updateTableConfig(tableConfig);
      rebalanceConfig.setForceCommit(false);
      rebalanceConfig.setMinAvailableReplicas(0);
      rebalanceConfig.setDowntime(false);
      rebalanceConfig.setIncludeConsuming(true);
      // notice that this could get an HTTP 409 CONFLICT, when the test failed due to the timeout waiting on the table
      // to converge, and try to rebalance again here. So we need to cancel the original job first.
      getOrCreateAdminClient().getRebalanceClient()
          .cancelRebalance(getTableName(), tableConfig.getTableType().toString());
      RebalanceResult rebalanceResult = triggerTableRebalance(rebalanceConfig, TableType.REALTIME);
      waitForRebalanceToComplete(rebalanceResult.getJobId(), FORCE_COMMIT_REBALANCE_TIMEOUT_MS);
      waitForTableEVISConverge(getTableName(), FORCE_COMMIT_REBALANCE_TIMEOUT_MS);
      serverStarter0.stop();
      serverStarter1.stop();
      serverStarter2.stop();
      serverStarter3.stop();
    }
  }
}
