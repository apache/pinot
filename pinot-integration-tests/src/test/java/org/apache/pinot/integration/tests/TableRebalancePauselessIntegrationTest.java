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

import java.util.Map;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceConfig;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceResult;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceSummaryResult;
import org.apache.pinot.server.starter.helix.BaseServerStarter;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.StringUtil;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class TableRebalancePauselessIntegrationTest extends BasePauselessRealtimeIngestionTest {
  private final static int FORCE_COMMIT_REBALANCE_TIMEOUT_MS = 600_000;

  @Override
  protected String getFailurePoint() {
    return null;  // No failure point for basic test
  }

  @Override
  protected int getExpectedSegmentsWithFailure() {
    return NUM_REALTIME_SEGMENTS;  // Always expect full segments
  }

  @Override
  protected int getExpectedZKMetadataWithFailure() {
    return NUM_REALTIME_SEGMENTS;  // Always expect full metadata
  }

  @Override
  protected long getCountStarResultWithFailure() {
    return DEFAULT_COUNT_STAR_RESULT;  // Always expect full count
  }

  @Override
  protected void overrideControllerConf(Map<String, Object> properties) {
    properties.put(ControllerConf.CLUSTER_TENANT_ISOLATION_ENABLE, false);
  }

  @Override
  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    startZk();
    startController();
    startBroker();
    startServer();
    createServerTenant(getServerTenant(), 0, 1);
    createBrokerTenant(getBrokerTenant(), 1);
    setMaxSegmentCompletionTimeMillis();
    setupNonPauselessTable();
    injectFailure();
    setupPauselessTable();
    waitForAllDocsLoaded(600_000L);
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

    BaseServerStarter serverStarter0 = startOneServer(0);
    BaseServerStarter serverStarter1 = startOneServer(1);
    createServerTenant(tenantA, 0, 2);

    BaseServerStarter serverStarter2 = startOneServer(2);
    BaseServerStarter serverStarter3 = startOneServer(3);
    createServerTenant(tenantB, 0, 2);
    RebalanceConfig rebalanceConfig = new RebalanceConfig();
    try {
      // Prepare the table to replicate segments across two servers on tenantA
      updateTableConfig(tableConfig);
      rebalanceConfig.setDryRun(false);
      rebalanceConfig.setIncludeConsuming(true);

      String response = sendPostRequest(getTableRebalanceUrl(rebalanceConfig, TableType.REALTIME));
      RebalanceResult rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
      RebalanceSummaryResult summary = rebalanceResult.getRebalanceSummaryResult();
      assertEquals(
          summary.getServerInfo().getNumServers().getExpectedValueAfterRebalance(),
          2);
      assertEquals(summary.getSegmentInfo().getConsumingSegmentToBeMovedSummary().getNumConsumingSegmentsToBeMoved(),
          4);

      waitForRebalanceToComplete(rebalanceResult.getJobId(), FORCE_COMMIT_REBALANCE_TIMEOUT_MS);

      if (tableConfig.getRoutingConfig() != null
          && RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE.equalsIgnoreCase(
          tableConfig.getRoutingConfig().getInstanceSelectorType())) {
        // test: move segments from tenantA to tenantB
        performForceCommitSegmentMovingTest(rebalanceConfig, tableConfig, tenantB, true,
            FORCE_COMMIT_REBALANCE_TIMEOUT_MS);

        // test: move segment from tenantB to tenantA with batch size
        rebalanceConfig.setBatchSizePerServer(1);
        performForceCommitSegmentMovingTest(rebalanceConfig, tableConfig, tenantA, true,
            FORCE_COMMIT_REBALANCE_TIMEOUT_MS);

        // test: move segment from tenantA to tenantB with includeConsuming = false, consuming segment should not be
        // committed
        rebalanceConfig.setIncludeConsuming(false);
        performForceCommitSegmentMovingTest(rebalanceConfig, tableConfig, tenantB, false,
            FORCE_COMMIT_REBALANCE_TIMEOUT_MS);
      } else {
        // test: move segments from tenantA to tenantB
        performForceCommitSegmentMovingTest(rebalanceConfig, tableConfig, tenantB, true,
            FORCE_COMMIT_REBALANCE_TIMEOUT_MS);

        // test: move segment from tenantB to tenantA with includeConsuming = false, consuming segment should not be
        // committed
        rebalanceConfig.setIncludeConsuming(false);
        performForceCommitSegmentMovingTest(rebalanceConfig, tableConfig, tenantA, false,
            FORCE_COMMIT_REBALANCE_TIMEOUT_MS);
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
      sendDeleteRequest(
          StringUtil.join("/", getControllerRequestURLBuilder().getBaseUrl(), "tables", getTableName(), "rebalance")
              + "?type=" + tableConfig.getTableType().toString());
      String response = sendPostRequest(getTableRebalanceUrl(rebalanceConfig, TableType.REALTIME));
      RebalanceResult rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
      waitForRebalanceToComplete(rebalanceResult.getJobId(), FORCE_COMMIT_REBALANCE_TIMEOUT_MS);
      serverStarter0.stop();
      serverStarter1.stop();
      serverStarter2.stop();
      serverStarter3.stop();
    }
  }
}
