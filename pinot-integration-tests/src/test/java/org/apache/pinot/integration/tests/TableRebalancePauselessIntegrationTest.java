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

import java.net.URL;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.resources.ServerRebalanceJobStatusResponse;
import org.apache.pinot.controller.api.resources.TableViews;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceConfig;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceResult;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceSummaryResult;
import org.apache.pinot.controller.util.ConsumingSegmentInfoReader;
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

  private static String getQueryString(RebalanceConfig rebalanceConfig) {
    return "dryRun=" + rebalanceConfig.isDryRun() + "&preChecks=" + rebalanceConfig.isPreChecks()
        + "&reassignInstances=" + rebalanceConfig.isReassignInstances()
        + "&includeConsuming=" + rebalanceConfig.isIncludeConsuming()
        + "&minimizeDataMovement=" + rebalanceConfig.getMinimizeDataMovement()
        + "&bootstrap=" + rebalanceConfig.isBootstrap() + "&downtime=" + rebalanceConfig.isDowntime()
        + "&minAvailableReplicas=" + rebalanceConfig.getMinAvailableReplicas()
        + "&bestEfforts=" + rebalanceConfig.isBestEfforts()
        + "&batchSizePerServer=" + rebalanceConfig.getBatchSizePerServer()
        + "&externalViewCheckIntervalInMs=" + rebalanceConfig.getExternalViewCheckIntervalInMs()
        + "&externalViewStabilizationTimeoutInMs=" + rebalanceConfig.getExternalViewStabilizationTimeoutInMs()
        + "&updateTargetTier=" + rebalanceConfig.isUpdateTargetTier()
        + "&heartbeatIntervalInMs=" + rebalanceConfig.getHeartbeatIntervalInMs()
        + "&heartbeatTimeoutInMs=" + rebalanceConfig.getHeartbeatTimeoutInMs()
        + "&maxAttempts=" + rebalanceConfig.getMaxAttempts()
        + "&retryInitialDelayInMs=" + rebalanceConfig.getRetryInitialDelayInMs()
        + "&forceCommit=" + rebalanceConfig.isForceCommit();
  }

  private String getRebalanceUrl(RebalanceConfig rebalanceConfig, TableType tableType) {
    return StringUtil.join("/", getControllerRequestURLBuilder().getBaseUrl(), "tables", getTableName(), "rebalance")
        + "?type=" + tableType.toString() + "&" + getQueryString(rebalanceConfig);
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

      String response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
      RebalanceResult rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
      RebalanceSummaryResult summary = rebalanceResult.getRebalanceSummaryResult();
      assertEquals(
          summary.getServerInfo().getNumServers().getExpectedValueAfterRebalance(),
          2);
      assertEquals(summary.getSegmentInfo().getConsumingSegmentToBeMovedSummary().getNumConsumingSegmentsToBeMoved(),
          4);

      waitForRebalanceToComplete(rebalanceResult.getJobId(), 30000);

      if (tableConfig.getRoutingConfig() != null
          && RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE.equalsIgnoreCase(
          tableConfig.getRoutingConfig().getInstanceSelectorType())) {
        // test: move segments from tenantA to tenantB
        performSegmentMovingTest(rebalanceConfig, tableConfig, tenantB, true, 30000);

        // test: move segment from tenantB to tenantA with batch size
        rebalanceConfig.setBatchSizePerServer(1);
        performSegmentMovingTest(rebalanceConfig, tableConfig, tenantA, true, 30000);

        // test: move segment from tenantA to tenantB with includeConsuming = false, consuming segment should not be
        // committed
        rebalanceConfig.setIncludeConsuming(false);
        performSegmentMovingTest(rebalanceConfig, tableConfig, tenantB, false, 30000);
      } else {
        // test: move segments from tenantA to tenantB
        performSegmentMovingTest(rebalanceConfig, tableConfig, tenantB, true, 30000);

        // test: move segment from tenantB to tenantA with includeConsuming = false, consuming segment should not be
        // committed
        rebalanceConfig.setIncludeConsuming(false);
        performSegmentMovingTest(rebalanceConfig, tableConfig, tenantA, false, 30000);
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
      String response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
      RebalanceResult rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
      waitForRebalanceToComplete(rebalanceResult.getJobId(), 30000);
      serverStarter0.stop();
      serverStarter1.stop();
      serverStarter2.stop();
      serverStarter3.stop();
    }
  }

  private void waitForRebalanceToComplete(String rebalanceJobId, long timeoutMs) {
    TestUtils.waitForCondition(aVoid -> {
      try {
        String requestUrl = getControllerRequestURLBuilder().forTableRebalanceStatus(rebalanceJobId);
        SimpleHttpResponse httpResponse =
            HttpClient.wrapAndThrowHttpException(getHttpClient().sendGetRequest(new URL(requestUrl).toURI(), null));
        ServerRebalanceJobStatusResponse rebalanceStatus =
            JsonUtils.stringToObject(httpResponse.getResponse(), ServerRebalanceJobStatusResponse.class);
        return rebalanceStatus.getTableRebalanceProgressStats().getStatus() == RebalanceResult.Status.DONE;
      } catch (Exception e) {
        return null;
      }
    }, 1000L, timeoutMs, "Failed to complete rebalance");
  }

  private void waitForTableEVISConverge(String tableName, long timeoutMs) {
    TestUtils.waitForCondition(aVoid -> {
      try {
        String requestUrl = getControllerRequestURLBuilder().forIdealState(tableName);
        SimpleHttpResponse httpResponse =
            HttpClient.wrapAndThrowHttpException(getHttpClient().sendGetRequest(new URL(requestUrl).toURI(), null));
        TableViews.TableView idealState =
            JsonUtils.stringToObject(httpResponse.getResponse(), TableViews.TableView.class);

        requestUrl = getControllerRequestURLBuilder().forExternalView(tableName);
        httpResponse = getHttpClient().sendGetRequest(new URL(requestUrl).toURI(), null);
        TableViews.TableView externalView =
            JsonUtils.stringToObject(httpResponse.getResponse(), TableViews.TableView.class);
        return idealState._realtime.equals(externalView._realtime) && idealState._offline.equals(externalView._offline);
      } catch (Exception e) {
        Assert.fail("Caught exception while waiting for table EV and IS to converge", e);
        return null;
      }
    }, 1000L, timeoutMs, "Failed to converge EV and IS for table: " + tableName);
  }

  /**
   * Helper method to perform segment moving test with specified configuration.
   * Changes the table tenant, executes rebalance with force commit, and verifies if segments were committed.
   */
  void performSegmentMovingTest(RebalanceConfig rebalanceConfig, TableConfig tableConfig, String newTenant,
      boolean shouldCommit, long timeoutMs)
      throws Exception {
    performSegmentMovingTest(rebalanceConfig, tableConfig, newTenant, shouldCommit, timeoutMs, false);
  }

  /**
   * Helper method to perform segment moving test with EVIS convergence wait.
   * Similar to performSegmentMovingTest but waits for external view/ideal state convergence instead of rebalance
   * completion.
   */
  void performSegmentMovingTestWithEVISConverge(RebalanceConfig rebalanceConfig, TableConfig tableConfig,
      String newTenant, boolean shouldCommit, long timeoutMs)
      throws Exception {
    performSegmentMovingTest(rebalanceConfig, tableConfig, newTenant, shouldCommit, timeoutMs, true);
  }

  /**
   * Helper method to perform segment moving test with specified configuration.
   * Changes the table tenant, executes rebalance with force commit, and verifies if segments were committed.
   *
   * @param rebalanceConfig the rebalance configuration
   * @param tableConfig the table configuration
   * @param newTenant the new tenant to move segments to
   * @param shouldCommit whether segments should be committed (affects verification)
   * @param timeoutMs timeout in milliseconds
   * @param waitForEVISConverge if true, waits for external view/ideal state convergence; if false, waits for
   *                            rebalance completion
   */
  private void performSegmentMovingTest(RebalanceConfig rebalanceConfig, TableConfig tableConfig, String newTenant,
      boolean shouldCommit, long timeoutMs, boolean waitForEVISConverge)
      throws Exception {
    // Change tenant
    tableConfig.setTenantConfig(new TenantConfig(getBrokerTenant(), newTenant, null));
    updateTableConfig(tableConfig);

    // Set force commit
    rebalanceConfig.setForceCommit(true);

    // Execute rebalance
    String response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    RebalanceResult rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);

    // Get original consuming segments (if present)
    Set<String> originalConsumingSegmentsToMove = null;
    if (rebalanceResult.getRebalanceSummaryResult() != null &&
        rebalanceResult.getRebalanceSummaryResult().getSegmentInfo() != null &&
        rebalanceResult.getRebalanceSummaryResult().getSegmentInfo().getConsumingSegmentToBeMovedSummary() != null) {
      originalConsumingSegmentsToMove = rebalanceResult.getRebalanceSummaryResult().getSegmentInfo()
          .getConsumingSegmentToBeMovedSummary()
          .getConsumingSegmentsToBeMovedWithMostOffsetsToCatchUp()
          .keySet();
    }

    // Wait for completion based on the flag
    if (waitForEVISConverge) {
      waitForTableEVISConverge(getTableName(), timeoutMs);
    } else {
      waitForRebalanceToComplete(rebalanceResult.getJobId(), timeoutMs);
    }

    // Check if segments were committed (only if there were consuming segments to move)
    if (originalConsumingSegmentsToMove != null && !originalConsumingSegmentsToMove.isEmpty()) {
      response = sendGetRequest(getControllerRequestURLBuilder().forTableConsumingSegmentsInfo(getTableName()));
      ConsumingSegmentInfoReader.ConsumingSegmentsInfoMap consumingSegmentInfoResponse =
          JsonUtils.stringToObject(response, ConsumingSegmentInfoReader.ConsumingSegmentsInfoMap.class);
      LLCSegmentName consumingSegmentNow = new LLCSegmentName(
          consumingSegmentInfoResponse._segmentToConsumingInfoMap.keySet().stream().sorted().iterator().next());
      LLCSegmentName consumingSegmentOriginal =
          new LLCSegmentName(originalConsumingSegmentsToMove.stream().sorted().iterator().next());

      if (shouldCommit) {
        assertEquals(consumingSegmentNow.getSequenceNumber(), consumingSegmentOriginal.getSequenceNumber() + 1);
      } else {
        assertEquals(consumingSegmentNow.getSequenceNumber(), consumingSegmentOriginal.getSequenceNumber());
      }
    }
  }
}
