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
import org.testng.annotations.BeforeClass;
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
    waitForAllDocsLoaded(600_000L);    // Disable the pre-checks for the rebalance job
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
        + "&forceCommitBeforeRebalance=" + rebalanceConfig.isForceCommitBeforeRebalance();
  }

  private String getRebalanceUrl(RebalanceConfig rebalanceConfig, TableType tableType) {
    return StringUtil.join("/", getControllerRequestURLBuilder().getBaseUrl(), "tables", getTableName(), "rebalance")
        + "?type=" + tableType.toString() + "&" + getQueryString(rebalanceConfig);
  }

  @Test
  public void testForceCommitBeforeRebalance()
      throws Exception {
    final String tenantA = "tenantA";
    final String tenantB = "tenantB";

    TableConfig tableConfig = getRealtimeTableConfig();

    BaseServerStarter serverStarter0 = startOneServer(0);
    BaseServerStarter serverStarter1 = startOneServer(1);
    createServerTenant(tenantA, 0, 2);

    BaseServerStarter serverStarter2 = startOneServer(2);
    BaseServerStarter serverStarter3 = startOneServer(3);
    createServerTenant(tenantB, 0, 2);

    // Prepare the table to replicate segments across two servers on tenantA
    tableConfig.setTenantConfig(new TenantConfig(getBrokerTenant(), tenantA, null));
    tableConfig.getValidationConfig().setReplication("2");
    tableConfig.getValidationConfig().setPeerSegmentDownloadScheme("http");
    updateTableConfig(tableConfig);
    RebalanceConfig rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setDryRun(false);
    rebalanceConfig.setIncludeConsuming(true);

    String response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    RebalanceResult rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
    RebalanceSummaryResult summary = rebalanceResult.getRebalanceSummaryResult();
    assertEquals(
        summary.getServerInfo().getNumServers().getExpectedValueAfterRebalance(),
        2);
    Set<String> originalConsumingSegmentsToMove = summary.getSegmentInfo()
        .getConsumingSegmentToBeMovedSummary()
        .getConsumingSegmentsToBeMovedWithMostOffsetsToCatchUp()
        .keySet();
    assertEquals(originalConsumingSegmentsToMove.size(), 2);

    waitForRebalanceToComplete(rebalanceResult.getJobId(), 15000);

    // test: move segments from tenantA to tenantB
    tableConfig.setTenantConfig(new TenantConfig(getBrokerTenant(), tenantB, null));
    updateTableConfig(tableConfig);

    rebalanceConfig.setForceCommitBeforeRebalance(true);

    response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
    assertEquals(
        rebalanceResult.getRebalanceSummaryResult().getServerInfo().getNumServers().getExpectedValueAfterRebalance(),
        2);
    summary = rebalanceResult.getRebalanceSummaryResult();
    originalConsumingSegmentsToMove = summary.getSegmentInfo()
        .getConsumingSegmentToBeMovedSummary()
        .getConsumingSegmentsToBeMovedWithMostOffsetsToCatchUp()
        .keySet();
    waitForRebalanceToComplete(rebalanceResult.getJobId(), 15000);

    response = sendGetRequest(getControllerRequestURLBuilder().forTableConsumingSegmentsInfo(getTableName()));
    ConsumingSegmentInfoReader.ConsumingSegmentsInfoMap consumingSegmentInfoResponse =
        JsonUtils.stringToObject(response, ConsumingSegmentInfoReader.ConsumingSegmentsInfoMap.class);
    LLCSegmentName consumingSegmentNow = new LLCSegmentName(
        consumingSegmentInfoResponse._segmentToConsumingInfoMap.keySet().stream().sorted().iterator().next());
    LLCSegmentName consumingSegmentOriginal =
        new LLCSegmentName(originalConsumingSegmentsToMove.stream().sorted().iterator().next());
    assertEquals(consumingSegmentNow.getSequenceNumber(), consumingSegmentOriginal.getSequenceNumber() + 1);
    assertEquals(consumingSegmentInfoResponse._segmentToConsumingInfoMap.size(),
        originalConsumingSegmentsToMove.size());

    // test: move segment from tenantB to tenantA with downtime

    tableConfig.setTenantConfig(new TenantConfig(getBrokerTenant(), tenantA, null));
    updateTableConfig(tableConfig);

    rebalanceConfig.setForceCommitBeforeRebalance(true);
    rebalanceConfig.setIncludeConsuming(false);

    response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);

    waitForRebalanceToComplete(rebalanceResult.getJobId(), 60000);

    response = sendGetRequest(getControllerRequestURLBuilder().forTableConsumingSegmentsInfo(getTableName()));
    consumingSegmentInfoResponse =
        JsonUtils.stringToObject(response, ConsumingSegmentInfoReader.ConsumingSegmentsInfoMap.class);
    consumingSegmentOriginal = consumingSegmentNow;
    consumingSegmentNow = new LLCSegmentName(
        consumingSegmentInfoResponse._segmentToConsumingInfoMap.keySet().stream().sorted().iterator().next());
    // the sequence number should not increase since the consuming segment is not committed
    assertEquals(consumingSegmentNow.getSequenceNumber(), consumingSegmentOriginal.getSequenceNumber());
    assertEquals(consumingSegmentInfoResponse._segmentToConsumingInfoMap.size(),
        originalConsumingSegmentsToMove.size());

    // Resume the table
    tableConfig.setTenantConfig(new TenantConfig(getBrokerTenant(), getServerTenant(), null));
    tableConfig.getValidationConfig().setReplication("1");
    tableConfig.getValidationConfig().setPeerSegmentDownloadScheme(null);
    updateTableConfig(tableConfig);
    rebalanceConfig.setForceCommitBeforeRebalance(false);
    rebalanceConfig.setMinAvailableReplicas(0);
    rebalanceConfig.setDowntime(false);
    rebalanceConfig.setIncludeConsuming(true);
    response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
    waitForRebalanceToComplete(rebalanceResult.getJobId(), 15000);

    serverStarter0.stop();
    serverStarter1.stop();
    serverStarter2.stop();
    serverStarter3.stop();
  }

  @Test
  void testForceCommitBeforeRebalanceStrictReplicaGroup()
      throws Exception {
    final String tenantA = "tenantA_strictRG";
    final String tenantB = "tenantB_strictRG";

    BaseServerStarter serverStarter0 = startOneServer(0);
    BaseServerStarter serverStarter1 = startOneServer(1);
    createServerTenant(tenantA, 0, 2);

    BaseServerStarter serverStarter2 = startOneServer(2);
    BaseServerStarter serverStarter3 = startOneServer(3);
    createServerTenant(tenantB, 0, 2);

    // Prepare the table to replicate segments across two servers on tenantA
    TableConfig tableConfig = getRealtimeTableConfig();
    tableConfig.setRoutingConfig(
        new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE,
            false));
    tableConfig.setTenantConfig(new TenantConfig(getBrokerTenant(), tenantA, null));
    tableConfig.getValidationConfig().setReplication("2");
    tableConfig.getValidationConfig().setPeerSegmentDownloadScheme("http");

    updateTableConfig(tableConfig);

    RebalanceConfig rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setDryRun(false);
    rebalanceConfig.setIncludeConsuming(true);

    String response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    RebalanceResult rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
    RebalanceSummaryResult summary = rebalanceResult.getRebalanceSummaryResult();
    assertEquals(
        summary.getServerInfo().getNumServers().getExpectedValueAfterRebalance(),
        2);
    Set<String> originalConsumingSegmentsToMove = summary.getSegmentInfo()
        .getConsumingSegmentToBeMovedSummary()
        .getConsumingSegmentsToBeMovedWithMostOffsetsToCatchUp()
        .keySet();
    assertEquals(originalConsumingSegmentsToMove.size(), 2);

    waitForRebalanceToComplete(rebalanceResult.getJobId(), 15000);

    // test: move segments from tenantA to tenantB
    tableConfig.setTenantConfig(new TenantConfig(getBrokerTenant(), tenantB, null));
    updateTableConfig(tableConfig);

    rebalanceConfig.setForceCommitBeforeRebalance(true);

    response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
    assertEquals(
        rebalanceResult.getRebalanceSummaryResult().getServerInfo().getNumServers().getExpectedValueAfterRebalance(),
        2);
    summary = rebalanceResult.getRebalanceSummaryResult();
    originalConsumingSegmentsToMove = summary.getSegmentInfo()
        .getConsumingSegmentToBeMovedSummary()
        .getConsumingSegmentsToBeMovedWithMostOffsetsToCatchUp()
        .keySet();
    waitForRebalanceToComplete(rebalanceResult.getJobId(), 15000);

    response = sendGetRequest(getControllerRequestURLBuilder().forTableConsumingSegmentsInfo(getTableName()));
    ConsumingSegmentInfoReader.ConsumingSegmentsInfoMap consumingSegmentInfoResponse =
        JsonUtils.stringToObject(response, ConsumingSegmentInfoReader.ConsumingSegmentsInfoMap.class);
    LLCSegmentName consumingSegmentNow = new LLCSegmentName(
        consumingSegmentInfoResponse._segmentToConsumingInfoMap.keySet().stream().sorted().iterator().next());
    LLCSegmentName consumingSegmentOriginal =
        new LLCSegmentName(originalConsumingSegmentsToMove.stream().sorted().iterator().next());
    assertEquals(consumingSegmentNow.getSequenceNumber(), consumingSegmentOriginal.getSequenceNumber() + 1);
    assertEquals(consumingSegmentInfoResponse._segmentToConsumingInfoMap.size(),
        originalConsumingSegmentsToMove.size());

    // test: move segment from tenantB to tenantA with batch size

    tableConfig.setTenantConfig(new TenantConfig(getBrokerTenant(), tenantA, null));
    updateTableConfig(tableConfig);

    rebalanceConfig.setForceCommitBeforeRebalance(true);
    rebalanceConfig.setBatchSizePerServer(1);

    response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
    summary = rebalanceResult.getRebalanceSummaryResult();
    originalConsumingSegmentsToMove = summary.getSegmentInfo()
        .getConsumingSegmentToBeMovedSummary()
        .getConsumingSegmentsToBeMovedWithMostOffsetsToCatchUp()
        .keySet();
    assertEquals(
        rebalanceResult.getRebalanceSummaryResult().getServerInfo().getNumServers().getExpectedValueAfterRebalance(),
        2);

    waitForRebalanceToComplete(rebalanceResult.getJobId(), 15000);

    response = sendGetRequest(getControllerRequestURLBuilder().forTableConsumingSegmentsInfo(getTableName()));
    consumingSegmentInfoResponse =
        JsonUtils.stringToObject(response, ConsumingSegmentInfoReader.ConsumingSegmentsInfoMap.class);
    consumingSegmentNow = new LLCSegmentName(
        consumingSegmentInfoResponse._segmentToConsumingInfoMap.keySet().stream().sorted().iterator().next());
    consumingSegmentOriginal = new LLCSegmentName(originalConsumingSegmentsToMove.stream().sorted().iterator().next());
    assertEquals(consumingSegmentNow.getSequenceNumber(), consumingSegmentOriginal.getSequenceNumber() + 1);
    assertEquals(consumingSegmentInfoResponse._segmentToConsumingInfoMap.size(),
        originalConsumingSegmentsToMove.size());

    // test: move segment from tenantA to tenantB with includeConsuming = false, consuming segment should not be
    // committed

    tableConfig.setTenantConfig(new TenantConfig(getBrokerTenant(), tenantB, null));
    updateTableConfig(tableConfig);

    rebalanceConfig.setForceCommitBeforeRebalance(true);
    rebalanceConfig.setIncludeConsuming(false);

    response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);

    waitForRebalanceToComplete(rebalanceResult.getJobId(), 15000);

    response = sendGetRequest(getControllerRequestURLBuilder().forTableConsumingSegmentsInfo(getTableName()));
    consumingSegmentInfoResponse =
        JsonUtils.stringToObject(response, ConsumingSegmentInfoReader.ConsumingSegmentsInfoMap.class);
    consumingSegmentOriginal = consumingSegmentNow;
    consumingSegmentNow = new LLCSegmentName(
        consumingSegmentInfoResponse._segmentToConsumingInfoMap.keySet().stream().sorted().iterator().next());
    // the sequence number should not increase since the consuming segment is not committed
    assertEquals(consumingSegmentNow.getSequenceNumber(), consumingSegmentOriginal.getSequenceNumber());
    assertEquals(consumingSegmentInfoResponse._segmentToConsumingInfoMap.size(),
        originalConsumingSegmentsToMove.size());

    // Resume the table
    tableConfig.setTenantConfig(new TenantConfig(getBrokerTenant(), getServerTenant(), null));
    tableConfig.getValidationConfig().setReplication("1");
    tableConfig.getValidationConfig().setPeerSegmentDownloadScheme(null);
    updateTableConfig(tableConfig);
    rebalanceConfig.setForceCommitBeforeRebalance(false);
    rebalanceConfig.setMinAvailableReplicas(0);
    rebalanceConfig.setDowntime(false);
    rebalanceConfig.setIncludeConsuming(true);
    response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
    waitForRebalanceToComplete(rebalanceResult.getJobId(), 15000);

    serverStarter0.stop();
    serverStarter1.stop();
    serverStarter2.stop();
    serverStarter3.stop();
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

        requestUrl = getControllerRequestURLBuilder().forIdealState(tableName);
        httpResponse =
            HttpClient.wrapAndThrowHttpException(getHttpClient().sendGetRequest(new URL(requestUrl).toURI(), null));
        TableViews.TableView externalView =
            JsonUtils.stringToObject(httpResponse.getResponse(), TableViews.TableView.class);
        return idealState._realtime.equals(externalView._realtime) && idealState._offline.equals(externalView._offline);
      } catch (Exception e) {
        return null;
      }
    }, 1000L, timeoutMs, "Failed to converge EV and IS for table: " + tableName);
  }
}
