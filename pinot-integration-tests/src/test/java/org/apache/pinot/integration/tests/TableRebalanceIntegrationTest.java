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
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.common.utils.regex.JavaUtilPattern;
import org.apache.pinot.common.utils.regex.Matcher;
import org.apache.pinot.common.utils.regex.Pattern;
import org.apache.pinot.controller.api.resources.ServerReloadControllerJobStatusResponse;
import org.apache.pinot.controller.helix.core.rebalance.DefaultRebalancePreChecker;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceConfig;
import org.apache.pinot.controller.helix.core.rebalance.RebalancePreCheckerResult;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceResult;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceSummaryResult;
import org.apache.pinot.server.starter.helix.BaseServerStarter;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TagOverrideConfig;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceConstraintConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceTagPoolConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.Enablement;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.StringUtil;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class TableRebalanceIntegrationTest extends HybridClusterIntegrationTest {
  private static String getQueryString(RebalanceConfig rebalanceConfig) {
    return "dryRun=" + rebalanceConfig.isDryRun() + "&preChecks=" + rebalanceConfig.isPreChecks()
        + "&reassignInstances=" + rebalanceConfig.isReassignInstances()
        + "&includeConsuming=" + rebalanceConfig.isIncludeConsuming()
        + "&minimizeDataMovement=" + rebalanceConfig.getMinimizeDataMovement()
        + "&bootstrap=" + rebalanceConfig.isBootstrap() + "&downtime=" + rebalanceConfig.isDowntime()
        + "&minAvailableReplicas=" + rebalanceConfig.getMinAvailableReplicas()
        + "&bestEfforts=" + rebalanceConfig.isBestEfforts()
        + "&externalViewCheckIntervalInMs=" + rebalanceConfig.getExternalViewCheckIntervalInMs()
        + "&externalViewStabilizationTimeoutInMs=" + rebalanceConfig.getExternalViewStabilizationTimeoutInMs()
        + "&updateTargetTier=" + rebalanceConfig.isUpdateTargetTier()
        + "&heartbeatIntervalInMs=" + rebalanceConfig.getHeartbeatIntervalInMs()
        + "&heartbeatTimeoutInMs=" + rebalanceConfig.getHeartbeatTimeoutInMs()
        + "&maxAttempts=" + rebalanceConfig.getMaxAttempts()
        + "&retryInitialDelayInMs=" + rebalanceConfig.getRetryInitialDelayInMs();
  }

  private String getRebalanceUrl(RebalanceConfig rebalanceConfig, TableType tableType) {
    return StringUtil.join("/", getControllerRequestURLBuilder().getBaseUrl(), "tables", getTableName(), "rebalance")
        + "?type=" + tableType.toString() + "&" + getQueryString(rebalanceConfig);
  }

  @Test
  public void testRealtimeRebalancePreCheckMinimizeDataMovement()
      throws Exception {
    // setup the rebalance config
    RebalanceConfig rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setDryRun(true);

    TableConfig tableConfig = getRealtimeTableConfig();
    TableConfig originalTableConfig = new TableConfig(tableConfig);

    // Ensure pre-check status is null if not enabled
    String response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    RebalanceResult rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
    assertNull(rebalanceResult.getPreChecksResult());

    rebalanceConfig.setPreChecks(true);
    rebalanceConfig.setIncludeConsuming(true);
    response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
    checkRebalancePreCheckStatus(rebalanceResult, RebalanceResult.Status.NO_OP,
        "Instance assignment not allowed, no need for minimizeDataMovement",
        RebalancePreCheckerResult.PreCheckStatus.PASS, "No need to reload",
        RebalancePreCheckerResult.PreCheckStatus.PASS, "All rebalance parameters look good",
        RebalancePreCheckerResult.PreCheckStatus.PASS,
        "COMPLETED segments - Replica Groups are not enabled, replication: " + tableConfig.getReplication()
            + "\nCONSUMING segments - Replica Groups are not enabled, replication: " + tableConfig.getReplication(),
        RebalancePreCheckerResult.PreCheckStatus.PASS);

    // Use MinimizeDataMovementOptions.DEFAULT and disable it in table config for COMPLETED ONLY
    Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap =
        Collections.singletonMap("COMPLETED", createInstanceAssignmentConfig(false, TableType.REALTIME));
    InstanceReplicaGroupPartitionConfig replicaGroupPartitionConfig =
        instanceAssignmentConfigMap.get("COMPLETED").getReplicaGroupPartitionConfig();
    tableConfig.setInstanceAssignmentConfigMap(instanceAssignmentConfigMap);
    updateTableConfig(tableConfig);
    rebalanceConfig.setMinimizeDataMovement(Enablement.DEFAULT);
    response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
    checkRebalancePreCheckStatus(rebalanceResult, RebalanceResult.Status.NO_OP,
        "minimizeDataMovement is not enabled for COMPLETED segments, but instance assignment is allowed",
        RebalancePreCheckerResult.PreCheckStatus.WARN, "No need to reload",
        RebalancePreCheckerResult.PreCheckStatus.PASS, "All rebalance parameters look good",
        RebalancePreCheckerResult.PreCheckStatus.PASS,
        "reassignInstances is disabled, replica groups may not be updated.\nCOMPLETED segments - numReplicaGroups: "
            + replicaGroupPartitionConfig.getNumReplicaGroups() + ", numInstancesPerReplicaGroup: "
            + (replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup() == 0
            ? "0 (using as many instances as possible)" : replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup())
            + "\nCONSUMING segments - Replica Groups are not enabled, replication: " + tableConfig.getReplication(),
        RebalancePreCheckerResult.PreCheckStatus.WARN);

    // response will be the same for MinimizeDataMovementOptions.DISABLE and MinimizeDataMovementOptions.DEFAULT in
    // this case
    rebalanceConfig.setMinimizeDataMovement(Enablement.DISABLE);
    response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    assertEquals(JsonUtils.stringToObject(response, RebalanceResult.class)
            .getPreChecksResult().get(DefaultRebalancePreChecker.IS_MINIMIZE_DATA_MOVEMENT).getMessage(),
        rebalanceResult.getPreChecksResult().get(DefaultRebalancePreChecker.IS_MINIMIZE_DATA_MOVEMENT).getMessage());

    // Use MinimizeDataMovementOptions.ENABLE
    rebalanceConfig.setMinimizeDataMovement(Enablement.ENABLE);
    response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
    checkRebalancePreCheckStatus(rebalanceResult, RebalanceResult.Status.NO_OP,
        "minimizeDataMovement is enabled",
        RebalancePreCheckerResult.PreCheckStatus.PASS, "No need to reload",
        RebalancePreCheckerResult.PreCheckStatus.PASS, "All rebalance parameters look good",
        RebalancePreCheckerResult.PreCheckStatus.PASS,
        "reassignInstances is disabled, replica groups may not be updated.\nCOMPLETED segments - numReplicaGroups: "
            + replicaGroupPartitionConfig.getNumReplicaGroups() + ", numInstancesPerReplicaGroup: "
            + (replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup() == 0
            ? "0 (using as many instances as possible)" : replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup())
            + "\nCONSUMING segments - Replica Groups are not enabled, replication: " + tableConfig.getReplication(),
        RebalancePreCheckerResult.PreCheckStatus.WARN);

    instanceAssignmentConfigMap =
        Collections.singletonMap("COMPLETED", createInstanceAssignmentConfig(true, TableType.REALTIME));
    replicaGroupPartitionConfig = instanceAssignmentConfigMap.get("COMPLETED").getReplicaGroupPartitionConfig();
    tableConfig.setInstanceAssignmentConfigMap(instanceAssignmentConfigMap);
    updateTableConfig(tableConfig);
    rebalanceConfig.setMinimizeDataMovement(Enablement.DISABLE);
    response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
    checkRebalancePreCheckStatus(rebalanceResult, RebalanceResult.Status.NO_OP,
        "minimizeDataMovement is enabled for COMPLETED segments in table config but it's overridden with disabled",
        RebalancePreCheckerResult.PreCheckStatus.WARN, "No need to reload",
        RebalancePreCheckerResult.PreCheckStatus.PASS, "All rebalance parameters look good",
        RebalancePreCheckerResult.PreCheckStatus.PASS,
        "reassignInstances is disabled, replica groups may not be updated.\nCOMPLETED segments - numReplicaGroups: "
            + replicaGroupPartitionConfig.getNumReplicaGroups() + ", numInstancesPerReplicaGroup: "
            + (replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup() == 0
            ? "0 (using as many instances as possible)" : replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup())
            + "\nCONSUMING segments - Replica Groups are not enabled, replication: " + tableConfig.getReplication(),
        RebalancePreCheckerResult.PreCheckStatus.WARN);

    // Use MinimizeDataMovementOptions.DEFAULT and disable it in table config for CONSUMING ONLY
    instanceAssignmentConfigMap =
        Collections.singletonMap("CONSUMING", createInstanceAssignmentConfig(false, TableType.REALTIME));
    replicaGroupPartitionConfig = instanceAssignmentConfigMap.get("CONSUMING").getReplicaGroupPartitionConfig();
    tableConfig.setInstanceAssignmentConfigMap(instanceAssignmentConfigMap);
    updateTableConfig(tableConfig);
    rebalanceConfig.setMinimizeDataMovement(Enablement.DEFAULT);
    response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
    checkRebalancePreCheckStatus(rebalanceResult, RebalanceResult.Status.NO_OP,
        "minimizeDataMovement is not enabled for CONSUMING segments, but instance assignment is allowed",
        RebalancePreCheckerResult.PreCheckStatus.WARN, "No need to reload",
        RebalancePreCheckerResult.PreCheckStatus.PASS, "All rebalance parameters look good",
        RebalancePreCheckerResult.PreCheckStatus.PASS,
        "reassignInstances is disabled, replica groups may not be updated.\nCOMPLETED segments - Replica Groups are "
            + "not enabled, replication: " + tableConfig.getReplication() + "\nCONSUMING segments - numReplicaGroups: "
            + replicaGroupPartitionConfig.getNumReplicaGroups() + ", numInstancesPerReplicaGroup: "
            + (replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup() == 0
            ? "0 (using as many instances as possible)" : replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup()),
        RebalancePreCheckerResult.PreCheckStatus.WARN);

    // response will be the same for MinimizeDataMovementOptions.DISABLE and MinimizeDataMovementOptions.DEFAULT in
    // this case
    rebalanceConfig.setMinimizeDataMovement(Enablement.DISABLE);
    response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    assertEquals(JsonUtils.stringToObject(response, RebalanceResult.class)
            .getPreChecksResult().get(DefaultRebalancePreChecker.IS_MINIMIZE_DATA_MOVEMENT).getMessage(),
        rebalanceResult.getPreChecksResult().get(DefaultRebalancePreChecker.IS_MINIMIZE_DATA_MOVEMENT).getMessage());

    // Use MinimizeDataMovementOptions.ENABLE
    rebalanceConfig.setMinimizeDataMovement(Enablement.ENABLE);
    response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
    checkRebalancePreCheckStatus(rebalanceResult, RebalanceResult.Status.NO_OP,
        "minimizeDataMovement is enabled",
        RebalancePreCheckerResult.PreCheckStatus.PASS, "No need to reload",
        RebalancePreCheckerResult.PreCheckStatus.PASS, "All rebalance parameters look good",
        RebalancePreCheckerResult.PreCheckStatus.PASS,
        "reassignInstances is disabled, replica groups may not be updated.\nCOMPLETED segments - Replica Groups are "
            + "not enabled, replication: " + tableConfig.getReplication() + "\nCONSUMING segments - numReplicaGroups: "
            + replicaGroupPartitionConfig.getNumReplicaGroups() + ", numInstancesPerReplicaGroup: "
            + (replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup() == 0
            ? "0 (using as many instances as possible)" : replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup()),
        RebalancePreCheckerResult.PreCheckStatus.WARN);

    // Use MinimizeDataMovementOptions.DISABLE and enable it in table config for CONSUMING ONLY
    instanceAssignmentConfigMap =
        Collections.singletonMap("CONSUMING", createInstanceAssignmentConfig(true, TableType.REALTIME));
    replicaGroupPartitionConfig = instanceAssignmentConfigMap.get("CONSUMING").getReplicaGroupPartitionConfig();
    tableConfig.setInstanceAssignmentConfigMap(instanceAssignmentConfigMap);
    updateTableConfig(tableConfig);
    rebalanceConfig.setMinimizeDataMovement(Enablement.DISABLE);
    response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
    checkRebalancePreCheckStatus(rebalanceResult, RebalanceResult.Status.NO_OP,
        "minimizeDataMovement is enabled for CONSUMING segments in table config but it's overridden with disabled",
        RebalancePreCheckerResult.PreCheckStatus.WARN, "No need to reload",
        RebalancePreCheckerResult.PreCheckStatus.PASS, "All rebalance parameters look good",
        RebalancePreCheckerResult.PreCheckStatus.PASS,
        "reassignInstances is disabled, replica groups may not be updated.\nCOMPLETED segments - Replica Groups are "
            + "not enabled, replication: " + tableConfig.getReplication() + "\nCONSUMING segments - numReplicaGroups: "
            + replicaGroupPartitionConfig.getNumReplicaGroups() + ", numInstancesPerReplicaGroup: "
            + (replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup() == 0
            ? "0 (using as many instances as possible)" : replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup()),
        RebalancePreCheckerResult.PreCheckStatus.WARN);

    // Set minimizeDataMovement to true and false respectively for COMPLETED and CONSUMING segments
    instanceAssignmentConfigMap = new HashMap<>();
    instanceAssignmentConfigMap.put("CONSUMING", createInstanceAssignmentConfig(true, TableType.REALTIME));
    instanceAssignmentConfigMap.put("COMPLETED", createInstanceAssignmentConfig(false, TableType.REALTIME));
    replicaGroupPartitionConfig = instanceAssignmentConfigMap.get("CONSUMING").getReplicaGroupPartitionConfig();
    tableConfig.setInstanceAssignmentConfigMap(instanceAssignmentConfigMap);
    updateTableConfig(tableConfig);
    rebalanceConfig.setMinimizeDataMovement(Enablement.DEFAULT);
    response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
    checkRebalancePreCheckStatus(rebalanceResult, RebalanceResult.Status.NO_OP,
        "minimizeDataMovement is not enabled for either or both COMPLETED and CONSUMING segments, but instance "
            + "assignment is allowed for both",
        RebalancePreCheckerResult.PreCheckStatus.WARN, "No need to reload",
        RebalancePreCheckerResult.PreCheckStatus.PASS, "All rebalance parameters look good",
        RebalancePreCheckerResult.PreCheckStatus.PASS,
        "reassignInstances is disabled, replica groups may not be updated.\nCOMPLETED segments - numReplicaGroups: "
            + replicaGroupPartitionConfig.getNumReplicaGroups() + ", numInstancesPerReplicaGroup: "
            + (replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup() == 0
            ? "0 (using as many instances as possible)" : replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup())
            + "\nCONSUMING segments - numReplicaGroups: " + replicaGroupPartitionConfig.getNumReplicaGroups()
            + ", numInstancesPerReplicaGroup: "
            + (replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup() == 0
            ? "0 (using as many instances as possible)" : replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup()),
        RebalancePreCheckerResult.PreCheckStatus.WARN);

    // response will be the same for MinimizeDataMovementOptions.DISABLE and MinimizeDataMovementOptions.DEFAULT in
    // this case
    rebalanceConfig.setMinimizeDataMovement(Enablement.DISABLE);
    response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    assertEquals(JsonUtils.stringToObject(response, RebalanceResult.class)
            .getPreChecksResult().get(DefaultRebalancePreChecker.IS_MINIMIZE_DATA_MOVEMENT).getMessage(),
        rebalanceResult.getPreChecksResult().get(DefaultRebalancePreChecker.IS_MINIMIZE_DATA_MOVEMENT).getMessage());

    // Use MinimizeDataMovementOptions.ENABLE
    rebalanceConfig.setMinimizeDataMovement(Enablement.ENABLE);
    response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
    checkRebalancePreCheckStatus(rebalanceResult, RebalanceResult.Status.NO_OP,
        "minimizeDataMovement is enabled",
        RebalancePreCheckerResult.PreCheckStatus.PASS, "No need to reload",
        RebalancePreCheckerResult.PreCheckStatus.PASS, "All rebalance parameters look good",
        RebalancePreCheckerResult.PreCheckStatus.PASS,
        "reassignInstances is disabled, replica groups may not be updated.\nCOMPLETED segments - numReplicaGroups: "
            + replicaGroupPartitionConfig.getNumReplicaGroups() + ", numInstancesPerReplicaGroup: "
            + (replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup() == 0
            ? "0 (using as many instances as possible)" : replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup())
            + "\nCONSUMING segments - numReplicaGroups: " + replicaGroupPartitionConfig.getNumReplicaGroups()
            + ", numInstancesPerReplicaGroup: "
            + (replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup() == 0
            ? "0 (using as many instances as possible)" : replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup()),
        RebalancePreCheckerResult.PreCheckStatus.WARN);

    instanceAssignmentConfigMap.put("CONSUMING", createInstanceAssignmentConfig(true, TableType.REALTIME));
    instanceAssignmentConfigMap.put("COMPLETED", createInstanceAssignmentConfig(true, TableType.REALTIME));
    replicaGroupPartitionConfig = instanceAssignmentConfigMap.get("CONSUMING").getReplicaGroupPartitionConfig();
    tableConfig.setInstanceAssignmentConfigMap(instanceAssignmentConfigMap);
    updateTableConfig(tableConfig);
    rebalanceConfig.setMinimizeDataMovement(Enablement.DEFAULT);
    response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
    checkRebalancePreCheckStatus(rebalanceResult, RebalanceResult.Status.NO_OP,
        "minimizeDataMovement is enabled",
        RebalancePreCheckerResult.PreCheckStatus.PASS, "No need to reload",
        RebalancePreCheckerResult.PreCheckStatus.PASS, "All rebalance parameters look good",
        RebalancePreCheckerResult.PreCheckStatus.PASS,
        "reassignInstances is disabled, replica groups may not be updated.\nCOMPLETED segments - numReplicaGroups: "
            + replicaGroupPartitionConfig.getNumReplicaGroups() + ", numInstancesPerReplicaGroup: "
            + (replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup() == 0
            ? "0 (using as many instances as possible)" : replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup())
            + "\nCONSUMING segments - numReplicaGroups: " + replicaGroupPartitionConfig.getNumReplicaGroups()
            + ", numInstancesPerReplicaGroup: "
            + (replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup() == 0
            ? "0 (using as many instances as possible)" : replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup()),
        RebalancePreCheckerResult.PreCheckStatus.WARN);

    // response will be the same for MinimizeDataMovementOptions.ENABLE and MinimizeDataMovementOptions.DEFAULT in
    rebalanceConfig.setMinimizeDataMovement(Enablement.ENABLE);
    response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    assertEquals(JsonUtils.stringToObject(response, RebalanceResult.class)
            .getPreChecksResult().get(DefaultRebalancePreChecker.IS_MINIMIZE_DATA_MOVEMENT).getMessage(),
        rebalanceResult.getPreChecksResult().get(DefaultRebalancePreChecker.IS_MINIMIZE_DATA_MOVEMENT).getMessage());

    rebalanceConfig.setMinimizeDataMovement(Enablement.DISABLE);
    response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
    checkRebalancePreCheckStatus(rebalanceResult, RebalanceResult.Status.NO_OP,
        "minimizeDataMovement is enabled for both COMPLETED and CONSUMING segments in table config but it's "
            + "overridden with disabled",
        RebalancePreCheckerResult.PreCheckStatus.WARN, "No need to reload",
        RebalancePreCheckerResult.PreCheckStatus.PASS, "All rebalance parameters look good",
        RebalancePreCheckerResult.PreCheckStatus.PASS,
        "reassignInstances is disabled, replica groups may not be updated.\nCOMPLETED segments - numReplicaGroups: "
            + replicaGroupPartitionConfig.getNumReplicaGroups() + ", numInstancesPerReplicaGroup: "
            + (replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup() == 0
            ? "0 (using as many instances as possible)" : replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup())
            + "\nCONSUMING segments - numReplicaGroups: " + replicaGroupPartitionConfig.getNumReplicaGroups()
            + ", numInstancesPerReplicaGroup: "
            + (replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup() == 0
            ? "0 (using as many instances as possible)" : replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup()),
        RebalancePreCheckerResult.PreCheckStatus.WARN);

    instanceAssignmentConfigMap.put("CONSUMING", createInstanceAssignmentConfig(false, TableType.REALTIME));
    instanceAssignmentConfigMap.put("COMPLETED", createInstanceAssignmentConfig(false, TableType.REALTIME));
    replicaGroupPartitionConfig = instanceAssignmentConfigMap.get("CONSUMING").getReplicaGroupPartitionConfig();
    tableConfig.setInstanceAssignmentConfigMap(instanceAssignmentConfigMap);
    updateTableConfig(tableConfig);
    rebalanceConfig.setMinimizeDataMovement(Enablement.DEFAULT);
    response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
    checkRebalancePreCheckStatus(rebalanceResult, RebalanceResult.Status.NO_OP,
        "minimizeDataMovement is not enabled for either or both COMPLETED and CONSUMING segments, but instance "
            + "assignment is allowed for both",
        RebalancePreCheckerResult.PreCheckStatus.WARN, "No need to reload",
        RebalancePreCheckerResult.PreCheckStatus.PASS, "All rebalance parameters look good",
        RebalancePreCheckerResult.PreCheckStatus.PASS,
        "reassignInstances is disabled, replica groups may not be updated.\nCOMPLETED segments - numReplicaGroups: "
            + replicaGroupPartitionConfig.getNumReplicaGroups() + ", numInstancesPerReplicaGroup: "
            + (replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup() == 0
            ? "0 (using as many instances as possible)" : replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup())
            + "\nCONSUMING segments - numReplicaGroups: " + replicaGroupPartitionConfig.getNumReplicaGroups()
            + ", numInstancesPerReplicaGroup: "
            + (replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup() == 0
            ? "0 (using as many instances as possible)" : replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup()),
        RebalancePreCheckerResult.PreCheckStatus.WARN);

    // Use MinimizeDataMovementOptions.DISABLE and enable it in table config for CONSUMING ONLY
    instanceAssignmentConfigMap =
        Collections.singletonMap("CONSUMING", createInstanceAssignmentConfig(true, TableType.REALTIME));
    replicaGroupPartitionConfig = instanceAssignmentConfigMap.get("CONSUMING").getReplicaGroupPartitionConfig();
    tableConfig.setInstanceAssignmentConfigMap(instanceAssignmentConfigMap);
    TenantConfig tenantConfig = new TenantConfig(getBrokerTenant(), getServerTenant(),
        new TagOverrideConfig(null, TagNameUtils.getRealtimeTagForTenant(getServerTenant())));
    tableConfig.setTenantConfig(tenantConfig);
    updateTableConfig(tableConfig);
    rebalanceConfig.setMinimizeDataMovement(Enablement.DISABLE);
    response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
    checkRebalancePreCheckStatus(rebalanceResult, RebalanceResult.Status.NO_OP,
        "minimizeDataMovement is enabled for CONSUMING segments in table config but it's overridden with disabled",
        RebalancePreCheckerResult.PreCheckStatus.WARN, "No need to reload",
        RebalancePreCheckerResult.PreCheckStatus.PASS, "All rebalance parameters look good",
        RebalancePreCheckerResult.PreCheckStatus.PASS,
        "reassignInstances is disabled, replica groups may not be updated.\nCOMPLETED segments - Replica Groups are "
            + "not enabled, replication: " + tableConfig.getReplication() + "\nCONSUMING segments - numReplicaGroups: "
            + replicaGroupPartitionConfig.getNumReplicaGroups() + ", numInstancesPerReplicaGroup: "
            + (replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup() == 0
            ? "0 (using as many instances as possible)" : replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup()),
        RebalancePreCheckerResult.PreCheckStatus.WARN);

    updateTableConfig(originalTableConfig);
  }

  @Test
  public void testRealtimeRebalancePreChecks()
      throws Exception {
    // setup the rebalance config
    RebalanceConfig rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setDryRun(true);

    TableConfig tableConfig = getRealtimeTableConfig();
    TableConfig originalTableConfig = new TableConfig(tableConfig);

    // Ensure pre-check status is null if not enabled
    String response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    RebalanceResult rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
    assertNull(rebalanceResult.getPreChecksResult());

    // Enable pre-checks, nothing is set
    rebalanceConfig.setPreChecks(true);
    response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
    checkRebalancePreCheckStatus(rebalanceResult, RebalanceResult.Status.NO_OP,
        "Instance assignment not allowed, no need for minimizeDataMovement",
        RebalancePreCheckerResult.PreCheckStatus.PASS, "No need to reload",
        RebalancePreCheckerResult.PreCheckStatus.PASS, "includeConsuming is disabled for a realtime table.",
        RebalancePreCheckerResult.PreCheckStatus.WARN,
        "COMPLETED segments - Replica Groups are not enabled, replication: " + tableConfig.getReplication()
            + "\nCONSUMING segments - Replica Groups are not enabled, replication: " + tableConfig.getReplication(),
        RebalancePreCheckerResult.PreCheckStatus.PASS);

    // Enable minimizeDataMovement, enable replica group only for COMPLETED segments
    Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap =
        Collections.singletonMap("COMPLETED", createInstanceAssignmentConfig(true, TableType.REALTIME));
    InstanceReplicaGroupPartitionConfig replicaGroupPartitionConfig =
        instanceAssignmentConfigMap.get("COMPLETED").getReplicaGroupPartitionConfig();
    tableConfig.setInstanceAssignmentConfigMap(instanceAssignmentConfigMap);
    rebalanceConfig.setIncludeConsuming(true);
    updateTableConfig(tableConfig);
    response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
    checkRebalancePreCheckStatus(rebalanceResult, RebalanceResult.Status.NO_OP,
        "minimizeDataMovement is enabled", RebalancePreCheckerResult.PreCheckStatus.PASS,
        "No need to reload", RebalancePreCheckerResult.PreCheckStatus.PASS,
        "All rebalance parameters look good",
        RebalancePreCheckerResult.PreCheckStatus.PASS,
        "reassignInstances is disabled, replica groups may not be updated.\nCOMPLETED segments - numReplicaGroups: "
            + replicaGroupPartitionConfig.getNumReplicaGroups() + ", numInstancesPerReplicaGroup: "
            + (replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup() == 0
            ? "0 (using as many instances as possible)" : replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup())
            + "\nCONSUMING segments - Replica Groups are not enabled, replication: " + tableConfig.getReplication(),
        RebalancePreCheckerResult.PreCheckStatus.WARN);

    // Undo minimizeDataMovement, update the table config to add a column to bloom filter
    rebalanceConfig.setMinimizeDataMovement(Enablement.ENABLE);
    tableConfig.getIndexingConfig().getBloomFilterColumns().add("Quarter");
    tableConfig.setInstanceAssignmentConfigMap(null);
    updateTableConfig(tableConfig);
    response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
    checkRebalancePreCheckStatus(rebalanceResult, RebalanceResult.Status.NO_OP,
        "Instance assignment not allowed, no need for minimizeDataMovement",
        RebalancePreCheckerResult.PreCheckStatus.PASS, "Reload needed prior to running rebalance",
        RebalancePreCheckerResult.PreCheckStatus.WARN, "All rebalance parameters look good",
        RebalancePreCheckerResult.PreCheckStatus.PASS,
        "COMPLETED segments - Replica Groups are not enabled, replication: " + tableConfig.getReplication()
            + "\nCONSUMING segments - Replica Groups are not enabled, replication: " + tableConfig.getReplication(),
        RebalancePreCheckerResult.PreCheckStatus.PASS);

    // Undo tableConfig change
    tableConfig.getIndexingConfig().getBloomFilterColumns().remove("Quarter");
    updateTableConfig(tableConfig);
    response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
    checkRebalancePreCheckStatus(rebalanceResult, RebalanceResult.Status.NO_OP,
        "Instance assignment not allowed, no need for minimizeDataMovement",
        RebalancePreCheckerResult.PreCheckStatus.PASS, "No need to reload",
        RebalancePreCheckerResult.PreCheckStatus.PASS, "All rebalance parameters look good",
        RebalancePreCheckerResult.PreCheckStatus.PASS,
        "COMPLETED segments - Replica Groups are not enabled, replication: " + tableConfig.getReplication()
            + "\nCONSUMING segments - Replica Groups are not enabled, replication: " + tableConfig.getReplication(),
        RebalancePreCheckerResult.PreCheckStatus.PASS);

    // Add a new server (to force change in instance assignment) and enable reassignInstances
    // Validate that the status for reload is still PASS (i.e. even though an extra server is tagged which has no
    // segments assigned for this table, we don't try to get needReload status from that extra server, otherwise
    // ERROR status would be returned)
    BaseServerStarter serverStarter0 = startOneServer(NUM_SERVERS);
    createServerTenant(getServerTenant(), 0, 1);
    rebalanceConfig.setReassignInstances(true);
    tableConfig.setInstanceAssignmentConfigMap(null);
    updateTableConfig(tableConfig);
    response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
    checkRebalancePreCheckStatus(rebalanceResult, RebalanceResult.Status.DONE,
        "Instance assignment not allowed, no need for minimizeDataMovement",
        RebalancePreCheckerResult.PreCheckStatus.PASS, "No need to reload",
        RebalancePreCheckerResult.PreCheckStatus.PASS, "All rebalance parameters look good",
        RebalancePreCheckerResult.PreCheckStatus.PASS,
        "COMPLETED segments - Replica Groups are not enabled, replication: " + tableConfig.getReplication()
            + "\nCONSUMING segments - Replica Groups are not enabled, replication: " + tableConfig.getReplication(),
        RebalancePreCheckerResult.PreCheckStatus.PASS);
    rebalanceConfig.setReassignInstances(false);

    // Stop the added server
    serverStarter0.stop();
    TestUtils.waitForCondition(
        aVoid -> getHelixResourceManager().dropInstance(serverStarter0.getInstanceId()).isSuccessful(),
        60_000L, "Failed to drop added server");

    // Add a schema change. Notice that this may affect other following tests
    Schema schema = getSchema(getTableName());
    schema.addField(new MetricFieldSpec("NewAddedIntMetricB", FieldSpec.DataType.INT, 1));
    updateSchema(schema);
    response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
    checkRebalancePreCheckStatus(rebalanceResult, RebalanceResult.Status.NO_OP,
        "Instance assignment not allowed, no need for minimizeDataMovement",
        RebalancePreCheckerResult.PreCheckStatus.PASS, "Reload needed prior to running rebalance",
        RebalancePreCheckerResult.PreCheckStatus.WARN, "All rebalance parameters look good",
        RebalancePreCheckerResult.PreCheckStatus.PASS,
        "COMPLETED segments - Replica Groups are not enabled, replication: " + tableConfig.getReplication()
            + "\nCONSUMING segments - Replica Groups are not enabled, replication: " + tableConfig.getReplication(),
        RebalancePreCheckerResult.PreCheckStatus.PASS);

    // Keep schema change and update table config to add minimizeDataMovement
    instanceAssignmentConfigMap =
        Collections.singletonMap("COMPLETED", createInstanceAssignmentConfig(true, TableType.REALTIME));
    replicaGroupPartitionConfig = instanceAssignmentConfigMap.get("COMPLETED").getReplicaGroupPartitionConfig();
    tableConfig.setInstanceAssignmentConfigMap(instanceAssignmentConfigMap);
    updateTableConfig(tableConfig);
    response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
    checkRebalancePreCheckStatus(rebalanceResult, RebalanceResult.Status.NO_OP,
        "minimizeDataMovement is enabled", RebalancePreCheckerResult.PreCheckStatus.PASS,
        "Reload needed prior to running rebalance", RebalancePreCheckerResult.PreCheckStatus.WARN,
        "All rebalance parameters look good",
        RebalancePreCheckerResult.PreCheckStatus.PASS,
        "reassignInstances is disabled, replica groups may not be updated.\nCOMPLETED segments - numReplicaGroups: "
            + replicaGroupPartitionConfig.getNumReplicaGroups() + ", numInstancesPerReplicaGroup: "
            + (replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup() == 0
            ? "0 (using as many instances as possible)" : replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup())
            + "\nCONSUMING segments - Replica Groups are not enabled, replication: " + tableConfig.getReplication(),
        RebalancePreCheckerResult.PreCheckStatus.WARN);

    // Keep schema change and update table config to add instance config map with minimizeDataMovement = false
    instanceAssignmentConfigMap =
        Collections.singletonMap("CONSUMING", createInstanceAssignmentConfig(false, TableType.REALTIME));
    replicaGroupPartitionConfig = instanceAssignmentConfigMap.get("CONSUMING").getReplicaGroupPartitionConfig();
    tableConfig.setInstanceAssignmentConfigMap(instanceAssignmentConfigMap);
    updateTableConfig(tableConfig);
    response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
    checkRebalancePreCheckStatus(rebalanceResult, RebalanceResult.Status.NO_OP,
        "minimizeDataMovement is enabled",
        RebalancePreCheckerResult.PreCheckStatus.PASS, "Reload needed prior to running rebalance",
        RebalancePreCheckerResult.PreCheckStatus.WARN, "All rebalance parameters look good",
        RebalancePreCheckerResult.PreCheckStatus.PASS,
        "reassignInstances is disabled, replica groups may not be updated.\nCOMPLETED segments - Replica Groups are "
            + "not enabled, replication: " + tableConfig.getReplication() + "\nCONSUMING segments - numReplicaGroups: "
            + replicaGroupPartitionConfig.getNumReplicaGroups() + ", numInstancesPerReplicaGroup: "
            + (replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup() == 0
            ? "0 (using as many instances as possible)" : replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup()),
        RebalancePreCheckerResult.PreCheckStatus.WARN);

    // Add a new server (to force change in instance assignment) and enable reassignInstances
    // Trigger rebalance config warning
    BaseServerStarter serverStarter1 = startOneServer(NUM_SERVERS + 1);
    createServerTenant(getServerTenant(), 0, 1);
    rebalanceConfig.setReassignInstances(true);
    rebalanceConfig.setBestEfforts(true);
    rebalanceConfig.setBootstrap(true);
    rebalanceConfig.setMinAvailableReplicas(-1);
    tableConfig.setInstanceAssignmentConfigMap(null);
    updateTableConfig(tableConfig);
    response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
    checkRebalancePreCheckStatus(rebalanceResult, RebalanceResult.Status.DONE,
        "Instance assignment not allowed, no need for minimizeDataMovement",
        RebalancePreCheckerResult.PreCheckStatus.PASS, "Reload needed prior to running rebalance",
        RebalancePreCheckerResult.PreCheckStatus.WARN,
        "bestEfforts is enabled, only enable it if you know what you are doing\n"
            + "bootstrap is enabled which can cause a large amount of data movement, double check if this is "
            + "intended", RebalancePreCheckerResult.PreCheckStatus.WARN,
        "COMPLETED segments - Replica Groups are not enabled, replication: " + tableConfig.getReplication()
            + "\nCONSUMING segments - Replica Groups are not enabled, replication: " + tableConfig.getReplication(),
        RebalancePreCheckerResult.PreCheckStatus.PASS);

    response =
        sendPostRequest(getControllerRequestURLBuilder().forTableReload(getTableName(), TableType.REALTIME, false));
    waitForReloadToComplete(getReloadJobIdFromResponse(response), 30_000);

    rebalanceConfig.setBestEfforts(false);
    rebalanceConfig.setBootstrap(false);
    response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
    checkRebalancePreCheckStatus(rebalanceResult, RebalanceResult.Status.DONE,
        "Instance assignment not allowed, no need for minimizeDataMovement",
        RebalancePreCheckerResult.PreCheckStatus.PASS, "No need to reload",
        RebalancePreCheckerResult.PreCheckStatus.PASS, "All rebalance parameters look good",
        RebalancePreCheckerResult.PreCheckStatus.PASS,
        "COMPLETED segments - Replica Groups are not enabled, replication: " + tableConfig.getReplication()
            + "\nCONSUMING segments - Replica Groups are not enabled, replication: " + tableConfig.getReplication(),
        RebalancePreCheckerResult.PreCheckStatus.PASS);

    // Stop the added server
    serverStarter1.stop();
    TestUtils.waitForCondition(
        aVoid -> getHelixResourceManager().dropInstance(serverStarter1.getInstanceId()).isSuccessful(),
        60_000L, "Failed to drop added server");
    updateTableConfig(originalTableConfig);
  }

  private void checkRebalancePreCheckStatus(RebalanceResult rebalanceResult, RebalanceResult.Status expectedStatus,
      String expectedMinimizeDataMovement, RebalancePreCheckerResult.PreCheckStatus expectedMinimizeDataMovementStatus,
      String expectedNeedsReloadMessage, RebalancePreCheckerResult.PreCheckStatus expectedNeedsReloadStatus,
      String expectedRebalanceConfig, RebalancePreCheckerResult.PreCheckStatus expectedRebalanceConfigStatus,
      String expectedReplicaGroupMessage, RebalancePreCheckerResult.PreCheckStatus expectedReplicaGroupStatus) {
    assertEquals(rebalanceResult.getStatus(), expectedStatus);
    Map<String, RebalancePreCheckerResult> preChecksResult = rebalanceResult.getPreChecksResult();
    assertNotNull(preChecksResult);
    assertEquals(preChecksResult.size(), 6);
    assertTrue(preChecksResult.containsKey(DefaultRebalancePreChecker.IS_MINIMIZE_DATA_MOVEMENT));
    assertTrue(preChecksResult.containsKey(DefaultRebalancePreChecker.NEEDS_RELOAD_STATUS));
    assertTrue(preChecksResult.containsKey(DefaultRebalancePreChecker.DISK_UTILIZATION_DURING_REBALANCE));
    assertTrue(preChecksResult.containsKey(DefaultRebalancePreChecker.DISK_UTILIZATION_AFTER_REBALANCE));
    assertTrue(preChecksResult.containsKey(DefaultRebalancePreChecker.REBALANCE_CONFIG_OPTIONS));
    assertEquals(preChecksResult.get(DefaultRebalancePreChecker.IS_MINIMIZE_DATA_MOVEMENT).getPreCheckStatus(),
        expectedMinimizeDataMovementStatus);
    assertEquals(preChecksResult.get(DefaultRebalancePreChecker.IS_MINIMIZE_DATA_MOVEMENT).getMessage(),
        expectedMinimizeDataMovement);
    assertEquals(preChecksResult.get(DefaultRebalancePreChecker.NEEDS_RELOAD_STATUS).getPreCheckStatus(),
        expectedNeedsReloadStatus);
    assertEquals(preChecksResult.get(DefaultRebalancePreChecker.NEEDS_RELOAD_STATUS).getMessage(),
        expectedNeedsReloadMessage);
    assertEquals(preChecksResult.get(DefaultRebalancePreChecker.REBALANCE_CONFIG_OPTIONS).getPreCheckStatus(),
        expectedRebalanceConfigStatus);
    assertEquals(preChecksResult.get(DefaultRebalancePreChecker.REBALANCE_CONFIG_OPTIONS).getMessage(),
        expectedRebalanceConfig);
    assertEquals(preChecksResult.get(DefaultRebalancePreChecker.REPLICA_GROUPS_INFO).getPreCheckStatus(),
        expectedReplicaGroupStatus);
    assertEquals(preChecksResult.get(DefaultRebalancePreChecker.REPLICA_GROUPS_INFO).getMessage(),
        expectedReplicaGroupMessage);
    // As the disk utilization check periodic task was disabled in the test controller (ControllerConf
    // .RESOURCE_UTILIZATION_CHECKER_INITIAL_DELAY was set to 30000s, see org.apache.pinot.controller.helix
    // .ControllerTest.getDefaultControllerConfiguration), server's disk util should be unavailable on all servers if
    // not explicitly set via org.apache.pinot.controller.validation.ResourceUtilizationInfo.setDiskUsageInfo
    assertEquals(preChecksResult.get(DefaultRebalancePreChecker.DISK_UTILIZATION_DURING_REBALANCE).getPreCheckStatus(),
        RebalancePreCheckerResult.PreCheckStatus.WARN);
    assertEquals(preChecksResult.get(DefaultRebalancePreChecker.DISK_UTILIZATION_AFTER_REBALANCE).getPreCheckStatus(),
        RebalancePreCheckerResult.PreCheckStatus.WARN);
  }

  private InstanceAssignmentConfig createInstanceAssignmentConfig(boolean minimizeDataMovement, TableType tableType) {
    InstanceTagPoolConfig instanceTagPoolConfig =
        new InstanceTagPoolConfig(TagNameUtils.getServerTagForTenant(getServerTenant(), tableType), false, 1, null);
    List<String> constraints = new ArrayList<>();
    constraints.add("constraints1");
    InstanceConstraintConfig instanceConstraintConfig = new InstanceConstraintConfig(constraints);
    InstanceReplicaGroupPartitionConfig instanceReplicaGroupPartitionConfig =
        new InstanceReplicaGroupPartitionConfig(true, 1, 1,
            1, 1, 1, minimizeDataMovement,
            null);
    return new InstanceAssignmentConfig(instanceTagPoolConfig,
        instanceConstraintConfig, instanceReplicaGroupPartitionConfig, null, minimizeDataMovement);
  }

  @Test
  public void testRealtimeRebalanceDryRunSummary()
      throws Exception {
    // setup the rebalance config
    RebalanceConfig rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setDryRun(true);

    TableConfig tableConfig = getRealtimeTableConfig();
    TableConfig originalTableConfig = new TableConfig(tableConfig);

    // Ensure summary status is non-null always
    String response = sendPostRequest(getRebalanceUrl(rebalanceConfig, TableType.REALTIME));
    RebalanceResult rebalanceResult = JsonUtils.stringToObject(response, RebalanceResult.class);
    assertNotNull(rebalanceResult.getRebalanceSummaryResult());
    checkRebalanceDryRunSummary(rebalanceResult, RebalanceResult.Status.NO_OP, false, NUM_SERVERS_REALTIME,
        NUM_SERVERS_REALTIME,
        tableConfig.getReplication(), true);

    updateTableConfig(originalTableConfig);
  }

  private void checkRebalanceDryRunSummary(RebalanceResult rebalanceResult, RebalanceResult.Status expectedStatus,
      boolean isSegmentsToBeMoved, int existingNumServers, int newNumServers, int replicationFactor,
      boolean isRealtime) {
    assertEquals(rebalanceResult.getStatus(), expectedStatus);
    RebalanceSummaryResult summaryResult = rebalanceResult.getRebalanceSummaryResult();
    assertNotNull(summaryResult);
    assertNotNull(summaryResult.getServerInfo());
    assertNotNull(summaryResult.getSegmentInfo());
    assertEquals(summaryResult.getSegmentInfo().getReplicationFactor().getValueBeforeRebalance(), replicationFactor,
        "Existing replication factor doesn't match expected");
    assertEquals(summaryResult.getSegmentInfo().getReplicationFactor().getValueBeforeRebalance(),
        summaryResult.getSegmentInfo().getReplicationFactor().getExpectedValueAfterRebalance(),
        "Existing and new replication factor doesn't match");
    assertEquals(summaryResult.getServerInfo().getNumServers().getValueBeforeRebalance(), existingNumServers,
        "Existing number of servers don't match");
    assertEquals(summaryResult.getServerInfo().getNumServers().getExpectedValueAfterRebalance(), newNumServers,
        "New number of servers don't match");
    // In this cluster integration test, servers are tagged with DefaultTenant only
    assertEquals(summaryResult.getTagsInfo().size(), 1);
    if (isRealtime) {
      assertEquals(summaryResult.getTagsInfo().get(0).getTagName(),
          TagNameUtils.getRealtimeTagForTenant(getServerTenant()));
    } else {
      assertEquals(summaryResult.getTagsInfo().get(0).getTagName(),
          TagNameUtils.getOfflineTagForTenant(getServerTenant()));
    }
    assertEquals(summaryResult.getTagsInfo().get(0).getNumServerParticipants(), newNumServers);
    assertEquals(summaryResult.getSegmentInfo().getTotalSegmentsToBeMoved(),
        summaryResult.getTagsInfo().get(0).getNumSegmentsToDownload());
    // For this single tenant, the number of unchanged segments and the number of received segments should add up to
    // the total present segment
    assertEquals(summaryResult.getSegmentInfo().getNumSegmentsAcrossAllReplicas().getExpectedValueAfterRebalance(),
        summaryResult.getTagsInfo().get(0).getNumSegmentsUnchanged() + summaryResult.getTagsInfo()
            .get(0)
            .getNumSegmentsToDownload());
    long tableSize = 0;
    try {
      tableSize = getTableSize(getTableName());
    } catch (IOException e) {
      Assert.fail("Failed to get table size", e);
    }
    if (tableSize > 0) {
      assertTrue(summaryResult.getSegmentInfo().getEstimatedAverageSegmentSizeInBytes() > 0L,
          "Avg segment size expected to be > 0 but found to be 0");
    }
    assertEquals(summaryResult.getServerInfo().getNumServersGettingNewSegments(),
        summaryResult.getServerInfo().getServersGettingNewSegments().size());
    if (existingNumServers != newNumServers) {
      assertTrue(summaryResult.getServerInfo().getNumServersGettingNewSegments() > 0,
          "Expected number of servers should be > 0");
    } else {
      assertEquals(summaryResult.getServerInfo().getNumServersGettingNewSegments(), 0,
          "Expected number of servers getting new segments should be 0");
    }

    if (isSegmentsToBeMoved) {
      assertTrue(summaryResult.getSegmentInfo().getTotalSegmentsToBeMoved() > 0,
          "Segments to be moved should be > 0");
      assertTrue(summaryResult.getSegmentInfo().getTotalSegmentsToBeDeleted() > 0,
          "Segments to be moved should be > 0");
      assertEquals(summaryResult.getSegmentInfo().getTotalEstimatedDataToBeMovedInBytes(),
          summaryResult.getSegmentInfo().getTotalSegmentsToBeMoved()
              * summaryResult.getSegmentInfo().getEstimatedAverageSegmentSizeInBytes(),
          "Estimated data to be moved in bytes doesn't match");
      assertTrue(summaryResult.getSegmentInfo().getMaxSegmentsAddedToASingleServer() > 0,
          "Estimated max number of segments to move in a single server should be > 0");
    } else {
      assertEquals(summaryResult.getSegmentInfo().getTotalSegmentsToBeMoved(), 0, "Segments to be moved should be 0");
      assertEquals(summaryResult.getSegmentInfo().getTotalEstimatedDataToBeMovedInBytes(), 0L,
          "Estimated data to be moved in bytes should be 0");
      assertEquals(summaryResult.getSegmentInfo().getMaxSegmentsAddedToASingleServer(), 0,
          "Estimated max number of segments to move in a single server should be 0");
    }

    // Validate server status stats with numServers information
    Map<String, RebalanceSummaryResult.ServerSegmentChangeInfo> serverSegmentChangeInfoMap =
        summaryResult.getServerInfo().getServerSegmentChangeInfo();
    int numServersAdded = 0;
    int numServersRemoved = 0;
    int numServersUnchanged = 0;
    for (RebalanceSummaryResult.ServerSegmentChangeInfo serverSegmentChangeInfo : serverSegmentChangeInfoMap.values()) {
      switch (serverSegmentChangeInfo.getServerStatus()) {
        case UNCHANGED:
          numServersUnchanged++;
          break;
        case ADDED:
          numServersAdded++;
          break;
        case REMOVED:
          numServersRemoved++;
          break;
        default:
          Assert.fail(String.format("Unknown server status encountered: %s",
              serverSegmentChangeInfo.getServerStatus()));
          break;
      }
    }
    if (isRealtime) {
      Assert.assertNotNull(summaryResult.getSegmentInfo().getConsumingSegmentToBeMovedSummary());
    }

    Assert.assertEquals(summaryResult.getServerInfo().getNumServers().getValueBeforeRebalance(),
        numServersRemoved + numServersUnchanged);
    Assert.assertEquals(summaryResult.getServerInfo().getNumServers().getExpectedValueAfterRebalance(),
        numServersAdded + numServersUnchanged);

    assertEquals(numServersAdded, summaryResult.getServerInfo().getServersAdded().size());
    assertEquals(numServersRemoved, summaryResult.getServerInfo().getServersRemoved().size());
    assertEquals(numServersUnchanged, summaryResult.getServerInfo().getServersUnchanged().size());
  }

  private String getReloadJobIdFromResponse(String response) {
    Pattern pattern = new JavaUtilPattern("([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})");
    Matcher matcher = pattern.matcher(response);
    String jobId = matcher.find() ? matcher.group(1) : null;
    if (jobId == null) {
      return "";
    }
    return jobId;
  }

  private void waitForReloadToComplete(String reloadJobId, long timeoutMs) {
    TestUtils.waitForCondition(aVoid -> {
      try {
        String requestUrl = getControllerRequestURLBuilder().forSegmentReloadStatus(reloadJobId);
        SimpleHttpResponse httpResponse =
            HttpClient.wrapAndThrowHttpException(getHttpClient().sendGetRequest(new URL(requestUrl).toURI(), null));
        ServerReloadControllerJobStatusResponse reloadResult =
            JsonUtils.stringToObject(httpResponse.getResponse(), ServerReloadControllerJobStatusResponse.class);
        return reloadResult.getEstimatedTimeRemainingInMinutes() == 0.0;
      } catch (Exception e) {
        return null;
      }
    }, 1000L, timeoutMs, "Failed to reload all segments");
  }
}
