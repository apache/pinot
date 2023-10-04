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

package org.apache.pinot.controller.helix.core.rebalance.tenant;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.metadata.controllerjob.ControllerJobType;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceJobConstants;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceResult;
import org.apache.pinot.controller.utils.SegmentMetadataMockUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class TenantRebalancerTest extends ControllerTest {

  private static final String DEFAULT_TENANT_NAME = "DefaultTenant";
  private static final String TENANT_NAME = "TestTenant";
  private static final String RAW_TABLE_NAME_A = "testTableA";
  private static final String OFFLINE_TABLE_NAME_A = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME_A);
  private static final String RAW_TABLE_NAME_B = "testTableB";
  private static final String OFFLINE_TABLE_NAME_B = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME_B);
  private static final int NUM_REPLICAS = 3;
  ExecutorService _executorService;

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    startController();
    addFakeBrokerInstancesToAutoJoinHelixCluster(1, true);
    _executorService = Executors.newFixedThreadPool(3);
  }

  @Test
  public void testRebalance()
      throws Exception {
    int numServers = 3;
    for (int i = 0; i < numServers; i++) {
      addFakeServerInstanceToAutoJoinHelixCluster(SERVER_INSTANCE_ID_PREFIX + i, true);
    }

    TenantRebalancer tenantRebalancer = new DefaultTenantRebalancer(_helixResourceManager, _executorService);

    // tag all servers and brokers to test tenant
    addTenantTagToInstances(TENANT_NAME);

    // create 2 tables, one on each of test tenant and default tenant
    createTableWithSegments(RAW_TABLE_NAME_A, DEFAULT_TENANT_NAME);
    createTableWithSegments(RAW_TABLE_NAME_B, TENANT_NAME);

    // Add 3 more servers which will be tagged to default tenant
    int numServersToAdd = 3;
    for (int i = 0; i < numServersToAdd; i++) {
      addFakeServerInstanceToAutoJoinHelixCluster(SERVER_INSTANCE_ID_PREFIX + (numServers + i), true);
    }

    Map<String, Map<String, String>> oldSegmentAssignment =
        _helixResourceManager.getTableIdealState(OFFLINE_TABLE_NAME_B).getRecord().getMapFields();

    // rebalance the tables on test tenant
    TenantRebalanceConfig config = new TenantRebalanceConfig();
    config.setTenantName(TENANT_NAME);
    config.setVerboseResult(true);
    TenantRebalanceResult result = tenantRebalancer.rebalance(config);
    RebalanceResult rebalanceResult = result.getRebalanceTableResults().get(OFFLINE_TABLE_NAME_B);
    Map<String, Map<String, String>> rebalancedAssignment = rebalanceResult.getSegmentAssignment();
    // assignment should not change, with a NO_OP status as no now server is added to test tenant
    assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.NO_OP);
    assertEquals(oldSegmentAssignment, rebalancedAssignment);

    // rebalance the tables on default tenant
    config.setTenantName(DEFAULT_TENANT_NAME);
    result = tenantRebalancer.rebalance(config);
    // rebalancing default tenant should distribute the segment of table A over 6 servers
    rebalanceResult = result.getRebalanceTableResults().get(OFFLINE_TABLE_NAME_A);
    InstancePartitions partitions = rebalanceResult.getInstanceAssignment().get(InstancePartitionsType.OFFLINE);
    assertEquals(partitions.getPartitionToInstancesMap().get("0_0").size(), 6);

    // ensure the ideal state and external view converges
    assertTrue(waitForCompletion(result.getJobId()));
    TenantRebalanceProgressStats progressStats = getProgress(result.getJobId());
    assertTrue(progressStats.getTableRebalanceJobIdMap().containsKey(OFFLINE_TABLE_NAME_A));
    assertEquals(progressStats.getTableStatusMap().get(OFFLINE_TABLE_NAME_A),
        TenantRebalanceProgressStats.TableStatus.PROCESSED.name());
    Map<String, Map<String, String>> idealState =
        _helixResourceManager.getTableIdealState(OFFLINE_TABLE_NAME_A).getRecord().getMapFields();
    Map<String, Map<String, String>> externalView =
        _helixResourceManager.getTableExternalView(OFFLINE_TABLE_NAME_A).getRecord().getMapFields();
    assertEquals(idealState, externalView);
  }

  private boolean waitForCompletion(String jobId) {
    int retries = 5;
    while (retries > 0) {
      try {
        TenantRebalanceProgressStats stats = getProgress(jobId);
        if (stats != null && stats.getRemainingTables() == 0) {
          return true;
        }
        retries--;
        Thread.sleep(2000);
      } catch (JsonProcessingException | InterruptedException e) {
        return false;
      }
    }
    return false;
  }

  private TenantRebalanceProgressStats getProgress(String jobId)
      throws JsonProcessingException {
    Map<String, String> controllerJobZKMetadata =
        _helixResourceManager.getControllerJobZKMetadata(jobId, ControllerJobType.TENANT_REBALANCE);
    if (controllerJobZKMetadata == null) {
      return null;
    }
    return JsonUtils.stringToObject(
        controllerJobZKMetadata.get(RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_PROGRESS_STATS),
        TenantRebalanceProgressStats.class);
  }

  private void createTableWithSegments(String rawTableName, String tenant)
      throws IOException {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(rawTableName)
        .setServerTenant(tenant).setBrokerTenant(tenant).setNumReplicas(NUM_REPLICAS).build();
    // Create the table
    _helixResourceManager.addTable(tableConfig);
    // Add the segments
    int numSegments = 10;
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName);
    for (int i = 0; i < numSegments; i++) {
      _helixResourceManager.addNewSegment(offlineTableName,
          SegmentMetadataMockUtils.mockSegmentMetadata(rawTableName, "segment_" + i), null);
    }
  }

  private void addTenantTagToInstances(String testTenant) {
    String offlineTag = TagNameUtils.getOfflineTagForTenant(testTenant);
    String brokerTag = TagNameUtils.getBrokerTagForTenant(testTenant);
    _helixResourceManager.getAllInstances().forEach(instance -> {
      List<String> existingTags = _helixResourceManager.getHelixInstanceConfig(instance).getTags();
      if (instance.startsWith(SERVER_INSTANCE_ID_PREFIX)) {
        existingTags.add(offlineTag);
      } else if (instance.startsWith(BROKER_INSTANCE_ID_PREFIX)) {
        existingTags.add(brokerTag);
      }
      _helixResourceManager.updateInstanceTags(instance, String.join(",", existingTags), true);
    });
  }

  @AfterClass
  public void tearDown() {
    stopFakeInstances();
    stopController();
    stopZk();
    _executorService.shutdown();
  }
}
