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
package org.apache.pinot.controller.helix.core;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.tier.TierFactory;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.utils.SegmentMetadataMockUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.config.tenant.Tenant;
import org.apache.pinot.spi.config.tenant.TenantRole;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


@Test(groups = "stateless")
public class PinotHelixResourceManagerAssignmentTest extends ControllerTest {
  private static final int NUM_BROKER_INSTANCES = 3;
  private static final int NUM_OFFLINE_SERVER_INSTANCES = 2;
  private static final int NUM_OFFLINE_COLD_SERVER_INSTANCES = 2;
  private static final int NUM_REALTIME_SERVER_INSTANCES = 2;
  private static final int NUM_SERVER_INSTANCES =
      NUM_OFFLINE_SERVER_INSTANCES + NUM_REALTIME_SERVER_INSTANCES + NUM_OFFLINE_COLD_SERVER_INSTANCES;
  private static final String BROKER_TENANT_NAME = "brokerTenant";
  private static final String SERVER_TENANT_NAME = "serverTenant";
  private static final String SERVER_COLD_TENANT_NAME = "coldServerTenant";

  private static final String RAW_TABLE_NAME = "testTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();

    Map<String, Object> properties = getDefaultControllerConfiguration();
    properties.put(ControllerConf.CONTROLLER_ENABLE_TIERED_SEGMENT_ASSIGNMENT, true);
    properties.put(ControllerConf.CLUSTER_TENANT_ISOLATION_ENABLE, false);
    startController(properties);

    addFakeBrokerInstancesToAutoJoinHelixCluster(NUM_BROKER_INSTANCES, false);
    addFakeServerInstancesToAutoJoinHelixCluster(NUM_SERVER_INSTANCES, false);

    resetBrokerTags();
    resetServerTags();
  }

  private void untagBrokers() {
    for (String brokerInstance : _helixResourceManager.getAllInstancesForBrokerTenant(BROKER_TENANT_NAME)) {
      _helixResourceManager.updateInstanceTags(brokerInstance, Helix.UNTAGGED_BROKER_INSTANCE, false);
    }
  }

  private void resetBrokerTags() {
    untagBrokers();
    assertEquals(_helixResourceManager.getOnlineUnTaggedBrokerInstanceList().size(), NUM_BROKER_INSTANCES);
    Tenant brokerTenant = new Tenant(TenantRole.BROKER, BROKER_TENANT_NAME, NUM_BROKER_INSTANCES, 0, 0);
    _helixResourceManager.createBrokerTenant(brokerTenant);
    assertEquals(_helixResourceManager.getOnlineUnTaggedBrokerInstanceList().size(), 0);
  }

  private void untagServers() {
    for (String serverInstance : _helixResourceManager.getAllInstancesForServerTenant(SERVER_TENANT_NAME)) {
      _helixResourceManager.updateInstanceTags(serverInstance, Helix.UNTAGGED_SERVER_INSTANCE, false);
    }

    for (String serverInstance : _helixResourceManager.getAllInstancesForServerTenant(SERVER_COLD_TENANT_NAME)) {
      _helixResourceManager.updateInstanceTags(serverInstance, Helix.UNTAGGED_SERVER_INSTANCE, false);
    }
  }

  private void resetServerTags() {
    untagServers();
    assertEquals(_helixResourceManager.getOnlineUnTaggedServerInstanceList().size(), NUM_SERVER_INSTANCES);

    // Create default tenant
    Tenant serverTenant =
        new Tenant(TenantRole.SERVER, SERVER_TENANT_NAME, NUM_SERVER_INSTANCES - NUM_OFFLINE_COLD_SERVER_INSTANCES,
            NUM_OFFLINE_SERVER_INSTANCES, NUM_REALTIME_SERVER_INSTANCES);
    _helixResourceManager.createServerTenant(serverTenant);

    // Create cold tenant
    Tenant coldTenant =
        new Tenant(TenantRole.SERVER, SERVER_COLD_TENANT_NAME, NUM_OFFLINE_COLD_SERVER_INSTANCES,
            NUM_OFFLINE_COLD_SERVER_INSTANCES, 0);
    _helixResourceManager.createServerTenant(coldTenant);

    assertEquals(_helixResourceManager.getOnlineUnTaggedServerInstanceList().size(), 0);
  }

  @Test
  public void testAssignTargetTier()
      throws Exception {
    String coldOfflineServerTag = SERVER_COLD_TENANT_NAME + "_OFFLINE";
    TierConfig tierConfig =
        new TierConfig("tier1", TierFactory.FIXED_SEGMENT_SELECTOR_TYPE, null, Collections.singletonList("testSegment"),
            TierFactory.PINOT_SERVER_STORAGE_TYPE, coldOfflineServerTag, null, null);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setBrokerTenant(BROKER_TENANT_NAME)
            .setTierConfigList(Collections.singletonList(tierConfig)).setServerTenant(SERVER_TENANT_NAME).build();
    waitForEVToDisappear(tableConfig.getTableName());
    _helixResourceManager.addTable(tableConfig);

    String segmentName = "testSegment";
    ZKMetadataProvider.setSegmentZKMetadata(_propertyStore, OFFLINE_TABLE_NAME, new SegmentZKMetadata(segmentName));
    _helixResourceManager.addNewSegment(OFFLINE_TABLE_NAME,
        SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, segmentName), "downloadUrl");

    List<SegmentZKMetadata> retrievedSegmentsZKMetadata =
        _helixResourceManager.getSegmentsZKMetadata(OFFLINE_TABLE_NAME);
    SegmentZKMetadata retrievedSegmentZKMetadata = retrievedSegmentsZKMetadata.get(0);
    assertEquals(retrievedSegmentZKMetadata.getSegmentName(), segmentName);
    assertEquals(retrievedSegmentZKMetadata.getTier(), "tier1");

    // Retrieve current assignment of the table and ensure segment's presence there
    IdealState idealState = HelixHelper.getTableIdealState(_helixManager, OFFLINE_TABLE_NAME);
    assertNotNull(idealState);
    Map<String, Map<String, String>> currentAssignment = idealState.getRecord().getMapFields();
    assertTrue(currentAssignment.size() == 1 && currentAssignment.get(segmentName).size() == 1);

    // Ensure that the server instance belongs to the cold tenant
    String coldServerName = currentAssignment.get(segmentName).keySet().iterator().next();
    InstanceConfig coldServerConfig = HelixHelper.getInstanceConfig(_helixManager, coldServerName);
    assertTrue(coldServerConfig.containsTag(coldOfflineServerTag));
  }

  @AfterClass
  public void tearDown() {
    stopFakeInstances();
    stopController();
    stopZk();
  }
}
