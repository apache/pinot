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
package org.apache.pinot.controller.validation;

import java.util.List;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Tests for the ValidationManagers.
 */
@Test(groups = "stateless")
public class ValidationManagerStatelessTest extends ControllerTest {
  private static final String TEST_TABLE_NAME = "testTable";
  private static final String TEST_TABLE_TWO = "testTable2";
  private static final String TEST_SEGMENT_NAME = "testSegment";

  private TableConfig _offlineTableConfig;

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    startController();
    addFakeBrokerInstancesToAutoJoinHelixCluster(2, true);
    addFakeServerInstancesToAutoJoinHelixCluster(2, true);
    // Create a schema
    addDummySchema(TEST_TABLE_NAME);
    // Create a table
    _offlineTableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TEST_TABLE_NAME).setNumReplicas(2).build();
    _helixResourceManager.addTable(_offlineTableConfig);
  }

  @Test
  public void testRebuildBrokerResourceWhenBrokerAdded()
      throws Exception {
    // Check that the first table we added doesn't need to be rebuilt(case where ideal state brokers and brokers in
    // broker resource are the same.
    String partitionName = _offlineTableConfig.getTableName();
    HelixAdmin helixAdmin = _helixManager.getClusterManagmentTool();

    IdealState idealState = HelixHelper.getBrokerIdealStates(helixAdmin, getHelixClusterName());
    // Ensure that the broker resource is not rebuilt.
    Assert.assertTrue(idealState.getInstanceSet(partitionName)
        .equals(_helixResourceManager.getAllInstancesForBrokerTenant(TagNameUtils.DEFAULT_TENANT_NAME)));
    _helixResourceManager.rebuildBrokerResourceFromHelixTags(partitionName);

    // Add another table that needs to be rebuilt
    addDummySchema(TEST_TABLE_TWO);
    TableConfig offlineTableConfigTwo = new TableConfigBuilder(TableType.OFFLINE).setTableName(TEST_TABLE_TWO).build();
    _helixResourceManager.addTable(offlineTableConfigTwo);
    String partitionNameTwo = offlineTableConfigTwo.getTableName();

    // Add a new broker manually such that the ideal state is not updated and ensure that rebuild broker resource is
    // called
    final String brokerId = "Broker_localhost_2";
    InstanceConfig instanceConfig = new InstanceConfig(brokerId);
    instanceConfig.setInstanceEnabled(true);
    instanceConfig.setHostName("Broker_localhost");
    instanceConfig.setPort("2");
    helixAdmin.addInstance(getHelixClusterName(), instanceConfig);
    helixAdmin.addInstanceTag(getHelixClusterName(), instanceConfig.getInstanceName(),
        TagNameUtils.getBrokerTagForTenant(TagNameUtils.DEFAULT_TENANT_NAME));
    idealState = HelixHelper.getBrokerIdealStates(helixAdmin, getHelixClusterName());
    // Assert that the two don't equal before the call to rebuild the broker resource.
    Assert.assertTrue(!idealState.getInstanceSet(partitionNameTwo)
        .equals(_helixResourceManager.getAllInstancesForBrokerTenant(TagNameUtils.DEFAULT_TENANT_NAME)));
    _helixResourceManager.rebuildBrokerResourceFromHelixTags(partitionNameTwo);
    idealState = HelixHelper.getBrokerIdealStates(helixAdmin, getHelixClusterName());
    // Assert that the two do equal after being rebuilt.
    Assert.assertTrue(idealState.getInstanceSet(partitionNameTwo)
        .equals(_helixResourceManager.getAllInstancesForBrokerTenant(TagNameUtils.DEFAULT_TENANT_NAME)));
  }

  /**
   * Verifies that rebuildBrokerResourceFromHelixTags works for a logical table partition when a new broker
   * is added manually and the ideal state is out of sync (Issue #15751).
   */
  @Test
  public void testRebuildBrokerResourceWhenBrokerAddedForLogicalTable()
      throws Exception {
    // Add realtime table so we can create a logical table with both offline and realtime
    TableConfig realtimeTableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(TEST_TABLE_NAME).setNumReplicas(2)
            .setStreamConfigs(FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap()).build();
    _helixResourceManager.addTable(realtimeTableConfig);

    String logicalTableName = "test_logical_rebuild";
    addDummySchema(logicalTableName);
    List<String> physicalTableNamesWithType =
        List.of(TableNameBuilder.OFFLINE.tableNameWithType(TEST_TABLE_NAME),
            TableNameBuilder.REALTIME.tableNameWithType(TEST_TABLE_NAME));
    LogicalTableConfig logicalTableConfig =
        ControllerTest.getDummyLogicalTableConfig(logicalTableName, physicalTableNamesWithType,
            TagNameUtils.DEFAULT_TENANT_NAME);
    addLogicalTableConfig(logicalTableConfig);

    HelixAdmin helixAdmin = _helixManager.getClusterManagmentTool();
    IdealState idealState = HelixHelper.getBrokerIdealStates(helixAdmin, getHelixClusterName());
    Assert.assertTrue(idealState.getPartitionSet().contains(logicalTableName));

    // Add a new broker manually so the logical table partition is missing it
    final String newBrokerId = "Broker_localhost_3";
    InstanceConfig instanceConfig = new InstanceConfig(newBrokerId);
    instanceConfig.setInstanceEnabled(true);
    instanceConfig.setHostName("Broker_localhost");
    instanceConfig.setPort("3");
    helixAdmin.addInstance(getHelixClusterName(), instanceConfig);
    helixAdmin.addInstanceTag(getHelixClusterName(), instanceConfig.getInstanceName(),
        TagNameUtils.getBrokerTagForTenant(TagNameUtils.DEFAULT_TENANT_NAME));

    idealState = HelixHelper.getBrokerIdealStates(helixAdmin, getHelixClusterName());
    Assert.assertFalse(idealState.getInstanceSet(logicalTableName)
        .equals(_helixResourceManager.getAllInstancesForBrokerTenant(TagNameUtils.DEFAULT_TENANT_NAME)));

    _helixResourceManager.rebuildBrokerResourceFromHelixTags(logicalTableName);
    idealState = HelixHelper.getBrokerIdealStates(helixAdmin, getHelixClusterName());
    Assert.assertTrue(idealState.getInstanceSet(logicalTableName)
        .equals(_helixResourceManager.getAllInstancesForBrokerTenant(TagNameUtils.DEFAULT_TENANT_NAME)));

    // Cleanup
    _helixResourceManager.deleteLogicalTableConfig(logicalTableName);
    _helixResourceManager.deleteRealtimeTable(TEST_TABLE_NAME);
    // Remove the manually added broker instance so that subsequent tests see a clean cluster state
    helixAdmin.removeInstanceTag(getHelixClusterName(), instanceConfig.getInstanceName(),
        TagNameUtils.getBrokerTagForTenant(TagNameUtils.DEFAULT_TENANT_NAME));
    instanceConfig.setInstanceEnabled(false);
    helixAdmin.dropInstance(getHelixClusterName(), instanceConfig);
  }

  @AfterClass
  public void tearDown() {
    stopFakeInstances();
    stopController();
    stopZk();
  }
}
