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

import org.apache.helix.HelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
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

  @AfterClass
  public void tearDown() {
    stopFakeInstances();
    stopController();
    stopZk();
  }
}
