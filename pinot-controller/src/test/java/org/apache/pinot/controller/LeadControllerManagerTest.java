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
package org.apache.pinot.controller;

import org.apache.pinot.common.utils.helix.LeadControllerUtils;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.CommonConstants.Helix.NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class LeadControllerManagerTest {

  @Test
  public void testLeadControllerManager() {
    LeadControllerManager leadControllerManager = new LeadControllerManager();
    PinotHelixResourceManager pinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    leadControllerManager.registerResourceManager(pinotHelixResourceManager);
    String tableName = "testTable";
    int expectedPartitionIndex =
        LeadControllerUtils.getPartitionIdForTable(tableName, NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE);
    String partitionName = LeadControllerUtils.generatePartitionName(expectedPartitionIndex);

    // When there's no resource config change nor helix controller change, leadControllerManager should return false.
    Assert.assertFalse(leadControllerManager.isLeaderForTable(tableName));

    when(pinotHelixResourceManager.isLeadControllerResourceEnabled()).thenReturn(true);
    leadControllerManager.onResourceConfigChange();

    // Even resource config is enabled, leadControllerManager should return false because no index is cached yet.
    Assert.assertFalse(leadControllerManager.isLeaderForTable(tableName));
    Assert.assertTrue(leadControllerManager.isLeadControllerResourceEnabled());

    // After the target partition index is cached, leadControllerManager should return true.
    leadControllerManager.addPartitionLeader(partitionName);
    Assert.assertTrue(leadControllerManager.isLeaderForTable(tableName));

    // When the target partition index is removed, leadControllerManager should return false.
    leadControllerManager.removePartitionLeader(partitionName);
    Assert.assertFalse(leadControllerManager.isLeaderForTable(tableName));

    // When resource config is set to false, the cache should be disabled, even if the target partition index is in the cache.
    // The leader depends on whether the current controller is helix leader.
    when(pinotHelixResourceManager.isLeadControllerResourceEnabled()).thenReturn(false);
    leadControllerManager.onResourceConfigChange();

    Assert.assertFalse(leadControllerManager.isLeadControllerResourceEnabled());
    Assert.assertFalse(leadControllerManager.isLeaderForTable(tableName));
    leadControllerManager.addPartitionLeader(partitionName);
    Assert.assertFalse(leadControllerManager.isLeaderForTable(tableName));

    // When the current controller becomes helix leader and resource is disabled, leadControllerManager should return false.
    when(pinotHelixResourceManager.isHelixLeader()).thenReturn(true);
    leadControllerManager.onHelixControllerChange();
    Assert.assertTrue(leadControllerManager.isLeaderForTable(tableName));
  }
}
