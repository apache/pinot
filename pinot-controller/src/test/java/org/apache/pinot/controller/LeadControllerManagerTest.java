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

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.ResourceConfig;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.PinotMetricUtils;
import org.apache.pinot.common.utils.helix.LeadControllerUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class LeadControllerManagerTest {
  private static final String CONTROLLER_HOST = "localhost";
  private static final int CONTROLLER_PORT = 18998;

  private HelixManager _helixManager;
  private ControllerMetrics _controllerMetrics;
  private LiveInstance _liveInstance;
  private ResourceConfig _resourceConfig;

  @BeforeMethod
  public void setup() {
    _controllerMetrics = new ControllerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    _helixManager = mock(HelixManager.class);
    HelixDataAccessor helixDataAccessor = mock(HelixDataAccessor.class);
    when(_helixManager.getHelixDataAccessor()).thenReturn(helixDataAccessor);

    PropertyKey.Builder keyBuilder = mock(PropertyKey.Builder.class);
    when(helixDataAccessor.keyBuilder()).thenReturn(keyBuilder);

    PropertyKey controllerLeader = mock(PropertyKey.class);
    when(keyBuilder.controllerLeader()).thenReturn(controllerLeader);
    _liveInstance = mock(LiveInstance.class);
    when(helixDataAccessor.getProperty(controllerLeader)).thenReturn(_liveInstance);

    String instanceId = LeadControllerUtils.generateParticipantInstanceId(CONTROLLER_HOST, CONTROLLER_PORT);
    when(_helixManager.getInstanceName()).thenReturn(instanceId);

    ConfigAccessor configAccessor = mock(ConfigAccessor.class);
    when(_helixManager.getConfigAccessor()).thenReturn(configAccessor);
    _resourceConfig = mock(ResourceConfig.class);
    when(configAccessor.getResourceConfig(any(), anyString())).thenReturn(_resourceConfig);
  }

  @Test
  public void testLeadControllerManager() {
    LeadControllerManager leadControllerManager = new LeadControllerManager(_helixManager, _controllerMetrics);
    String tableName = "leadControllerTestTable";
    int expectedPartitionIndex = LeadControllerUtils.getPartitionIdForTable(tableName);
    String partitionName = LeadControllerUtils.generatePartitionName(expectedPartitionIndex);

    becomeHelixLeader(false);
    leadControllerManager.onHelixControllerChange();

    // When there's no resource config change nor helix controller change, leadControllerManager should return false.
    Assert.assertFalse(leadControllerManager.isLeaderForTable(tableName));

    enableResourceConfig(true);
    leadControllerManager.onResourceConfigChange();

    // Even resource config is enabled, leadControllerManager should return false because no index is cached yet.
    Assert.assertFalse(leadControllerManager.isLeaderForTable(tableName));
    Assert.assertTrue(LeadControllerUtils.isLeadControllerResourceEnabled(_helixManager));

    // After the target partition index is cached, leadControllerManager should return true.
    leadControllerManager.addPartitionLeader(partitionName);
    Assert.assertTrue(leadControllerManager.isLeaderForTable(tableName));

    // When the target partition index is removed, leadControllerManager should return false.
    leadControllerManager.removePartitionLeader(partitionName);
    Assert.assertFalse(leadControllerManager.isLeaderForTable(tableName));

    // When resource config is set to false, the cache should be disabled, even if the target partition index is in the cache.
    // The leader depends on whether the current controller is helix leader.
    enableResourceConfig(false);
    leadControllerManager.onResourceConfigChange();

    Assert.assertFalse(LeadControllerUtils.isLeadControllerResourceEnabled(_helixManager));
    Assert.assertFalse(leadControllerManager.isLeaderForTable(tableName));
    leadControllerManager.addPartitionLeader(partitionName);
    Assert.assertFalse(leadControllerManager.isLeaderForTable(tableName));

    // When the current controller becomes helix leader and resource is disabled, leadControllerManager should return false.
    becomeHelixLeader(true);
    leadControllerManager.onHelixControllerChange();
    Assert.assertTrue(leadControllerManager.isLeaderForTable(tableName));
  }

  private void becomeHelixLeader(boolean becomeHelixLeader) {
    if (becomeHelixLeader) {
      when(_liveInstance.getInstanceName()).thenReturn(CONTROLLER_HOST + "_" + CONTROLLER_PORT);
    }
  }

  private void enableResourceConfig(boolean enable) {
    when(_resourceConfig.getSimpleConfig(anyString())).thenReturn(Boolean.toString(enable));
  }
}
