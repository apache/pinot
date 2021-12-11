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
package org.apache.pinot.controller.helix;

import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ClusterConstraints;
import org.apache.helix.model.ConstraintItem;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.pinot.common.utils.helix.LeadControllerUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.ControllerStarter;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class PinotControllerModeStatelessTest extends ControllerTest {
  private static final long TIMEOUT_IN_MS = 10_000L;

  @BeforeClass
  public void setUp() {
    startZk();
  }

  @Test
  public void testHelixOnlyController()
      throws Exception {
    // Start a Helix-only controller
    Map<String, Object> properties = getDefaultControllerConfiguration();
    properties.put(ControllerConf.CONTROLLER_MODE, ControllerConf.ControllerMode.HELIX_ONLY);

    startController(properties);
    TestUtils.waitForCondition(aVoid -> _helixManager.isConnected(), TIMEOUT_IN_MS,
        "Failed to start the Helix-only controller");
    checkHelixConstraints(_helixAdmin);
    stopController();
  }

  @Test
  public void testDualModeController()
      throws Exception {
    // Start the first dual-mode controller
    Map<String, Object> properties = getDefaultControllerConfiguration();
    properties.put(ControllerConf.CONTROLLER_MODE, ControllerConf.ControllerMode.DUAL);

    startController(properties);

    // check the throttling constraint
    checkHelixConstraints(_helixAdmin);

    // Disable delay rebalance feature
    IdealState idealState = _helixResourceManager.getTableIdealState(Helix.LEAD_CONTROLLER_RESOURCE_NAME);
    idealState.setDelayRebalanceEnabled(false);
    idealState.setMinActiveReplicas(1);
    _helixAdmin.updateIdealState(getHelixClusterName(), Helix.LEAD_CONTROLLER_RESOURCE_NAME, idealState);

    TestUtils.waitForCondition(aVoid -> _helixManager.isConnected(), TIMEOUT_IN_MS,
        "Failed to start the first dual-mode controller");
    // The first controller should be the MASTER for all partitions
    checkInstanceState(_helixAdmin);

    final LeadControllerManager firstLeadControllerManager = _controllerStarter.getLeadControllerManager();
    String firstTableName = "firstTableName";
    String secondTableName = "secondTableName";

    Assert.assertNotEquals(LeadControllerUtils.getPartitionIdForTable(firstTableName),
        LeadControllerUtils.getPartitionIdForTable(secondTableName));

    // The first controller should be the leader for both tables, which is the Helix leader, before the resource is
    // enabled.
    Assert.assertTrue(firstLeadControllerManager.isLeaderForTable(firstTableName));
    Assert.assertTrue(firstLeadControllerManager.isLeaderForTable(secondTableName));

    enableResourceConfigForLeadControllerResource(true);
    TestUtils.waitForCondition(aVoid -> {
      try {
        return LeadControllerUtils.isLeadControllerResourceEnabled(_helixManager);
      } catch (Exception e) {
        Assert.fail("Exception when checking whether lead controller resource is enabled or not.");
        return false;
      }
    }, TIMEOUT_IN_MS, "Failed to mark lead controller resource as enabled");
    // The first controller should still be the leader for both tables, which is the partition leader, after the
    // resource is enabled.
    Assert.assertTrue(firstLeadControllerManager.isLeaderForTable(firstTableName));
    Assert.assertTrue(firstLeadControllerManager.isLeaderForTable(secondTableName));

    // Start the second dual-mode controller
    properties = getDefaultControllerConfiguration();
    properties.put(ControllerConf.CONTROLLER_MODE, ControllerConf.ControllerMode.DUAL);
    ControllerStarter secondDualModeController = new ControllerStarter();
    secondDualModeController.init(new PinotConfiguration(properties));
    secondDualModeController.start();
    TestUtils
        .waitForCondition(aVoid -> secondDualModeController.getHelixResourceManager().getHelixZkManager().isConnected(),
            TIMEOUT_IN_MS, "Failed to start the second dual-mode controller");
    // There should still be only one MASTER instance for each partition
    checkInstanceState(_helixAdmin);

    LeadControllerManager secondLeadControllerManager = secondDualModeController.getLeadControllerManager();
    Assert.assertTrue(LeadControllerUtils.isLeadControllerResourceEnabled(_helixManager));

    // Either one of the controllers is the only leader for a given table name.
    TestUtils.waitForCondition(aVoid -> {
      boolean result;
      result = firstLeadControllerManager.isLeaderForTable(firstTableName) ^ secondLeadControllerManager
          .isLeaderForTable(firstTableName);
      result &= firstLeadControllerManager.isLeaderForTable(secondTableName) ^ secondLeadControllerManager
          .isLeaderForTable(secondTableName);
      return result;
    }, TIMEOUT_IN_MS, "Either one of the controllers is the only leader for a given table");

    // Stop the second controller, and there should still be only one MASTER instance for each partition
    secondDualModeController.stop();
    checkInstanceState(_helixAdmin);

    // Stop the first controller
    stopController();

    // Both controllers are stopped and no one should be the partition leader for tables.
    TestUtils.waitForCondition(aVoid -> {
      boolean result;
      result = !firstLeadControllerManager.isLeaderForTable(firstTableName);
      result &= !firstLeadControllerManager.isLeaderForTable(secondTableName);
      result &= !secondLeadControllerManager.isLeaderForTable(firstTableName);
      result &= !secondLeadControllerManager.isLeaderForTable(secondTableName);
      return result;
    }, TIMEOUT_IN_MS, "No one should be the partition leader for tables");

    ZkClient zkClient = new ZkClient(getZkUrl());
    TestUtils
        .waitForCondition(aVoid -> !zkClient.exists("/" + getHelixClusterName() + "/CONTROLLER/LEADER"), TIMEOUT_IN_MS,
            "No cluster leader should be shown in Helix cluster");
    zkClient.close();

    properties = getDefaultControllerConfiguration();
    properties.put(ControllerConf.CONTROLLER_MODE, ControllerConf.ControllerMode.DUAL);

    ControllerStarter thirdDualModeController = new ControllerStarter();
    thirdDualModeController.init(new PinotConfiguration(properties));
    thirdDualModeController.start();
    PinotHelixResourceManager pinotHelixResourceManager = thirdDualModeController.getHelixResourceManager();
    _helixManager = pinotHelixResourceManager.getHelixZkManager();
    _helixAdmin = _helixManager.getClusterManagmentTool();
    TestUtils
        .waitForCondition(aVoid -> thirdDualModeController.getHelixResourceManager().getHelixZkManager().isConnected(),
            TIMEOUT_IN_MS, "Failed to start the 3rd dual-mode controller");
    // There should still be only one MASTER instance for each partition
    checkInstanceState(_helixAdmin);

    enableResourceConfigForLeadControllerResource(false);
    final LeadControllerManager thirdLeadControllerManager = thirdDualModeController.getLeadControllerManager();

    TestUtils.waitForCondition(aVoid -> {
      try {
        return !LeadControllerUtils.isLeadControllerResourceEnabled(_helixManager);
      } catch (Exception e) {
        // Exception caught.
        Assert.fail("Exception when checking whether lead controller resource is enabled or not.");
        return false;
      }
    }, TIMEOUT_IN_MS, "Lead controller resource should be disabled.");

    // After disabling lead controller resource, helix leader should be the leader for both tables.
    TestUtils.waitForCondition(
        aVoid -> thirdLeadControllerManager.isLeaderForTable(firstTableName) && thirdLeadControllerManager
            .isLeaderForTable(secondTableName), TIMEOUT_IN_MS,
        "The 3rd controller should be the leader for both tables, which is the Helix leader.");

    // Stop the 3rd controller
    thirdDualModeController.stop();
  }

  @Test
  public void testPinotOnlyController()
      throws Exception {
    Map<String, Object> properties = getDefaultControllerConfiguration();
    properties.put(ControllerConf.CONTROLLER_MODE, ControllerConf.ControllerMode.PINOT_ONLY);

    ControllerStarter firstPinotOnlyController = new ControllerStarter();
    firstPinotOnlyController.init(new PinotConfiguration(properties));

    // Starting Pinot-only controller without Helix controller should fail
    try {
      firstPinotOnlyController.start();
      Assert.fail("Starting Pinot-only controller without Helix controller should fail");
    } catch (Exception e) {
      // Expected
    }

    // Start a Helix-only controller
    properties = getDefaultControllerConfiguration();
    properties.put(ControllerConf.CONTROLLER_MODE, ControllerConf.ControllerMode.HELIX_ONLY);

    ControllerStarter helixOnlyController = new ControllerStarter();
    helixOnlyController.init(new PinotConfiguration(properties));
    helixOnlyController.start();
    HelixManager helixControllerManager = helixOnlyController.getHelixControllerManager();
    HelixAdmin helixAdmin = helixControllerManager.getClusterManagmentTool();
    TestUtils.waitForCondition(aVoid -> helixControllerManager.isConnected(), TIMEOUT_IN_MS,
        "Failed to start the Helix-only controller");

    // Start the first Pinot-only controller
    firstPinotOnlyController.start();
    PinotHelixResourceManager helixResourceManager = firstPinotOnlyController.getHelixResourceManager();
    TestUtils.waitForCondition(aVoid -> helixResourceManager.getHelixZkManager().isConnected(), TIMEOUT_IN_MS,
        "Failed to start the first Pinot-only controller");
    // The first Pinot-only controller should be the MASTER for all partitions
    checkInstanceState(helixAdmin);

    // Start the second Pinot-only controller
    properties = getDefaultControllerConfiguration();
    properties.put(ControllerConf.CONTROLLER_MODE, ControllerConf.ControllerMode.PINOT_ONLY);

    ControllerStarter secondPinotOnlyController = new ControllerStarter();
    secondPinotOnlyController.init(new PinotConfiguration(properties));
    secondPinotOnlyController.start();
    TestUtils.waitForCondition(
        aVoid -> secondPinotOnlyController.getHelixResourceManager().getHelixZkManager().isConnected(), TIMEOUT_IN_MS,
        "Failed to start the second Pinot-only controller");
    // There should still be only one MASTER instance for each partition
    checkInstanceState(helixAdmin);
    // Stop the second Pinot-only controller, and there should still be only one MASTER instance for each partition
    secondPinotOnlyController.stop();
    checkInstanceState(helixAdmin);

    // Stop the first Pinot-only controller, and there should be no partition in the external view
    firstPinotOnlyController.stop();
    TestUtils.waitForCondition(aVoid -> {
      ExternalView leadControllerResourceExternalView =
          helixAdmin.getResourceExternalView(getHelixClusterName(), Helix.LEAD_CONTROLLER_RESOURCE_NAME);
      return leadControllerResourceExternalView.getPartitionSet().isEmpty();
    }, TIMEOUT_IN_MS, "Without live instance, there should be no partition in the external view");

    // Stop the Helix-only controller
    helixOnlyController.stop();
  }

  private void checkHelixConstraints(HelixAdmin helixAdmin) {
    ClusterConstraints constraints =
        helixAdmin.getConstraints(getHelixClusterName(), ClusterConstraints.ConstraintType.MESSAGE_CONSTRAINT);
    ConstraintItem item = constraints.getConstraintItem("MaxStateTransitionsPerInstance");
    Assert.assertEquals(item.getAttributeValue(ClusterConstraints.ConstraintAttribute.INSTANCE), ".*");
    Assert.assertEquals(item.getAttributeValue(ClusterConstraints.ConstraintAttribute.MESSAGE_TYPE),
        Message.MessageType.STATE_TRANSITION.name());
    Assert.assertEquals(item.getConstraintValue(), CommonConstants.Helix.DEFAULT_HELIX_INSTANCE_MAX_STATE_TRANSITIONS);
  }

  private void checkInstanceState(HelixAdmin helixAdmin) {
    String expectedInstanceState = "MASTER";
    TestUtils.waitForCondition(aVoid -> {
      ExternalView leadControllerResourceExternalView =
          helixAdmin.getResourceExternalView(getHelixClusterName(), Helix.LEAD_CONTROLLER_RESOURCE_NAME);
      Set<String> partitionSet = leadControllerResourceExternalView.getPartitionSet();
      if (partitionSet.size() != Helix.NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE) {
        return false;
      }
      for (String partition : partitionSet) {
        Map<String, String> stateMap = leadControllerResourceExternalView.getStateMap(partition);
        if (stateMap.size() != 1 || !stateMap.values().contains(expectedInstanceState)) {
          return false;
        }
      }
      return true;
    }, TIMEOUT_IN_MS, "Failed to pick only one instance as: " + expectedInstanceState);
  }

  @AfterMethod
  public void cleanUpCluster() {
    ZkClient zkClient = new ZkClient(getZkUrl());
    if (zkClient.exists("/" + getHelixClusterName())) {
      zkClient.deleteRecursive("/" + getHelixClusterName());
    }
    zkClient.close();
  }

  @AfterClass
  public void tearDown() {
    stopZk();
  }
}
