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
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.ControllerStarter;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.CommonConstants.Helix.LEAD_CONTROLLER_RESOURCE_NAME;
import static org.apache.pinot.common.utils.CommonConstants.Helix.NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE;


public class PinotControllerModeTest extends ControllerTest {
  private static long TIMEOUT_IN_MS = 10_000L;

  @BeforeClass
  public void setUp() {
    startZk();
  }

  @Test
  public void testHelixOnlyController() {
    // Start a Helix-only controller
    ControllerConf helixOnlyControllerConfig = getDefaultControllerConfiguration();
    helixOnlyControllerConfig.setControllerMode(ControllerConf.ControllerMode.HELIX_ONLY);
    startController(helixOnlyControllerConfig);
    TestUtils.waitForCondition(aVoid -> _helixManager.isConnected(), TIMEOUT_IN_MS,
        "Failed to start the Helix-only controller");

    stopController();
  }

  @Test(enabled = false)
  public void testDualModeController() {
    // Start the first dual-mode controller
    ControllerConf firstDualModeControllerConfig = getDefaultControllerConfiguration();
    firstDualModeControllerConfig.setControllerMode(ControllerConf.ControllerMode.DUAL);
    startController(firstDualModeControllerConfig);
    TestUtils.waitForCondition(aVoid -> _helixManager.isConnected(), TIMEOUT_IN_MS,
        "Failed to start the first dual-mode controller");

    // There should be no partition in the external view because the resource is disabled
    TestUtils.waitForCondition(aVoid -> {
      ExternalView leadControllerResourceExternalView =
          _helixAdmin.getResourceExternalView(getHelixClusterName(), LEAD_CONTROLLER_RESOURCE_NAME);
      return leadControllerResourceExternalView.getPartitionSet().isEmpty();
    }, TIMEOUT_IN_MS, "There should be no partition in the disabled resource's external view");

    // Enable the lead controller resource, and the first controller should be the MASTER for all partitions
    _helixAdmin.enableResource(getHelixClusterName(), LEAD_CONTROLLER_RESOURCE_NAME, true);
    checkInstanceState(_helixAdmin, "MASTER");

    // Start the second dual-mode controller
    ControllerConf secondDualModeControllerConfig = getDefaultControllerConfiguration();
    secondDualModeControllerConfig.setControllerMode(ControllerConf.ControllerMode.DUAL);
    secondDualModeControllerConfig.setControllerPort(Integer.toString(DEFAULT_CONTROLLER_PORT + 1));
    ControllerStarter secondDualModeController = getControllerStarter(secondDualModeControllerConfig);
    secondDualModeController.start();
    TestUtils
        .waitForCondition(aVoid -> secondDualModeController.getHelixResourceManager().getHelixZkManager().isConnected(),
            TIMEOUT_IN_MS, "Failed to start the second dual-mode controller");

    // There should still be only one MASTER instance for each partition
    checkInstanceState(_helixAdmin, "MASTER");

    // Disable the lead controller resource, and there should be only one OFFLINE instance for each partition
    _helixAdmin.enableResource(getHelixClusterName(), LEAD_CONTROLLER_RESOURCE_NAME, false);
    checkInstanceState(_helixAdmin, "OFFLINE");

    // Re-enable the lead controller resource, and there should be only one MASTER instance for each partition
    _helixAdmin.enableResource(getHelixClusterName(), LEAD_CONTROLLER_RESOURCE_NAME, true);
    checkInstanceState(_helixAdmin, "MASTER");

    // Stop the second controller, and there should still be only one MASTER instance for each partition
    secondDualModeController.stop();
    checkInstanceState(_helixAdmin, "MASTER");

    // Stop the first controller
    stopController();
  }

  // TODO: enable it after removing HelixControllerLeadershipManager which requires both CONTROLLER and PARTICIPANT
  //       HelixManager
  @Test
  public void testPinotOnlyController() {
    ControllerConf firstPinotOnlyControllerConfig = getDefaultControllerConfiguration();
    firstPinotOnlyControllerConfig.setControllerMode(ControllerConf.ControllerMode.PINOT_ONLY);
    ControllerStarter firstPinotOnlyController = getControllerStarter(firstPinotOnlyControllerConfig);

    // Starting Pinot-only controller without Helix controller should fail
    try {
      firstPinotOnlyController.start();
      Assert.fail("Starting Pinot-only controller without Helix controller should fail");
    } catch (Exception e) {
      // Expected
    }

    // Start a Helix-only controller
    ControllerConf helixOnlyControllerConfig = getDefaultControllerConfiguration();
    helixOnlyControllerConfig.setControllerMode(ControllerConf.ControllerMode.HELIX_ONLY);
    helixOnlyControllerConfig.setControllerPort(Integer.toString(DEFAULT_CONTROLLER_PORT + 1));
    ControllerStarter helixOnlyController = new ControllerStarter(helixOnlyControllerConfig);
    helixOnlyController.start();
    HelixManager helixControllerManager = helixOnlyController.getHelixControllerManager();
    HelixAdmin helixAdmin = helixControllerManager.getClusterManagmentTool();
    TestUtils.waitForCondition(aVoid -> helixControllerManager.isConnected(), TIMEOUT_IN_MS,
        "Failed to start the Helix-only controller");

    // Enabling the lead controller resource before setting up the Pinot cluster should fail
    try {
      helixAdmin.enableResource(getHelixClusterName(), LEAD_CONTROLLER_RESOURCE_NAME, true);
      Assert.fail("Enabling the lead controller resource before setting up the Pinot cluster should fail");
    } catch (Exception e) {
      // Expected
    }

    // Start the first Pinot-only controller
    firstPinotOnlyController.start();
    PinotHelixResourceManager helixResourceManager = firstPinotOnlyController.getHelixResourceManager();
    TestUtils.waitForCondition(aVoid -> helixResourceManager.getHelixZkManager().isConnected(), TIMEOUT_IN_MS,
        "Failed to start the first Pinot-only controller");

    // There should be no partition in the external view because the resource is disabled
    TestUtils.waitForCondition(aVoid -> {
      ExternalView leadControllerResourceExternalView =
          helixAdmin.getResourceExternalView(getHelixClusterName(), LEAD_CONTROLLER_RESOURCE_NAME);
      return leadControllerResourceExternalView.getPartitionSet().isEmpty();
    }, TIMEOUT_IN_MS, "There should be no partition in the disabled resource's external view");

    // Enable the lead controller resource, and the first Pinot-only controller should be the MASTER for all partitions
    helixAdmin.enableResource(getHelixClusterName(), LEAD_CONTROLLER_RESOURCE_NAME, true);
    checkInstanceState(helixAdmin, "MASTER");

    // Start the second Pinot-only controller
    ControllerConf secondPinotOnlyControllerConfig = getDefaultControllerConfiguration();
    secondPinotOnlyControllerConfig.setControllerMode(ControllerConf.ControllerMode.PINOT_ONLY);
    secondPinotOnlyControllerConfig.setControllerPort(Integer.toString(DEFAULT_CONTROLLER_PORT + 2));
    ControllerStarter secondPinotOnlyController = getControllerStarter(secondPinotOnlyControllerConfig);
    secondPinotOnlyController.start();
    TestUtils.waitForCondition(
        aVoid -> secondPinotOnlyController.getHelixResourceManager().getHelixZkManager().isConnected(), TIMEOUT_IN_MS,
        "Failed to start the second Pinot-only controller");

    // There should still be only one MASTER instance for each partition
    checkInstanceState(helixAdmin, "MASTER");

    // Disable the lead controller resource, and there should be only one OFFLINE instance for each partition
    helixAdmin.enableResource(getHelixClusterName(), LEAD_CONTROLLER_RESOURCE_NAME, false);
    checkInstanceState(helixAdmin, "OFFLINE");

    // Re-enable the lead controller resource, and there should be only one MASTER instance for each partition
    helixAdmin.enableResource(getHelixClusterName(), LEAD_CONTROLLER_RESOURCE_NAME, true);
    checkInstanceState(helixAdmin, "MASTER");

    // Stop the second Pinot-only controller, and there should still be only one MASTER instance for each partition
    secondPinotOnlyController.stop();
    checkInstanceState(helixAdmin, "MASTER");

    // Stop the first Pinot-only controller, and there should be no partition in the external view
    firstPinotOnlyController.stop();
    TestUtils.waitForCondition(aVoid -> {
      ExternalView leadControllerResourceExternalView =
          helixAdmin.getResourceExternalView(getHelixClusterName(), LEAD_CONTROLLER_RESOURCE_NAME);
      return leadControllerResourceExternalView.getPartitionSet().isEmpty();
    }, TIMEOUT_IN_MS, "Without live instance, there should be no partition in the external view");

    // Stop the Helix-only controller
    helixOnlyController.stop();
  }

  private void checkInstanceState(HelixAdmin helixAdmin, String expectedInstanceState) {
    TestUtils.waitForCondition(aVoid -> {
      ExternalView leadControllerResourceExternalView =
          helixAdmin.getResourceExternalView(getHelixClusterName(), LEAD_CONTROLLER_RESOURCE_NAME);
      Set<String> partitionSet = leadControllerResourceExternalView.getPartitionSet();
      if (partitionSet.size() != NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE) {
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

  @AfterClass
  public void tearDown() {
    stopZk();
  }
}
