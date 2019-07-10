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

import java.util.Arrays;
import java.util.Map;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.ControllerStarter;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.CommonConstants.Helix.LEAD_CONTROLLER_RESOURCE_NAME;
import static org.apache.pinot.common.utils.CommonConstants.Helix.LEAD_CONTROLLER_RESOURCE_REPLICA_COUNT;


public class PinotControllerModeTest extends ControllerTest {
  private static long TIMEOUT_IN_MS = 10_000L;

  @BeforeClass
  public void setUp() {
    startZk();
  }

  @Test
  public void testHelixOnlyController() {
    ControllerConf helixOnlyControllerConfig = getDefaultControllerConfiguration();
    helixOnlyControllerConfig.setControllerMode(ControllerConf.ControllerMode.HELIX_ONLY);
    ControllerStarter helixOnlyController = getControllerStarter(helixOnlyControllerConfig);
    helixOnlyController.start();
    TestUtils.waitForCondition(aVoid -> helixOnlyController.getHelixControllerManager().isConnected(), TIMEOUT_IN_MS,
        "Failed to start the Helix-only controller");

    helixOnlyController.stop();
  }

  @Test
  public void testDualModeController() {
    // Helix cluster will be set up when starting the first controller.
    ControllerConf firstDualModeControllerConfig = getDefaultControllerConfiguration();
    firstDualModeControllerConfig.setControllerMode(ControllerConf.ControllerMode.DUAL);
    ControllerStarter firstDualModeController = getControllerStarter(firstDualModeControllerConfig);
    firstDualModeController.start();
    TestUtils
        .waitForCondition(aVoid -> firstDualModeController.getHelixControllerManager().isConnected(), TIMEOUT_IN_MS,
            "Failed to start the first dual-mode controller");

    // Starting a second dual-mode controller. Helix cluster has already been set up.
    ControllerConf secondDualModeControllerConfig = getDefaultControllerConfiguration();
    secondDualModeControllerConfig.setControllerMode(ControllerConf.ControllerMode.DUAL);
    secondDualModeControllerConfig.setControllerPort(Integer.toString(DEFAULT_CONTROLLER_PORT + 1));
    ControllerStarter secondDualModeController = getControllerStarter(secondDualModeControllerConfig);
    secondDualModeController.start();
    TestUtils
        .waitForCondition(aVoid -> secondDualModeController.getHelixResourceManager().getHelixZkManager().isConnected(),
            TIMEOUT_IN_MS, "Failed to start the second dual-mode controller");

    secondDualModeController.stop();
    firstDualModeController.stop();
  }

  // TODO: enable it after removing ControllerLeadershipManager which requires both CONTROLLER and PARTICIPANT
  //       HelixManager
  @Test(enabled = false)
  public void testPinotOnlyController() {
    ControllerConf firstPinotOnlyControllerConfig = getDefaultControllerConfiguration();
    firstPinotOnlyControllerConfig.setControllerMode(ControllerConf.ControllerMode.PINOT_ONLY);
    ControllerStarter firstPinotOnlyController = getControllerStarter(firstPinotOnlyControllerConfig);

    // Starting pinot only controller before starting helix controller should fail.
    try {
      firstPinotOnlyController.start();
      Assert.fail("Starting Pinot-only controller without Helix controller should fail");
    } catch (Exception e) {
      // Expected
    }

    // Starting a helix controller.
    ControllerConf helixOnlyControllerConfig = getDefaultControllerConfiguration();
    helixOnlyControllerConfig.setControllerMode(ControllerConf.ControllerMode.HELIX_ONLY);
    helixOnlyControllerConfig.setControllerPort(Integer.toString(DEFAULT_CONTROLLER_PORT + 1));
    ControllerStarter helixOnlyController = new ControllerStarter(helixOnlyControllerConfig);
    helixOnlyController.start();
    HelixManager helixControllerManager = helixOnlyController.getHelixControllerManager();
    HelixAdmin helixAdmin = helixControllerManager.getClusterManagmentTool();
    TestUtils.waitForCondition(aVoid -> helixControllerManager.isConnected(), TIMEOUT_IN_MS,
        "Failed to start the Helix-only controller");

    // Starting a pinot only controller.
    firstPinotOnlyController.start();
    PinotHelixResourceManager helixResourceManager = firstPinotOnlyController.getHelixResourceManager();
    TestUtils.waitForCondition(aVoid -> helixResourceManager.getHelixZkManager().isConnected(), TIMEOUT_IN_MS,
        "Failed to start the first Pinot-only controller");

    // Enable the lead controller resource.
    helixAdmin.enableResource(getHelixClusterName(), LEAD_CONTROLLER_RESOURCE_NAME, true);
    helixAdmin.rebalance(getHelixClusterName(), LEAD_CONTROLLER_RESOURCE_NAME, LEAD_CONTROLLER_RESOURCE_REPLICA_COUNT);
    TestUtils.waitForCondition(aVoid -> {
      ExternalView leadControllerResourceExternalView =
          helixAdmin.getResourceExternalView(getHelixClusterName(), LEAD_CONTROLLER_RESOURCE_NAME);
      for (String partition : leadControllerResourceExternalView.getPartitionSet()) {
        Map<String, String> stateMap = leadControllerResourceExternalView.getStateMap(partition);
        if (stateMap.size() != 1 || stateMap.values().contains("MASTER")) {
          return false;
        }
      }
      return true;
    }, TIMEOUT_IN_MS, "Failed to choose the only participant as MASTER");

    // Start a second Pinot only controller.
    ControllerConf secondPinotOnlyControllerConfig = getDefaultControllerConfiguration();
    secondPinotOnlyControllerConfig.setControllerMode(ControllerConf.ControllerMode.PINOT_ONLY);
    secondPinotOnlyControllerConfig.setControllerPort(Integer.toString(DEFAULT_CONTROLLER_PORT + 2));
    ControllerStarter secondPinotOnlyController = getControllerStarter(secondPinotOnlyControllerConfig);
    secondPinotOnlyController.start();
    TestUtils.waitForCondition(
        aVoid -> secondPinotOnlyController.getHelixResourceManager().getHelixZkManager().isConnected(), TIMEOUT_IN_MS,
        "Failed to start the second Pinot-only controller");
    TestUtils.waitForCondition(aVoid -> {
      ExternalView leadControllerResourceExternalView =
          helixAdmin.getResourceExternalView(getHelixClusterName(), LEAD_CONTROLLER_RESOURCE_NAME);
      for (String partition : leadControllerResourceExternalView.getPartitionSet()) {
        Map<String, String> stateMap = leadControllerResourceExternalView.getStateMap(partition);
        if (stateMap.size() != 2 || stateMap.values().containsAll(Arrays.asList("MASTER", "SLAVE"))) {
          return false;
        }
      }
      return true;
    }, TIMEOUT_IN_MS, "Failed to choose two participants as MASTER and SLAVE");

    // Disable lead controller resource, all the participants are in offline state (from slave state).
    helixAdmin.enableResource(getHelixClusterName(), LEAD_CONTROLLER_RESOURCE_NAME, false);
    TestUtils.waitForCondition(aVoid -> {
      ExternalView leadControllerResourceExternalView =
          helixAdmin.getResourceExternalView(getHelixClusterName(), LEAD_CONTROLLER_RESOURCE_NAME);
      for (String partition : leadControllerResourceExternalView.getPartitionSet()) {
        Map<String, String> stateMap = leadControllerResourceExternalView.getStateMap(partition);
        if (stateMap.size() != 2) {
          return false;
        }
        for (String value : stateMap.values()) {
          if (!value.equals("OFFLINE")) {
            return false;
          }
        }
      }
      return true;
    }, TIMEOUT_IN_MS, "Failed to turn all the participants OFFLINE");

    // Re-enable lead controller resource, all the participants are in healthy state (either MASTER or SLAVE).
    helixAdmin.enableResource(getHelixClusterName(), LEAD_CONTROLLER_RESOURCE_NAME, true);
    TestUtils.waitForCondition(aVoid -> {
      ExternalView leadControllerResourceExternalView =
          helixAdmin.getResourceExternalView(getHelixClusterName(), LEAD_CONTROLLER_RESOURCE_NAME);
      for (String partition : leadControllerResourceExternalView.getPartitionSet()) {
        Map<String, String> stateMap = leadControllerResourceExternalView.getStateMap(partition);
        if (stateMap.size() != 2 || stateMap.values().containsAll(Arrays.asList("MASTER", "SLAVE"))) {
          return false;
        }
      }
      return true;
    }, TIMEOUT_IN_MS, "Failed to choose two participants as MASTER and SLAVE from OFFLINE");

    // Shutdown one controller, it will be removed from external view of lead controller resource.
    secondPinotOnlyController.stop();
    TestUtils.waitForCondition(aVoid -> {
      ExternalView leadControllerResourceExternalView =
          helixAdmin.getResourceExternalView(getHelixClusterName(), LEAD_CONTROLLER_RESOURCE_NAME);
      for (String partition : leadControllerResourceExternalView.getPartitionSet()) {
        Map<String, String> stateMap = leadControllerResourceExternalView.getStateMap(partition);
        if (stateMap.size() != 1 || stateMap.values().contains("MASTER")) {
          return false;
        }
      }
      return true;
    }, TIMEOUT_IN_MS, "Failed to drop the first disconnected participant");

    // Shutdown the only one controller left, the partition map should be empty.
    firstPinotOnlyController.stop();
    TestUtils.waitForCondition(aVoid -> {
      ExternalView leadControllerResourceExternalView =
          helixAdmin.getResourceExternalView(getHelixClusterName(), LEAD_CONTROLLER_RESOURCE_NAME);
      for (String partition : leadControllerResourceExternalView.getPartitionSet()) {
        Map<String, String> stateMap = leadControllerResourceExternalView.getStateMap(partition);
        if (!stateMap.isEmpty()) {
          return false;
        }
      }
      return true;
    }, TIMEOUT_IN_MS, "Failed to drop the second disconnected participant");

    helixOnlyController.stop();
  }

  @AfterClass
  public void tearDown() {
    stopZk();
  }
}
