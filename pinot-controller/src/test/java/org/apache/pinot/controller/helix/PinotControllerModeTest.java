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

import org.apache.helix.HelixManager;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.ControllerLeadershipManager;
import org.apache.pinot.controller.ControllerStarter;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceSegmentStrategyFactory;
import org.apache.pinot.controller.helix.core.util.HelixSetupUtils;
import org.apache.pinot.util.TestUtils;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.Assert;
import org.testng.IObjectFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;


@PowerMockIgnore({"org.mockito.*", "javax.management.*", "javax.security.*", "javax.net.ssl.*", "org.glassfish.jersey.*", "org.glassfish.*", "org.apache.log4j.*"})
@PrepareForTest({ControllerLeadershipManager.class, PinotLLCRealtimeSegmentManager.class, RebalanceSegmentStrategyFactory.class})
public class PinotControllerModeTest extends ControllerTest {
  private static long TIMEOUT_IN_MS = 10_000L;
  private ControllerConf config;
  private int controllerPortOffset;

  @Mock
  private ControllerLeadershipManager _controllerLeadershipManager;

  @Mock
  private PinotLLCRealtimeSegmentManager _pinotLLCRealtimeSegmentManager;

  @ObjectFactory
  public IObjectFactory getObjectFactory() {
    return new org.powermock.modules.testng.PowerMockObjectFactory();
  }

  @BeforeClass
  public void setUp() {
    startZk();
    config = getDefaultControllerConfiguration();
    controllerPortOffset = 0;

    PowerMockito.mockStatic(ControllerLeadershipManager.class);
    when(ControllerLeadershipManager.getInstance()).thenReturn(_controllerLeadershipManager);
    Assert.assertEquals(ControllerLeadershipManager.getInstance(), _controllerLeadershipManager);
  }

  @Test
  public void testHelixOnlyController()
      throws Exception {
    config.setControllerMode(HelixSetupUtils.ControllerMode.HELIX_ONLY.name());
    config.setControllerPort(Integer.toString(Integer.parseInt(config.getControllerPort()) + controllerPortOffset++));

    startController(config);
    TestUtils.waitForCondition(aVoid -> _helixManager.isConnected(), TIMEOUT_IN_MS,
        "Failed to start " + config.getControllerMode() + " controller in " + TIMEOUT_IN_MS + "ms.");

    Assert.assertEquals(_helixResourceManager.getControllerMode(), HelixSetupUtils.ControllerMode.HELIX_ONLY);
    Assert.assertTrue(_helixManager.isLeader());

    stopController();
    _controllerStarter = null;
  }

  @Test
  public void testDualModeController()
      throws Exception {
    config.setControllerMode(HelixSetupUtils.ControllerMode.DUAL.name());
    config.setControllerPort(Integer.toString(Integer.parseInt(config.getControllerPort()) + controllerPortOffset++));

    startController(config);
    TestUtils.waitForCondition(aVoid -> _helixManager.isConnected(), TIMEOUT_IN_MS,
        "Failed to start " + config.getControllerMode() + " controller in " + TIMEOUT_IN_MS + "ms.");
    Assert.assertEquals(_helixResourceManager.getControllerMode(), HelixSetupUtils.ControllerMode.DUAL);
    Assert.assertTrue(_helixManager.isLeader());

    stopController();
    _controllerStarter = null;
  }

  @Test
  public void testPinotOnlyController()
      throws Exception {
    PowerMockito.mockStatic(PinotLLCRealtimeSegmentManager.class);
    when(PinotLLCRealtimeSegmentManager.getInstance()).thenReturn(_pinotLLCRealtimeSegmentManager);
    Assert.assertEquals(PinotLLCRealtimeSegmentManager.getInstance(), _pinotLLCRealtimeSegmentManager);

    PowerMockito.mockStatic(RebalanceSegmentStrategyFactory.class);

    config.setControllerMode(HelixSetupUtils.ControllerMode.PINOT_ONLY.name());
    config.setControllerPort(Integer.toString(Integer.parseInt(config.getControllerPort()) + controllerPortOffset++));

    // Starting pinot only controller before starting helix controller should fail.
    try {
      startController(config);
      Assert.fail("Starting pinot only controller should fail!");
    } catch (RuntimeException e) {
      _controllerStarter = null;
    }

    // Starting a helix controller.
    ControllerConf config2 = getDefaultControllerConfiguration();
    config2.setHelixClusterName(getHelixClusterName());
    config2.setControllerMode(HelixSetupUtils.ControllerMode.HELIX_ONLY.name());
    config2.setControllerPort(Integer.toString(Integer.parseInt(config.getControllerPort()) + controllerPortOffset++));
    ControllerStarter helixControllerStarter = new ControllerStarter(config2);
    helixControllerStarter.start();
    HelixManager helixManager = helixControllerStarter.getHelixResourceManager().getHelixZkManager();
    TestUtils.waitForCondition(aVoid -> helixManager.isConnected(), TIMEOUT_IN_MS,
        "Failed to start " + config2.getControllerMode() + " controller in " + TIMEOUT_IN_MS + "ms.");
    Assert.assertTrue(helixManager.isLeader());

    // Starting a pinot only controller.
    startController(config, false);
    TestUtils.waitForCondition(aVoid -> _helixManager.isConnected(), TIMEOUT_IN_MS,
        "Failed to start " + config.getControllerMode() + " controller in " + TIMEOUT_IN_MS + "ms.");
    Assert.assertEquals(_helixResourceManager.getControllerMode(), HelixSetupUtils.ControllerMode.PINOT_ONLY);

    stopController();
    _controllerStarter = null;
    helixControllerStarter.stop();
  }

  @AfterClass
  public void tearDown() {
    stopZk();
  }
}
