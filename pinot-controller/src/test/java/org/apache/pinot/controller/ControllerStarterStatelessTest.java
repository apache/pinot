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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.utils.NetUtils;
import org.testng.annotations.Test;

import static org.apache.pinot.controller.ControllerConf.CONTROLLER_HOST;
import static org.apache.pinot.controller.ControllerConf.CONTROLLER_PORT;
import static org.apache.pinot.spi.utils.CommonConstants.Controller.CONFIG_OF_INSTANCE_ID;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.CONTROLLER_INSTANCE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;


@Test(groups = "stateless")
public class ControllerStarterStatelessTest extends ControllerTest {
  private final Map<String, Object> _configOverride = new HashMap<>();

  @Override
  protected void overrideControllerConf(Map<String, Object> properties) {
    properties.putAll(_configOverride);
  }

  @Test
  public void testHostnamePortOverride()
      throws Exception {
    int controllerPort = NetUtils.findOpenPort(_nextControllerPort);
    _configOverride.clear();
    _configOverride.put(CONFIG_OF_INSTANCE_ID, "Controller_myInstance");
    _configOverride.put(CONTROLLER_HOST, "myHost");
    _configOverride.put(CONTROLLER_PORT, controllerPort);

    startZk();
    startController();

    String instanceId = _controllerStarter.getInstanceId();
    assertEquals(instanceId, "Controller_myInstance");
    InstanceConfig instanceConfig = HelixHelper.getInstanceConfig(_helixManager, instanceId);
    assertEquals(instanceConfig.getInstanceName(), instanceId);
    assertEquals(instanceConfig.getHostName(), "myHost");
    assertEquals(instanceConfig.getPort(), Integer.toString(controllerPort));
    assertEquals(instanceConfig.getTags(), Set.of(CONTROLLER_INSTANCE));

    stopController();
    stopZk();
  }

  @Test
  public void testInvalidInstanceId()
      throws Exception {
    int controllerPort = NetUtils.findOpenPort(_nextControllerPort);
    _configOverride.clear();
    _configOverride.put(CONFIG_OF_INSTANCE_ID, "myInstance");
    _configOverride.put(CONTROLLER_HOST, "myHost");
    _configOverride.put(CONTROLLER_PORT, controllerPort);

    startZk();
    try {
      startController();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    } finally {
      stopZk();
    }
  }

  @Test
  public void testDefaultInstanceId()
      throws Exception {
    int controllerPort = NetUtils.findOpenPort(_nextControllerPort);
    _configOverride.clear();
    _configOverride.put(CONTROLLER_HOST, "myHost");
    _configOverride.put(CONTROLLER_PORT, controllerPort);

    startZk();
    startController();

    String instanceId = _controllerStarter.getInstanceId();
    assertEquals(instanceId, "Controller_myHost_" + controllerPort);
    InstanceConfig instanceConfig = HelixHelper.getInstanceConfig(_helixManager, instanceId);
    assertEquals(instanceConfig.getInstanceName(), instanceId);
    assertEquals(instanceConfig.getHostName(), "myHost");
    assertEquals(instanceConfig.getPort(), Integer.toString(controllerPort));
    assertEquals(instanceConfig.getTags(), Set.of(CONTROLLER_INSTANCE));

    stopController();
    stopZk();
  }
}
