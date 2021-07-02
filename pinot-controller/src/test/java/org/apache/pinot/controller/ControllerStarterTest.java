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
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.helix.ControllerTest;
import org.testng.annotations.Test;

import static org.apache.pinot.controller.ControllerConf.CONTROLLER_HOST;
import static org.apache.pinot.controller.ControllerConf.CONTROLLER_PORT;
import static org.apache.pinot.spi.utils.CommonConstants.Controller.CONFIG_OF_INSTANCE_ID;
import static org.testng.Assert.assertEquals;


public class ControllerStarterTest extends ControllerTest {
  private final Map<String, Object> _configOverride = new HashMap<>();

  @Override
  public Map<String, Object> getDefaultControllerConfiguration() {
    Map<String, Object> defaultConfig = super.getDefaultControllerConfiguration();
    defaultConfig.putAll(_configOverride);
    return defaultConfig;
  }

  @Test
  public void testHostnamePortOverride()
      throws Exception {
    _configOverride.put(CONFIG_OF_INSTANCE_ID, "myInstance");
    _configOverride.put(CONTROLLER_HOST, "myHost");
    _configOverride.put(CONTROLLER_PORT, 1234);

    startZk();
    startController();

    String instanceId = _controllerStarter.getInstanceId();
    assertEquals(instanceId, "myInstance");
    InstanceConfig instanceConfig = HelixHelper.getInstanceConfig(_helixManager, instanceId);
    assertEquals(instanceConfig.getInstanceName(), instanceId);
    assertEquals(instanceConfig.getHostName(), "myHost");
    assertEquals(instanceConfig.getPort(), "1234");

    stopController();
    stopZk();
  }

  @Test
  public void testDefaultInstanceId()
      throws Exception {
    _configOverride.put(CONTROLLER_HOST, "myHost");
    _configOverride.put(CONTROLLER_PORT, 1234);

    startZk();
    startController();

    String instanceId = _controllerStarter.getInstanceId();
    assertEquals(instanceId, "Controller_myHost_1234");
    InstanceConfig instanceConfig = HelixHelper.getInstanceConfig(_helixManager, instanceId);
    assertEquals(instanceConfig.getInstanceName(), instanceId);
    assertEquals(instanceConfig.getHostName(), "myHost");
    assertEquals(instanceConfig.getPort(), "1234");

    stopController();
    stopZk();
  }
}
