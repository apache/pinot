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
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.controller.helix.ControllerTest;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.pinot.controller.ControllerConf.CONTROLLER_HOST;
import static org.apache.pinot.controller.ControllerConf.CONTROLLER_PORT;
import static org.apache.pinot.controller.ControllerConf.HELIX_CLUSTER_NAME;

public class ControllerStarterTest extends ControllerTest {
  private Map<String, Object> controllerConfigMap = new HashMap<>();
  @Override
  public Map<String, Object> getDefaultControllerConfiguration() {
    Map<String, Object> legacy = super.getDefaultControllerConfiguration();
    legacy.keySet().forEach(key -> controllerConfigMap.putIfAbsent(key, legacy.get(key)));
    return controllerConfigMap;
  }

  @Test
  public void testControllerHostNameOverride() throws Exception {
    boolean controllerStarted = false;
    try {
      controllerConfigMap.clear();
      controllerConfigMap.put(CONTROLLER_HOST, "strange_name.com");
      controllerConfigMap.put(CONTROLLER_PORT, 9527);
      startZk();
      // startController will call getDefaultControllerConfiguration() to fetch and apply controllerConfigMap
      startController();
      controllerStarted = true;
      HelixAdmin admin = _controllerStarter.getHelixControllerManager().getClusterManagmentTool();
      InstanceConfig instanceConfig = admin.getInstanceConfig(_controllerStarter.getConfig().getProperty(HELIX_CLUSTER_NAME), _controllerStarter.getInstanceId());
      Assert.assertNotNull(instanceConfig);
      Assert.assertEquals("strange_name.com", instanceConfig.getHostName());
      Assert.assertEquals("9527", instanceConfig.getPort());
    } finally{
      if (controllerStarted) {
        stopController();
      }
      stopZk();
    }
  }

  @Test
  public void testSkippingControllerHostNameOverride() throws Exception {
    boolean controllerStarted = false;
    try {
      controllerConfigMap.clear();
      controllerConfigMap.put(CONTROLLER_HOST, "strange_name.com");
      controllerConfigMap.put(CONTROLLER_PORT, 9527);
      startZk();
      // startController will call getDefaultControllerConfiguration() to fetch and apply controllerConfigMap
      startController();
      controllerStarted = true;
      HelixAdmin admin = _controllerStarter.getHelixControllerManager().getClusterManagmentTool();
      InstanceConfig instanceConfig = admin.getInstanceConfig(_controllerStarter.getConfig().getProperty(HELIX_CLUSTER_NAME), _controllerStarter.getInstanceId());
      Assert.assertNotNull(instanceConfig);
      Assert.assertNotEquals("strange_name.com", instanceConfig.getHostName());
      Assert.assertNotEquals("9527", instanceConfig.getPort());
    } finally{
      if (controllerStarted) {
        stopController();
      }
      stopZk();
    }
  }

  @Test
  public void testMissingControllerHostName() throws Exception {
    boolean controllerStarted = false;
    try {
      controllerConfigMap.clear();
      controllerConfigMap.put(CONTROLLER_HOST, "");
      controllerConfigMap.put(CONTROLLER_PORT, 9527);
      startZk();
      // startController will call getDefaultControllerConfiguration() to fetch and apply controllerConfigMap
      startController();
      controllerStarted = true;
      HelixAdmin admin = _controllerStarter.getHelixControllerManager().getClusterManagmentTool();
      InstanceConfig instanceConfig = admin.getInstanceConfig(_controllerStarter.getConfig().getProperty(HELIX_CLUSTER_NAME), _controllerStarter.getInstanceId());
      Assert.assertNotNull(instanceConfig);
      Assert.assertNotEquals("", instanceConfig.getHostName());
    } finally{
      if (controllerStarted) {
        stopController();
      }
      stopZk();
    }
  }

  @Test
  public void testIntegerPortBlock() throws Exception {
    boolean controllerStarted = false;
    boolean validated = false;
    boolean numberFormatCaught = false;
    try {
      controllerConfigMap.clear();
      controllerConfigMap.put(CONTROLLER_HOST, "strange_host.com");
      controllerConfigMap.put(CONTROLLER_PORT, "not-a-number");
      startZk();
      // startController will call getDefaultControllerConfiguration() to fetch and apply controllerConfigMap
      startController();
      controllerStarted = true;
      HelixAdmin admin = _controllerStarter.getHelixControllerManager().getClusterManagmentTool();
      InstanceConfig instanceConfig = admin.getInstanceConfig(_controllerStarter.getConfig().getProperty(HELIX_CLUSTER_NAME), _controllerStarter.getInstanceId());
      Assert.assertNotNull(instanceConfig);
      Assert.assertNotEquals("strange_host.com", instanceConfig.getHostName());
      Assert.assertNotEquals("not-a-number", instanceConfig.getPort());
      validated = true;
    } catch (NumberFormatException ex) {
      numberFormatCaught = true;
    } finally{
      if (controllerStarted) {
        stopController();
      }
      stopZk();
    }
    Assert.assertTrue(numberFormatCaught || validated);
  }
}
