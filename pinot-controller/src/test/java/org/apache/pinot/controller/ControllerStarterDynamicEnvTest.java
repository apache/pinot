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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.spi.utils.builder.ControllerRequestURLBuilder;
import org.testng.annotations.Test;

import static org.apache.pinot.controller.ControllerConf.CONTROLLER_HOST;
import static org.apache.pinot.controller.ControllerConf.CONTROLLER_PORT;
import static org.apache.pinot.spi.utils.CommonConstants.Controller.CONFIG_OF_INSTANCE_ID;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.CONTROLLER_INSTANCE;
import static org.testng.Assert.*;


/**
 * This class tests env variables when starting controller from configs
 */
public class ControllerStarterDynamicEnvTest extends ControllerTest {
  private final Map<String, Object> _configOverride = new HashMap<>();

  @Override
  protected void overrideControllerConf(Map<String, Object> properties) {
    //To not test dynamic config env variables through this class. Host is not dynamic for this class config
    properties.putAll(_configOverride);
  }

  @Test
  public void testNoVariable()
      throws Exception {
    _configOverride.clear();
    _configOverride.put(CONTROLLER_HOST, "myHost");
    _configOverride.put(CONFIG_OF_INSTANCE_ID, "Controller_myInstance");
    _configOverride.put(CONTROLLER_PORT, 1234);

    startZk();
    this.startController();

    String instanceId = _controllerStarter.getInstanceId();
    assertEquals(instanceId, "Controller_myInstance");
    InstanceConfig instanceConfig = HelixHelper.getInstanceConfig(_helixManager, instanceId);
    assertEquals(instanceConfig.getInstanceName(), instanceId);
    assertEquals(instanceConfig.getHostName(), "myHost");
    assertEquals(instanceConfig.getPort(), "1234");
    assertEquals(instanceConfig.getTags(), Collections.singleton(CONTROLLER_INSTANCE));

    stopController();
    stopZk();
  }

  @Test
  public void testOneVariable()
      throws Exception {
    _configOverride.clear();
    _configOverride.put("dynamic.env.config", "controller.host");
    _configOverride.put(CONTROLLER_HOST, "HOST");
    _configOverride.put(CONFIG_OF_INSTANCE_ID, "Controller_myInstance");
    _configOverride.put(CONTROLLER_PORT, 1234);

    startZk();
    this.startController();

    String instanceId = _controllerStarter.getInstanceId();
    assertEquals(instanceId, "Controller_myInstance");
    InstanceConfig instanceConfig = HelixHelper.getInstanceConfig(_helixManager, instanceId);
    assertEquals(instanceConfig.getInstanceName(), instanceId);
    assertEquals(instanceConfig.getHostName(), "myHost");
    assertEquals(instanceConfig.getPort(), "1234");
    assertEquals(instanceConfig.getTags(), Collections.singleton(CONTROLLER_INSTANCE));

    stopController();
    stopZk();
  }

  @Test
  public void testMultipleVariables()
      throws Exception {
    _configOverride.clear();
    _configOverride.put("dynamic.env.config", "controller.host,controller.port");
    _configOverride.put(CONTROLLER_HOST, "HOST");
    _configOverride.put(CONTROLLER_PORT, "PORT");
    _configOverride.put(CONFIG_OF_INSTANCE_ID, "Controller_myInstance");

    startZk();
    this.startController();

    String instanceId = _controllerStarter.getInstanceId();
    assertEquals(instanceId, "Controller_myInstance");
    InstanceConfig instanceConfig = HelixHelper.getInstanceConfig(_helixManager, instanceId);
    assertEquals(instanceConfig.getInstanceName(), instanceId);
    assertEquals(instanceConfig.getHostName(), "myHost");
    assertEquals(instanceConfig.getPort(), "1234");
    assertEquals(instanceConfig.getTags(), Collections.singleton(CONTROLLER_INSTANCE));

    stopController();
    stopZk();
  }

  @Override
  public Map<String, Object> getDefaultControllerConfiguration() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(ControllerConf.ZK_STR, getZkUrl());
    properties.put(ControllerConf.HELIX_CLUSTER_NAME, getHelixClusterName());
    properties.put("dynamic.env.config", "controller.host,controller.port");
    properties.put(CONTROLLER_HOST, "HOST");
    properties.put(CONTROLLER_PORT, "PORT");

    int controllerPort = NetUtils.findOpenPort(_nextControllerPort);
    properties.put(ControllerConf.CONTROLLER_PORT, controllerPort);
    if (_controllerPort == 0) {
      _controllerPort = controllerPort;
    }
    _nextControllerPort = controllerPort + 1;
    properties.put(ControllerConf.DATA_DIR, DEFAULT_DATA_DIR);
    properties.put(ControllerConf.LOCAL_TEMP_DIR, DEFAULT_LOCAL_TEMP_DIR);
    // Enable groovy on the controller
    properties.put(ControllerConf.DISABLE_GROOVY, false);
    properties.put(CommonConstants.CONFIG_OF_TIMEZONE, "UTC");
    overrideControllerConf(properties);
    return properties;
  }

  @Override
  public void startController(Map<String, Object> properties)
      throws Exception {
    Map<String, String> envVariables = new HashMap<>();
    envVariables.put("HOST", "myHost");
    envVariables.put("PORT", "1234");
    _controllerStarter = createControllerStarter();
    _controllerStarter.init(new PinotConfiguration(properties, envVariables));
    _controllerStarter.start();
    _controllerConfig = _controllerStarter.getConfig();
    _controllerBaseApiUrl = _controllerConfig.generateVipUrl();
    _controllerRequestURLBuilder = ControllerRequestURLBuilder.baseUrl(_controllerBaseApiUrl);
    _controllerDataDir = _controllerConfig.getDataDir();
    _helixResourceManager = _controllerStarter.getHelixResourceManager();
    _helixManager = _controllerStarter.getHelixControllerManager();
    _helixDataAccessor = _helixManager.getHelixDataAccessor();
    ConfigAccessor configAccessor = _helixManager.getConfigAccessor();
    // HelixResourceManager is null in Helix only mode, while HelixManager is null in Pinot only mode.
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(getHelixClusterName())
            .build();
    switch (_controllerStarter.getControllerMode()) {
      case DUAL:
      case PINOT_ONLY:
        _helixAdmin = _helixResourceManager.getHelixAdmin();
        _propertyStore = _helixResourceManager.getPropertyStore();
        // TODO: Enable periodic rebalance per 10 seconds as a temporary work-around for the Helix issue:
        //       https://github.com/apache/helix/issues/331 and https://github.com/apache/helix/issues/2309.
        //       Remove this after Helix fixing the issue.
        configAccessor.set(scope, ClusterConfig.ClusterConfigProperty.REBALANCE_TIMER_PERIOD.name(), "10000");
        break;
      case HELIX_ONLY:
        _helixAdmin = _helixManager.getClusterManagmentTool();
        _propertyStore = _helixManager.getHelixPropertyStore();
        break;
      default:
        break;
    }
    assertEquals(System.getProperty("user.timezone"), "UTC");
  }
}
