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
package org.apache.pinot.integration.tests;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.utils.NetUtil;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.server.starter.helix.HelixServerStarter;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.CommonConstants.Helix.*;
import static org.apache.pinot.common.utils.CommonConstants.Server.CONFIG_OF_INSTANCE_ID;
import static org.testng.Assert.assertEquals;


public class ServerStarterIntegrationTest extends ControllerTest {
  private static final String CUSTOM_INSTANCE_ID = "CustomInstance";
  private static final String CUSTOM_HOST = "CustomHost";
  private static final int CUSTOM_PORT = 10001;

  @BeforeClass
  public void setUp() {
    startZk();
    startController();
  }

  @AfterClass
  public void tearDown() {
    stopController();
    stopZk();
  }

  private void verifyInstanceConfig(Configuration serverConf, String expectedInstanceId, String expectedHost,
      int expectedPort)
      throws Exception {
    HelixServerStarter helixServerStarter =
        new HelixServerStarter(getHelixClusterName(), ZkStarter.DEFAULT_ZK_STR, serverConf);
    helixServerStarter.stop();

    assertEquals(helixServerStarter.getInstanceId(), expectedInstanceId);
    InstanceConfig instanceConfig =
        _helixDataAccessor.getProperty(_helixDataAccessor.keyBuilder().instanceConfig(expectedInstanceId));
    assertEquals(instanceConfig.getHostName(), expectedHost);
    assertEquals(Integer.parseInt(instanceConfig.getPort()), expectedPort);
  }

  @Test
  public void testDefaultServerConf()
      throws Exception {
    Configuration serverConf = new BaseConfiguration();
    String expectedHost = NetUtil.getHostAddress();
    String expectedInstanceId = PREFIX_OF_SERVER_INSTANCE + expectedHost + "_" + DEFAULT_SERVER_NETTY_PORT;
    verifyInstanceConfig(serverConf, expectedInstanceId, expectedHost, DEFAULT_SERVER_NETTY_PORT);
  }

  @Test
  public void testSetInstanceIdToHostname()
      throws Exception {
    Configuration serverConf = new BaseConfiguration();
    serverConf.addProperty(SET_INSTANCE_ID_TO_HOSTNAME_KEY, true);
    String expectedHost = NetUtil.getHostnameOrAddress();
    String expectedInstanceId = PREFIX_OF_SERVER_INSTANCE + expectedHost + "_" + DEFAULT_SERVER_NETTY_PORT;
    verifyInstanceConfig(serverConf, expectedInstanceId, expectedHost, DEFAULT_SERVER_NETTY_PORT);
  }

  @Test
  public void testCustomInstanceId()
      throws Exception {
    Configuration serverConf = new BaseConfiguration();
    serverConf.addProperty(CONFIG_OF_INSTANCE_ID, CUSTOM_INSTANCE_ID);
    verifyInstanceConfig(serverConf, CUSTOM_INSTANCE_ID, NetUtil.getHostAddress(), DEFAULT_SERVER_NETTY_PORT);
  }

  @Test
  public void testCustomHost()
      throws Exception {
    Configuration serverConf = new BaseConfiguration();
    serverConf.addProperty(KEY_OF_SERVER_NETTY_HOST, CUSTOM_HOST);
    String expectedInstanceId = PREFIX_OF_SERVER_INSTANCE + CUSTOM_HOST + "_" + DEFAULT_SERVER_NETTY_PORT;
    verifyInstanceConfig(serverConf, expectedInstanceId, CUSTOM_HOST, DEFAULT_SERVER_NETTY_PORT);
  }

  @Test
  public void testCustomPort()
      throws Exception {
    Configuration serverConf = new BaseConfiguration();
    serverConf.addProperty(KEY_OF_SERVER_NETTY_PORT, CUSTOM_PORT);
    String expectedHost = NetUtil.getHostAddress();
    String expectedInstanceId = PREFIX_OF_SERVER_INSTANCE + expectedHost + "_" + CUSTOM_PORT;
    verifyInstanceConfig(serverConf, expectedInstanceId, expectedHost, CUSTOM_PORT);
  }

  @Test
  public void testAllCustomServerConf()
      throws Exception {
    Configuration serverConf = new BaseConfiguration();
    serverConf.addProperty(CONFIG_OF_INSTANCE_ID, CUSTOM_INSTANCE_ID);
    serverConf.addProperty(KEY_OF_SERVER_NETTY_HOST, CUSTOM_HOST);
    serverConf.addProperty(KEY_OF_SERVER_NETTY_PORT, CUSTOM_PORT);
    verifyInstanceConfig(serverConf, CUSTOM_INSTANCE_ID, CUSTOM_HOST, CUSTOM_PORT);
  }
}
