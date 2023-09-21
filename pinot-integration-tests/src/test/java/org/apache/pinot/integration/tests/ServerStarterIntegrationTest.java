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

import java.util.HashMap;
import java.util.Map;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.server.starter.helix.HelixServerStarter;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.NetUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Helix.*;
import static org.apache.pinot.spi.utils.CommonConstants.Server.CONFIG_OF_INSTANCE_ID;
import static org.testng.Assert.assertEquals;


@Test(groups = {"integration-suite-2"})
public class ServerStarterIntegrationTest extends ControllerTest {
  private static final String CUSTOM_INSTANCE_ID = "CustomInstance";
  private static final String CUSTOM_HOST = "CustomHost";
  private static final int CUSTOM_PORT = 10001;

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    startController();
  }

  @AfterClass
  public void tearDown() {
    stopController();
    stopZk();
  }

  private void verifyInstanceConfig(PinotConfiguration serverConf, String expectedInstanceId, String expectedHost,
      int expectedPort)
      throws Exception {
    serverConf.setProperty(CONFIG_OF_CLUSTER_NAME, getHelixClusterName());
    serverConf.setProperty(CONFIG_OF_ZOOKEEPR_SERVER, getZkUrl());
    HelixServerStarter helixServerStarter = new HelixServerStarter();
    helixServerStarter.init(serverConf);
    helixServerStarter.start();
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
    String expectedHost = NetUtils.getHostAddress();
    String expectedInstanceId = PREFIX_OF_SERVER_INSTANCE + expectedHost + "_" + DEFAULT_SERVER_NETTY_PORT;

    verifyInstanceConfig(new PinotConfiguration(), expectedInstanceId, expectedHost, DEFAULT_SERVER_NETTY_PORT);
  }

  @Test
  public void testSetInstanceIdToHostname()
      throws Exception {
    String expectedHost = NetUtils.getHostnameOrAddress();
    String expectedInstanceId = PREFIX_OF_SERVER_INSTANCE + expectedHost + "_" + DEFAULT_SERVER_NETTY_PORT;

    Map<String, Object> properties = new HashMap<>();
    properties.put(SET_INSTANCE_ID_TO_HOSTNAME_KEY, true);

    verifyInstanceConfig(new PinotConfiguration(properties), expectedInstanceId, expectedHost,
        DEFAULT_SERVER_NETTY_PORT);
  }

  @Test
  public void testCustomInstanceId()
      throws Exception {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CONFIG_OF_INSTANCE_ID, CUSTOM_INSTANCE_ID);

    verifyInstanceConfig(new PinotConfiguration(properties), CUSTOM_INSTANCE_ID, NetUtils.getHostAddress(),
        DEFAULT_SERVER_NETTY_PORT);
  }

  @Test
  public void testCustomHost()
      throws Exception {
    String expectedInstanceId = PREFIX_OF_SERVER_INSTANCE + CUSTOM_HOST + "_" + DEFAULT_SERVER_NETTY_PORT;

    Map<String, Object> properties = new HashMap<>();
    properties.put(KEY_OF_SERVER_NETTY_HOST, CUSTOM_HOST);

    verifyInstanceConfig(new PinotConfiguration(properties), expectedInstanceId, CUSTOM_HOST,
        DEFAULT_SERVER_NETTY_PORT);
  }

  @Test
  public void testCustomPort()
      throws Exception {
    String expectedHost = NetUtils.getHostAddress();
    String expectedInstanceId = PREFIX_OF_SERVER_INSTANCE + expectedHost + "_" + CUSTOM_PORT;

    Map<String, Object> properties = new HashMap<>();
    properties.put(KEY_OF_SERVER_NETTY_PORT, CUSTOM_PORT);

    verifyInstanceConfig(new PinotConfiguration(properties), expectedInstanceId, expectedHost, CUSTOM_PORT);
  }

  @Test
  public void testAllCustomServerConf()
      throws Exception {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CONFIG_OF_INSTANCE_ID, CUSTOM_INSTANCE_ID);
    properties.put(KEY_OF_SERVER_NETTY_HOST, CUSTOM_HOST);
    properties.put(KEY_OF_SERVER_NETTY_PORT, CUSTOM_PORT);
    verifyInstanceConfig(new PinotConfiguration(properties), CUSTOM_INSTANCE_ID, CUSTOM_HOST, CUSTOM_PORT);
  }
}
