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

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.server.starter.helix.HelixServerStarter;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.CommonConstants.Helix.KEY_OF_SERVER_NETTY_HOST;
import static org.apache.pinot.common.utils.CommonConstants.Helix.KEY_OF_SERVER_NETTY_PORT;
import static org.apache.pinot.common.utils.CommonConstants.Server.CONFIG_OF_INSTANCE_ID;
import static org.testng.Assert.assertEquals;


public class ServerStarterIntegrationTest extends ControllerTest {
  public static final String SERVER1 = "Server1";

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    startController();
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    stopController();
    stopZk();
  }

  private void verifyZkConfigData(HelixServerStarter helixServerStarter, String expectedInstanceName,
      String expectedHostname, String expectedPort) {
    // Verify the serverId, host and port are set correctly in Zk.
    HelixManager helixManager = helixServerStarter.getHelixManager();
    PropertyKey.Builder keyBuilder = helixManager.getHelixDataAccessor().keyBuilder();
    InstanceConfig config = helixManager.getHelixDataAccessor().
        getProperty(keyBuilder.instanceConfig(helixServerStarter.getHelixManager().getInstanceName()));
    helixServerStarter.stop();

    assertEquals(config.getInstanceName(), expectedInstanceName);
    // By default (auto joined instances), server instance name is of format: {@code Server_<hostname>_<port>}, e.g.
    // {@code Server_localhost_12345}, hostname is of format: {@code Server_<hostname>}, e.g. {@code Server_localhost}.
    // More details refer to the class ServerInstance.
    assertEquals(config.getHostName(), expectedHostname);
    assertEquals(config.getPort(), expectedPort);
  }

  @Test
  public void testWithNoInstanceIdNoHostnamePort()
      throws Exception {
    // Test the behavior when no instance id nor hostname/port is specified in server conf.
    Configuration serverConf = new PropertiesConfiguration();
    // Start the server.
    HelixServerStarter helixServerStarter =
        new HelixServerStarter(getHelixClusterName(), ZkStarter.DEFAULT_ZK_STR, serverConf);
    String expectedInstanceName = helixServerStarter.getHelixManager().getInstanceName();
    String expectedHostname = expectedInstanceName.substring(0, expectedInstanceName.lastIndexOf('_'));
    verifyZkConfigData(helixServerStarter, expectedInstanceName, expectedHostname, "8098");
  }

  @Test
  public void testWithNoInstanceIdButWithHostnamePort()
      throws Exception {
    // Test the behavior when no instance id specified but hostname/port is specified in server conf.
    Configuration serverConf = new PropertiesConfiguration();
    serverConf.setProperty(KEY_OF_SERVER_NETTY_HOST, "host1");
    serverConf.setProperty(KEY_OF_SERVER_NETTY_PORT, 10001);
    // Start the server
    HelixServerStarter helixServerStarter =
        new HelixServerStarter(getHelixClusterName(), ZkStarter.DEFAULT_ZK_STR, serverConf);

    // Verify the serverId, host and port are set correctly in Zk.
    verifyZkConfigData(helixServerStarter, "Server_host1_10001", "Server_host1", "10001");
  }

  @Test
  public void testInstanceIdWithHostPortInfo()
      throws Exception {
    // Test the behavior when logical server id and hostname/port are all specified in server conf.
    // Unlike testInstanceIdOnly(), the host and port info will be overwritten with those in config.
    Configuration serverConf = new PropertiesConfiguration();
    serverConf.setProperty(CONFIG_OF_INSTANCE_ID, SERVER1);
    serverConf.setProperty(KEY_OF_SERVER_NETTY_HOST, "host1");
    serverConf.setProperty(KEY_OF_SERVER_NETTY_PORT, 10001);
    // Start the server
    HelixServerStarter helixServerStarter =
        new HelixServerStarter(getHelixClusterName(), ZkStarter.DEFAULT_ZK_STR, serverConf);

    // Verify the serverId, host and port are set correctly in Zk.
    verifyZkConfigData(helixServerStarter, SERVER1, "host1", "10001");
  }

  @Test
  public void testInstanceIdOnly()
      throws Exception {
    // If the CONFIG_OF_USE_LOGICAL_INSTANCE_ID flag is NOT on, no server host/port info overwrite will happen.
    Configuration serverConf = new PropertiesConfiguration();
    serverConf.setProperty(CONFIG_OF_INSTANCE_ID, SERVER1);
    // Start the server
    HelixServerStarter helixServerStarter =
        new HelixServerStarter(getHelixClusterName(), ZkStarter.DEFAULT_ZK_STR, serverConf);

    // Verify the serverId, host and port are set correctly in Zk.
    // Note that helix will use INSTANCE_ID to extract the hostname instead of using netty host/port in config.
    verifyZkConfigData(helixServerStarter, SERVER1, SERVER1, "");
  }

  @Test
  public void testSetInstanceIdToHostname()
      throws Exception {
    // Test the behavior when no instance id nor hostname/port is specified in server conf.
    // Compared with testWithNoInstanceIdNoHostnamePort(), here the server id use hostname (i.e., localhost) instead of
    // host address (i.e., 127.0.0.1).
    Configuration serverConf = new PropertiesConfiguration();
    serverConf.setProperty(CommonConstants.Helix.SET_INSTANCE_ID_TO_HOSTNAME_KEY, true);
    // Start the server
    HelixServerStarter helixServerStarter =
        new HelixServerStarter(getHelixClusterName(), ZkStarter.DEFAULT_ZK_STR, serverConf);

    // Verify the serverId, host and port are set correctly in Zk.
    verifyZkConfigData(helixServerStarter, "Server_localhost_8098", "Server_localhost", "8098");
  }
}
