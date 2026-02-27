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
package org.apache.pinot.tools.utils;

import java.io.File;
import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Map;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class PinotConfigUtilsTest {

  @Test
  public void testGenerateControllerConf()
      throws SocketException, UnknownHostException {
    // Test with all parameters provided
    String zkAddress = "localhost:2181";
    String clusterName = "testCluster";
    String controllerHost = "localhost";
    String controllerPort = "9000";
    String dataDir = "/tmp/pinot";
    ControllerConf.ControllerMode controllerMode = ControllerConf.ControllerMode.DUAL;
    boolean tenantIsolation = true;

    Map<String, Object> config = PinotConfigUtils.generateControllerConf(
        zkAddress, clusterName, controllerHost, controllerPort, dataDir, controllerMode, tenantIsolation);

    assertEquals(config.get(ControllerConf.ZK_STR), zkAddress);
    assertEquals(config.get(ControllerConf.HELIX_CLUSTER_NAME), clusterName);
    assertEquals(config.get(ControllerConf.CONTROLLER_HOST), controllerHost);
    assertEquals(config.get(ControllerConf.CONTROLLER_PORT), controllerPort);
    assertEquals(config.get(ControllerConf.DATA_DIR), dataDir);
    assertEquals(config.get(ControllerConf.CONTROLLER_VIP_HOST), controllerHost);
    assertEquals(config.get(ControllerConf.CLUSTER_TENANT_ISOLATION_ENABLE), tenantIsolation);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testGenerateControllerConfWithEmptyZkAddress()
      throws SocketException, UnknownHostException {
    PinotConfigUtils.generateControllerConf(
        "", "testCluster", "localhost", "9000", "/tmp/pinot", ControllerConf.ControllerMode.DUAL, true);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testGenerateControllerConfWithEmptyClusterName()
      throws SocketException, UnknownHostException {
    PinotConfigUtils.generateControllerConf(
        "localhost:2181", "", "localhost", "9000", "/tmp/pinot", ControllerConf.ControllerMode.DUAL, true);
  }

  @Test
  public void testGenerateMinionConf()
      throws SocketException, UnknownHostException {
    String clusterName = "testCluster";
    String zkAddress = "localhost:2181";
    String minionHost = "localhost";
    int minionPort = 9000;

    Map<String, Object> config = PinotConfigUtils.generateMinionConf(clusterName, zkAddress, minionHost, minionPort);

    assertEquals(config.get(CommonConstants.Helix.CONFIG_OF_CLUSTER_NAME), clusterName);
    assertEquals(config.get(CommonConstants.Helix.CONFIG_OF_ZOOKEEPR_SERVER), zkAddress);
    assertEquals(config.get(CommonConstants.Helix.KEY_OF_MINION_HOST), minionHost);
    assertEquals(config.get(CommonConstants.Helix.KEY_OF_MINION_PORT), minionPort);
  }

  @Test
  public void testGetResourceTrackingConf() {
    Map<String, Object> config = PinotConfigUtils.getResourceTrackingConf();

    assertTrue((Boolean) config.get(CommonConstants.Server.CONFIG_OF_ENABLE_THREAD_CPU_TIME_MEASUREMENT));
    assertTrue((Boolean) config.get(CommonConstants.Server.CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT));
    assertTrue((Boolean) config.get(CommonConstants.Broker.CONFIG_OF_ENABLE_THREAD_CPU_TIME_MEASUREMENT));
    assertTrue((Boolean) config.get(CommonConstants.Broker.CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT));
    assertTrue((Boolean) config.get(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_CPU_SAMPLING));
    assertTrue((Boolean) config.get(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING));
  }

  @Test(expectedExceptions = ConfigurationException.class)
  public void testReadConfigWithDuplicateKeys()
      throws IOException, ConfigurationException {
    // Create a temporary config file with duplicate keys
    File tempFile = File.createTempFile("test-config", ".properties");
    try {
      String configContent = "key1=value1\n"
          + "key2=value2\n"
          + "key1=value3\n"
          + "key4=value4\n";
      FileUtils.writeStringToFile(tempFile, configContent, "UTF-8");

      // This should throw ConfigurationException due to duplicate keys
      PinotConfigUtils.readConfigFromFile(tempFile.getAbsolutePath());
    } finally {
      FileUtils.deleteQuietly(tempFile);
    }
  }

  @Test
  public void testReadConfigWithValidKeys()
      throws IOException, ConfigurationException {
    // Create a temporary config file with valid keys
    File tempFile = File.createTempFile("test-config", ".properties");
    try {
      String configContent = "key1=value1\n"
          + "key2=value2\n"
          + "key3=value3\n"
          + "key4=value4\n";
      FileUtils.writeStringToFile(tempFile, configContent, "UTF-8");

      Map<String, Object> config = PinotConfigUtils.readConfigFromFile(tempFile.getAbsolutePath());
      assertNotNull(config);
      assertEquals(config.get("key1"), "value1");
      assertEquals(config.get("key2"), "value2");
      assertEquals(config.get("key3"), "value3");
      assertEquals(config.get("key4"), "value4");
    } finally {
      FileUtils.deleteQuietly(tempFile);
    }
  }
}
