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
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.controller.ControllerConf;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


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
  public void testReadConfigWithDuplicateKeys()
      throws IOException {
    File tempFile = File.createTempFile("test-config", ".properties");
    try {
      String configContent = "key1=value1\n"
          + "key2=value2\n"
          + "key1=value3\n"
          + "key4=value4\n";
      FileUtils.writeStringToFile(tempFile, configContent, StandardCharsets.UTF_8);

      try {
        PinotConfigUtils.readConfigFromFile(tempFile.getAbsolutePath());
        fail("Expected ConfigurationException for duplicate keys");
      } catch (ConfigurationException e) {
        assertTrue(e.getMessage().contains("Duplicate key 'key1'"));
        assertTrue(e.getMessage().contains("line 3"));
        assertTrue(e.getMessage().contains("line 1"));
      }
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
      FileUtils.writeStringToFile(tempFile, configContent, StandardCharsets.UTF_8);

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

  @Test
  public void testReadConfigWithDoubleSlashComments()
      throws IOException, ConfigurationException {
    File tempFile = File.createTempFile("test-config", ".conf");
    try {
      String configContent = "//\n"
          + "// Licensed header line\n"
          + "//\n"
          + "key1=value1\n"
          + "key2=value2\n";
      FileUtils.writeStringToFile(tempFile, configContent, StandardCharsets.UTF_8);

      Map<String, Object> config = PinotConfigUtils.readConfigFromFile(tempFile.getAbsolutePath());
      assertNotNull(config);
      assertEquals(config.get("key1"), "value1");
      assertEquals(config.get("key2"), "value2");
    } finally {
      FileUtils.deleteQuietly(tempFile);
    }
  }
}
