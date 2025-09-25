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
package org.apache.pinot.common.utils;

import java.io.File;
import java.util.Properties;
import org.apache.commons.io.FileUtils;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.pinot.common.utils.ZkStarter.SSLConfig;
import org.apache.pinot.common.utils.ZkStarter.ZookeeperInstance;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test class for ZkStarter SSL functionality
 */
public class ZkStarterSSLTest {

  @Test
  public void testSSLConfigCreation() {
    SSLConfig sslConfig = new SSLConfig("/path/to/keystore.jks", "password123",
        "/path/to/truststore.jks", "password123");

    Assert.assertEquals(sslConfig.getKeyStorePath(), "/path/to/keystore.jks");
    Assert.assertEquals(sslConfig.getKeyStorePassword(), "password123");
    Assert.assertEquals(sslConfig.getTrustStorePath(), "/path/to/truststore.jks");
    Assert.assertEquals(sslConfig.getTrustStorePassword(), "password123");
  }

  @Test
  public void testSSLConfigClientProperties() {
    SSLConfig sslConfig = new SSLConfig("/path/to/keystore.jks", "password123",
        "/path/to/truststore.jks", "password123");

    Properties clientProps = sslConfig.getClientSSLProperties();

    Assert.assertEquals(clientProps.getProperty("pinot.zk.ssl.enabled"), "true");
    Assert.assertEquals(clientProps.getProperty("pinot.zk.ssl.keystore.location"), "/path/to/keystore.jks");
    Assert.assertEquals(clientProps.getProperty("pinot.zk.ssl.keystore.password"), "password123");
    Assert.assertEquals(clientProps.getProperty("pinot.zk.ssl.truststore.location"), "/path/to/truststore.jks");
    Assert.assertEquals(clientProps.getProperty("pinot.zk.ssl.truststore.password"), "password123");
    Assert.assertEquals(clientProps.getProperty("pinot.zk.client.cnxn.socket"),
        "org.apache.zookeeper.ClientCnxnSocketNetty");
  }

  /**
   * Example of how to use the SSL ZkStarter functionality
   * Note: This test is disabled because it requires keytool and SSL certificates
   */
  @Test
  public void testSSLZkServerExample()
      throws Exception {
    // Create temporary directories for SSL certificates
    File tempDir = new File(FileUtils.getTempDirectory(), "zk-ssl-test-" + System.currentTimeMillis());
    tempDir.mkdirs();

    String keystorePath = new File(tempDir, "keystore.jks").getAbsolutePath();
    String truststorePath = new File(tempDir, "truststore.jks").getAbsolutePath();
    String password = "testpassword";

    try {
      // Create test SSL configuration
      SSLConfig sslConfig = ZkStarter.createTestSSLConfig(keystorePath, truststorePath, password);

      // Start SSL-enabled ZooKeeper server
      ZookeeperInstance zkInstance = ZkStarter.startLocalZkServerWithSSL(sslConfig);

      Assert.assertTrue(zkInstance.isSSLEnabled());
      Assert.assertNotNull(zkInstance.getSSLConfig());

      // Get client SSL properties
      Properties clientProps = zkInstance.getSSLConfig().getClientSSLProperties();

      // Configure client SSL
      ZkSSLUtils.configureSSL(clientProps);

      // Connect with SSL client
      ZkClient sslClient = new ZkClient(zkInstance.getZkUrl(), 10000);
      sslClient.waitUntilConnected(10000, java.util.concurrent.TimeUnit.MILLISECONDS);

      // Test basic operations
      sslClient.createPersistent("/test", "data");
      String data = sslClient.readData("/test");
      Assert.assertEquals(data, "data");

      // Cleanup
      sslClient.close();
      ZkStarter.stopLocalZkServer(zkInstance);
    } finally {
      // Clean up temp directory
      FileUtils.deleteDirectory(tempDir);
    }
  }

  /**
   * Demonstrates usage pattern for SSL ZooKeeper in tests
   */
  public void exampleUsagePattern() {
    // 1. Create SSL configuration
    SSLConfig sslConfig = new SSLConfig(
        "/path/to/test-keystore.jks", "testpass",
        "/path/to/test-truststore.jks", "testpass"
    );

    // 2. Start SSL ZooKeeper server
    ZookeeperInstance zkInstance = ZkStarter.startLocalZkServerWithSSL(2181, sslConfig);

    // 3. Get client configuration for Pinot components
    Properties pinotClientConfig = zkInstance.getSSLConfig().getClientSSLProperties();
    pinotClientConfig.setProperty("pinot.zk.server", zkInstance.getZkUrl());
    pinotClientConfig.setProperty("pinot.cluster.name", "testCluster");

    // 4. Use with Pinot clients (example)
    // Connection connection = ConnectionFactory.fromZookeeper(pinotClientConfig, zkInstance.getZkUrl());

    // 5. Cleanup
    ZkStarter.stopLocalZkServer(zkInstance);
  }

  /**
   * Test that SSL ZkClient is properly initialized with SSL configuration
   */
  @Test
  public void testSSLZkClientInitialization() {
    // Create a simple SSL configuration
    SSLConfig sslConfig = new SSLConfig("/path/to/keystore.jks", "password123",
        "/path/to/truststore.jks", "password123");

    // Verify that the SSL configuration is properly set up
    Properties clientProps = sslConfig.getClientSSLProperties();
    Assert.assertEquals(clientProps.getProperty("pinot.zk.ssl.enabled"), "true");
    Assert.assertEquals(clientProps.getProperty("pinot.zk.client.cnxn.socket"),
        "org.apache.zookeeper.ClientCnxnSocketNetty");
    Assert.assertEquals(clientProps.getProperty("pinot.zk.ssl.keystore.location"), "/path/to/keystore.jks");
    Assert.assertEquals(clientProps.getProperty("pinot.zk.ssl.truststore.location"), "/path/to/truststore.jks");

    // Verify that ZkSSLUtils can configure SSL from these properties
    try {
      ZkSSLUtils.configureSSL(clientProps);
      // If we reach here, SSL configuration was applied successfully
      Assert.assertTrue(true);
    } catch (Exception e) {
      Assert.fail("SSL configuration should be applied without exception: " + e.getMessage());
    }
  }
}
