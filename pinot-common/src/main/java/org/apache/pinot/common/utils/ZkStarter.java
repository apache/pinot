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
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.admin.AdminServer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZkStarter {
  private ZkStarter() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(ZkStarter.class);
  public static final int DEFAULT_ZK_TEST_PORT = 2191;
  private static final int DEFAULT_ZK_CLIENT_RETRIES = 10;

  public static class ZookeeperInstance {
    private PublicZooKeeperServerMain _serverMain;
    private final String _dataDirPath;
    private final int _port;
    private final SSLConfig _sslConfig;

    private ZookeeperInstance(PublicZooKeeperServerMain serverMain, String dataDirPath, int port) {
      _serverMain = serverMain;
      _dataDirPath = dataDirPath;
      _port = port;
      _sslConfig = null;
    }

    private ZookeeperInstance(PublicZooKeeperServerMain serverMain, String dataDirPath, int port, SSLConfig sslConfig) {
      _serverMain = serverMain;
      _dataDirPath = dataDirPath;
      _port = port;
      _sslConfig = sslConfig;
    }

    public String getZkUrl() {
      return "localhost:" + _port;
    }

    public boolean isSSLEnabled() {
      return _sslConfig != null;
    }

    public SSLConfig getSSLConfig() {
      return _sslConfig;
    }
  }

  /**
   * SSL configuration for ZooKeeper test instance
   */
  public static class SSLConfig {
    private final String _keyStorePath;
    private final String _keyStorePassword;
    private final String _trustStorePath;
    private final String _trustStorePassword;

    public SSLConfig(String keyStorePath, String keyStorePassword, String trustStorePath, String trustStorePassword) {
      _keyStorePath = keyStorePath;
      _keyStorePassword = keyStorePassword;
      _trustStorePath = trustStorePath;
      _trustStorePassword = trustStorePassword;
    }

    public String getKeyStorePath() {
      return _keyStorePath;
    }

    public String getKeyStorePassword() {
      return _keyStorePassword;
    }

    public String getTrustStorePath() {
      return _trustStorePath;
    }

    public String getTrustStorePassword() {
      return _trustStorePassword;
    }

    /**
     * Get Properties for Pinot client SSL configuration
     */
    public Properties getClientSSLProperties() {
      Properties props = new Properties();
      props.setProperty("pinot.zk.ssl.enabled", "true");
      props.setProperty("pinot.zk.client.cnxn.socket", "org.apache.zookeeper.ClientCnxnSocketNetty");
      props.setProperty("pinot.zk.ssl.keystore.location", _keyStorePath);
      props.setProperty("pinot.zk.ssl.keystore.password", _keyStorePassword);
      props.setProperty("pinot.zk.ssl.keystore.type", "JKS");
      props.setProperty("pinot.zk.ssl.truststore.location", _trustStorePath);
      props.setProperty("pinot.zk.ssl.truststore.password", _trustStorePassword);
      props.setProperty("pinot.zk.ssl.truststore.type", "JKS");
      return props;
    }
  }

  /**
   * Silly class to make protected methods public.
   */
  static class PublicZooKeeperServerMain extends ZooKeeperServerMain {
    @Override
    public void initializeAndRun(String[] args)
        throws QuorumPeerConfig.ConfigException, IOException, AdminServer.AdminServerException {
      // org.apache.log4j.jmx.* is not compatible under log4j-1.2-api, which provides the backward compatibility for
      // log4j 1.* api for log4j2. In order to avoid 'class not found error', the following line disables log4j jmx
      // bean registration for local zookeeper instance
      System.setProperty("zookeeper.jmx.log4j.disable", "true");
      System.setProperty("zookeeper.admin.enableServer", "false");
      super.initializeAndRun(args);
    }

    @Override
    public void runFromConfig(final ServerConfig config)
        throws IOException, AdminServer.AdminServerException {
      ServerConfig newServerConfig = new ServerConfig() {

        public void parse(String[] args) {
          config.parse(args);
        }

        public void parse(String path)
            throws QuorumPeerConfig.ConfigException {
          config.parse(path);
        }

        public void readFrom(QuorumPeerConfig otherConfig) {
          config.readFrom(otherConfig);
        }

        public InetSocketAddress getClientPortAddress() {
          return config.getClientPortAddress();
        }

        public File getDataDir() {
          return config.getDataDir();
        }

        public File getDataLogDir() {
          return config.getDataLogDir();
        }

        public int getTickTime() {
          return config.getTickTime();
        }

        public int getMaxClientCnxns() {
          dataDir = getDataDir();
          dataLogDir = getDataLogDir();
          tickTime = getTickTime();
          minSessionTimeout = getMinSessionTimeout();
          maxSessionTimeout = getMaxSessionTimeout();
          maxClientCnxns = config.getMaxClientCnxns();
          return maxClientCnxns;
        }

        public int getMinSessionTimeout() {
          return config.getMinSessionTimeout();
        }

        public int getMaxSessionTimeout() {
          return config.getMaxSessionTimeout();
        }
      };

      newServerConfig.getMaxClientCnxns();

      super.runFromConfig(newServerConfig);
    }

    @Override
    public void shutdown() {
      super.shutdown();
    }
  }

  /**
   * Starts an empty local Zk instance on the default port
   */
  public static ZookeeperInstance startLocalZkServer() {
    return startLocalZkServer(NetUtils.findOpenPort(DEFAULT_ZK_TEST_PORT));
  }

  public static String getDefaultZkStr() {
    return "localhost:" + DEFAULT_ZK_TEST_PORT;
  }

  /**
   * Starts a local Zk instance with a generated empty data directory
   * @param port The port to listen on
   */
  public static ZookeeperInstance startLocalZkServer(final int port) {
    return startLocalZkServer(port,
        org.apache.commons.io.FileUtils.getTempDirectoryPath() + File.separator + "test-" + System.currentTimeMillis());
  }

  /**
   * Starts a local ZooKeeper instance with SSL enabled
   * @param sslConfig SSL configuration containing keystore and truststore paths
   * @return A ZookeeperInstance with SSL configuration
   */
  public static ZookeeperInstance startLocalZkServerWithSSL(SSLConfig sslConfig) {
    return startLocalZkServerWithSSL(NetUtils.findOpenPort(DEFAULT_ZK_TEST_PORT), sslConfig);
  }

  /**
   * Starts a local ZooKeeper instance with SSL enabled on a specific port
   * @param port The port to listen on
   * @param sslConfig SSL configuration containing keystore and truststore paths
   * @return A ZookeeperInstance with SSL configuration
   */
  public static ZookeeperInstance startLocalZkServerWithSSL(final int port, SSLConfig sslConfig) {
    return startLocalZkServerWithSSL(port,
        org.apache.commons.io.FileUtils.getTempDirectoryPath() + File.separator + "test-" + System.currentTimeMillis(),
        sslConfig);
  }

  /**
   * Starts a local ZooKeeper instance with SSL enabled
   * @param port The port to listen on
   * @param dataDirPath The path for the Zk data directory
   * @param sslConfig SSL configuration containing keystore and truststore paths
   * @return A ZookeeperInstance with SSL configuration
   */
  public synchronized static ZookeeperInstance startLocalZkServerWithSSL(final int port, final String dataDirPath,
      SSLConfig sslConfig) {
    // Configure ZooKeeper SSL system properties for server
    System.setProperty("zookeeper.serverCnxnFactory", "org.apache.zookeeper.server.NettyServerCnxnFactory");
    System.setProperty("zookeeper.ssl.keyStore.location", sslConfig.getKeyStorePath());
    System.setProperty("zookeeper.ssl.keyStore.password", sslConfig.getKeyStorePassword());
    System.setProperty("zookeeper.ssl.keyStore.type", "JKS");
    System.setProperty("zookeeper.ssl.trustStore.location", sslConfig.getTrustStorePath());
    System.setProperty("zookeeper.ssl.trustStore.password", sslConfig.getTrustStorePassword());
    System.setProperty("zookeeper.ssl.trustStore.type", "JKS");

    // Configure secure port for SSL ZooKeeper
    System.setProperty("zookeeper.secureClientPort", String.valueOf(port));
    System.setProperty("zookeeper.secureClientPortAddress", "0.0.0.0");

    // Enable SSL for server
    System.setProperty("zookeeper.sslQuorum", "true");
    System.setProperty("zookeeper.ssl.quorum.keyStore.location", sslConfig.getKeyStorePath());
    System.setProperty("zookeeper.ssl.quorum.keyStore.password", sslConfig.getKeyStorePassword());
    System.setProperty("zookeeper.ssl.quorum.trustStore.location", sslConfig.getTrustStorePath());
    System.setProperty("zookeeper.ssl.quorum.trustStore.password", sslConfig.getTrustStorePassword());

    // Configure SSL protocol and cipher suites
    System.setProperty("zookeeper.ssl.protocol", "TLS");
    System.setProperty("zookeeper.ssl.enabledProtocols", "TLSv1.2,TLSv1.3");
    System.setProperty("zookeeper.ssl.ciphersuites",
        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384");

    // Disable regular client port for SSL-only mode
    System.setProperty("zookeeper.clientPort", "-1");

    // Additional SSL configuration for better compatibility
    System.setProperty("zookeeper.ssl.hostnameVerification", "false");
    System.setProperty("zookeeper.ssl.crl", "false");
    System.setProperty("zookeeper.ssl.ocsp", "false");

    // Start the local ZK server with SSL
    try {
      final PublicZooKeeperServerMain zookeeperServerMain = new PublicZooKeeperServerMain();
      final String[] args = new String[]{Integer.toString(port), dataDirPath};
      new Thread() {
        @Override
        public void run() {
          try {
            zookeeperServerMain.initializeAndRun(args);
          } catch (Exception e) {
            LOGGER.warn("Caught exception while starting SSL ZK", e);
          }
        }
      }.start();

      // Wait until the SSL ZK server is started
      for (int retry = 0; retry < DEFAULT_ZK_CLIENT_RETRIES; retry++) {
        try {
          // Configure SSL for the test client
          ZkSSLUtils.configureSSL(sslConfig.getClientSSLProperties());

          // Create SSL-enabled ZkClient with proper configuration
          // For SSL ZooKeeper, we need to use the SSL connection string format
          // The SSL configuration is handled through system properties set by ZkSSLUtils.configureSSL()
          Thread.sleep(100);

          // Use SSL connection string format for SSL ZooKeeper
          // The SSL configuration is handled through system properties, so we use the regular connection string
          String sslConnectionString = "localhost:" + port;

          // Set SSL client properties
          System.setProperty("zookeeper.clientCnxnSocket", "org.apache.zookeeper.ClientCnxnSocketNetty");
          System.setProperty("zookeeper.client.secure", "true");

          // Configure SSL protocol and cipher suites for client
          System.setProperty("zookeeper.ssl.protocol", "TLS");
          System.setProperty("zookeeper.ssl.enabledProtocols", "TLSv1.2,TLSv1.3");
          System.setProperty("zookeeper.ssl.ciphersuites",
              "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384");

          // Additional SSL configuration for better compatibility
          System.setProperty("zookeeper.ssl.hostnameVerification", "false");
          System.setProperty("zookeeper.ssl.crl", "false");
          System.setProperty("zookeeper.ssl.ocsp", "false");

          // Wait a bit longer for SSL configuration to be properly applied
          Thread.sleep(500);

          ZkClient client = new ZkClient(sslConnectionString, 1000 * (DEFAULT_ZK_CLIENT_RETRIES - retry));
          client.waitUntilConnected(DEFAULT_ZK_CLIENT_RETRIES - retry, TimeUnit.SECONDS);
          closeAsync(client);
          break;
        } catch (Exception e) {
          if (retry < DEFAULT_ZK_CLIENT_RETRIES - 1) {
            LOGGER.warn("Failed to connect to SSL zk server, retry: {}", retry, e);
          } else {
            LOGGER.warn("Failed to connect to SSL zk server.", e);
            throw e;
          }
          Thread.sleep(50L);
        }
      }
      return new ZookeeperInstance(zookeeperServerMain, dataDirPath, port, sslConfig);
    } catch (Exception e) {
      LOGGER.warn("Caught exception while starting SSL ZK", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates test SSL certificates for development/testing purposes
   * @param keystorePath Path where the keystore will be created
   * @param truststorePath Path where the truststore will be created
   * @param password Password for both keystore and truststore
   * @return SSLConfig with the created certificate paths
   */
  public static SSLConfig createTestSSLConfig(String keystorePath, String truststorePath, String password) {
    try {
      // Create keystore and truststore using keytool (requires keytool to be available)
      ProcessBuilder keystoreBuilder = new ProcessBuilder(
          "keytool", "-genkeypair", "-alias", "localhost",
          "-keyalg", "RSA", "-keysize", "2048",
          "-keystore", keystorePath, "-storepass", password,
          "-dname", "CN=localhost,OU=Test,O=Apache Pinot,L=Test,ST=Test,C=US",
          "-validity", "365", "-keypass", password
      );
      Process keystoreProcess = keystoreBuilder.start();
      keystoreProcess.waitFor();

      ProcessBuilder truststoreBuilder = new ProcessBuilder(
          "keytool", "-exportcert", "-alias", "localhost",
          "-keystore", keystorePath, "-storepass", password,
          "-file", keystorePath + ".cert"
      );
      Process exportProcess = truststoreBuilder.start();
      exportProcess.waitFor();

      ProcessBuilder importBuilder = new ProcessBuilder(
          "keytool", "-importcert", "-alias", "localhost",
          "-keystore", truststorePath, "-storepass", password,
          "-file", keystorePath + ".cert", "-noprompt"
      );
      Process importProcess = importBuilder.start();
      importProcess.waitFor();

      // Clean up certificate file
      new File(keystorePath + ".cert").delete();

      return new SSLConfig(keystorePath, password, truststorePath, password);
    } catch (Exception e) {
      LOGGER.warn("Failed to create test SSL certificates. Please ensure keytool is available in PATH", e);
      throw new RuntimeException("Failed to create test SSL certificates", e);
    }
  }

  /**
   * Starts a local Zk instance
   * @param port The port to listen on
   * @param dataDirPath The path for the Zk data directory
   */
  public synchronized static ZookeeperInstance startLocalZkServer(final int port, final String dataDirPath) {
    // Start the local ZK server
    try {
      final PublicZooKeeperServerMain zookeeperServerMain = new PublicZooKeeperServerMain();
      final String[] args = new String[]{Integer.toString(port), dataDirPath};
      new Thread() {
        @Override
        public void run() {
          try {
            zookeeperServerMain.initializeAndRun(args);
          } catch (Exception e) {
            LOGGER.warn("Caught exception while starting ZK", e);
          }
        }
      }.start();

      // Wait until the ZK server is started
      for (int retry = 0; retry < DEFAULT_ZK_CLIENT_RETRIES; retry++) {
        try {
          ZkClient client = new ZkClient("localhost:" + port, 1000 * (DEFAULT_ZK_CLIENT_RETRIES - retry));
          client.waitUntilConnected(DEFAULT_ZK_CLIENT_RETRIES - retry, TimeUnit.SECONDS);
          closeAsync(client);
          break;
        } catch (Exception e) {
          if (retry < DEFAULT_ZK_CLIENT_RETRIES - 1) {
            LOGGER.warn("Failed to connect to zk server, retry: {}", retry, e);
          } else {
            LOGGER.warn("Failed to connect to zk server.", e);
            throw e;
          }
          Thread.sleep(50L);
        }
      }
      return new ZookeeperInstance(zookeeperServerMain, dataDirPath, port);
    } catch (Exception e) {
      LOGGER.warn("Caught exception while starting ZK", e);
      throw new RuntimeException(e);
    }
  }

  public static void closeAsync(ZkClient client) {
    if (client != null) {
      ZK_DISCONNECTOR.submit(() -> {
        client.close();
      });
    }
  }

  private static final ExecutorService ZK_DISCONNECTOR =
      Executors.newFixedThreadPool(1, new NamedThreadFactory("zk-disconnector"));

  /**
   * Stops a local Zk instance, deleting its data directory
   */
  public static void stopLocalZkServer(final ZookeeperInstance instance) {
    stopLocalZkServer(instance, true);
  }

  /**
   * Stops a local Zk instance.
   * @param deleteDataDir Whether or not to delete the data directory
   */
  public synchronized static void stopLocalZkServer(final ZookeeperInstance instance, final boolean deleteDataDir) {
    if (instance._serverMain != null) {
      try {
        // Shut down ZK
        instance._serverMain.shutdown();
        instance._serverMain = null;

        // Delete the data dir
        if (deleteDataDir) {
          org.apache.commons.io.FileUtils.deleteDirectory(new File(instance._dataDirPath));
        }
      } catch (Exception e) {
        LOGGER.warn("Caught exception while stopping ZK server", e);
        throw new RuntimeException(e);
      }
    }
  }
}
