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
import java.util.Random;
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
    private String _dataDirPath;
    private int _port;

    private ZookeeperInstance(PublicZooKeeperServerMain serverMain, String dataDirPath, int port) {
      _serverMain = serverMain;
      _dataDirPath = dataDirPath;
      _port = port;
    }

    public String getZkUrl() {
      return "localhost:" + _port;
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
          maxClientCnxns = 0;
          return 0;
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
    return startLocalZkServer(NetUtils.findOpenPort(DEFAULT_ZK_TEST_PORT + new Random().nextInt(10000)));
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
          Thread.sleep(1000L);
          ZkClient client = new ZkClient("localhost:" + port, 1000 * (DEFAULT_ZK_CLIENT_RETRIES - retry));
          client.waitUntilConnected(DEFAULT_ZK_CLIENT_RETRIES - retry, TimeUnit.SECONDS);
          client.close();
          break;
        } catch (Exception e) {
          if (retry < DEFAULT_ZK_CLIENT_RETRIES - 1) {
            LOGGER.warn("Failed to connect to zk server, retry: {}", retry, e);
          } else {
            LOGGER.warn("Failed to connect to zk server.", e);
            throw e;
          }
        }
      }
      return new ZookeeperInstance(zookeeperServerMain, dataDirPath, port);
    } catch (Exception e) {
      LOGGER.warn("Caught exception while starting ZK", e);
      throw new RuntimeException(e);
    }
  }

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
