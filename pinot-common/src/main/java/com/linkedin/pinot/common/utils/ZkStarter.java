/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.utils;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZkStarter {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZkStarter.class);
  public static final int DEFAULT_ZK_TEST_PORT = 2191;
  public static final String DEFAULT_ZK_STR = "localhost:" + DEFAULT_ZK_TEST_PORT;

  public static class ZookeeperInstance {
    private PublicZooKeeperServerMain _serverMain;
    private String _dataDirPath;

    private ZookeeperInstance(PublicZooKeeperServerMain serverMain, String dataDirPath) {
      _serverMain = serverMain;
      _dataDirPath = dataDirPath;
    }
  }

  /**
   * Silly class to make protected methods public.
   */
  static class PublicZooKeeperServerMain extends ZooKeeperServerMain {
    @Override
    public void initializeAndRun(String[] args) throws QuorumPeerConfig.ConfigException, IOException {
      super.initializeAndRun(args);
    }

    @Override
    public void runFromConfig(final ServerConfig config) throws IOException {
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

        public String getDataDir() {
          return config.getDataDir();
        }

        public String getDataLogDir() {
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
    return startLocalZkServer(DEFAULT_ZK_TEST_PORT);
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
      final String[] args = new String[] { Integer.toString(port), dataDirPath };
      new Thread() {
        @Override
        public void run() {
          try {
            zookeeperServerMain.initializeAndRun(args);
          } catch (QuorumPeerConfig.ConfigException e) {
            LOGGER.warn("Caught exception while starting ZK", e);
          } catch (IOException e) {
            LOGGER.warn("Caught exception while starting ZK", e);
          }
        }
      }.start();

      // Wait until the ZK server is started
      ZkClient client = new ZkClient("localhost:" + port, 10000);
      client.waitUntilConnected(10L, TimeUnit.SECONDS);
      client.close();

      return new ZookeeperInstance(zookeeperServerMain, dataDirPath);
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
