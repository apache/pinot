/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;


public class ZkStarter {
  public static final int DEFAULT_ZK_TEST_PORT = 2191;
  public static final String DEFAULT_ZK_STR = "localhost:" + DEFAULT_ZK_TEST_PORT;

  private static PublicZooKeeperServerMain _zookeeperServerMain = null;
  private static String _zkDataDir = null;

  /**
   * Silly class to make protected methods public.
   */
  static class PublicZooKeeperServerMain extends ZooKeeperServerMain {
    @Override
    public void initializeAndRun(String[] args) throws QuorumPeerConfig.ConfigException, IOException {
      super.initializeAndRun(args);
    }

    @Override
    public void shutdown() {
      super.shutdown();
    }
  }

  /**
   * Starts an empty local Zk instance on the default port
   */
  public static void startLocalZkServer() {
    startLocalZkServer(DEFAULT_ZK_TEST_PORT);
  }

  /**
   * Starts a local Zk instance with a generated empty data directory
   * @param port The port to listen on
   */
  public static void startLocalZkServer(final int port) {
    startLocalZkServer(port,
        org.apache.commons.io.FileUtils.getTempDirectoryPath() + File.separator + "test-" + System.currentTimeMillis());
  }

  /**
   * Starts a local Zk instance
   * @param port The port to listen on
   * @param dataDirPath The path for the Zk data directory
   */
  public synchronized static void startLocalZkServer(final int port, final String dataDirPath) {
    if (_zookeeperServerMain != null) {
      throw new RuntimeException("Zookeeper server is already started!");
    }

    // Start the local ZK server
    try {
      _zookeeperServerMain = new PublicZooKeeperServerMain();
      _zkDataDir = dataDirPath;
      final String[] args = new String[] { Integer.toString(port), dataDirPath };
      new Thread() {
        @Override
        public void run() {
          try {
            _zookeeperServerMain.initializeAndRun(args);
          } catch (QuorumPeerConfig.ConfigException e) {
            e.printStackTrace();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    // Wait until the ZK server is started
    ZkClient client = new ZkClient("localhost:" + port, 10000);
    client.waitUntilConnected(10L, TimeUnit.SECONDS);
    client.close();
  }

  /**
   * Stops a local Zk instance, deleting its data directory
   */
  public static void stopLocalZkServer() {
    stopLocalZkServer(true);
  }

  /**
   * Stops a local Zk instance.
   * @param deleteDataDir Whether or not to delete the data directory
   */
  public synchronized static void stopLocalZkServer(final boolean deleteDataDir) {
    if (_zookeeperServerMain != null) {
      try {
        // Shut down ZK
        _zookeeperServerMain.shutdown();
        _zookeeperServerMain = null;

        // Delete the data dir
        if (deleteDataDir) {
          org.apache.commons.io.FileUtils.deleteDirectory(new File(_zkDataDir));
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
