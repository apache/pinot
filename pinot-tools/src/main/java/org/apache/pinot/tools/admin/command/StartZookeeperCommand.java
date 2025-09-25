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
package org.apache.pinot.tools.admin.command;

import java.io.File;
import java.io.IOException;
import org.I0Itec.zkclient.IDefaultNameSpace;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.utils.PinotConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


/**
 * Class for command to start ZooKeeper.
 *
 *
 */
@CommandLine.Command(name = "StartZookeeper", mixinStandardHelpOptions = true)
public class StartZookeeperCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(StartZookeeperCommand.class);

  @CommandLine.Option(names = {"-zkPort"}, required = false, description = "Port to start zookeeper server on.")
  private int _zkPort = 2181;

  @CommandLine.Option(names = {"-dataDir"}, required = false, description = "Directory for zookeeper data.")
  private String _dataDir = PinotConfigUtils.TMP_DIR + "PinotAdmin/zkData";

  @CommandLine.Option(names = {"-sslEnabled"}, required = false, description = "Enable SSL for zookeeper")
  private boolean _sslEnabled;
  @CommandLine.Option(names = {"-sslKeyStorePath"}, required = false, description = "SSL KeyStore path")
  private String _keyStorePath;
  @CommandLine.Option(names = {"-sslKeyStorePassword"}, required = false, description = "SSL KeyStore password")
  private String _keyStorePassword;
  @CommandLine.Option(names = {"-sslTrustStorePath"}, required = false, description = "SSL TrustStore path")
  private String _trustStorePath;
  @CommandLine.Option(names = {"-sslTrustStorePassword"}, required = false, description = "SSL TrustStore password")
  private String _trustStorePassword;

  @Override
  public String getName() {
    return "StartZookeeper";
  }

  @Override
  public String toString() {
    return ("StartZookeeper -zkPort " + _zkPort + " -dataDir " + _dataDir);
  }

  @Override
  public void cleanup() {
  }

  @Override
  public String description() {
    return "Start the Zookeeper process at the specified port.";
  }

  public StartZookeeperCommand setPort(int port) {
    _zkPort = port;
    return this;
  }

  public StartZookeeperCommand setDataDir(String dataDir) {
    _dataDir = dataDir;
    return this;
  }

  public StartZookeeperCommand setSslEnabled(boolean sslEnabled) {
    _sslEnabled = sslEnabled;
    return this;
  }

  public StartZookeeperCommand setKeyStorePath(String keyStorePath) {
    _keyStorePath = keyStorePath;
    return this;
  }

  public StartZookeeperCommand setKeyStorePassword(String keyStorePassword) {
    _keyStorePassword = keyStorePassword;
    return this;
  }

  public StartZookeeperCommand setTrustStorePath(String trustStorePath) {
    _trustStorePath = trustStorePath;
    return this;
  }

  public StartZookeeperCommand setTrustStorePassword(String trustStorePassword) {
    _trustStorePassword = trustStorePassword;
    return this;
  }

  public ZkStarter.SSLConfig getSSLConfig() {
    if (_sslEnabled) {
      return new ZkStarter.SSLConfig(_keyStorePath, _keyStorePassword, _trustStorePath, _trustStorePassword);
    } else {
      return null;
    }
  }

  private ZkStarter.ZookeeperInstance _zookeeperInstance;

  public void init(int zkPort, String dataDir) {
    _zkPort = zkPort;
    _dataDir = dataDir;
  }

  @Override
  public boolean execute()
      throws IOException {
    LOGGER.info("Executing command: {}", this);

    IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace() {
      @Override
      public void createDefaultNameSpace(org.I0Itec.zkclient.ZkClient zkClient) {
        // init any zk paths if needed
      }
    };

    if (_sslEnabled) {
      ZkStarter.SSLConfig sslConfig =
          new ZkStarter.SSLConfig(_keyStorePath, _keyStorePassword, _trustStorePath, _trustStorePassword);
      _zookeeperInstance = ZkStarter.startLocalZkServer(_zkPort, _dataDir, sslConfig);
    } else {
      _zookeeperInstance = ZkStarter.startLocalZkServer(_zkPort, _dataDir, null);
    }

    LOGGER.info("Start zookeeper at localhost:{} in thread {}", _zkPort, Thread.currentThread().getName());

    savePID(System.getProperty("java.io.tmpdir") + File.separator + ".zooKeeper.pid");
    return true;
  }

  public boolean stop() {
    ZkStarter.stopLocalZkServer(_zookeeperInstance);
    return true;
  }

  public static void main(String[] args)
      throws Exception {
    StartZookeeperCommand zkc = new StartZookeeperCommand();
    zkc.execute();
  }
}
