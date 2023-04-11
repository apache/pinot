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
@CommandLine.Command(name = "StartZookeeper", description = "Start the Zookeeper process at the specified port.",
    mixinStandardHelpOptions = true)
public class StartZookeeperCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(StartZookeeperCommand.class);

  @CommandLine.Option(names = {"-zkPort"}, required = false, description = "Port to start zookeeper server on.")
  private int _zkPort = 2181;

  @CommandLine.Option(names = {"-dataDir"}, required = false, description = "Directory for zookeper data.")
  private String _dataDir = PinotConfigUtils.TMP_DIR + "PinotAdmin/zkData";

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

  public StartZookeeperCommand setPort(int port) {
    _zkPort = port;
    return this;
  }

  public StartZookeeperCommand setDataDir(String dataDir) {
    _dataDir = dataDir;
    return this;
  }

  private ZkStarter.ZookeeperInstance _zookeeperInstance;

  public void init(int zkPort, String dataDir) {
    _zkPort = zkPort;
    _dataDir = dataDir;
  }

  @Override
  public boolean execute()
      throws IOException {
    LOGGER.info("Executing command: " + toString());

    IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace() {
      @Override
      public void createDefaultNameSpace(org.I0Itec.zkclient.ZkClient zkClient) {
        // init any zk paths if needed
      }
    };

    _zookeeperInstance = ZkStarter.startLocalZkServer(_zkPort, _dataDir);

    LOGGER.info("Start zookeeper at localhost:" + _zkPort + " in thread " + Thread.currentThread().getName());

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
