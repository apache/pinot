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
package com.linkedin.pinot.tools.admin.command;


import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.tools.Command;
import java.io.File;
import java.io.IOException;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkServer;
import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

/**
 * Class for command to start ZooKeeper.
 *
 *
 */
public class StartZookeeperCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(StartZookeeperCommand.class);

  @Option(name="-zkPort", required=false, metaVar="<int>", usage="Port to start zookeeper server on.")
  private int _zkPort=2181;

  @Option(name="-dataDir", required=false, metaVar="<string>", usage="Directory for zookeper data.")
  private String _dataDir = TMP_DIR + "PinotAdmin/zkData";

  @Option(name="-help", required=false, help=true, aliases={"-h", "--h", "--help"}, usage="Print this message.")
  private boolean _help = false;

  @Override
  public boolean getHelp() {
    return _help;
  }

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
    if (_tmpdir != null) {
      try {
        FileUtils.deleteDirectory(_tmpdir);
      } catch (IOException e) {
        // Nothing to do right now.
      }
    }
  }

  @Override
  public String description() {
    return "Start the Zookeeper process at the specified port.";
  }

  StartZookeeperCommand setPort(int port) {
    _zkPort = port;
    return this;
  }

  StartZookeeperCommand setDataDir(String dataDir) {
    _dataDir = dataDir;
    return this;
  }

  private ZkStarter.ZookeeperInstance _zookeeperInstance;
  private File _tmpdir;

  public void init(int zkPort, String dataDir) {
    _zkPort = zkPort;
    _dataDir = dataDir;
  }

  @Override
  public boolean execute() throws IOException {
    LOGGER.info("Executing command: " + toString());
    _tmpdir = createAutoDeleteTempDir();

    File logdir = new File(_tmpdir + File.separator + "translog");
    File datadir = new File(_tmpdir + File.separator + "snapshot");

    IDefaultNameSpace _defaultNameSpace = new IDefaultNameSpace() {
      @Override
      public void createDefaultNameSpace(org.I0Itec.zkclient.ZkClient zkClient) {
        // init any zk paths if needed
      }
    };

    _zookeeperInstance = ZkStarter.startLocalZkServer(_zkPort, datadir.getAbsolutePath());

    LOGGER.info("Start zookeeper at localhost:" + _zkPort + " in thread "
        + Thread.currentThread().getName());

    savePID(System.getProperty("java.io.tmpdir") + File.separator + ".zooKeeper.pid");
    return true;
  }

  public static File createAutoDeleteTempDir() {
    File tempdir = Files.createTempDir();
    tempdir.delete();
    tempdir.mkdir();

    tempdir.deleteOnExit();
    return tempdir;
  }

  public boolean stop() {
    ZkStarter.stopLocalZkServer(_zookeeperInstance);
    return true;
  }

  public static void main(String[] args) throws Exception {
    StartZookeeperCommand zkc = new StartZookeeperCommand();
    zkc.execute();
  }
}
