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
package com.linkedin.pinot.tools.admin.command;


import java.io.File;

import org.apache.helix.manager.zk.ZkClient;
import org.kohsuke.args4j.Option;

import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.ControllerStarter;

/**
 * Class to implement StartController command.
 *
 */
public class StartControllerCommand extends AbstractBaseCommand implements Command {
  @Option(name="-controllerPort", required=true, metaVar="<int>", usage="Port number to start the controller at.")
  private String _controllerPort = null;

  @Option(name="-dataDir", required=false, metaVar="<string>", usage="Path to directory containging data.")
  private String _dataDir = "/tmp/PinotController";

  @Option(name="-zkAddress", required=true, metaVar="<http>", usage="Http address of Zookeeper.")
  private String _zkAddress = null;

  private String _clusterName = "PinotCluster";

  @Option(name="-help", required=false, help=true, aliases={"-h", "--h", "--help"}, usage="Print this message.")
  private boolean _help = false;

  public boolean getHelp() {
    return _help;
  }

  public StartControllerCommand setControllerPort(String controllerPort) {
    _controllerPort = controllerPort;
    return this;
  }

  public StartControllerCommand setDataDir(String dataDir) {
    _dataDir = dataDir;
    return this;
  }

  public StartControllerCommand setZkAddress(String zkAddress) {
    _zkAddress = zkAddress;
    return this;
  }

  public StartControllerCommand setClusterName(String clusterName) {
    _clusterName = clusterName;
    return this;
  }

  @Override
  public String getName() {
    return "StartController";
  }

  @Override
  public String toString() {
    return ("StartControllerCommand -clusterName " + _clusterName +
        " -controllerPort " + _controllerPort + " -dataDir " + _dataDir + " -zkAddress " + _zkAddress);
  }

  @Override
  public void cleanup() {

  }

  @Override
  public String description() {
    return "Star the Pinot Controller Process at the specified port.";
  }

  @Override
  public boolean execute() throws Exception {
    final ControllerConf conf = new ControllerConf();

    conf.setControllerHost("localhost");
    conf.setControllerPort(_controllerPort);
    conf.setDataDir(_dataDir);
    conf.setZkStr(_zkAddress);

    conf.setHelixClusterName(_clusterName);
    conf.setControllerVipHost("localhost");

    conf.setRetentionControllerFrequencyInSeconds(3600 * 6);
    conf.setValidationControllerFrequencyInSeconds(3600);

    ZkClient zkClient = new ZkClient(_zkAddress);
    String helixClusterName = "/" + _clusterName;

    if (zkClient.exists(helixClusterName)) {
      zkClient.deleteRecursive(helixClusterName);
    }

    final ControllerStarter starter = new ControllerStarter(conf);
    System.out.println(conf.getQueryConsole());

    starter.start();

    savePID(System.getProperty("java.io.tmpdir") + File.separator + ".pinotAdminController.pid");
    return true;
  }
}
