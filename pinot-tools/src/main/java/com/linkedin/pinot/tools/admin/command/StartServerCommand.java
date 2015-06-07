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

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.kohsuke.args4j.Option;

import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.server.starter.helix.HelixServerStarter;

/**
 * Class to implement StartServer command.
 *
 */
public class StartServerCommand extends AbstractBaseCommand implements Command {
  @Option(name="-serverPort", required=true, metaVar="<int>", usage="Port number to start the server at.")
  private int _serverPort = CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT;

  @Option(name="-dataDir", required=true, metaVar="<string>", usage="Path to directory containing data.")
  private String _dataDir = null;

  @Option(name="-segmentDir", required=true, metaVar="<string>", usage="Path to directory containing segments.")
  private String _segmentDir = null;

  @Option(name="-zkAddress", required=true, metaVar="<http>", usage="Http address of Zookeeper.")
  private String _zkAddress = null;

  private String _clusterName = "PinotCluster";

  @Option(name="-help", required=false, help=true, aliases={"-h", "--h", "--help"}, usage="Print this message.")
  private boolean _help = false;

  public boolean getHelp() {
    return _help;
  }

  public StartServerCommand setClusterName(String clusterName) {
    _clusterName = clusterName;
    return this;
  }

  public StartServerCommand setZkAddress(String zkAddress) {
    _zkAddress = zkAddress;
    return this;
  }

  public StartServerCommand setPort(int port) {
    _serverPort = port;
    return this;
  }

  public StartServerCommand setDataDir(String dataDir) {
    _dataDir = dataDir;
    return this;
  }

  public StartServerCommand setSegmentDir(String segmentDir) {
    _segmentDir = segmentDir;
    return this;
  }

  @Override
  public String toString() {
    return ("StartServerCommand -clusterName " + _clusterName + " -serverPort " + _serverPort +
        " -dataDir " + _dataDir + " -segmentDir " + _segmentDir + " -zkAddress " + _zkAddress);
  }

  @Override
  public String getName() {
    return "StartServer";
  }

  @Override
  public void cleanup() {

  }

  @Override
  public String description() {
    return "Start the Pinot Server process at the specified port.";
  }

  @Override
  public boolean execute() throws Exception {
    final Configuration configuration = new PropertiesConfiguration();

    configuration.addProperty(CommonConstants.Helix.KEY_OF_SERVER_NETTY_PORT, _serverPort);
    configuration.addProperty("pinot.server.instance.dataDir", _dataDir + _serverPort + "/index");
    configuration.addProperty("pinot.server.instance.segmentTarDir", _segmentDir + _serverPort + "/segmentTar");

    final HelixServerStarter pinotHelixStarter =
        new HelixServerStarter(_clusterName, _zkAddress, configuration);

    savePID(System.getProperty("java.io.tmpdir") + File.separator + ".pinotAdminServer.pid");
    return true;
  }
}
