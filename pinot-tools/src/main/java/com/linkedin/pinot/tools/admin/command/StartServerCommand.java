/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.NetUtil;
import com.linkedin.pinot.server.starter.helix.HelixServerStarter;
import com.linkedin.pinot.tools.Command;
import java.io.File;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to implement StartServer command.
 *
 */
public class StartServerCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(StartServerCommand.class);

  @Option(name = "-serverHost", required = false, metaVar = "<String>", usage = "Host name for controller.")
  private String _serverHost;

  @Option(name = "-serverPort", required = false, metaVar = "<int>", usage = "Port number to start the server at.")
  private int _serverPort = CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT;

  @Option(name = "-serverAdminPort", required = false, metaVar = "<int>",
      usage = "Port number to serve the server admin API at.")
  private int _serverAdminPort = CommonConstants.Server.DEFAULT_ADMIN_API_PORT;

  @Option(name = "-dataDir", required = false, metaVar = "<string>", usage = "Path to directory containing data.")
  private String _dataDir = TMP_DIR + "pinotServerData";

  @Option(name = "-segmentDir", required = false, metaVar = "<string>",
      usage = "Path to directory containing segments.")
  private String _segmentDir = TMP_DIR + "pinotSegments";

  @Option(name = "-zkAddress", required = false, metaVar = "<http>", usage = "Http address of Zookeeper.")
  private String _zkAddress = DEFAULT_ZK_ADDRESS;

  @Option(name = "-clusterName", required = false, metaVar = "<String>", usage = "Pinot cluster name.")
  private String _clusterName = "PinotCluster";

  @Option(name = "-configFileName", required = false, metaVar = "<Config File Name>",
      usage = "Broker Starter Config file.", forbids = { "-serverHost", "-serverPort", "-dataDir", "-segmentDir", })
  private String _configFileName;

  @Option(name = "-help", required = false, help = true, aliases = { "-h", "--h", "--help" },
      usage = "Print this message.")
  private boolean _help = false;

  @Override
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

  public StartServerCommand setAdminPort(int adminPort) {
    _serverAdminPort = adminPort;
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

  public StartServerCommand setConfigFileName(String configFileName) {
    _configFileName = configFileName;
    return this;
  }

  @Override
  public String toString() {
    if (_configFileName != null) {
      return ("StartServer -clusterName " + _clusterName + " -serverHost " + _serverHost + " -serverPort " + _serverPort
          + " -serverAdminPort " + _serverAdminPort + " -configFileName " + _configFileName + " -zkAddress "
          + _zkAddress);
    } else {
      return ("StartServer -clusterName " + _clusterName + " -serverHost " + _serverHost + " -serverPort " + _serverPort
          + " -serverAdminPort " + _serverAdminPort + " -dataDir " + _dataDir + " -segmentDir " + _segmentDir
          + " -zkAddress " + _zkAddress);
    }
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
    try {
      if (_serverHost == null) {
        _serverHost = NetUtil.getHostAddress();
      }

      Configuration configuration = readConfigFromFile(_configFileName);
      if (configuration == null) {
        if (_configFileName != null) {
          LOGGER.error("Error: Unable to find file {}.", _configFileName);
          return false;
        }

        configuration = new PropertiesConfiguration();
        configuration.addProperty(CommonConstants.Helix.KEY_OF_SERVER_NETTY_HOST, _serverHost);
        configuration.addProperty(CommonConstants.Helix.KEY_OF_SERVER_NETTY_PORT, _serverPort);
        configuration.addProperty(CommonConstants.Server.CONFIG_OF_ADMIN_API_PORT, _serverAdminPort);
        configuration.addProperty("pinot.server.instance.dataDir", _dataDir + _serverPort + "/index");
        configuration.addProperty("pinot.server.instance.segmentTarDir", _segmentDir + _serverPort + "/segmentTar");
      }

      LOGGER.info("Executing command: " + toString());
      new HelixServerStarter(_clusterName, _zkAddress, configuration);
      String pidFile = ".pinotAdminServer-" + String.valueOf(System.currentTimeMillis()) + ".pid";
      savePID(System.getProperty("java.io.tmpdir") + File.separator + pidFile);
      return true;
    } catch (Exception e) {
      LOGGER.error("Caught exception while starting Pinot server, exiting.", e);
      System.exit(-1);
      return false;
    }
  }
}
