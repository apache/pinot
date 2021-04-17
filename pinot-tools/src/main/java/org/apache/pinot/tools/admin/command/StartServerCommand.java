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
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.pinot.spi.services.ServiceRole;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.utils.PinotConfigUtils;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to implement StartServer command.
 *
 */
public class StartServerCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(StartServerCommand.class);
  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"},
      usage = "Print this message.")
  private boolean _help = false;
  @Option(name = "-serverHost", required = false, metaVar = "<String>", usage = "Host name for server.")
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
      usage = "Server Starter Config file.", forbids = {"-serverHost", "-serverPort", "-dataDir", "-segmentDir",})
  private String _configFileName;

  private Map<String, Object> _configOverrides = new HashMap<>();

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

  public StartServerCommand setConfigOverrides(Map<String, Object> configs) {
    _configOverrides = configs;
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
      LOGGER.info("Executing command: " + toString());
      StartServiceManagerCommand startServiceManagerCommand =
          new StartServiceManagerCommand().setZkAddress(_zkAddress).setClusterName(_clusterName).setPort(-1)
              .setBootstrapServices(new String[0]).addBootstrapService(ServiceRole.SERVER, getServerConf());
      startServiceManagerCommand.execute();
      String pidFile = ".pinotAdminServer-" + System.currentTimeMillis() + ".pid";
      savePID(System.getProperty("java.io.tmpdir") + File.separator + pidFile);
      return true;
    } catch (Exception e) {
      LOGGER.error("Caught exception while starting Pinot server, exiting.", e);
      System.exit(-1);
      return false;
    }
  }

  private Map<String, Object> getServerConf() throws ConfigurationException, SocketException, UnknownHostException {
    Map<String, Object> properties = new HashMap<>();
    if (_configFileName != null) {
      properties.putAll(PinotConfigUtils.readConfigFromFile(_configFileName));
    } else {
      properties.putAll(
          PinotConfigUtils.generateServerConf(_serverHost, _serverPort, _serverAdminPort, _dataDir, _segmentDir));
    }
    if (_configOverrides != null) {
      properties.putAll(_configOverrides);
    }
    return properties;
  }
}
