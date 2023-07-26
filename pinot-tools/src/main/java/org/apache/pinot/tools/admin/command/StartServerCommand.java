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
import org.apache.commons.collections.MapUtils;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.pinot.query.service.QueryConfig;
import org.apache.pinot.spi.services.ServiceRole;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.utils.PinotConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


/**
 * Class to implement StartServer command.
 *
 */
@CommandLine.Command(name = "StartServer")
public class StartServerCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(StartServerCommand.class);
  @CommandLine.Option(names = {"-help", "-h", "--h", "--help"}, required = false, help = true,
      description = "Print this message.")
  private boolean _help = false;

  @CommandLine.Option(names = {"-serverHost"}, required = false, description = "Host name for server.")
  private String _serverHost;

  @CommandLine.Option(names = {"-serverPort"}, required = false, description = "Port number to start the server at.")
  private int _serverPort = CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT;

  @CommandLine.Option(names = {"-serverAdminPort"}, required = false,
      description = "Port number to serve the server admin API at.")
  private int _serverAdminPort = CommonConstants.Server.DEFAULT_ADMIN_API_PORT;

  @CommandLine.Option(names = {"-serverGrpcPort"}, required = false,
      description = "Port number to serve the grpc query.")
  private int _serverGrpcPort = CommonConstants.Server.DEFAULT_GRPC_PORT;

  @CommandLine.Option(names = {"-serverMultiStageServerPort"}, required = false,
      description = "Port number to multi-stage query engine service entrypoint.")
  private int _serverMultiStageServerPort = QueryConfig.DEFAULT_QUERY_SERVER_PORT;

  @CommandLine.Option(names = {"-serverMultiStageRunnerPort"}, required = false,
      description = "Port number to multi-stage query engine runner communication.")
  private int _serverMultiStageRunnerPort = QueryConfig.DEFAULT_QUERY_RUNNER_PORT;

  @CommandLine.Option(names = {"-dataDir"}, required = false, description = "Path to directory containing data.")
  private String _dataDir = PinotConfigUtils.TMP_DIR + "data/pinotServerData";

  @CommandLine.Option(names = {"-segmentDir"}, required = false,
      description = "Path to directory containing segments.")
  private String _segmentDir = PinotConfigUtils.TMP_DIR + "data/pinotSegments";

  @CommandLine.Option(names = {"-zkAddress"}, required = false, description = "Http address of Zookeeper.")
  private String _zkAddress = DEFAULT_ZK_ADDRESS;

  @CommandLine.Option(names = {"-clusterName"}, required = false, description = "Pinot cluster name.")
  private String _clusterName = "PinotCluster";

  @CommandLine.Option(names = {"-configFileName", "-config", "-configFile", "-serverConfig", "-serverConf"},
      required = false, description = "Server Starter Config file.")
  // TODO support forbids = {"-serverHost", "-serverPort", "-dataDir", "-segmentDir"}
  private String _configFileName;

  @CommandLine.Option(names = {"-configOverride"}, required = false, split = ",")
  private Map<String, Object> _configOverrides = new HashMap<>();

  @Override
  public boolean getHelp() {
    return _help;
  }

  public String getServerHost() {
    return _serverHost;
  }

  public int getServerPort() {
    return _serverPort;
  }

  public int getServerAdminPort() {
    return _serverAdminPort;
  }

  public int getServerGrpcPort() {
    return _serverGrpcPort;
  }

  public int getServerMultiStageServerPort() {
    return _serverMultiStageServerPort;
  }

  public int getServerMultiStageRunnerPort() {
    return _serverMultiStageRunnerPort;
  }

  public String getDataDir() {
    return _dataDir;
  }

  public String getSegmentDir() {
    return _segmentDir;
  }

  public String getZkAddress() {
    return _zkAddress;
  }

  public String getClusterName() {
    return _clusterName;
  }

  public String getConfigFileName() {
    return _configFileName;
  }

  public Map<String, Object> getConfigOverrides() {
    return _configOverrides;
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

  public StartServerCommand setGrpcPort(int grpcPort) {
    _serverGrpcPort = grpcPort;
    return this;
  }

  public StartServerCommand setMultiStageServerPort(int multiStageServerPort) {
    _serverMultiStageServerPort = multiStageServerPort;
    return this;
  }

  public StartServerCommand setMultiStageRunnerPort(int multiStageRunnerPort) {
    _serverMultiStageRunnerPort = multiStageRunnerPort;
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
          + " -serverAdminPort " + _serverAdminPort + " -serverGrpcPort " + _serverGrpcPort
          + " -serverMultistageServerPort " + _serverMultiStageServerPort
          + " -serverMultistageRunnerPort " + _serverMultiStageRunnerPort + " -configFileName " + _configFileName
          + " -zkAddress " + _zkAddress);
    } else {
      return ("StartServer -clusterName " + _clusterName + " -serverHost " + _serverHost + " -serverPort " + _serverPort
          + " -serverAdminPort " + _serverAdminPort + " -serverGrpcPort " + _serverGrpcPort
          + " -serverMultistageServerPort " + _serverMultiStageServerPort
          + " -serverMultistageRunnerPort " + _serverMultiStageRunnerPort + " -dataDir " + _dataDir
          + " -segmentDir " + _segmentDir + " -zkAddress " + _zkAddress);
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
  public boolean execute()
      throws Exception {
    try {
      LOGGER.info("Executing command: " + toString());
      Map<String, Object> serverConf = getServerConf();
      StartServiceManagerCommand startServiceManagerCommand =
          new StartServiceManagerCommand().setZkAddress(_zkAddress).setClusterName(_clusterName).setPort(-1)
              .setBootstrapServices(new String[0]).addBootstrapService(ServiceRole.SERVER, serverConf);
      if (startServiceManagerCommand.execute()) {
        String pidFile = ".pinotAdminServer-" + System.currentTimeMillis() + ".pid";
        savePID(System.getProperty("java.io.tmpdir") + File.separator + pidFile);
        return true;
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception while starting Pinot server, exiting.", e);
    }
    System.exit(-1);
    return false;
  }

  protected Map<String, Object> getServerConf()
      throws ConfigurationException, SocketException, UnknownHostException {
    Map<String, Object> properties = new HashMap<>();
    if (_configFileName != null) {
      properties.putAll(PinotConfigUtils.readConfigFromFile(_configFileName));
      // Override the zkAddress and clusterName to ensure ServiceManager is connecting to the right Zookeeper and
      // Cluster.
      _zkAddress = MapUtils.getString(properties, CommonConstants.Helix.CONFIG_OF_ZOOKEEPR_SERVER, _zkAddress);
      _clusterName = MapUtils.getString(properties, CommonConstants.Helix.CONFIG_OF_CLUSTER_NAME, _clusterName);
    } else {
      properties.putAll(PinotConfigUtils
          .generateServerConf(_clusterName, _zkAddress, _serverHost, _serverPort, _serverAdminPort, _serverGrpcPort,
              _serverMultiStageServerPort, _serverMultiStageRunnerPort, _dataDir, _segmentDir));
    }
    if (_configOverrides != null) {
      properties.putAll(_configOverrides);
    }
    return properties;
  }
}
