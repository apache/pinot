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
import org.apache.pinot.spi.services.ServiceRole;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.utils.PinotConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


/**
 * Class to implement StartMinion command.
 *
 */
@CommandLine.Command(name = "StartMinion", description = "Start the Pinot Minion process at the specified port",
    mixinStandardHelpOptions = true)
public class StartMinionCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(StartMinionCommand.class);
  @CommandLine.Option(names = {"-minionHost"}, required = false, description = "Host name for minion.")
  private String _minionHost;
  @CommandLine.Option(names = {"-minionPort"}, required = false, description = "Port number to start the minion at.")
  private int _minionPort = CommonConstants.Minion.DEFAULT_HELIX_PORT;
  @CommandLine.Option(names = {"-zkAddress"}, required = false, description = "HTTP address of Zookeeper.")
  private String _zkAddress = DEFAULT_ZK_ADDRESS;
  @CommandLine.Option(names = {"-clusterName"}, required = false, description = "Pinot cluster name.")
  private String _clusterName = "PinotCluster";
  @CommandLine.Option(names = {"-configFileName"}, required = false,
      description = "Minion Starter Config file.")
      // TODO: support forbids = {"-minionHost", "-minionPort"}
  private String _configFileName;

  private Map<String, Object> _configOverrides = new HashMap<>();

  public StartMinionCommand setConfigOverrides(Map<String, Object> configs) {
    _configOverrides = configs;
    return this;
  }

  public String getMinionHost() {
    return _minionHost;
  }

  public int getMinionPort() {
    return _minionPort;
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

  @Override
  public String getName() {
    return "StartMinion";
  }

  @Override
  public String toString() {
    if (_configFileName != null) {
      return ("StartMinion -zkAddress " + _zkAddress + " -configFileName " + _configFileName);
    } else {
      return ("StartMinion -minionHost " + _minionHost + " -minionPort " + _minionPort + " -zkAddress " + _zkAddress);
    }
  }

  @Override
  public void cleanup() {
  }

  @Override
  public boolean execute()
      throws Exception {
    try {
      LOGGER.info("Executing command: " + toString());
      Map<String, Object> minionConf = getMinionConf();
      StartServiceManagerCommand startServiceManagerCommand =
          new StartServiceManagerCommand().setZkAddress(_zkAddress).setClusterName(_clusterName).setPort(-1)
              .setBootstrapServices(new String[0]).addBootstrapService(ServiceRole.MINION, minionConf);
      if (startServiceManagerCommand.execute()) {
        String pidFile = ".pinotAdminMinion-" + System.currentTimeMillis() + ".pid";
        savePID(System.getProperty("java.io.tmpdir") + File.separator + pidFile);
        return true;
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception while starting minion, exiting", e);
    }
    System.exit(-1);
    return false;
  }

  protected Map<String, Object> getMinionConf()
      throws ConfigurationException, SocketException, UnknownHostException {
    Map<String, Object> properties = new HashMap<>();
    if (_configFileName != null) {
      properties.putAll(PinotConfigUtils.readConfigFromFile(_configFileName));
      // Override the zkAddress and clusterName to ensure ServiceManager is connecting to the right Zookeeper and
      // Cluster.
      _zkAddress = MapUtils.getString(properties, CommonConstants.Helix.CONFIG_OF_ZOOKEEPR_SERVER, _zkAddress);
      _clusterName = MapUtils.getString(properties, CommonConstants.Helix.CONFIG_OF_CLUSTER_NAME, _clusterName);
    } else {
      properties.putAll(PinotConfigUtils.generateMinionConf(_clusterName, _zkAddress, _minionHost, _minionPort));
    }
    if (_configOverrides != null) {
      properties.putAll(_configOverrides);
    }
    return properties;
  }

  public StartMinionCommand setMinionHost(String minionHost) {
    _minionHost = minionHost;
    return this;
  }

  public StartMinionCommand setMinionPort(int minionPort) {
    _minionPort = minionPort;
    return this;
  }

  public StartMinionCommand setZkAddress(String zkAddress) {
    _zkAddress = zkAddress;
    return this;
  }

  public StartMinionCommand setClusterName(String clusterName) {
    _clusterName = clusterName;
    return this;
  }

  public void setConfigFileName(String configFileName) {
    _configFileName = configFileName;
  }
}
