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
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to implement StartBroker command.
 *
 */
public class StartBrokerCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(StartBrokerCommand.class);
  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help = false;

  @Option(name = "-brokerHost", required = false, metaVar = "<String>", usage = "host name for broker.")
  private String _brokerHost;

  @Option(name = "-brokerPort", required = false, metaVar = "<int>", usage = "Broker port number to use for query.")
  private int _brokerPort = CommonConstants.Helix.DEFAULT_BROKER_QUERY_PORT;

  @Option(name = "-zkAddress", required = false, metaVar = "<http>", usage = "HTTP address of Zookeeper.")
  private String _zkAddress = DEFAULT_ZK_ADDRESS;

  @Option(name = "-clusterName", required = false, metaVar = "<String>", usage = "Pinot cluster name.")
  private String _clusterName = "PinotCluster";

  @Option(name = "-configFileName", required = false, aliases = {"-config", "-configFile", "-brokerConfig", "-brokerConf"}, metaVar = "<Config File Name>", usage = "Broker Starter Config file.", forbids = {"-brokerHost", "-brokerPort"})
  private String _configFileName;

  private Map<String, Object> _configOverrides = new HashMap<>();

  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return "StartBroker";
  }

  @Override
  public String toString() {
    if (_configFileName != null) {
      return ("StartBroker -zkAddress " + _zkAddress + " -configFileName " + _configFileName);
    } else {
      return ("StartBroker -brokerHost " + _brokerHost + " -brokerPort " + _brokerPort + " -zkAddress " + _zkAddress);
    }
  }

  @Override
  public void cleanup() {

  }

  @Override
  public String description() {
    return "Start the Pinot Broker process at the specified port";
  }

  public StartBrokerCommand setClusterName(String clusterName) {
    _clusterName = clusterName;
    return this;
  }

  public StartBrokerCommand setPort(int port) {
    _brokerPort = port;
    return this;
  }

  public StartBrokerCommand setZkAddress(String zkAddress) {
    _zkAddress = zkAddress;
    return this;
  }

  public StartBrokerCommand setConfigFileName(String configFileName) {
    _configFileName = configFileName;
    return this;
  }

  public StartBrokerCommand setConfigOverrides(Map<String, Object> configs) {
    _configOverrides = configs;
    return this;
  }

  @Override
  public boolean execute()
      throws Exception {
    try {
      LOGGER.info("Executing command: " + toString());
      Map<String, Object> brokerConf = getBrokerConf();
      StartServiceManagerCommand startServiceManagerCommand =
          new StartServiceManagerCommand().setZkAddress(_zkAddress).setClusterName(_clusterName).setPort(-1)
              .setBootstrapServices(new String[0]).addBootstrapService(ServiceRole.BROKER, brokerConf);
      startServiceManagerCommand.execute();
      String pidFile = ".pinotAdminBroker-" + System.currentTimeMillis() + ".pid";
      savePID(System.getProperty("java.io.tmpdir") + File.separator + pidFile);
      return true;
    } catch (Exception e) {
      LOGGER.error("Caught exception while starting broker, exiting", e);
      System.exit(-1);
      return false;
    }
  }

  private Map<String, Object> getBrokerConf()
      throws ConfigurationException, SocketException, UnknownHostException {
    Map<String, Object> properties = new HashMap<>();
    if (_configFileName != null) {
      properties.putAll(PinotConfigUtils.readConfigFromFile(_configFileName));
      // Override the zkAddress and clusterName to ensure ServiceManager is connecting to the right Zookeeper and Cluster.
      _zkAddress = MapUtils.getString(properties, CommonConstants.Helix.CONFIG_OF_ZOOKEEPR_SERVER, _zkAddress);
      _clusterName = MapUtils.getString(properties, CommonConstants.Helix.CONFIG_OF_CLUSTER_NAME, _clusterName);
    } else {
      properties.putAll(PinotConfigUtils.generateBrokerConf(_clusterName, _zkAddress, _brokerHost, _brokerPort));
    }
    if (_configOverrides != null) {
      properties.putAll(_configOverrides);
    }
    return properties;
  }
}
