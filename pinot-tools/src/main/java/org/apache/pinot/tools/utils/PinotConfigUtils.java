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
package org.apache.pinot.tools.utils;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.ControllerConf.ControllerPeriodicTasksConf;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotConfigUtils {
  public static final String TMP_DIR = System.getProperty("java.io.tmpdir") + File.separator;
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotConfigUtils.class);
  private static final String CONTROLLER_CONFIG_VALIDATION_ERROR_MESSAGE_FORMAT =
      "Pinot Controller Config Validation Error: %s";

  public static Map<String, Object> generateControllerConf(String zkAddress, String clusterName, String controllerHost,
      String controllerPort, String dataDir, ControllerConf.ControllerMode controllerMode, boolean tenantIsolation)
      throws SocketException, UnknownHostException {
    if (StringUtils.isEmpty(zkAddress)) {
      throw new RuntimeException("zkAddress cannot be empty.");
    }
    if (StringUtils.isEmpty(clusterName)) {
      throw new RuntimeException("clusterName cannot be empty.");
    }
    Map<String, Object> properties = new HashMap<>();
    properties.put(ControllerConf.ZK_STR, zkAddress);
    properties.put(ControllerConf.HELIX_CLUSTER_NAME, clusterName);
    properties.put(ControllerConf.CONTROLLER_HOST, !StringUtils.isEmpty(controllerHost) ? controllerHost : NetUtils.getHostAddress());
    properties.put(ControllerConf.CONTROLLER_PORT, !StringUtils.isEmpty(controllerPort) ? controllerPort:getAvailablePort());
    properties.put(ControllerConf.DATA_DIR, !StringUtils.isEmpty(dataDir) ? dataDir : TMP_DIR + String.format("Controller_%s_%s/controller/data", controllerHost, controllerPort));
    properties.put(ControllerConf.CONTROLLER_VIP_HOST, controllerHost);
    properties.put(ControllerConf.CLUSTER_TENANT_ISOLATION_ENABLE, tenantIsolation);
    properties.put(ControllerPeriodicTasksConf.RETENTION_MANAGER_FREQUENCY_IN_SECONDS, 3600 * 6);
    properties.put(ControllerPeriodicTasksConf.OFFLINE_SEGMENT_INTERVAL_CHECKER_FREQUENCY_IN_SECONDS, 3600);
    properties.put(ControllerPeriodicTasksConf.REALTIME_SEGMENT_VALIDATION_FREQUENCY_IN_SECONDS, 3600);
    properties.put(ControllerPeriodicTasksConf.BROKER_RESOURCE_VALIDATION_FREQUENCY_IN_SECONDS, 3600);
    properties.put(ControllerConf.CONTROLLER_MODE, controllerMode.toString());

    return properties;
  }

  public static Map<String, Object> generateControllerConf(String configFileName) throws ConfigurationException {
    Map<String, Object> properties = readControllerConfigFromFile(configFileName);
    if (properties == null) {
      throw new RuntimeException("Error: Unable to find controller config file " + configFileName);
    }
    return properties;
  }

  public static Map<String, Object> readControllerConfigFromFile(String configFileName) throws ConfigurationException {
    if (configFileName == null) {
      return null;
    }

    File configFile = new File(configFileName);
    if (!configFile.exists()) {
      return null;
    }

    Map<String, Object> properties = CommonsConfigurationUtils.toMap(new PropertiesConfiguration(configFile));
    ControllerConf conf = new ControllerConf(properties);

    conf.setPinotFSFactoryClasses(null);

    if (!validateControllerConfig(conf)) {
      LOGGER.error("Failed to validate controller conf.");
      throw new ConfigurationException("Pinot Controller Conf validation failure");
    }

    return properties;
  }

  public static boolean validateControllerConfig(ControllerConf conf) throws ConfigurationException {
    if (conf == null) {
      throw new ConfigurationException(
          String.format(CONTROLLER_CONFIG_VALIDATION_ERROR_MESSAGE_FORMAT, "null conf object."));
    }

    List<String> protocols = validateControllerAccessProtocols(conf);

    if (conf.getControllerPort() == null && protocols.isEmpty()) {
      throw new ConfigurationException(String.format(CONTROLLER_CONFIG_VALIDATION_ERROR_MESSAGE_FORMAT,
          "missing controller port, please specify 'controller.port' property in config file."));
    }
    if (conf.getZkStr() == null) {
      throw new ConfigurationException(String.format(CONTROLLER_CONFIG_VALIDATION_ERROR_MESSAGE_FORMAT,
          "missing Zookeeper address, please specify 'controller.zk.str' property in config file."));
    }
    if (conf.getHelixClusterName() == null) {
      throw new ConfigurationException(String.format(CONTROLLER_CONFIG_VALIDATION_ERROR_MESSAGE_FORMAT,
          "missing helix cluster name, please specify 'controller.helix.cluster.name' property in config file."));
    }
    return true;
  }

  public static Map<String, Object> readConfigFromFile(String configFileName) throws ConfigurationException {
    if (configFileName == null) {
      return null;
    }
    File configFile = new File(configFileName);
    if (configFile.exists()) {
      return CommonsConfigurationUtils.toMap(new PropertiesConfiguration(configFile));
    }

    return null;
  }

  public static Map<String, Object> generateBrokerConf(String clusterName, String zkAddress, String brokerHost,
      int brokerPort)
      throws SocketException, UnknownHostException {
    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonConstants.Helix.CONFIG_OF_CLUSTER_NAME, clusterName);
    properties.put(CommonConstants.Helix.CONFIG_OF_ZOOKEEPR_SERVER, zkAddress);
    properties.put(CommonConstants.Broker.CONFIG_OF_BROKER_HOSTNAME, !StringUtils.isEmpty(brokerHost) ? brokerHost : NetUtils.getHostAddress());
    properties.put(CommonConstants.Helix.KEY_OF_BROKER_QUERY_PORT, brokerPort != 0 ? brokerPort : getAvailablePort());
    return properties;
  }

  public static Map<String, Object> generateServerConf(String clusterName, String zkAddress, String serverHost,
      int serverPort, int serverAdminPort, String serverDataDir, String serverSegmentDir)
      throws SocketException, UnknownHostException {
    if (serverHost == null) {
      serverHost = NetUtils.getHostAddress();
    }
    if (serverPort == 0) {
      serverPort = getAvailablePort();
    }
    if (serverAdminPort == 0) {
      serverAdminPort = getAvailablePort();
    }
    if (serverDataDir == null) {
      serverDataDir = TMP_DIR + String.format("Server_%s_%d/server/data", serverHost, serverPort);
    }
    if (serverSegmentDir == null) {
      serverSegmentDir = TMP_DIR + String.format("Server_%s_%d/server/segment", serverHost, serverPort);
    }
    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonConstants.Helix.CONFIG_OF_CLUSTER_NAME, clusterName);
    properties.put(CommonConstants.Helix.CONFIG_OF_ZOOKEEPR_SERVER, zkAddress);
    properties.put(CommonConstants.Helix.KEY_OF_SERVER_NETTY_HOST, serverHost);
    properties.put(CommonConstants.Helix.KEY_OF_SERVER_NETTY_PORT, serverPort);
    properties.put(CommonConstants.Server.CONFIG_OF_ADMIN_API_PORT, serverAdminPort);
    properties.put(CommonConstants.Server.CONFIG_OF_INSTANCE_DATA_DIR, serverDataDir);
    properties.put(CommonConstants.Server.CONFIG_OF_INSTANCE_SEGMENT_TAR_DIR, serverSegmentDir);

    return properties;
  }

  public static Map<String, Object> generateMinionConf(String clusterName, String zkAddress, String minionHost, int minionPort)
      throws SocketException, UnknownHostException {
    if (minionHost == null) {
      minionHost = NetUtils.getHostAddress();
    }
    Map<String, Object> properties = new HashMap<>();
    properties.put(CommonConstants.Helix.CONFIG_OF_CLUSTER_NAME, clusterName);
    properties.put(CommonConstants.Helix.CONFIG_OF_ZOOKEEPR_SERVER, zkAddress);
    properties.put(CommonConstants.Helix.KEY_OF_MINION_HOST, minionHost);
    properties.put(CommonConstants.Helix.KEY_OF_MINION_PORT, minionPort != 0 ? minionPort : getAvailablePort());

    return properties;
  }

  public static int getAvailablePort() {
    try {
      try (ServerSocket socket = new ServerSocket(0)) {
        return socket.getLocalPort();
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to find an available port to use", e);
    }
  }

  private static List<String> validateControllerAccessProtocols(ControllerConf conf) throws ConfigurationException {
    List<String> protocols = conf.getControllerAccessProtocols();

    if(!protocols.isEmpty()) {
      Optional<String> invalidProtocol =
          protocols.stream().filter(protocol -> !protocol.equals("http") && !protocol.equals("https")).findFirst();

      if (invalidProtocol.isPresent()) {
        throw new ConfigurationException(String.format(CONTROLLER_CONFIG_VALIDATION_ERROR_MESSAGE_FORMAT,
            invalidProtocol.get() + " is not a valid protocol for the 'controller.access.protocols' property."));
      }

      Optional<ConfigurationException> invalidPort = protocols.stream()
          .map(protocol -> validatePort(protocol, conf.getControllerAccessProtocolProperty(protocol, "port")))

          .filter(Optional::isPresent)

          .map(Optional::get)

          .findAny();

      if (invalidPort.isPresent()) {
        throw invalidPort.get();
      }
    }

    return protocols;
  }

  private static Optional<ConfigurationException> validatePort(String protocol, String port) {
    if (port == null) {
      return Optional.of(new ConfigurationException(
          String.format(CONTROLLER_CONFIG_VALIDATION_ERROR_MESSAGE_FORMAT, "missing controller " + protocol
              + " port, please fix 'controller.access.protocols." + protocol + ".port' property in config file.")));
    }

    try {
      Integer.parseInt(port);
    } catch (NumberFormatException e) {
      return Optional.of(new ConfigurationException(String.format(CONTROLLER_CONFIG_VALIDATION_ERROR_MESSAGE_FORMAT,
          port + " is not a valid port, please fix 'controller.access.protocols." + protocol
              + ".port' property in config file.")));
    }

    return Optional.empty();
  }

}
