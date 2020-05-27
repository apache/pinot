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
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.NetUtil;
import org.apache.pinot.controller.ControllerConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotConfigUtils {

  public static final String TMP_DIR = System.getProperty("java.io.tmpdir") + File.separator;
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotConfigUtils.class);
  private static final String CONTROLLER_CONFIG_VALIDATION_ERROR_MESSAGE_FORMAT =
      "Pinot Controller Config Validation Error: %s";

  public static ControllerConf generateControllerConf(String zkAddress, String clusterName, String controllerHost,
      String controllerPort, String dataDir, ControllerConf.ControllerMode controllerMode, boolean tenantIsolation)
      throws SocketException, UnknownHostException {
    if (StringUtils.isEmpty(zkAddress)) {
      throw new RuntimeException("zkAddress cannot be empty.");
    }
    if (StringUtils.isEmpty(clusterName)) {
      throw new RuntimeException("clusterName cannot be empty.");
    }
    ControllerConf conf = new ControllerConf();
    conf.setZkStr(zkAddress);
    conf.setHelixClusterName(clusterName);
    if (StringUtils.isEmpty(controllerHost)) {
      controllerHost = NetUtil.getHostAddress();
    }
    conf.setControllerHost(controllerHost);
    if (StringUtils.isEmpty(controllerPort)) {
      controllerPort = Integer.toString(getAvailablePort());
    }
    conf.setControllerPort(controllerPort);

    if (StringUtils.isEmpty(dataDir)) {
      dataDir = TMP_DIR + String.format("Controller_%s_%s/controller/data", controllerHost, controllerPort);
    }
    conf.setDataDir(dataDir);
    conf.setControllerVipHost(controllerHost);
    conf.setTenantIsolationEnabled(tenantIsolation);

    conf.setRetentionControllerFrequencyInSeconds(3600 * 6);
    conf.setOfflineSegmentIntervalCheckerFrequencyInSeconds(3600);
    conf.setRealtimeSegmentValidationFrequencyInSeconds(3600);
    conf.setBrokerResourceValidationFrequencyInSeconds(3600);

    conf.setControllerMode(controllerMode);
    return conf;
  }

  public static ControllerConf generateControllerConf(String configFileName)
      throws ConfigurationException {
    ControllerConf conf = readControllerConfigFromFile(configFileName);
    if (conf == null) {
      throw new RuntimeException("Error: Unable to find controller config file " + configFileName);
    }
    return conf;
  }

  public static ControllerConf readControllerConfigFromFile(String configFileName)
      throws ConfigurationException {
    if (configFileName == null) {
      return null;
    }

    File configFile = new File(configFileName);
    if (!configFile.exists()) {
      return null;
    }

    ControllerConf conf = new ControllerConf(configFile);
    conf.setPinotFSFactoryClasses(null);
    if (!validateControllerConfig(conf)) {
      LOGGER.error("Failed to validate controller conf.");
      throw new ConfigurationException("Pinot Controller Conf validation failure");
    }
    return (validateControllerConfig(conf)) ? conf : null;
  }

  public static boolean validateControllerConfig(ControllerConf conf)
      throws ConfigurationException {
    if (conf == null) {
      throw new ConfigurationException(
          String.format(CONTROLLER_CONFIG_VALIDATION_ERROR_MESSAGE_FORMAT, "null conf object."));
    }
    if (conf.getControllerPort() == null) {
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

  public static PropertiesConfiguration readConfigFromFile(String configFileName)
      throws ConfigurationException {
    if (configFileName == null) {
      return null;
    }
    File configFile = new File(configFileName);
    if (configFile.exists()) {
      return new PropertiesConfiguration(configFile);
    }
    return null;
  }

  public static Configuration generateBrokerConf(int brokerPort) {
    if (brokerPort == 0) {
      brokerPort = getAvailablePort();
    }
    Configuration brokerConf = new BaseConfiguration();
    brokerConf.addProperty(CommonConstants.Helix.KEY_OF_BROKER_QUERY_PORT, brokerPort);
    return brokerConf;
  }

  public static Configuration generateServerConf(String serverHost, int serverPort, int serverAdminPort,
      String serverDataDir, String serverSegmentDir)
      throws SocketException, UnknownHostException {
    if (serverHost == null) {
      serverHost = NetUtil.getHostAddress();
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
    Configuration serverConf = new PropertiesConfiguration();
    serverConf.addProperty(CommonConstants.Helix.KEY_OF_SERVER_NETTY_HOST, serverHost);
    serverConf.addProperty(CommonConstants.Helix.KEY_OF_SERVER_NETTY_PORT, serverPort);
    serverConf.addProperty(CommonConstants.Server.CONFIG_OF_ADMIN_API_PORT, serverAdminPort);
    serverConf.addProperty(CommonConstants.Server.CONFIG_OF_INSTANCE_DATA_DIR, serverDataDir);
    serverConf.addProperty(CommonConstants.Server.CONFIG_OF_INSTANCE_SEGMENT_TAR_DIR, serverSegmentDir);
    return serverConf;
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
}
