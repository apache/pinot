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
package org.apache.pinot.common.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class for ZooKeeper SSL configuration
 */
public class ZkSSLUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZkSSLUtils.class);

  private ZkSSLUtils() {
    // Utility class
  }

  /**
   * Configures ZooKeeper SSL system properties based on the provided configuration.
   * This method sets the necessary system properties for ZooKeeper SSL connections.
   *
   * @param config Configuration containing ZooKeeper SSL settings
   */
  public static void configureSSL(PinotConfiguration config) {
    boolean sslEnabled = config.getProperty(CommonConstants.Helix.CONFIG_OF_ZOOKEEPER_SSL_ENABLED,
        CommonConstants.Helix.DEFAULT_ZOOKEEPER_SSL_ENABLED);

    if (!sslEnabled) {
      LOGGER.info("ZooKeeper SSL is disabled");
      return;
    }

    LOGGER.info("Configuring ZooKeeper SSL");

    // Enable Netty client connection socket for SSL
    String clientCnxnSocket = config.getProperty(CommonConstants.Helix.CONFIG_OF_ZOOKEEPER_CLIENT_CNXN_SOCKET,
        CommonConstants.Helix.DEFAULT_ZOOKEEPER_NETTY_CLIENT_CNXN_SOCKET);
    System.setProperty("zookeeper.clientCnxnSocket", clientCnxnSocket);

    // Configure KeyStore
    String keyStoreLocation = config.getProperty(CommonConstants.Helix.CONFIG_OF_ZOOKEEPER_SSL_KEYSTORE_LOCATION);
    if (keyStoreLocation != null) {
      System.setProperty("zookeeper.ssl.keyStore.location", keyStoreLocation);
      LOGGER.info("ZooKeeper SSL KeyStore location: {}", keyStoreLocation);
    }

    String keyStorePassword = config.getProperty(CommonConstants.Helix.CONFIG_OF_ZOOKEEPER_SSL_KEYSTORE_PASSWORD);
    if (keyStorePassword != null) {
      System.setProperty("zookeeper.ssl.keyStore.password", keyStorePassword);
      LOGGER.info("ZooKeeper SSL KeyStore password configured");
    }

    String keyStoreType = config.getProperty(CommonConstants.Helix.CONFIG_OF_ZOOKEEPER_SSL_KEYSTORE_TYPE,
        CommonConstants.Helix.DEFAULT_ZOOKEEPER_SSL_KEYSTORE_TYPE);
    System.setProperty("zookeeper.ssl.keyStore.type", keyStoreType);

    // Configure TrustStore
    String trustStoreLocation = config.getProperty(CommonConstants.Helix.CONFIG_OF_ZOOKEEPER_SSL_TRUSTSTORE_LOCATION);
    if (trustStoreLocation != null) {
      System.setProperty("zookeeper.ssl.trustStore.location", trustStoreLocation);
      LOGGER.info("ZooKeeper SSL TrustStore location: {}", trustStoreLocation);
    }

    String trustStorePassword = config.getProperty(CommonConstants.Helix.CONFIG_OF_ZOOKEEPER_SSL_TRUSTSTORE_PASSWORD);
    if (trustStorePassword != null) {
      System.setProperty("zookeeper.ssl.trustStore.password", trustStorePassword);
      LOGGER.info("ZooKeeper SSL TrustStore password configured");
    }

    String trustStoreType = config.getProperty(CommonConstants.Helix.CONFIG_OF_ZOOKEEPER_SSL_TRUSTSTORE_TYPE,
        CommonConstants.Helix.DEFAULT_ZOOKEEPER_SSL_TRUSTSTORE_TYPE);
    System.setProperty("zookeeper.ssl.trustStore.type", trustStoreType);

    LOGGER.info("ZooKeeper SSL configuration applied successfully");
  }

  /**
   * Configures ZooKeeper SSL system properties based on Java Properties.
   *
   * @param properties Properties containing ZooKeeper SSL settings
   */
  public static void configureSSL(Properties properties) {
    Map<String, Object> configMap = new HashMap<>();
    for (String key : properties.stringPropertyNames()) {
      configMap.put(key, properties.getProperty(key));
    }
    configureSSL(new PinotConfiguration(configMap));
  }

  /**
   * Checks if ZooKeeper SSL is enabled in the given configuration.
   *
   * @param config Configuration to check
   * @return true if ZooKeeper SSL is enabled, false otherwise
   */
  public static boolean isSSLEnabled(PinotConfiguration config) {
    return config.getProperty(CommonConstants.Helix.CONFIG_OF_ZOOKEEPER_SSL_ENABLED,
        CommonConstants.Helix.DEFAULT_ZOOKEEPER_SSL_ENABLED);
  }

  /**
   * Checks if ZooKeeper SSL is enabled in the given properties.
   *
   * @param properties Properties to check
   * @return true if ZooKeeper SSL is enabled, false otherwise
   */
  public static boolean isSSLEnabled(Properties properties) {
    Map<String, Object> configMap = new HashMap<>();
    for (String key : properties.stringPropertyNames()) {
      configMap.put(key, properties.getProperty(key));
    }
    return isSSLEnabled(new PinotConfiguration(configMap));
  }

  /**
   * Clear SSL configs in System Properties
   */
  public static void clearSystemPropertiesForSSL() {
    System.clearProperty("zookeeper.clientCnxnSocket");
    System.clearProperty("zookeeper.ssl.keyStore.location");
    System.clearProperty("zookeeper.ssl.keyStore.password");
    System.clearProperty("zookeeper.ssl.keyStore.type");
    System.clearProperty("zookeeper.ssl.trustStore.location");
    System.clearProperty("zookeeper.ssl.trustStore.password");
    System.clearProperty("zookeeper.ssl.trustStore.type");
  }
}
