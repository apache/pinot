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
        CommonConstants.Helix.ZOOKEEPER_NETTY_CLIENT_CNXN_SOCKET);
    System.setProperty("zookeeper.clientCnxnSocket", clientCnxnSocket);
    System.setProperty("zookeeper.client.secure", "true");

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

    // Configure SSL Protocol
    String protocol = config.getProperty(CommonConstants.Helix.CONFIG_OF_ZOOKEEPER_SSL_PROTOCOL,
        CommonConstants.Helix.DEFAULT_ZOOKEEPER_SSL_PROTOCOL);
    System.setProperty("zookeeper.ssl.protocol", protocol);

    String enabledProtocols = config.getProperty(CommonConstants.Helix.CONFIG_OF_ZOOKEEPER_SSL_ENABLED_PROTOCOLS);
    if (enabledProtocols != null) {
      System.setProperty("zookeeper.ssl.enabledProtocols", enabledProtocols);
    }

    String cipherSuites = config.getProperty(CommonConstants.Helix.CONFIG_OF_ZOOKEEPER_SSL_CIPHER_SUITES);
    if (cipherSuites != null) {
      System.setProperty("zookeeper.ssl.ciphersuites", cipherSuites);
    }

    // Configure SSL Context Supplier
    String contextSupplierClass =
        config.getProperty(CommonConstants.Helix.CONFIG_OF_ZOOKEEPER_SSL_CONTEXT_SUPPLIER_CLASS);
    if (contextSupplierClass != null) {
      System.setProperty("zookeeper.ssl.context.supplier.class", contextSupplierClass);
    }

    // Configure SSL Verification
    boolean hostnameVerification =
        config.getProperty(CommonConstants.Helix.CONFIG_OF_ZOOKEEPER_SSL_HOSTNAME_VERIFICATION,
            CommonConstants.Helix.DEFAULT_ZOOKEEPER_SSL_HOSTNAME_VERIFICATION);
    System.setProperty("zookeeper.ssl.hostnameVerification", String.valueOf(hostnameVerification));

    // Configure Certificate Revocation
    boolean crl = config.getProperty(CommonConstants.Helix.CONFIG_OF_ZOOKEEPER_SSL_CRL,
        CommonConstants.Helix.DEFAULT_ZOOKEEPER_SSL_CRL);
    System.setProperty("zookeeper.ssl.crl", String.valueOf(crl));

    boolean ocsp = config.getProperty(CommonConstants.Helix.CONFIG_OF_ZOOKEEPER_SSL_OCSP,
        CommonConstants.Helix.DEFAULT_ZOOKEEPER_SSL_OCSP);
    System.setProperty("zookeeper.ssl.ocsp", String.valueOf(ocsp));

    // Configure SSL Handshake Timeout
    long handshakeTimeout =
        config.getProperty(CommonConstants.Helix.CONFIG_OF_ZOOKEEPER_SSL_HANDSHAKE_DETECTION_TIMEOUT_MS,
            CommonConstants.Helix.DEFAULT_ZOOKEEPER_SSL_HANDSHAKE_DETECTION_TIMEOUT_MS);
    System.setProperty("zookeeper.ssl.handshakeDetectionTimeoutMillis", String.valueOf(handshakeTimeout));

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
}
