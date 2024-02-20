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

import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.services.ServiceRole;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.CONFIG_OF_TIMEZONE;
import static org.apache.pinot.spi.utils.CommonConstants.DEFAULT_TIMEZONE;


public class ServiceStartableUtils {
  private ServiceStartableUtils() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceStartableUtils.class);
  private static final String CLUSTER_CONFIG_ZK_PATH_TEMPLATE = "/%s/CONFIGS/CLUSTER/%s";
  private static final String PINOT_ALL_CONFIG_KEY_PREFIX = "pinot.all.";
  private static final String PINOT_INSTANCE_CONFIG_KEY_PREFIX_TEMPLATE = "pinot.%s.";
  protected static String _timeZone;

  /**
   * Applies the ZK cluster config to the given instance config if it does not already exist.
   *
   * In the ZK cluster config:
   * - pinot.all.* will be replaced to role specific config, e.g. pinot.controller.* for controllers
   */
  public static void applyClusterConfig(PinotConfiguration instanceConfig, String zkAddress, String clusterName,
      ServiceRole serviceRole) {
    int zkClientSessionConfig =
        instanceConfig.getProperty(CommonConstants.Helix.ZkClient.ZK_CLIENT_SESSION_TIMEOUT_MS_CONFIG,
            CommonConstants.Helix.ZkClient.DEFAULT_SESSION_TIMEOUT_MS);
    int zkClientConnectionTimeoutMs =
        instanceConfig.getProperty(CommonConstants.Helix.ZkClient.ZK_CLIENT_CONNECTION_TIMEOUT_MS_CONFIG,
            CommonConstants.Helix.ZkClient.DEFAULT_CONNECT_TIMEOUT_MS);
    ZkClient zkClient = new ZkClient.Builder()
        .setZkSerializer(new ZNRecordSerializer())
        .setZkServer(zkAddress)
        .setConnectionTimeout(zkClientConnectionTimeoutMs)
        .setSessionTimeout(zkClientSessionConfig)
        .build();
    zkClient.waitUntilConnected(zkClientConnectionTimeoutMs, TimeUnit.MILLISECONDS);

    setupTimezone(instanceConfig);
    try {
      ZNRecord clusterConfigZNRecord =
          zkClient.readData(String.format(CLUSTER_CONFIG_ZK_PATH_TEMPLATE, clusterName, clusterName), true);
      if (clusterConfigZNRecord == null) {
        LOGGER.warn("Failed to find cluster config for cluster: {}, skipping applying cluster config", clusterName);
        return;
      }

      Map<String, String> clusterConfigs = clusterConfigZNRecord.getSimpleFields();
      String instanceConfigKeyPrefix =
          String.format(PINOT_INSTANCE_CONFIG_KEY_PREFIX_TEMPLATE, serviceRole.name().toLowerCase());
      for (Map.Entry<String, String> entry : clusterConfigs.entrySet()) {
        String key = entry.getKey();
        String value = entry.getValue();
        if (key.startsWith(PINOT_ALL_CONFIG_KEY_PREFIX)) {
          String instanceConfigKey = instanceConfigKeyPrefix + key.substring(PINOT_ALL_CONFIG_KEY_PREFIX.length());
          addConfigIfNotExists(instanceConfig, instanceConfigKey, value);
        } else {
          // TODO: Currently it puts all keys to the instance config. Consider standardizing instance config keys and
          //       only put keys with the instance config key prefix.
          addConfigIfNotExists(instanceConfig, key, value);
        }
      }
    } finally {
      zkClient.close();
    }
  }

  private static void addConfigIfNotExists(PinotConfiguration instanceConfig, String key, String value) {
    if (!instanceConfig.containsKey(key)) {
      instanceConfig.setProperty(key, value);
    }
  }

  private static void setupTimezone(PinotConfiguration instanceConfig) {
    _timeZone = instanceConfig.getProperty(CONFIG_OF_TIMEZONE, DEFAULT_TIMEZONE);
    System.setProperty("user.timezone", _timeZone);
    LOGGER.info("Timezone: {}", _timeZone);
  }
}
