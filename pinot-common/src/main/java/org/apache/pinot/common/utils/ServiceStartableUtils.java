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
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.services.ServiceRole;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ServiceStartableUtils {
  private ServiceStartableUtils() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceStartableUtils.class);
  private static final String CLUSTER_CONFIG_ZK_PATH_TEMPLATE = "/%s/CONFIGS/CLUSTER/%s";
  private static final String INSTANCE_CONFIG_ZK_PATH_TEMPLATE = "/%s/CONFIGS/PARTICIPANT/%s";
  private static final String PINOT_ALL_CONFIG_KEY_PREFIX = "pinot.all.";
  private static final String PINOT_TENANT_LEVEL_CONFIG_KEY_PREFIX = "pinot.tenant.";
  private static final String PINOT_INSTANCE_CONFIG_KEY_PREFIX_TEMPLATE = "pinot.%s.";

  public static void applyClusterConfig(PinotConfiguration instanceConfig, String zkAddress, String clusterName,
      ServiceRole serviceRole) {
    ZkClient zkClient = getZKClient(instanceConfig, zkAddress);
    try {
      applyClusterConfig(instanceConfig, zkClient, clusterName, serviceRole);
    } finally {
      zkClient.close();
    }
  }

  /**
   * Applies the ZK cluster config to the given instance config if it does not already exist.
   *
   * In the ZK cluster config:
   * - pinot.all.* will be replaced to role specific config, e.g. pinot.controller.* for controllers
   */
  public static void applyClusterConfig(PinotConfiguration instanceConfig, ZkClient zkClient, String clusterName,
      ServiceRole serviceRole) {
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
  }

  public static ZkClient getZKClient(PinotConfiguration instanceConfig, String zkAddress) {
    int zkClientSessionConfig =
        instanceConfig.getProperty(CommonConstants.Helix.ZkClient.ZK_CLIENT_SESSION_TIMEOUT_MS_CONFIG,
            CommonConstants.Helix.ZkClient.DEFAULT_SESSION_TIMEOUT_MS);
    int zkClientConnectionTimeoutMs =
        instanceConfig.getProperty(CommonConstants.Helix.ZkClient.ZK_CLIENT_CONNECTION_TIMEOUT_MS_CONFIG,
            CommonConstants.Helix.ZkClient.DEFAULT_CONNECT_TIMEOUT_MS);
    ZkClient zkClient = new ZkClient.Builder().setZkSerializer(new ZNRecordSerializer()).setZkServer(zkAddress)
        .setConnectionTimeout(zkClientConnectionTimeoutMs).setSessionTimeout(zkClientSessionConfig).build();
    zkClient.waitUntilConnected(zkClientConnectionTimeoutMs, TimeUnit.MILLISECONDS);
    return zkClient;
  }

  /**
   * Overrides the instance config with the tenant configs if the tenant is tagged on the instance.
   */
  public static void overrideTenantConfigs(String instanceId, ZkClient zkClient, String clusterName,
      PinotConfiguration instanceConfig) {
    ZNRecord instanceConfigZNRecord =
        zkClient.readData(String.format(INSTANCE_CONFIG_ZK_PATH_TEMPLATE, clusterName, instanceId), true);
    if (instanceConfigZNRecord == null) {
      LOGGER.warn("Failed to find instance config for instance: {}, skipping overriding tenant configs", instanceId);
      return;
    }
    InstanceConfig instanceZKConfig = new InstanceConfig(instanceConfigZNRecord);
    Set<String> tenantsRelaxedNames = instanceZKConfig.getTags().stream()
        .map(tag -> PinotConfiguration.relaxPropertyName(TagNameUtils.getTenantFromTag(tag)))
        .collect(Collectors.toSet());

    for (String key : instanceConfig.getKeys()) {
      if (key.startsWith(PINOT_TENANT_LEVEL_CONFIG_KEY_PREFIX)) {
        String instanceConfigKey = key.substring(PINOT_TENANT_LEVEL_CONFIG_KEY_PREFIX.length());
        String tenant = instanceConfigKey.substring(0, instanceConfigKey.indexOf('.'));
        String tenantKey = instanceConfigKey.substring(tenant.length() + 1);
        if (tenantsRelaxedNames.contains(tenant)) {
          instanceConfig.setProperty(tenantKey, instanceConfig.getProperty(key));
        }
      }
    }
  }

  private static void addConfigIfNotExists(PinotConfiguration instanceConfig, String key, String value) {
    if (!instanceConfig.containsKey(key)) {
      instanceConfig.setProperty(key, value);
    }
  }
}
