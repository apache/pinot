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
package org.apache.pinot.common.utils.config;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.config.instance.Instance;
import org.apache.pinot.spi.utils.CommonConstants.Helix;


public class InstanceUtils {
  private InstanceUtils() {
  }

  public static final String POOL_KEY = "pool";

  /**
   * Returns the Helix instance id (e.g. {@code Server_localhost_1234}) for the given instance.
   */
  public static String getHelixInstanceId(Instance instance) {
    String prefix;
    switch (instance.getType()) {
      case CONTROLLER:
        prefix = Helix.PREFIX_OF_CONTROLLER_INSTANCE;
        break;
      case BROKER:
        prefix = Helix.PREFIX_OF_BROKER_INSTANCE;
        break;
      case SERVER:
        prefix = Helix.PREFIX_OF_SERVER_INSTANCE;
        break;
      case MINION:
        prefix = Helix.PREFIX_OF_MINION_INSTANCE;
        break;
      default:
        throw new IllegalStateException();
    }
    return prefix + instance.getHost() + "_" + instance.getPort();
  }

  /**
   * Returns the Helix InstanceConfig for the given instance.
   */
  public static InstanceConfig toHelixInstanceConfig(Instance instance) {
    InstanceConfig instanceConfig = new InstanceConfig(getHelixInstanceId(instance));
    instanceConfig.setInstanceEnabled(true);
    updateHelixInstanceConfig(instanceConfig, instance);
    return instanceConfig;
  }

  /**
   * Updates the Helix InstanceConfig with the given instance configuration. Leaves the fields not included in the
   * instance configuration unchanged.
   */
  public static void updateHelixInstanceConfig(InstanceConfig instanceConfig, Instance instance) {
    ZNRecord znRecord = instanceConfig.getRecord();

    Map<String, String> simpleFields = znRecord.getSimpleFields();
    simpleFields.put(InstanceConfig.InstanceConfigProperty.HELIX_HOST.name(), instance.getHost());
    simpleFields.put(InstanceConfig.InstanceConfigProperty.HELIX_PORT.name(), Integer.toString(instance.getPort()));
    int grpcPort = instance.getGrpcPort();
    if (grpcPort > 0) {
      simpleFields.put(Helix.Instance.GRPC_PORT_KEY, Integer.toString(grpcPort));
    } else {
      simpleFields.remove(Helix.Instance.GRPC_PORT_KEY);
    }
    int adminPort = instance.getAdminPort();
    if (adminPort > 0) {
      simpleFields.put(Helix.Instance.ADMIN_PORT_KEY, Integer.toString(adminPort));
    } else {
      simpleFields.remove(Helix.Instance.ADMIN_PORT_KEY);
    }
    boolean queriesDisabled = instance.isQueriesDisabled();
    if (queriesDisabled) {
      simpleFields.put(Helix.QUERIES_DISABLED, Boolean.toString(true));
    } else {
      simpleFields.remove(Helix.QUERIES_DISABLED);
    }

    Map<String, List<String>> listFields = znRecord.getListFields();
    List<String> tags = instance.getTags();
    String tagsKey = InstanceConfig.InstanceConfigProperty.TAG_LIST.name();
    if (CollectionUtils.isNotEmpty(tags)) {
      listFields.put(tagsKey, tags);
    } else {
      listFields.remove(tagsKey);
    }

    Map<String, Map<String, String>> mapFields = znRecord.getMapFields();
    Map<String, Integer> pools = instance.getPools();
    if (MapUtils.isNotEmpty(pools)) {
      Map<String, String> mapValue = new TreeMap<>();
      for (Map.Entry<String, Integer> entry : pools.entrySet()) {
        mapValue.put(entry.getKey(), entry.getValue().toString());
      }
      mapFields.put(POOL_KEY, mapValue);
    } else {
      mapFields.remove(POOL_KEY);
    }
  }
}
