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
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.utils.CommonConstants.Helix;
import org.apache.pinot.spi.config.instance.Instance;


public class InstanceUtils {
  private InstanceUtils() {
  }

  public static final String POOL_KEY = "pool";
  public static final String GRPC_PORT_KEY = "grpcPort";

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
    InstanceConfig instanceConfig = InstanceConfig.toInstanceConfig(getHelixInstanceId(instance));
    List<String> tags = instance.getTags();
    if (tags != null) {
      for (String tag : tags) {
        instanceConfig.addTag(tag);
      }
    }
    Map<String, Integer> pools = instance.getPools();
    if (pools != null && !pools.isEmpty()) {
      Map<String, String> mapValue = new TreeMap<>();
      for (Map.Entry<String, Integer> entry : pools.entrySet()) {
        mapValue.put(entry.getKey(), entry.getValue().toString());
      }
      instanceConfig.getRecord().setMapField(POOL_KEY, mapValue);
    }
    instanceConfig.getRecord().setSimpleField(GRPC_PORT_KEY, Integer.toString(instance.getGrpcPort()));
    return instanceConfig;
  }
}
