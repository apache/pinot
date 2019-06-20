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
package org.apache.pinot.controller.helix.core.assignment;

import java.util.Collections;
import java.util.List;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.pinot.common.config.OfflineTagConfig;
import org.apache.pinot.common.config.RealtimeTagConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.utils.InstancePartitionsType;
import org.apache.pinot.common.utils.helix.HelixHelper;


/**
 * Utility class for instance partitions.
 */
public class InstancePartitionsUtils {
  private InstancePartitionsUtils() {
  }

  /**
   * Fetches the instance partitions from Helix property store if exists, or computes it for backward compatibility.
   */
  public static InstancePartitions fetchOrComputeInstancePartitions(HelixManager helixManager, TableConfig tableConfig,
      InstancePartitionsType instancePartitionsType) {
    String tableNameWithType = tableConfig.getTableName();
    String instancePartitionsName =
        instancePartitionsType.getInstancePartitionsName(TableNameBuilder.extractRawTableName(tableNameWithType));

    // Fetch the instance partitions from property store if exists
    String path = ZKMetadataProvider.constructPropertyStorePathForInstancePartitions(instancePartitionsName);
    ZNRecord znRecord = helixManager.getHelixPropertyStore().get(path, null, AccessOption.PERSISTENT);
    if (znRecord != null) {
      return InstancePartitions.fromZNRecord(znRecord);
    }

    // Compute the instance partitions (for backward compatible)
    // Sort all enabled instances with the server tag, rotate the list based on the table name to prevent creating
    // hotspot servers
    InstancePartitions instancePartitions = new InstancePartitions(instancePartitionsName);
    List<InstanceConfig> instanceConfigs = HelixHelper.getInstanceConfigs(helixManager);
    String serverTag;
    switch (instancePartitionsType) {
      case OFFLINE:
        serverTag = new OfflineTagConfig(tableConfig).getOfflineServerTag();
        break;
      case CONSUMING:
        serverTag = new RealtimeTagConfig(tableConfig).getConsumingServerTag();
        break;
      case COMPLETED:
        serverTag = new RealtimeTagConfig(tableConfig).getCompletedServerTag();
        break;
      default:
        throw new IllegalArgumentException();
    }
    List<String> instances = HelixHelper.getEnabledInstancesWithTag(instanceConfigs, serverTag);
    instances.sort(null);
    int numInstances = instances.size();
    Collections.rotate(instances, -(Math.abs(tableNameWithType.hashCode()) % numInstances));
    instancePartitions.setInstances(0, 0, instances);
    return instancePartitions;
  }

  /**
   * Persists the instance partitions to Helix property store.
   *
   * @return true if the instance partitions was successfully persisted, false otherwise
   */
  public static boolean persistInstancePartitions(HelixPropertyStore<ZNRecord> propertyStore,
      InstancePartitions instancePartitions) {
    String path = ZKMetadataProvider.constructPropertyStorePathForInstancePartitions(instancePartitions.getName());
    return propertyStore.set(path, instancePartitions.toZNRecord(), AccessOption.PERSISTENT);
  }
}
