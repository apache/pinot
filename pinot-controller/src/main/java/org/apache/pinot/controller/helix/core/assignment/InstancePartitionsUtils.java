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
import javax.annotation.Nullable;
import org.I0Itec.zkclient.exception.ZkException;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.config.TagNameUtils;
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
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    String instancePartitionsName = instancePartitionsType.getInstancePartitionsName(rawTableName);

    // Fetch the instance partitions from property store if exists
    ZkHelixPropertyStore<ZNRecord> propertyStore = helixManager.getHelixPropertyStore();
    InstancePartitions instancePartitions = fetchInstancePartitions(propertyStore, instancePartitionsName);
    if (instancePartitions != null) {
      return instancePartitions;
    }

    // Use the CONSUMING instance partitions for COMPLETED segments if exists
    if (instancePartitionsType == InstancePartitionsType.COMPLETED) {
      InstancePartitions consumingInstancePartitions = fetchInstancePartitions(propertyStore,
          InstancePartitionsType.CONSUMING.getInstancePartitionsName(rawTableName));
      if (consumingInstancePartitions != null) {
        return consumingInstancePartitions;
      }
    }

    // Compute the instance partitions (for backward compatible)
    // Sort all enabled instances with the server tag, rotate the list based on the table name to prevent creating
    // hotspot servers
    String serverTag =
        TagNameUtils.getServerTagFromTableConfigAndInstancePartitionsType(tableConfig, instancePartitionsType);
    List<String> instances = HelixHelper.getEnabledInstancesWithTag(helixManager, serverTag);
    instances.sort(null);
    int numInstances = instances.size();
    Collections.rotate(instances, -(Math.abs(tableNameWithType.hashCode()) % numInstances));
    instancePartitions = new InstancePartitions(instancePartitionsName);
    instancePartitions.setInstances(0, 0, instances);
    return instancePartitions;
  }

  /**
   * Fetches the instance partitions from Helix property store.
   */
  @Nullable
  public static InstancePartitions fetchInstancePartitions(HelixPropertyStore<ZNRecord> propertyStore,
      String instancePartitionsName) {
    String path = ZKMetadataProvider.constructPropertyStorePathForInstancePartitions(instancePartitionsName);
    ZNRecord znRecord = propertyStore.get(path, null, AccessOption.PERSISTENT);
    return znRecord != null ? InstancePartitions.fromZNRecord(znRecord) : null;
  }

  /**
   * Persists the instance partitions to Helix property store.
   */
  public static void persistInstancePartitions(HelixPropertyStore<ZNRecord> propertyStore,
      InstancePartitions instancePartitions) {
    String path = ZKMetadataProvider.constructPropertyStorePathForInstancePartitions(instancePartitions.getName());
    if (!propertyStore.set(path, instancePartitions.toZNRecord(), AccessOption.PERSISTENT)) {
      throw new ZkException("Failed to persist instance partitions: " + instancePartitions.getName());
    }
  }

  /**
   * Removes the instance partitions from Helix property store.
   */
  public static void removeInstancePartitions(HelixPropertyStore<ZNRecord> propertyStore,
      String instancePartitionsName) {
    String path = ZKMetadataProvider.constructPropertyStorePathForInstancePartitions(instancePartitionsName);
    if (!propertyStore.remove(path, AccessOption.PERSISTENT)) {
      throw new ZkException("Failed to remove instance partitions: " + instancePartitionsName);
    }
  }
}
