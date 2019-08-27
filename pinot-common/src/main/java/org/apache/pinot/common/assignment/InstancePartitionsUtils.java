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
package org.apache.pinot.common.assignment;

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
import org.apache.pinot.common.utils.helix.HelixHelper;


/**
 * Utility class for instance partitions.
 */
public class InstancePartitionsUtils {
  private InstancePartitionsUtils() {
  }

  /**
   * Returns the name of the instance partitions for the given table name (with or without type suffix) and instance
   * partitions type.
   */
  public static String getInstancePartitionsName(String tableName, InstancePartitionsType instancePartitionsType) {
    return instancePartitionsType.getInstancePartitionsName(TableNameBuilder.extractRawTableName(tableName));
  }

  /**
   * Fetches the instance partitions from Helix property store if exists, or computes it for backward-compatibility.
   */
  public static InstancePartitions fetchOrComputeInstancePartitions(HelixManager helixManager, TableConfig tableConfig,
      InstancePartitionsType instancePartitionsType) {
    String tableNameWithType = tableConfig.getTableName();

    // Fetch the instance partitions from property store if exists
    ZkHelixPropertyStore<ZNRecord> propertyStore = helixManager.getHelixPropertyStore();
    InstancePartitions instancePartitions =
        fetchInstancePartitions(propertyStore, getInstancePartitionsName(tableNameWithType, instancePartitionsType));
    if (instancePartitions != null) {
      return instancePartitions;
    }

    // Use the CONSUMING instance partitions for COMPLETED segments if exists
    if (instancePartitionsType == InstancePartitionsType.COMPLETED) {
      InstancePartitions consumingInstancePartitions = fetchInstancePartitions(propertyStore,
          getInstancePartitionsName(tableNameWithType, InstancePartitionsType.CONSUMING));
      if (consumingInstancePartitions != null) {
        return consumingInstancePartitions;
      }
    }

    // Compute the default instance partitions (for backward-compatibility)
    return computeDefaultInstancePartitions(helixManager, tableConfig, instancePartitionsType);
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
   * Computes the default instance partitions.
   * <p>For backward-compatibility, sort all enabled instances with the server tag, rotate the list based on the table
   * name name to prevent creating hotspot servers.
   */
  public static InstancePartitions computeDefaultInstancePartitions(HelixManager helixManager, TableConfig tableConfig,
      InstancePartitionsType instancePartitionsType) {
    String tableNameWithType = tableConfig.getTableName();
    String serverTag =
        TagNameUtils.getServerTagFromTableConfigAndInstancePartitionsType(tableConfig, instancePartitionsType);
    List<String> instances = HelixHelper.getEnabledInstancesWithTag(helixManager, serverTag);
    instances.sort(null);
    int numInstances = instances.size();
    Collections.rotate(instances, -(Math.abs(tableNameWithType.hashCode()) % numInstances));
    InstancePartitions instancePartitions =
        new InstancePartitions(getInstancePartitionsName(tableNameWithType, instancePartitionsType));
    instancePartitions.setInstances(0, 0, instances);
    return instancePartitions;
  }

  /**
   * Persists the instance partitions to Helix property store.
   */
  public static void persistInstancePartitions(HelixPropertyStore<ZNRecord> propertyStore,
      InstancePartitions instancePartitions) {
    String path = ZKMetadataProvider
        .constructPropertyStorePathForInstancePartitions(instancePartitions.getInstancePartitionsName());
    if (!propertyStore.set(path, instancePartitions.toZNRecord(), AccessOption.PERSISTENT)) {
      throw new ZkException("Failed to persist instance partitions: " + instancePartitions);
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
