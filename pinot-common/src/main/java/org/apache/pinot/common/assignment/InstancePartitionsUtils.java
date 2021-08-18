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

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.I0Itec.zkclient.exception.ZkException;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/**
 * Utility class for instance partitions.
 */
public class InstancePartitionsUtils {
  private InstancePartitionsUtils() {
  }

  public static final char TYPE_SUFFIX_SEPARATOR = '_';

  /**
   * Returns the name of the instance partitions for the given table name (with or without type suffix) and instance
   * partitions type.
   */
  public static String getInstancePartitionsName(String tableName, String instancePartitionsType) {
    return TableNameBuilder.extractRawTableName(tableName) + TYPE_SUFFIX_SEPARATOR + instancePartitionsType;
  }

  /**
   * Fetches the instance partitions from Helix property store if it exists, or computes it for backward-compatibility.
   */
  public static InstancePartitions fetchOrComputeInstancePartitions(HelixManager helixManager, TableConfig tableConfig,
      InstancePartitionsType instancePartitionsType) {
    String tableNameWithType = tableConfig.getTableName();

    // Fetch the instance partitions from property store if it exists
    ZkHelixPropertyStore<ZNRecord> propertyStore = helixManager.getHelixPropertyStore();
    InstancePartitions instancePartitions =
        fetchInstancePartitions(propertyStore, getInstancePartitionsName(tableNameWithType, instancePartitionsType.toString()));
    if (instancePartitions != null) {
      return instancePartitions;
    }

    // Compute the default instance partitions (for backward-compatibility)
    return computeDefaultInstancePartitions(helixManager, tableConfig, instancePartitionsType);
  }

  /**
   * Fetches the instance partitions from Helix property store.
   */
  @Nullable
  public static InstancePartitions fetchInstancePartitions(HelixPropertyStore<ZNRecord> propertyStore, String instancePartitionsName) {
    String path = ZKMetadataProvider.constructPropertyStorePathForInstancePartitions(instancePartitionsName);
    ZNRecord znRecord = propertyStore.get(path, null, AccessOption.PERSISTENT);
    return znRecord != null ? InstancePartitions.fromZNRecord(znRecord) : null;
  }

  /**
   * Computes the default instance partitions. Sort all qualified instances and rotate the list based on the table name
   * to prevent creating hotspot servers.
   * <p>Choose both enabled and disabled instances with the server tag as the qualified instances to avoid unexpected
   * data shuffling when instances get disabled.
   */
  public static InstancePartitions computeDefaultInstancePartitions(HelixManager helixManager, TableConfig tableConfig,
      InstancePartitionsType instancePartitionsType) {
    TenantConfig tenantConfig = tableConfig.getTenantConfig();
    String serverTag;
    switch (instancePartitionsType) {
      case OFFLINE:
        serverTag = TagNameUtils.extractOfflineServerTag(tenantConfig);
        break;
      case CONSUMING:
        serverTag = TagNameUtils.extractConsumingServerTag(tenantConfig);
        break;
      case COMPLETED:
        serverTag = TagNameUtils.extractCompletedServerTag(tenantConfig);
        break;
      default:
        throw new IllegalStateException();
    }
    return computeDefaultInstancePartitionsForTag(helixManager, tableConfig.getTableName(), instancePartitionsType.toString(), serverTag);
  }

  /**
   * Computes the default instance partitions using the given serverTag.
   * Sort all qualified instances and rotate the list based on the table name to prevent creating hotspot servers.
   * <p>Choose both enabled and disabled instances with the server tag as the qualified instances to avoid unexpected
   * data shuffling when instances get disabled.
   */
  public static InstancePartitions computeDefaultInstancePartitionsForTag(HelixManager helixManager, String tableNameWithType,
      String instancePartitionsType, String serverTag) {
    List<String> instances = HelixHelper.getInstancesWithTag(helixManager, serverTag);
    int numInstances = instances.size();
    Preconditions.checkState(numInstances > 0, "No instance found with tag: %s", serverTag);

    // Sort the instances and rotate the list based on the table name
    instances.sort(null);
    Collections.rotate(instances, -(Math.abs(tableNameWithType.hashCode()) % numInstances));
    InstancePartitions instancePartitions = new InstancePartitions(getInstancePartitionsName(tableNameWithType, instancePartitionsType));
    instancePartitions.setInstances(0, 0, instances);
    return instancePartitions;
  }

  /**
   * Persists the instance partitions to Helix property store.
   */
  public static void persistInstancePartitions(HelixPropertyStore<ZNRecord> propertyStore, InstancePartitions instancePartitions) {
    String path = ZKMetadataProvider.constructPropertyStorePathForInstancePartitions(instancePartitions.getInstancePartitionsName());
    if (!propertyStore.set(path, instancePartitions.toZNRecord(), AccessOption.PERSISTENT)) {
      throw new ZkException("Failed to persist instance partitions: " + instancePartitions);
    }
  }

  /**
   * Removes the instance partitions from Helix property store.
   */
  public static void removeInstancePartitions(HelixPropertyStore<ZNRecord> propertyStore, String instancePartitionsName) {
    String path = ZKMetadataProvider.constructPropertyStorePathForInstancePartitions(instancePartitionsName);
    if (!propertyStore.remove(path, AccessOption.PERSISTENT)) {
      throw new ZkException("Failed to remove instance partitions: " + instancePartitionsName);
    }
  }
}
