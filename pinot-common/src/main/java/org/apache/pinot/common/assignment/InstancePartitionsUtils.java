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
import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.utils.config.TableConfigUtils;
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
  public static final String TIER_SUFFIX = "__TIER__";

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
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);

    // If table has pre-configured instance partitions.
    if (TableConfigUtils.hasPreConfiguredInstancePartitions(tableConfig, instancePartitionsType)) {
      return fetchInstancePartitionsWithRename(helixManager.getHelixPropertyStore(),
          tableConfig.getInstancePartitionsMap().get(instancePartitionsType),
          instancePartitionsType.getInstancePartitionsName(rawTableName));
    }

    // Fetch the instance partitions from property store if it exists
    ZkHelixPropertyStore<ZNRecord> propertyStore = helixManager.getHelixPropertyStore();
    InstancePartitions instancePartitions = fetchInstancePartitions(propertyStore,
        getInstancePartitionsName(tableNameWithType, instancePartitionsType.toString()));
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
  public static InstancePartitions fetchInstancePartitions(HelixPropertyStore<ZNRecord> propertyStore,
      String instancePartitionsName) {
    String path = ZKMetadataProvider.constructPropertyStorePathForInstancePartitions(instancePartitionsName);
    ZNRecord znRecord = propertyStore.get(path, null, AccessOption.PERSISTENT);
    return znRecord != null ? InstancePartitions.fromZNRecord(znRecord) : null;
  }

  public static String getInstancePartitionsNameForTier(String tableName, String tierName) {
    return TableNameBuilder.extractRawTableName(tableName) + TIER_SUFFIX + tierName;
  }


  /**
   * Gets the instance partitions with the given name, and returns a re-named copy of the same.
   * This method is useful when we use a table with instancePartitionsMap since in that case
   * the value of a table's instance partitions are copied over from an existing instancePartitions.
   */
  public static InstancePartitions fetchInstancePartitionsWithRename(HelixPropertyStore<ZNRecord> propertyStore,
      String instancePartitionsName, String newName) {
    InstancePartitions instancePartitions = fetchInstancePartitions(propertyStore, instancePartitionsName);
    Preconditions.checkNotNull(instancePartitions,
        String.format("Couldn't find instance-partitions with name=%s. Cannot rename to %s",
            instancePartitionsName, newName));
    return instancePartitions.withName(newName);
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
    return computeDefaultInstancePartitionsForTag(helixManager, tableConfig.getTableName(),
        instancePartitionsType.toString(), serverTag);
  }

  /**
   * Computes the default instance partitions using the given serverTag.
   * Sort all qualified instances and rotate the list based on the table name to prevent creating hotspot servers.
   * <p>Choose both enabled and disabled instances with the server tag as the qualified instances to avoid unexpected
   * data shuffling when instances get disabled.
   */
  public static InstancePartitions computeDefaultInstancePartitionsForTag(HelixManager helixManager,
      String tableNameWithType, String instancePartitionsType, String serverTag) {
    List<String> instances = HelixHelper.getInstancesWithTag(helixManager, serverTag);
    int numInstances = instances.size();
    Preconditions.checkState(numInstances > 0, "No instance found with tag: %s", serverTag);

    // Sort the instances and rotate the list based on the table name
    instances.sort(null);
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

  public static void removeTierInstancePartitions(HelixPropertyStore<ZNRecord> propertyStore,
      String tableNameWithType) {
    List<InstancePartitions> instancePartitions = ZKMetadataProvider.getAllInstancePartitions(propertyStore);
    instancePartitions.stream().filter(instancePartition -> instancePartition.getInstancePartitionsName()
            .startsWith(TableNameBuilder.extractRawTableName(tableNameWithType) + TIER_SUFFIX))
        .forEach(instancePartition -> {
          removeInstancePartitions(propertyStore, instancePartition.getInstancePartitionsName());
        });
  }
}
