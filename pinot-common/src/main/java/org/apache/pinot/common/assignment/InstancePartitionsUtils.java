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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class for instance partitions.
 */
public class InstancePartitionsUtils {
  private InstancePartitionsUtils() {
  }

  public static final char TYPE_SUFFIX_SEPARATOR = '_';
  public static final String TIER_SUFFIX = "__TIER__";
  public static final String IDEAL_STATE_IP_PREFIX = "INSTANCE_PARTITIONS__";
  public static final String IDEAL_STATE_IP_SEPARATOR = "__";

  private static final Logger LOGGER = LoggerFactory.getLogger(InstancePartitionsUtils.class);

  /**
   * Takes in an InstancePartitions key like "1_1" and returns a pair of integers {1, 1} with the left integer
   * representing the partition ID and the right integer representing the replica group ID.
   */
  public static Pair<Integer, Integer> getPartitionIdAndReplicaGroupId(String key) {
    int separatorIndex = key.indexOf(InstancePartitions.PARTITION_REPLICA_GROUP_SEPARATOR);
    int partitionId = Integer.parseInt(key.substring(0, separatorIndex));
    int replicaGroupId = Integer.parseInt(key.substring(separatorIndex + 1));
    return Pair.of(partitionId, replicaGroupId);
  }

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

    // If table has pre-configured table-level instance partitions
    if (shouldFetchPreConfiguredInstancePartitions(tableConfig, instancePartitionsType)) {
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
        String.format("Couldn't find instance-partitions with name=%s. Cannot rename to %s", instancePartitionsName,
            newName));
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
    return computeDefaultInstancePartitionsForTag(helixManager, tableConfig, instancePartitionsType.toString(),
        serverTag);
  }

  /**
   * Computes the default instance partitions using the given serverTag.
   * Sort all qualified instances and rotate the list based on the table name to prevent creating hotspot servers.
   * <p>Choose both enabled and disabled instances with the server tag as the qualified instances to avoid unexpected
   * data shuffling when instances get disabled.
   */
  public static InstancePartitions computeDefaultInstancePartitionsForTag(HelixManager helixManager,
      TableConfig tableConfig, String instancePartitionsType, String serverTag) {
    String tableNameWithType = tableConfig.getTableName();
    List<String> instances = HelixHelper.getInstancesWithTag(helixManager, serverTag);
    int numInstances = instances.size();
    Preconditions.checkState(numInstances > 0, "No instance found with tag: %s for table: %s", serverTag,
        tableNameWithType);
    Preconditions.checkState(numInstances >= tableConfig.getReplication(),
        "Number of instances: %s with tag: %s < table replication: %s for table: %s", numInstances, serverTag,
        tableConfig.getReplication(), tableNameWithType);

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
    String path = ZKMetadataProvider.constructPropertyStorePathForInstancePartitions(
        instancePartitions.getInstancePartitionsName());
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
    instancePartitions.stream()
        .filter(instancePartition -> instancePartition.getInstancePartitionsName()
            .startsWith(TableNameBuilder.extractRawTableName(tableNameWithType) + TIER_SUFFIX))
        .forEach(instancePartition -> {
          removeInstancePartitions(propertyStore, instancePartition.getInstancePartitionsName());
        });
  }

  /// Returns `true` if the table has pre-configured instance partitions of the given type.
  public static boolean hasPreConfiguredInstancePartitions(TableConfig tableConfig,
      InstancePartitionsType instancePartitionsType) {
    Map<InstancePartitionsType, String> instancePartitionsMap = tableConfig.getInstancePartitionsMap();
    return instancePartitionsMap != null && instancePartitionsMap.containsKey(instancePartitionsType);
  }

  /// Returns `true` if the table should use pre-configured instance partitions of the given type.
  public static boolean shouldFetchPreConfiguredInstancePartitions(TableConfig tableConfig,
      InstancePartitionsType instancePartitionsType) {
    return hasPreConfiguredInstancePartitions(tableConfig, instancePartitionsType)
        && !InstanceAssignmentConfigUtils.isMirrorServerSetAssignment(tableConfig, instancePartitionsType);
  }

  /**
   * Update a given set of instance partitions into an ideal state's instance partitions metadata maintained in its
   * list fields. Previous instance partitions will be wiped out.
   *
   * @param idealState Current ideal state
   * @param instancePartitionsList List of instance partitions to be written into the ideal state instance partitions
   *                               metadata.
   */
  public static void replaceInstancePartitionsInIdealState(IdealState idealState,
      List<InstancePartitions> instancePartitionsList) {
    Map<String, List<String>> idealStateListFields = idealState.getRecord().getListFields();
    idealStateListFields.keySet().removeIf(key -> key.startsWith(InstancePartitionsUtils.IDEAL_STATE_IP_PREFIX));
    updateInstancePartitionsInIdealState(idealState, instancePartitionsList);
  }

  /**
   * Add a given set of instance partitions into an ideal state's instance partitions metadata maintained in its
   * list fields.
   *
   * @param idealState Current ideal state
   * @param instancePartitionsList List of instance partitions to be added to the ideal state instance partitions
   *                               metadata.
   */
  public static void updateInstancePartitionsInIdealState(IdealState idealState,
      List<InstancePartitions> instancePartitionsList) {
    Map<String, List<String>> idealStateListFields = idealState.getRecord().getListFields();
    for (InstancePartitions instancePartitions : instancePartitionsList) {
      String instancePartitionsName = instancePartitions.getInstancePartitionsName();
      for (String partitionReplica : instancePartitions.getPartitionToInstancesMap().keySet()) {
        String idealStateListKey = InstancePartitionsUtils.IDEAL_STATE_IP_PREFIX + instancePartitionsName
            + InstancePartitionsUtils.IDEAL_STATE_IP_SEPARATOR + partitionReplica;
        idealStateListFields.put(idealStateListKey, new ArrayList<>(instancePartitions.getInstances(partitionReplica)));
      }
    }
  }

  /**
   * Extracts all instance partitions from the ideal state's list fields.
   *
   * <p>The ideal state stores instance partitions metadata in list fields with keys of the format:
   * {@code INSTANCE_PARTITIONS__<instancePartitionsName>__<partitionId>_<replicaGroupId>}
   *
   * <p>The instance partitions name itself may contain {@code __} (e.g., for tier instance partitions like
   * {@code myTable__TIER__hotTier}), so we parse from the right by finding the last {@code __} separator
   * which should always be followed by a valid 'partitionId_replicaId' pattern ({@code <int>_<int>}).
   *
   * @param idealState The ideal state to extract instance partitions from
   * @return A map from instance partitions name to the reconstructed InstancePartitions object
   */
  public static Map<String, InstancePartitions> extractInstancePartitionsFromIdealState(IdealState idealState) {
    Map<String, InstancePartitions> instancePartitionsMap = new HashMap<>();
    for (Map.Entry<String, List<String>> entry : idealState.getRecord().getListFields().entrySet()) {
      String key = entry.getKey();
      // Keys look like 'INSTANCE_PARTITIONS__myTable_CONSUMING__0_1' and
      // 'INSTANCE_PARTITIONS__myTable__TIER__hotTier__0_1'
      if (key.startsWith(IDEAL_STATE_IP_PREFIX)) {
        String remainder = key.substring(IDEAL_STATE_IP_PREFIX.length());

        // The last '__' separates the instance partitions name from the partition-replica suffix
        int lastSeparatorIdx = remainder.lastIndexOf(IDEAL_STATE_IP_SEPARATOR);
        String instancePartitionsName = remainder.substring(0, lastSeparatorIdx);
        String partitionReplica = remainder.substring(lastSeparatorIdx + IDEAL_STATE_IP_SEPARATOR.length());
        List<String> instances = entry.getValue();

        instancePartitionsMap.computeIfAbsent(instancePartitionsName, InstancePartitions::new)
            .setInstances(partitionReplica, instances);
      }
    }
    return instancePartitionsMap;
  }

  /// Creates a map from server instance to replica group ID using the ideal state instance partitions metadata.
  public static Map<String, Integer> serverToReplicaGroupMap(IdealState idealState) {
    Map<String, Integer> serverToReplicaGroupMap = new HashMap<>();

    for (Map.Entry<String, List<String>> listFields : idealState.getRecord().getListFields().entrySet()) {
      String key = listFields.getKey();
      // key looks like "INSTANCE_PARTITIONS__<INSTANCE_PARTITION_NAME>"
      // <INSTANCE_PARTITION_NAME> typically looks like myTable_CONSUMING__0_1 (here, 0 would be the partition ID and
      // 1 would be the replica group ID)
      if (key.startsWith(InstancePartitionsUtils.IDEAL_STATE_IP_PREFIX)) {
        int separatorIndex = key.lastIndexOf(InstancePartitions.PARTITION_REPLICA_GROUP_SEPARATOR);
        Integer replicaGroup = Integer.parseInt(key.substring(separatorIndex + 1));
        listFields.getValue().forEach(value -> {
          if (serverToReplicaGroupMap.containsKey(value)) {
            LOGGER.warn("Server {} assigned to multiple replica groups ({}, {})", value, replicaGroup,
                serverToReplicaGroupMap.get(value));
          } else {
            serverToReplicaGroupMap.put(value, replicaGroup);
          }
        });
      }
    }

    return serverToReplicaGroupMap;
  }
}
