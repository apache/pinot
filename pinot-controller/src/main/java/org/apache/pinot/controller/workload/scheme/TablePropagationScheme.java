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
package org.apache.pinot.controller.workload.scheme;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.pinot.common.assignment.InstancePartitionsUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.workload.splitter.CostSplitter;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.workload.InstanceCost;
import org.apache.pinot.spi.config.workload.NodeConfig;
import org.apache.pinot.spi.config.workload.PropagationEntity;
import org.apache.pinot.spi.config.workload.PropagationEntityOverrides;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;

/**
 * A {@code TablePropagationScheme} resolves Pinot instances based on table names in a node
 * configuration.
 *
 */
public class TablePropagationScheme implements PropagationScheme {

  private final PinotHelixResourceManager _pinotHelixResourceManager;

  public TablePropagationScheme(PinotHelixResourceManager pinotHelixResourceManager) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
  }

  /**
   * Resolves the union of all instances across all cost splits for the given node config.
   *
   * Example:
   * <pre>
   *   { "Broker_Instance_1", "Broker_Instance_2", "Server_Instance_1" }
   * </pre>
   */
  public Set<String> resolveInstances(NodeConfig nodeConfig) {
    Set<String> instances = new HashSet<>();
    Map<String, Set<String>> partitionKeyToInstances =
        PropagationUtils.getPartitionConfigKeyToInstances(_pinotHelixResourceManager);
    HelixManager helixManager = _pinotHelixResourceManager.getHelixZkManager();
    ExternalView brokerResource = HelixHelper.getExternalViewForResource(helixManager.getClusterManagmentTool(),
        helixManager.getClusterName(), CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    NodeConfig.Type nodeType = nodeConfig.getNodeType();

    for (PropagationEntity entity : nodeConfig.getPropagationScheme().getPropagationEntities()) {
      String tableName = entity.getEntity();
      if (nodeConfig.getNodeType() == NodeConfig.Type.BROKER_NODE) {
        for (String tableWithType : expandToTablesWithType(tableName)) {
          instances.addAll(getBrokerInstances(brokerResource, tableWithType));
        }
      } else if (nodeType == NodeConfig.Type.SERVER_NODE) {
        for (String partitionKey : InstancePartitionsUtils.getAllPossibleInstancePartitionsName(tableName)) {
          instances.addAll(partitionKeyToInstances.getOrDefault(partitionKey, Collections.emptySet()));
        }
      } else {
        throw new IllegalStateException("Unsupported node type: " + nodeType);
      }
    }
    return instances;
  }
  /**
   * Computes the per-instance cost map for the given node config using the provided splitter.
   *
   * <p>
   * For each cost split in the node config, the relevant instances are resolved based on
   * table names and node type. The splitter is then used to compute costs for those instances.
   * If multiple splits resolve to the same instance, their costs are summed.
   * </p>
   *
   * Example:
   * <pre>
   *   {
   *     "Broker_Instance_1": { cpuCostNs: 1000000, memoryCostBytes: 1048576 },
   *     "Broker_Instance_2": { cpuCostNs: 1000000, memoryCostBytes: 1048576 },
   *     "Server_Instance_1": { cpuCostNs: 2000000, memoryCostBytes: 2097152 },
   *   }
   * </pre>
   */
  @Override
  public Map<String, InstanceCost> resolveInstanceCostMap(NodeConfig nodeConfig, CostSplitter costSplitter) {
    Map<String, InstanceCost> instanceCostMap = new HashMap<>();
    // One-time zk lookups reused across all entities
    HelixManager helixManager = _pinotHelixResourceManager.getHelixZkManager();
    ExternalView brokerResource = HelixHelper.getExternalViewForResource(helixManager.getClusterManagmentTool(),
        helixManager.getClusterName(), CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    Map<String, Set<String>> partitionKeyToInstances =
        PropagationUtils.getPartitionConfigKeyToInstances(_pinotHelixResourceManager);

    NodeConfig.Type nodeType = nodeConfig.getNodeType();

    for (PropagationEntity entity : nodeConfig.getPropagationScheme().getPropagationEntities()) {
      String tableName = entity.getEntity();
      Set<String> instances;
      Map<String, InstanceCost> deltaCost;
      if (nodeType == NodeConfig.Type.BROKER_NODE) {
        instances = getBrokerInstances(brokerResource, tableName);
        deltaCost = costSplitter.computeInstanceCostMap(entity.getCpuCostNs(), entity.getMemoryCostBytes(), instances);
      } else if (nodeType == NodeConfig.Type.SERVER_NODE) {
        deltaCost = resolveServerCosts(partitionKeyToInstances, entity, costSplitter);
      } else {
        throw new IllegalStateException("Unsupported node type: " + nodeType);
      }
      // Since entities are explicit, we fail if no instances are found.
      if (deltaCost == null || deltaCost.isEmpty()) {
        throw new IllegalArgumentException(
            "No instances found for entity: " + entity + " and node config: " + nodeConfig);
      }
      // For a workload, if the multiple entities resolve to the same instance, we sum the costs for that instance
      PropagationUtils.mergeCosts(instanceCostMap, deltaCost);
    }
    return instanceCostMap;
  }

  /**
   * Resolves the set of broker instances responsible for serving the given table name.
   *
   * Returns the first non-empty set of instances since the brokers are shared across all table types.
   */
  private Set<String> getBrokerInstances(ExternalView brokerResource, String tableName) {
    for (String tableWithType : expandToTablesWithType(tableName)) {
      Set<String> instances = brokerResource.getStateMap(tableWithType) != null
          ? brokerResource.getStateMap(tableWithType).keySet() : Collections.emptySet();
      if (!instances.isEmpty()) {
        return new HashSet<>(instances);
      }
    }
    return Collections.emptySet();
  }

  private Map<String, InstanceCost> resolveServerCosts(Map<String, Set<String>> partitionKeyToInstances,
                                                       PropagationEntity entity, CostSplitter costSplitter) {
    Map<String, InstanceCost> instanceCostMap = new HashMap<>();
    Long cpuCostNs = entity.getCpuCostNs();
    Long memoryCostBytes = entity.getMemoryCostBytes();
    List<PropagationEntityOverrides> overrides = entity.getOverrides();
    for (String tableWithType : expandToTablesWithType(entity.getEntity())) {
      TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableWithType);
      Map<String, InstanceCost> deltaCost;
      if (tableType == TableType.OFFLINE) {
        // We skip overrides since they are only applicable to REALTIME tables
        String offlineKey = InstancePartitionsUtils.getInstancePartitionsName(tableWithType,
            InstancePartitionsType.OFFLINE.toString());
        Set<String> instances = partitionKeyToInstances.get(offlineKey);
        // Given we allow for entity without table type, it's possible to not find any OFFLINE instances if the
        // table is REALTIME only. However, we have a check above to ensure at least one table type resolves
        // to instances.
        if (instances != null && !instances.isEmpty()) {
          deltaCost = costSplitter.computeInstanceCostMap(cpuCostNs, memoryCostBytes, instances);
          PropagationUtils.mergeCosts(instanceCostMap, deltaCost);
        }
      } else if (tableType == TableType.REALTIME) {
        // Overrides present → apply each override separately
        if (overrides != null && !overrides.isEmpty()) {
          handleOverrides(instanceCostMap, overrides, tableWithType, costSplitter, partitionKeyToInstances);
        } else {
          // No overrides → union CONSUMING + COMPLETED
          Set<String> instances = getRealtimeInstances(partitionKeyToInstances, tableWithType, null);
          if (!instances.isEmpty()) {
            deltaCost = costSplitter.computeInstanceCostMap(cpuCostNs, memoryCostBytes, instances);
            PropagationUtils.mergeCosts(instanceCostMap, deltaCost);
          }
        }
      }
    }
    return instanceCostMap;
  }

  private void handleOverrides(Map<String, InstanceCost> instanceCostMap, List<PropagationEntityOverrides> overrides,
                               String tableWithType, CostSplitter costSplitter,
                               Map<String, Set<String>> partitionKeyToInstances) {
    for (PropagationEntityOverrides override : overrides) {
      OverrideEntityType overrideEntityType = OverrideEntityType.fromString(override.getEntity());
      Set<String> instances = getRealtimeInstances(partitionKeyToInstances, tableWithType, overrideEntityType);
      // Since overrides are explicit, we fail if no instances are found.
      if (instances.isEmpty()) {
        throw new IllegalArgumentException(
            "No instances found for override: " + override + " and table: " + tableWithType);
      }
      Map<String, InstanceCost> deltaCost = costSplitter.computeInstanceCostMap(override.getCpuCostNs(),
          override.getMemoryCostBytes(), instances);
      PropagationUtils.mergeCosts(instanceCostMap, deltaCost);
    }
  }

  /**
   * Returns REALTIME instances for the given tableWithType.
   * If {@code type} is null, returns the union of CONSUMING and COMPLETED.
   */
  private Set<String> getRealtimeInstances(Map<String, Set<String>> partitionKeyToInstances, String tableWithType,
                                           @Nullable OverrideEntityType type) {
    String consumingKey = InstancePartitionsUtils.getInstancePartitionsName(
        tableWithType, InstancePartitionsType.CONSUMING.toString());
    String completedKey = InstancePartitionsUtils.getInstancePartitionsName(
        tableWithType, InstancePartitionsType.COMPLETED.toString());
    if (type == OverrideEntityType.CONSUMING) {
      Set<String> instances = partitionKeyToInstances.get(consumingKey);
      return (instances == null) ? Collections.emptySet() : new HashSet<>(instances);
    } else if (type == OverrideEntityType.COMPLETED) {
      Set<String> instances = partitionKeyToInstances.get(completedKey);
      return (instances == null) ? Collections.emptySet() : new HashSet<>(instances);
    }

    // Union CONSUMING + COMPLETED
    Set<String> consuming = partitionKeyToInstances.get(consumingKey);
    Set<String> completed = partitionKeyToInstances.get(completedKey);

    Set<String> union = new HashSet<>();
    if (consuming != null) {
      union.addAll(consuming);
    }
    if (completed != null) {
      union.addAll(completed);
    }
    return union;
  }

  /**
   * Expands a raw table name into type-qualified names.
   *
   * <p>
   * If the input lacks a type suffix, both {@code OFFLINE} and {@code REALTIME} variants are
   * returned. Otherwise, the name is returned as-is.
   * </p>
   *
   * @param tableName The raw or type-qualified table name.
   * @return A list of table names with type.
   */
  private static List<String> expandToTablesWithType(String tableName) {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType == null) {
      List<String> list = new ArrayList<>(2);
      list.add(TableNameBuilder.OFFLINE.tableNameWithType(tableName));
      list.add(TableNameBuilder.REALTIME.tableNameWithType(tableName));
      return list;
    }
    return Collections.singletonList(tableName);
  }

  private enum OverrideEntityType {
    CONSUMING,
    COMPLETED;

    public static OverrideEntityType fromString(String value) {
      for (OverrideEntityType type : OverrideEntityType.values()) {
        if (type.name().equalsIgnoreCase(value)) {
          return type;
        }
      }
      throw new IllegalArgumentException("Unknown OverrideEntityType: " + value);
    }
  }
}
