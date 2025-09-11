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
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.workload.splitter.CostSplitter;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.workload.CostSplit;
import org.apache.pinot.spi.config.workload.InstanceCost;
import org.apache.pinot.spi.config.workload.NodeConfig;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;

/**
 * A {@code TablePropagationScheme} resolves Pinot instances based on table names in a node
 * configuration.
 *
 * <p>
 * This scheme looks up Helix tags for offline and realtime tables and maps them to
 * instances, enabling workload propagation by table.
 * </p>
 */
public class TablePropagationScheme implements PropagationScheme {

  private final PinotHelixResourceManager _pinotHelixResourceManager;

  public TablePropagationScheme(PinotHelixResourceManager pinotHelixResourceManager) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
  }

  /**
   * Resolves the union of all instances across all cost splits for the given node config.
   *
   * @param nodeConfig Node configuration containing propagation scheme and cost splits.
   * @return A set of instance names that should receive workload messages.
   * @throws IllegalArgumentException If no instances are found for a cost split.
   */
  public Set<String> resolveInstances(NodeConfig nodeConfig) {
    Set<String> instances = new HashSet<>();
    Map<String, Map<NodeConfig.Type, Set<String>>> tableWithTypeToHelixTags =
        PropagationUtils.getTableToHelixTags(_pinotHelixResourceManager);
    Map<String, Set<String>> helixTagToInstances =
        PropagationUtils.getHelixTagToInstances(_pinotHelixResourceManager);

    NodeConfig.Type nodeType = nodeConfig.getNodeType();
    for (CostSplit costSplit : nodeConfig.getPropagationScheme().getCostSplits()) {
      Map<String, Set<String>> byTag = resolveInstancesByHelixTag(costSplit, nodeType, tableWithTypeToHelixTags,
          helixTagToInstances);
      if (!byTag.isEmpty()) {
        for (Set<String> set : byTag.values()) {
          instances.addAll(set);
        }
      }
    }
    return instances;
  }

  /**
   * Computes the per-instance cost map for the given node config using the provided splitter.
   *
   * <p>
   * This method supports sub-allocations: the cost-ids in the sub-allocations are the helix tags.
   * For example, for a realtime table having different consuming and completed servers and different
   * costs for each, the cost-ids can be "myTable_REALTIME" and "myTableCompleted_OFFLINE".
   * </p>
   *
   * @param nodeConfig Node configuration containing cost splits and scope.
   * @param costSplitter Strategy used to compute costs per instance.
   * @return A mapping of instance name to its computed {@link InstanceCost}.
   * @throws IllegalArgumentException If no instances are found for a cost split.
   */
  public Map<String, InstanceCost> resolveInstanceCostMap(NodeConfig nodeConfig, CostSplitter costSplitter) {
    Map<String, InstanceCost> instanceCostMap = new HashMap<>();
    Map<String, Map<NodeConfig.Type, Set<String>>> tableWithTypeToHelixTags =
        PropagationUtils.getTableToHelixTags(_pinotHelixResourceManager);
    Map<String, Set<String>> helixTagToInstances =
        PropagationUtils.getHelixTagToInstances(_pinotHelixResourceManager);

    NodeConfig.Type nodeType = nodeConfig.getNodeType();
    for (CostSplit costSplit : nodeConfig.getPropagationScheme().getCostSplits()) {
      // Gives a mapping of helix tag to instances for this cost split
      // e.g. "myTable_REALTIME" -> {"Server_1", "Server_2"}, "myTableCompleted_OFFLINE" -> {"Server_3", "Server_4"}
      // we get it this way to check for sub-allocations later that are based on helix tags
      Map<String, Set<String>> instancesByTag = resolveInstancesByHelixTag(costSplit, nodeType,
          tableWithTypeToHelixTags, helixTagToInstances);
      if (instancesByTag.isEmpty()) {
        // This is to ensure active table name is passed in the cost split
        throw new IllegalArgumentException("No instances found for CostSplit: " + costSplit);
      }

      for (Map.Entry<String, Set<String>> entry : instancesByTag.entrySet()) {
        String helixTag = entry.getKey();
        Set<String> instances = entry.getValue();

        // Support sub-allocations based on Helix tag ids
        if (costSplit.getSubAllocations() != null) {
          for (CostSplit split : costSplit.getSubAllocations()) {
            if (helixTag.equals(split.getCostId())) {
              Set<String> subInstances = instancesByTag.getOrDefault(split.getCostId(), instances);
              mergeCosts(instanceCostMap, costSplitter.computeInstanceCostMap(split, subInstances));
              break;
            }
          }
        } else {
          mergeCosts(instanceCostMap, costSplitter.computeInstanceCostMap(costSplit, instances));
        }
      }
    }
    return instanceCostMap;
  }

  /**
   * Resolves instances grouped by Helix tag for a given cost split and node type.
   *
   * <p>
   * This is the single source of truth for mapping table → Helix tag → instances. Copies of
   * instance sets are returned to avoid accidental external mutation.
   * </p>
   *
   * @param costSplit The cost split specifying the table name.
   * @param nodeType The node type (broker or server).
   * @param tableWithTypeToHelixTags Precomputed mapping of table → node type → Helix tags.
   * @param helixTagToInstances Precomputed mapping of Helix tag → instances.
   * @return A map of Helix tag → set of instances for this cost split.
   * @throws IllegalArgumentException If the cost split has no table name.
   */
  private Map<String, Set<String>> resolveInstancesByHelixTag(CostSplit costSplit, NodeConfig.Type nodeType,
      Map<String, Map<NodeConfig.Type, Set<String>>> tableWithTypeToHelixTags,
      Map<String, Set<String>> helixTagToInstances) {
    String tableName = costSplit.getCostId();
    Map<String, Set<String>> instancesByTag = new HashMap<>();
    for (String tableWithType : expandToTablesWithType(tableName)) {
      Map<NodeConfig.Type, Set<String>> nodeToHelixTags = tableWithTypeToHelixTags.get(tableWithType);
      if (nodeToHelixTags == null) {
        continue;
      }
      Set<String> helixTags = nodeToHelixTags.get(nodeType);
      if (helixTags == null) {
        continue;
      }
      for (String helixTag : helixTags) {
        Set<String> helixInstances = helixTagToInstances.get(helixTag);
        if (helixInstances != null && !helixInstances.isEmpty()) {
          // Use a copy to avoid accidental external mutation
          instancesByTag.put(helixTag, new HashSet<>(helixInstances));
        }
      }
    }
    return instancesByTag;
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

  /**
   * Merges the delta cost map into the target map by summing CPU and memory costs.
   *
   * @param target The target map to merge into.
   * @param delta The delta map whose values are added.
   */
  private static void mergeCosts(Map<String, InstanceCost> target, Map<String, InstanceCost> delta) {
    for (Map.Entry<String, InstanceCost> e : delta.entrySet()) {
      target.merge(e.getKey(), e.getValue(), (oldCost, newCost) ->
          new InstanceCost(
              oldCost.getCpuCostNs() + newCost.getCpuCostNs(),
              oldCost.getMemoryCostBytes() + newCost.getMemoryCostBytes()));
    }
  }
}
