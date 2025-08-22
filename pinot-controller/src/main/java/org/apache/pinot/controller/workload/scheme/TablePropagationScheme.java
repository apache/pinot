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
 * TablePropagationScheme is used to resolve instances based on the {@link NodeConfig} and {@link NodeConfig.Type}.
 * It resolves the instances based on the table names specified in the node configuration.
 */
public class TablePropagationScheme implements PropagationScheme {

  private final PinotHelixResourceManager _pinotHelixResourceManager;

  public TablePropagationScheme(PinotHelixResourceManager pinotHelixResourceManager) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
  }

  /**
   * Returns the union of instances across all cost splits for the given node config.
   */
  public Set<String> resolveInstances(NodeConfig nodeConfig) {
    Set<String> instances = new HashSet<>();
    Map<String, Map<NodeConfig.Type, Set<String>>> tableWithTypeToHelixTags =
        PropagationUtils.getTableToHelixTags(_pinotHelixResourceManager);
    Map<String, Set<String>> helixTagToInstances =
        PropagationUtils.getHelixTagToInstances(_pinotHelixResourceManager);

    NodeConfig.Type nodeType = nodeConfig.getNodeType();
    for (CostSplit costSplit : nodeConfig.getPropagationScheme().getCostSplits()) {
      Map<String, Set<String>> byTag = resolveInstancesByHelixTag(costSplit, nodeType,
          tableWithTypeToHelixTags, helixTagToInstances);
      if (byTag.isEmpty()) {
        throw new IllegalArgumentException("No instances found for CostSplit: " + costSplit);
      }
      for (Set<String> set : byTag.values()) {
        instances.addAll(set);
      }
    }
    return instances;
  }

  /**
   * Computes the per-instance cost map given node config and splitter, supporting sub-allocations for RT servers.
   */
  public Map<String, InstanceCost> resolveInstanceCostMap(NodeConfig nodeConfig, CostSplitter costSplitter) {
    Map<String, InstanceCost> instanceCostMap = new HashMap<>();
    Map<String, Map<NodeConfig.Type, Set<String>>> tableWithTypeToHelixTags =
        PropagationUtils.getTableToHelixTags(_pinotHelixResourceManager);
    Map<String, Set<String>> helixTagToInstances =
        PropagationUtils.getHelixTagToInstances(_pinotHelixResourceManager);

    NodeConfig.Type nodeType = nodeConfig.getNodeType();
    for (CostSplit costSplit : nodeConfig.getPropagationScheme().getCostSplits()) {
      Map<String, Set<String>> instancesByTag = resolveInstancesByHelixTag(costSplit, nodeType,
          tableWithTypeToHelixTags, helixTagToInstances);
      if (instancesByTag.isEmpty()) {
        throw new IllegalArgumentException("No instances found for CostSplit: " + costSplit);
      }

      for (Map.Entry<String, Set<String>> entry : instancesByTag.entrySet()) {
        String helixTag = entry.getKey();
        Set<String> instances = entry.getValue();

        // Support sub-allocations for realtime servers
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
   * Single source of truth for resolving instances grouped by Helix tag.
   */
  private Map<String, Set<String>> resolveInstancesByHelixTag(CostSplit costSplit, NodeConfig.Type nodeType,
      Map<String, Map<NodeConfig.Type, Set<String>>> tableWithTypeToHelixTags,
      Map<String, Set<String>> helixTagToInstances) {
    String tableName = costSplit.getCostId();
    if (tableName == null) {
      throw new IllegalArgumentException("Table name cannot be null in CostSplit: " + costSplit);
    }

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
   * Returns [OFFLINE, REALTIME] variants if type is absent; otherwise returns the provided table name as-is.
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

  private static void mergeCosts(Map<String, InstanceCost> into, Map<String, InstanceCost> delta) {
    for (Map.Entry<String, InstanceCost> e : delta.entrySet()) {
      into.merge(e.getKey(), e.getValue(), (oldCost, newCost) ->
          new InstanceCost(
              oldCost.getCpuCostNs() + newCost.getCpuCostNs(),
              oldCost.getMemoryCostBytes() + newCost.getMemoryCostBytes()));
    }
  }
}
