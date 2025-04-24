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
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.config.workload.NodeConfig;
import org.apache.pinot.spi.config.workload.PropagationScheme;
import org.apache.pinot.spi.config.workload.QueryWorkloadConfig;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/**
 * WorkloadPropagationUtils provides utility methods for workload propagation.
 */
public class WorkloadPropagationUtils {

  private WorkloadPropagationUtils() {
  }

  /**
   * Get the mapping tableNameWithType → {NON_LEAF_NODE→brokerTag, LEAF_NODE→(serverTag + overrides)}
   * 1. Get all table configs from the PinotHelixResourceManager
   * 2. For each table config, extract the tenant config
   * 3. For each tenant config, get the broker and server tags
   * 4. Populate the helix tags for NON_LEAF_NODE and LEAF_NODE separately
   */
  public static Map<String, Map<NodeConfig.Type, Set<String>>> getTableToHelixTags(
          PinotHelixResourceManager pinotResourceManager) {
    Map<String, Map<NodeConfig.Type, Set<String>>> tableToTags = new HashMap<>();
    for (TableConfig tableConfig : pinotResourceManager.getAllTableConfigs()) {
      TenantConfig tenantConfig = tableConfig.getTenantConfig();
      TableType tableType = tableConfig.getTableType();

      // Gather all relevant tags for this tenant
      Set<String> tenantTags = new HashSet<>();
      collectHelixTagsForTable(tenantTags, tenantConfig, tableType);

      // Populate the helix tags for NON_LEAF_NODE and LEAF_NODE separately to provide flexibility
      // in workload propagation to either leaf nodes or non-leaf nodes
      String brokerTag = TagNameUtils.getBrokerTagForTenant(tenantConfig.getBroker());
      Set<String> nonLeafTags = Collections.singleton(brokerTag);

      Set<String> leafTags = new HashSet<>(tenantTags);
      leafTags.remove(brokerTag);

      Map<NodeConfig.Type, Set<String>> nodeTypeToTags = new EnumMap<>(NodeConfig.Type.class);
      nodeTypeToTags.put(NodeConfig.Type.NON_LEAF_NODE, nonLeafTags);
      nodeTypeToTags.put(NodeConfig.Type.LEAF_NODE, leafTags);

      tableToTags.put(tableConfig.getTableName(), nodeTypeToTags);
    }
    return tableToTags;
  }

  private static void collectHelixTagsForTable(Set<String> tagSet, TenantConfig tenantConfig, TableType tableType) {
    tagSet.add(TagNameUtils.getBrokerTagForTenant(tenantConfig.getBroker()));
    if (tableType == TableType.OFFLINE) {
      tagSet.add(TagNameUtils.getOfflineTagForTenant(tenantConfig.getServer()));
    } else {
      tagSet.add(TagNameUtils.getRealtimeTagForTenant(tenantConfig.getServer()));
    }
  }

  /**
   * Get the helix tags for a given table name.
   * If the table name does not have a type suffix, it will return both offline and realtime tags.
   */
  public static Set<String> getHelixTagsForTable(PinotHelixResourceManager pinotResourceManager,
          String tableName) {
    Set<String> combinedTags = new HashSet<>();
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    List<String> tablesWithType = (tableType == null)
        ? Arrays.asList(TableNameBuilder.OFFLINE.tableNameWithType(tableName),
            TableNameBuilder.REALTIME.tableNameWithType(tableName))
        : Collections.singletonList(tableName);

    for (String table : tablesWithType) {
      TableConfig tableConfig = pinotResourceManager.getTableConfig(table);
      if (tableConfig != null) {
        collectHelixTagsForTable(combinedTags, tableConfig.getTenantConfig(), tableConfig.getTableType());
      }
    }
    return combinedTags;
  }

  /**
   * Get the mapping between helix tag -> instances
   */
  public static Map<String, Set<String>> getHelixTagToInstances(PinotHelixResourceManager pinotResourceManager) {
    Map<String, Set<String>> tagToInstances = new HashMap<>();
    for (InstanceConfig instanceConfig : pinotResourceManager.getAllHelixInstanceConfigs()) {
      String instanceName = instanceConfig.getInstanceName();
      for (String helixTag : instanceConfig.getTags()) {
        tagToInstances.computeIfAbsent(helixTag, tag -> new HashSet<>()).add(instanceName);
      }
    }
    return tagToInstances;
  }

  /**
   * Get the mapping between instance -> helix tags
   */
  public static Map<String, Set<String>> getInstanceToHelixTags(PinotHelixResourceManager pinotHelixResourceManager) {
    List<InstanceConfig> instanceConfigs = pinotHelixResourceManager.getAllHelixInstanceConfigs();
    Map<String, Set<String>> instanceToHelixTags = new HashMap<>();
    for (InstanceConfig instanceConfig : instanceConfigs) {
      String instanceName = instanceConfig.getInstanceName();
      List<String> tags = instanceConfig.getTags();
      instanceToHelixTags.computeIfAbsent(instanceName, k -> new HashSet<>()).addAll(tags);
    }
    return instanceToHelixTags;
  }

  /**
   * Returns a set of QueryWorkloadConfigs that are associated with the specified helix tags.
   * The method performs the following:
   * 1. Fetches all QueryWorkloadConfigs from the PinotHelixResourceManager.
   * 2. For each config, iterates through node types and their propagation schemes.
   * 3. For TENANT propagation:
   *    - Resolves tenant values to helix tags (direct tag or expanded list).
   *    - Adds QueryWorkloadConfigs if any tag intersects with the filterTags.
   * 4. For TABLE propagation:
   *    - Resolves table names to tableWithType forms.
   *    - Maps each tableWithType and node type to helix tags via getTableToHelixTags.
   *    - Adds QueryWorkloadConfigs if any tag intersects with the filterTags.
   */
  public static Set<QueryWorkloadConfig> getQueryWorkloadConfigsForTags(
      PinotHelixResourceManager pinotHelixResourceManager, Set<String> filterTags) {
    Set<QueryWorkloadConfig> matchedConfigs = new HashSet<>();
    List<QueryWorkloadConfig> queryWorkloadConfigs = pinotHelixResourceManager.getAllQueryWorkloadConfigs();
    if (queryWorkloadConfigs == null) {
      return matchedConfigs;
    }

    Map<String, Map<NodeConfig.Type, Set<String>>> tableToHelixTags = getTableToHelixTags(pinotHelixResourceManager);

    for (QueryWorkloadConfig queryWorkloadConfig : queryWorkloadConfigs) {
      for (Map.Entry<NodeConfig.Type, NodeConfig> entry : queryWorkloadConfig.getNodeConfigs().entrySet()) {
        NodeConfig.Type nodeType = entry.getKey();
        NodeConfig nodeConfig = entry.getValue();
        PropagationScheme scheme = nodeConfig.getPropagationScheme();

        if (scheme.getPropagationType() == PropagationScheme.Type.TENANT) {
          for (String tenant : scheme.getValues()) {
            Set<String> resolvedTags = TagNameUtils.isOfflineServerTag(tenant)
                    || TagNameUtils.isRealtimeServerTag(tenant) || TagNameUtils.isBrokerTag(tenant)
                ? Collections.singleton(tenant)
                : new HashSet<>(getAllPossibleHelixTagsFor(tenant));
            if (!Collections.disjoint(resolvedTags, filterTags)) {
              matchedConfigs.add(queryWorkloadConfig);
              break;
            }
          }
        } else if (scheme.getPropagationType() == PropagationScheme.Type.TABLE) {
          for (String tableName : scheme.getValues()) {
            TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
            List<String> tablesWithType = (tableType == null)
                ? Arrays.asList(TableNameBuilder.OFFLINE.tableNameWithType(tableName),
                    TableNameBuilder.REALTIME.tableNameWithType(tableName))
                : Collections.singletonList(tableName);
            for (String tableWithType : tablesWithType) {
              Set<String> resolvedTags = tableToHelixTags
                  .getOrDefault(tableWithType, Collections.emptyMap())
                  .getOrDefault(nodeType, Collections.emptySet());
              if (!Collections.disjoint(resolvedTags, filterTags)) {
                matchedConfigs.add(queryWorkloadConfig);
                break;
              }
            }
          }
        }
      }
    }
    return matchedConfigs;
  }

  private static List<String> getAllPossibleHelixTagsFor(String tenantName) {
    List<String> helixTags = new ArrayList<>();
    helixTags.add(TagNameUtils.getBrokerTagForTenant(tenantName));
    helixTags.add(TagNameUtils.getOfflineTagForTenant(tenantName));
    helixTags.add(TagNameUtils.getRealtimeTagForTenant(tenantName));
    return helixTags;
  }
}
