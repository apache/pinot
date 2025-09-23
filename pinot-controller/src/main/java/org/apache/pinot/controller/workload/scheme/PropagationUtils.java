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
import org.apache.pinot.spi.config.workload.InstanceCost;
import org.apache.pinot.spi.config.workload.NodeConfig;
import org.apache.pinot.spi.config.workload.PropagationEntity;
import org.apache.pinot.spi.config.workload.PropagationScheme;
import org.apache.pinot.spi.config.workload.QueryWorkloadConfig;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Provides utility methods for workload propagation in Pinot.
 *
 * <p>
 * This class centralizes logic for resolving Helix tags, instance mappings, and matching query
 * workload configs to their applicable propagation scope.
 * </p>
 */
public class PropagationUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(PropagationUtils.class);
  private PropagationUtils() {
  }

  /**
   * Builds a mapping from table name with type to Helix tags per node type.
   *
   * <p>For each table:</p>
   * <ol>
   *   <li>Fetch the {@link TenantConfig}.</li>
   *   <li>Derive broker and server tags from the tenant configuration.</li>
   *   <li>Associate {@link NodeConfig.Type#BROKER_NODE} with broker tags.</li>
   *   <li>Associate {@link NodeConfig.Type#SERVER_NODE} with server tags (consuming and/or completed
   *       for realtime, or offline for batch).</li>
   * </ol>
   *
   * @param pinotResourceManager Resource manager used to fetch table configs.
   * @return A mapping of tableNameWithType to node type → Helix tags.
   */
  public static Map<String, Map<NodeConfig.Type, Set<String>>> getTableToHelixTags(
          PinotHelixResourceManager pinotResourceManager) {
    Map<String, Map<NodeConfig.Type, Set<String>>> tableToTags = new HashMap<>();
    List<TableConfig> tableConfigs = pinotResourceManager.getAllTableConfigs();
    if (tableConfigs == null) {
      LOGGER.warn("No table configs found, returning empty mapping");
      return tableToTags;
    }
    for (TableConfig tableConfig : tableConfigs) {
      if (tableConfig == null) {
        LOGGER.warn("Skipping null table config");
        continue;
      }
      TenantConfig tenantConfig = tableConfig.getTenantConfig();
      TableType tableType = tableConfig.getTableType();
      if (tenantConfig == null || tableType == null) {
        LOGGER.warn("Skipping table config with null tenant config or table type: {}", tableConfig.getTableName());
        continue;
      }

      // Gather all relevant tags for this tenant
      List<String> tenantTags = new ArrayList<>();
      try {
        collectHelixTagsForTable(tenantTags, tenantConfig, tableType);
      } catch (Exception e) {
        LOGGER.error("Failed to collect Helix tags for table: {}", tableConfig.getTableName(), e);
        continue;
      }

      // Populate the helix tags for BROKER_NODE and SERVER_NODE separately to provide flexibility
      // in workload propagation to direct the workload to only specific node types
      String brokerTag = TagNameUtils.getBrokerTagForTenant(tenantConfig.getBroker());
      Set<String> brokerTags = Collections.singleton(brokerTag);

      Set<String> serverTags = new HashSet<>(tenantTags);
      serverTags.remove(brokerTag);

      Map<NodeConfig.Type, Set<String>> nodeTypeToTags = new EnumMap<>(NodeConfig.Type.class);
      nodeTypeToTags.put(NodeConfig.Type.BROKER_NODE, brokerTags);
      nodeTypeToTags.put(NodeConfig.Type.SERVER_NODE, serverTags);

      tableToTags.put(tableConfig.getTableName(), nodeTypeToTags);
    }
    return tableToTags;
  }

  /**
   * Collects all Helix tags for a given table.
   *
   * <p>
   * For offline tables, only the offline server tag is included. For realtime tables, consuming and
   * completed tags are both added if they differ; otherwise, a single realtime tag is added.
   * </p>
   *
   * @param tags The list to populate with resolved tags.
   * @param tenantConfig Tenant configuration containing tenant names.
   * @param tableType The type of the table (OFFLINE or REALTIME).
   */
  private static void collectHelixTagsForTable(List<String> tags, TenantConfig tenantConfig, TableType tableType) {
    tags.add(TagNameUtils.getBrokerTagForTenant(tenantConfig.getBroker()));
    if (tableType == TableType.OFFLINE) {
      tags.add(TagNameUtils.getOfflineTagForTenant(tenantConfig.getServer()));
    } else {
      // Returns the realtime tag if completed server tag is not set
      String completedServerTag = TagNameUtils.extractCompletedServerTag(tenantConfig);
      // Returns the realtime tag if consuming server tag is not set
      String consumingServerTag = TagNameUtils.extractConsumingServerTag(tenantConfig);
      if (completedServerTag.equals(consumingServerTag)) {
        tags.add(completedServerTag);
      } else {
        tags.add(consumingServerTag);
        tags.add(completedServerTag);
      }
    }
  }

  /**
   * Resolves Helix tags for a table.
   *
   * <p>
   * If the input table name lacks a type suffix, both offline and realtime table names are expanded
   * and resolved. Otherwise, the specific table name is used.
   * </p>
   *
   * @param pinotResourceManager Resource manager to fetch table configs.
   * @param tableName The raw or type-qualified table name.
   * @return A list of Helix tags associated with the table.
   */
  public static List<String> getHelixTagsForTable(PinotHelixResourceManager pinotResourceManager, String tableName) {
    if (tableName == null || tableName.trim().isEmpty()) {
      throw new IllegalArgumentException("Table name cannot be null or empty");
    }

    List<String> combinedTags = new ArrayList<>();
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    List<String> tablesWithType = (tableType == null)
        ? Arrays.asList(TableNameBuilder.OFFLINE.tableNameWithType(tableName),
            TableNameBuilder.REALTIME.tableNameWithType(tableName))
        : Collections.singletonList(tableName);
    for (String table : tablesWithType) {
      try {
        TableConfig tableConfig = pinotResourceManager.getTableConfig(table);
        if (tableConfig != null && tableConfig.getTenantConfig() != null && tableConfig.getTableType() != null) {
          collectHelixTagsForTable(combinedTags, tableConfig.getTenantConfig(), tableConfig.getTableType());
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to get table config for table: " + table, e);
      }
    }
    return combinedTags;
  }

  /**
   * Builds a mapping from Helix tag to the set of instances carrying that tag.
   *
   * @param pinotResourceManager Resource manager used to fetch instance configs.
   * @return A mapping of Helix tag → set of instance names.
   */
  public static Map<String, Set<String>> getHelixTagToInstances(PinotHelixResourceManager pinotResourceManager) {
    Map<String, Set<String>> tagToInstances = new HashMap<>();
    try {
      List<InstanceConfig> instanceConfigs = pinotResourceManager.getAllHelixInstanceConfigs();
      if (instanceConfigs == null) {
        LOGGER.warn("No instance configs found, returning empty mapping");
        return tagToInstances;
      }

      for (InstanceConfig instanceConfig : instanceConfigs) {
        if (instanceConfig == null) {
          LOGGER.warn("Skipping null instance config");
          continue;
        }
        String instanceName = instanceConfig.getInstanceName();
        List<String> tags = instanceConfig.getTags();
        if (tags != null) {
          for (String helixTag : tags) {
            if (helixTag != null && !helixTag.trim().isEmpty()) {
              tagToInstances.computeIfAbsent(helixTag, tag -> new HashSet<>()).add(instanceName);
            }
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to retrieve instance configurations", e);
    }
    return tagToInstances;
  }

  /**
   * Filters the provided list of {@link QueryWorkloadConfig}s to those that match the given Helix
   * tags.
   *
   * <p>Matching rules:</p>
   * <ul>
   *   <li><strong>TENANT propagation</strong>:
   *     <ol>
   *       <li>Each cost ID is treated as either a tenant name or Helix tag.</li>
   *       <li>If it is a Helix tag (broker/server), use directly.</li>
   *       <li>Otherwise, resolve it to possible broker/server tags.</li>
   *       <li>If any resolved tag intersects with {@code filterTags}, include the config.</li>
   *     </ol>
   *   </li>
   *   <li><strong>TABLE propagation</strong>:
   *     <ol>
   *       <li>Expand table names into type-qualified forms (OFFLINE/REALTIME).</li>
   *       <li>Resolve those table names into Helix tags per node type.</li>
   *       <li>If any resolved tag intersects with {@code filterTags}, include the config.</li>
   *     </ol>
   *   </li>
   * </ul>
   *
   * @param pinotHelixResourceManager Resource manager used for table/tenant lookups.
   * @param filterTags Helix tags used as the filter.
   * @param queryWorkloadConfigs Candidate workload configs to evaluate.
   * @return A set of configs whose propagation scope matches the filter tags.
   */
  public static Set<QueryWorkloadConfig> getQueryWorkloadConfigsForTags(
      PinotHelixResourceManager pinotHelixResourceManager, List<String> filterTags,
      List<QueryWorkloadConfig> queryWorkloadConfigs) {
    Set<QueryWorkloadConfig> matchedConfigs = new HashSet<>();
    Map<String, Map<NodeConfig.Type, Set<String>>> tableToHelixTags = getTableToHelixTags(pinotHelixResourceManager);

    for (QueryWorkloadConfig queryWorkloadConfig : queryWorkloadConfigs) {
      for (NodeConfig nodeConfig : queryWorkloadConfig.getNodeConfigs()) {
        PropagationScheme scheme = nodeConfig.getPropagationScheme();
        List<String> topLevelIds = getTopLevelCostIds(scheme);
        if (scheme.getPropagationType() == PropagationScheme.Type.TENANT) {
          for (String tenant : topLevelIds) {
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
          for (String tableName : topLevelIds) {
            TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
            List<String> tablesWithType = (tableType == null)
                ? Arrays.asList(TableNameBuilder.OFFLINE.tableNameWithType(tableName),
                    TableNameBuilder.REALTIME.tableNameWithType(tableName))
                : Collections.singletonList(tableName);
            for (String tableWithType : tablesWithType) {
              Set<String> resolvedTags = tableToHelixTags
                  .getOrDefault(tableWithType, Collections.emptyMap())
                  .getOrDefault(nodeConfig.getNodeType(), Collections.emptySet());
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

  /**
   * Returns all possible Helix tags for a given tenant name.
   *
   * <p>This includes broker, offline, and realtime tags.</p>
   *
   * @param tenantName Tenant name.
   * @return A list of Helix tags for the tenant.
   */
  private static List<String> getAllPossibleHelixTagsFor(String tenantName) {
    List<String> helixTags = new ArrayList<>();
    helixTags.add(TagNameUtils.getBrokerTagForTenant(tenantName));
    helixTags.add(TagNameUtils.getOfflineTagForTenant(tenantName));
    helixTags.add(TagNameUtils.getRealtimeTagForTenant(tenantName));
    return helixTags;
  }

  /**
   * Extracts all top-level cost IDs from the given propagation scheme.
   *
   * @param propagationScheme The propagation scheme containing cost splits.
   * @return A list of top-level cost IDs (non-null).
   */
  private static List<String> getTopLevelCostIds(PropagationScheme propagationScheme) {
    List<String> topLevelCostIds = new ArrayList<>();
    for (PropagationEntity propagationEntity : propagationScheme.getPropagationEntities()) {
      String propagationEntityId = propagationEntity.getEntity();
      if (propagationEntityId != null) {
        topLevelCostIds.add(propagationEntityId);
      }
    }
    return topLevelCostIds;
  }

  /**
   * Merges the delta cost map into the target map by summing CPU and memory costs.
   *
   * @param target The target map to merge into.
   * @param delta The delta map whose values are added.
   */
  public static void mergeCosts(Map<String, InstanceCost> target, Map<String, InstanceCost> delta) {
    for (Map.Entry<String, InstanceCost> e : delta.entrySet()) {
      target.merge(e.getKey(), e.getValue(), (oldCost, newCost) ->
          new InstanceCost(
              oldCost.getCpuCostNs() + newCost.getCpuCostNs(),
              oldCost.getMemoryCostBytes() + newCost.getMemoryCostBytes()));
    }
  }
}
