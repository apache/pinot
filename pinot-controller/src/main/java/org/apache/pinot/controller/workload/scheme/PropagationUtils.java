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
 * This class provides utility methods for workload propagation.
 */
public class PropagationUtils {

  private PropagationUtils() {
  }

  /**
   * Get the mapping tableNameWithType → {BROKER_NODE→brokerTag, SERVER_NODE→(serverTag + overrides)}
   * 1. Get all table configs from the PinotHelixResourceManager
   * 2. For each table config, extract the tenant config
   * 3. For each tenant config, get the broker and server tags
   * 4. Populate the helix tags for BROKER_NODE and SERVER_NODE separately
   */
  public static Map<String, Map<NodeConfig.Type, Set<String>>> getTableToHelixTags(
          PinotHelixResourceManager pinotResourceManager) {
    Map<String, Map<NodeConfig.Type, Set<String>>> tableToTags = new HashMap<>();
    for (TableConfig tableConfig : pinotResourceManager.getAllTableConfigs()) {
      TenantConfig tenantConfig = tableConfig.getTenantConfig();
      TableType tableType = tableConfig.getTableType();

      // Gather all relevant tags for this tenant
      List<String> tenantTags = new ArrayList<>();
      collectHelixTagsForTable(tenantTags, tenantConfig, tableType);

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
   * Get the helix tags for a given table name.
   * If the table name does not have a type suffix, it will return both offline and realtime tags.
   */
  public static List<String> getHelixTagsForTable(PinotHelixResourceManager pinotResourceManager, String tableName) {
    List<String> combinedTags = new ArrayList<>();
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
   * Returns the set of {@link QueryWorkloadConfig}s that match any of the given Helix tags.
   *
   * This method filters the provided list of QueryWorkloadConfigs based on whether their propagation
   * targets intersect with the specified `filterTags`. The matching is performed based on the
   * propagation type defined for each node in the config:
   *
   * - For {@code TENANT} propagation:
   *   1. Each value in the propagation scheme is treated as a tenant name or direct Helix tag.
   *   2. If the value is a recognized Helix tag (broker/server), it is used directly.
   *   3. Otherwise, the value is resolved to possible broker and server tags for the tenant.
   *   4. If any resolved tag matches one of the `filterTags`, the config is included.
   *
   * - For {@code TABLE} propagation:
   *   1. Table names are expanded to include type-suffixed forms (OFFLINE and/or REALTIME).
   *   2. These table names are mapped to corresponding Helix tags using node type.
   *   3. If any of the mapped tags intersect with `filterTags`, the config is included.
   *
   * @param pinotHelixResourceManager The resource manager used to look up table and instance metadata.
   * @param filterTags The set of Helix tags used for filtering configs.
   * @param queryWorkloadConfigs The full list of workload configs to evaluate.
   * @return A set of workload configs whose propagation targets intersect with the filterTags.
   */
  public static Set<QueryWorkloadConfig> getQueryWorkloadConfigsForTags(
      PinotHelixResourceManager pinotHelixResourceManager, List<String> filterTags,
      List<QueryWorkloadConfig> queryWorkloadConfigs) {
    Set<QueryWorkloadConfig> matchedConfigs = new HashSet<>();
    Map<String, Map<NodeConfig.Type, Set<String>>> tableToHelixTags = getTableToHelixTags(pinotHelixResourceManager);

    for (QueryWorkloadConfig queryWorkloadConfig : queryWorkloadConfigs) {
      for (NodeConfig nodeConfig : queryWorkloadConfig.getNodeConfigs()) {
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

  private static List<String> getAllPossibleHelixTagsFor(String tenantName) {
    List<String> helixTags = new ArrayList<>();
    helixTags.add(TagNameUtils.getBrokerTagForTenant(tenantName));
    helixTags.add(TagNameUtils.getOfflineTagForTenant(tenantName));
    helixTags.add(TagNameUtils.getRealtimeTagForTenant(tenantName));
    return helixTags;
  }
}
