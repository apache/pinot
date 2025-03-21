package org.apache.pinot.controller.workload.scheme;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TagOverrideConfig;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.config.workload.NodeConfig;
import org.apache.pinot.spi.config.workload.PropagationScheme;
import org.apache.pinot.spi.config.workload.QueryWorkloadConfig;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/**
 * PropagationUtils is used to get the mapping between
 * 1. Table to Helix tags
 * 2. Helix tags to instances
 * 3. Instance to Helix tags
 * 4. Helix tags to workload configs
 */
public class PropagationUtils {

  private PropagationUtils() {
  }

  /**
   * Get the mapping between table name -> node type -> helix tags
   * The node type additional maps is need to differentiate between LEAF_NODE and NON_LEAF_NODE tags, this provides the
   * flexibility to target only the leaf nodes or non-leaf nodes for workload propagation
   *
   */
  public static Map<String, Map<NodeConfig.Type, Set<String>>> getTableToHelixTags(
      PinotHelixResourceManager pinotHelixResourceManager) {
    List<TableConfig> tableConfigs = pinotHelixResourceManager.getAllTableConfigs();
    Map<String, Map<NodeConfig.Type, Set<String>>> tableToHelixTags = new HashMap<>();
    for (TableConfig tableConfig : tableConfigs) {
      String tableName = tableConfig.getTableName();
      TenantConfig tenantConfig = tableConfig.getTenantConfig();
      Map<NodeConfig.Type, Set<String>> nodeToHelixTags
          = tableToHelixTags.computeIfAbsent(tableName, k -> new HashMap<>());
      nodeToHelixTags.computeIfAbsent(NodeConfig.Type.NON_LEAF_NODE,
          k -> new HashSet<>()).add(TagNameUtils.getBrokerTagForTenant(tenantConfig.getBroker()));
      String serverTag = null;
      if (tableConfig.getTableType() == TableType.OFFLINE) {
        serverTag = TagNameUtils.getOfflineTagForTenant(tenantConfig.getServer());
      } else if (tableConfig.getTableType() == TableType.REALTIME) {
        serverTag = TagNameUtils.getRealtimeTagForTenant(tenantConfig.getServer());
      }
      nodeToHelixTags.computeIfAbsent(NodeConfig.Type.LEAF_NODE, k -> new HashSet<>()).add(serverTag);
      TagOverrideConfig tagOverrideConfig = tenantConfig.getTagOverrideConfig();
      if (tagOverrideConfig != null) {
        Set<String> leafNodeTenants = nodeToHelixTags.computeIfAbsent(NodeConfig.Type.LEAF_NODE, k -> new HashSet<>());
        Optional.ofNullable(tagOverrideConfig.getRealtimeCompleted()).ifPresent(leafNodeTenants::add);
        Optional.ofNullable(tagOverrideConfig.getRealtimeConsuming()).ifPresent(leafNodeTenants::add);
      }
    }
    return tableToHelixTags;
  }

  /**
   * Get the mapping between helix tag -> instances
   */
  public static Map<String, Set<String>> getHelixTagToInstances(PinotHelixResourceManager pinotHelixResourceManager) {
    List<InstanceConfig> instanceConfigs = pinotHelixResourceManager.getAllHelixInstanceConfigs();
    Map<String, Set<String>> helixTagToInstances = new HashMap<>();
    for (InstanceConfig instanceConfig : instanceConfigs) {
      String instanceName = instanceConfig.getInstanceName();
      org.apache.pinot.spi.config.instance.InstanceType instanceType = InstanceTypeUtils.getInstanceType(instanceName);
      List<String> tags = instanceConfig.getTags();
      for (String tag : tags) {
        if (instanceType == InstanceType.SERVER) {
          helixTagToInstances.computeIfAbsent(tag, k -> new HashSet<>()).add(instanceName);
        } else if (instanceType == InstanceType.BROKER) {
          helixTagToInstances.computeIfAbsent(tag, k -> new HashSet<>()).add(instanceName);
        }
      }
    }
    return helixTagToInstances;
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
   * Get the mapping between helix tag -> node type -> workload configs
   * The node type additional maps is need to differentiate between LEAF_NODE and NON_LEAF_NODE tags, this provides the
   * flexibility to target only the leaf nodes or non-leaf nodes for workload propagation
   */
  public static Map<String, Set<QueryWorkloadConfig>> getHelixTagToWorkloadConfigs(
      PinotHelixResourceManager pinotHelixResourceManager) {
    Map<String, Set<QueryWorkloadConfig>> helixTagsToWorkloadConfigs = new HashMap<>();
    List<QueryWorkloadConfig> queryWorkloadConfigs = pinotHelixResourceManager.getQueryWorkloadConfigs();
    if (queryWorkloadConfigs == null) {
      return helixTagsToWorkloadConfigs;
    }
    Map<String, Map<NodeConfig.Type, Set<String>>> tableToHelixTags = getTableToHelixTags(pinotHelixResourceManager);
    for (QueryWorkloadConfig queryWorkloadConfig : queryWorkloadConfigs) {
      Map<NodeConfig.Type, NodeConfig> nodeConfigs = queryWorkloadConfig.getNodeConfigs();
      nodeConfigs.forEach((nodeType, nodeConfig) -> {
        PropagationScheme.Type propagationType = nodeConfig.getPropagationScheme().getPropagationType();
        if (propagationType == PropagationScheme.Type.TENANT) {
          List<String> tenantNames = nodeConfig.getPropagationScheme().getValues();
          for (String tenantName : tenantNames) {
            if (nodeType == NodeConfig.Type.LEAF_NODE) {
              if (TagNameUtils.isOfflineServerTag(tenantName) || TagNameUtils.isRealtimeServerTag(tenantName)) {
                helixTagsToWorkloadConfigs.computeIfAbsent(tenantName, k -> new HashSet<>()).add(queryWorkloadConfig);
              } else {
                // Add both offline and realtime server tags
                helixTagsToWorkloadConfigs.computeIfAbsent(TagNameUtils.getOfflineTagForTenant(tenantName),
                    k -> new HashSet<>()).add(queryWorkloadConfig);
                helixTagsToWorkloadConfigs.computeIfAbsent(TagNameUtils.getRealtimeTagForTenant(tenantName),
                    k -> new HashSet<>()).add(queryWorkloadConfig);
              }
            } else if (nodeType == NodeConfig.Type.NON_LEAF_NODE) {
              tenantName = TagNameUtils.getBrokerTagForTenant(tenantName);
              helixTagsToWorkloadConfigs.computeIfAbsent(tenantName, k -> new HashSet<>()).add(queryWorkloadConfig);
            }
          }
        } else if (propagationType == PropagationScheme.Type.TABLE) {
          List<String> tableNames = nodeConfig.getPropagationScheme().getValues();
          for (String tableName : tableNames) {
            TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
            List<String> tablesWithType = new ArrayList<>();
            if (tableType == null) {
              // If table name does not have type suffix, get both offline and realtime table names
              tablesWithType.add(TableNameBuilder.OFFLINE.tableNameWithType(tableName));
              tablesWithType.add(TableNameBuilder.REALTIME.tableNameWithType(tableName));
            } else {
              tablesWithType.add(tableName);
            }
            for (String tableWithType : tablesWithType) {
              Map<NodeConfig.Type, Set<String>> tenants = tableToHelixTags.get(tableWithType);
              if (tenants != null) {
                Set<String> tenantNames = tenants.get(nodeType);
                if (tenantNames != null) {
                  for (String tenantName : tenantNames) {
                    helixTagsToWorkloadConfigs.computeIfAbsent(tenantName, k -> new HashSet<>())
                        .add(queryWorkloadConfig);
                  }
                }
              }
            }
          }
        }
      });
    }
    return helixTagsToWorkloadConfigs;
  }
}
