package org.apache.pinot.controller.workload.scheme;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.workload.NodeConfig;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;

/**
 * TablePropagationScheme is used to resolve instances based on the {@link NodeConfig} and {@link NodeConfig.Type}
 * It resolves the instances based on the table names specified in the node configuration
 */
public class TablePropagationScheme implements PropagationScheme {

  private static PinotHelixResourceManager _pinotHelixResourceManager;

  public TablePropagationScheme(PinotHelixResourceManager pinotHelixResourceManager) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
  }

  @Override
  public Set<String> resolveInstances(NodeConfig.Type nodeType, NodeConfig nodeConfig) {
    Set<String> instances = new HashSet<>();
    List<String> tableNames = nodeConfig.getPropagationScheme().getValues();
    Map<String, Map<NodeConfig.Type, Set<String>>> tableToHelixTags = PropagationUtils.getTableToHelixTags(_pinotHelixResourceManager);
    Map<String, Set<String>> helixTagToInstances = PropagationUtils.getHelixTagToInstances(_pinotHelixResourceManager);
    for (String tableName : tableNames) {
      TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
      List<String> tablesWithType = new ArrayList<>();
      if (tableType == null) {
        // Get both offline and realtime table names if type is not present.
        tablesWithType.add(TableNameBuilder.OFFLINE.tableNameWithType(tableName));
        tablesWithType.add(TableNameBuilder.REALTIME.tableNameWithType(tableName));
      } else {
        tablesWithType.add(tableName);
      }
      for (String tableWithType : tablesWithType) {
        Map<NodeConfig.Type, Set<String>> nodeToHelixTags = tableToHelixTags.get(tableWithType);
        if (nodeToHelixTags != null) {
          Set<String> helixTags = nodeToHelixTags.get(nodeType);
          if (helixTags != null) {
            for (String helixTag : helixTags) {
              Set<String> helixInstances = helixTagToInstances.get(helixTag);
              if (helixInstances != null) {
                instances.addAll(helixInstances);
              }
            }
          }
        }
      }
    }
    return instances;
  }
}
