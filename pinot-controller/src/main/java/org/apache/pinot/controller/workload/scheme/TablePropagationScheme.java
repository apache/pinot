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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    long startTime = System.currentTimeMillis();
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
    long endTime = System.currentTimeMillis();
    // Instance Resolution log
    System.out.println("TablePropagationScheme: Instance resolution took " + (endTime - startTime) + " ms for node type: " + nodeType);
    return instances;
  }
}
