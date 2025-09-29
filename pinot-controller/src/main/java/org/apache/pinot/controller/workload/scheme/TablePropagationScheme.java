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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.InstancePartitionsUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.workload.NodeConfig;
import org.apache.pinot.spi.config.workload.PropagationEntity;
import org.apache.pinot.spi.config.workload.PropagationEntityOverrides;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;

/**
 * A {@code TablePropagationScheme} resolves instances based on table names in a propagation entity
 *
 * Currently, this requires InstancePartitions to be set up for servers, and an ExternalView for brokers.
 * TODO: Add support for resolving instances based for Balanced Instance Assignment strategy.
 *
 */
public class TablePropagationScheme implements PropagationScheme {

  private final PinotHelixResourceManager _pinotHelixResourceManager;

  public TablePropagationScheme(PinotHelixResourceManager pinotHelixResourceManager) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
  }

  /**
   * Resolves instances based on table names in the propagation entity and node type.
   *
   * <p>
   * For BROKER nodes, all brokers serving the given table are returned.
   * For SERVER nodes, all servers serving the given table are returned. If the table is
   * REALTIME and overrides are provided, only the servers for the specified partition types
   * (CONSUMING or COMPLETED) are returned.
   * </p>
   *
   * Example:
   * <pre>
   *   ["Broker_instance_1", "Broker_instance_2"]
   *   ["Server_instance_1", "Server_instance_2", "Server_instance_3"]
   * </pre>
   */
  @Override
  public Set<String> resolveInstances(PropagationEntity entity, NodeConfig.Type nodeType,
                                      @Nullable PropagationEntityOverrides override) {
    Set<String> instances = new HashSet<>();
    String tableName = entity.getEntity();
    if (nodeType == NodeConfig.Type.BROKER_NODE) {
      instances = getBrokerInstances(tableName);
    } else if (nodeType == NodeConfig.Type.SERVER_NODE) {
      instances = getServerInstances(tableName, override);
    }
    if (instances.isEmpty()) {
      throw new IllegalArgumentException("No instances found for entity: " + entity + " and node type: " + nodeType);
    }
    return instances;
  }

  @Override
  public boolean isOverrideSupported(PropagationEntity entity) {
    // Overrides are only applicable to REALTIME tables
    for (String tableWithType : expandToTablesWithType(entity.getEntity())) {
      TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableWithType);
      if (tableType == TableType.REALTIME) {
        return true;
      }
    }
    return false;
  }

  /**
   * Resolves the set of broker instances responsible for serving the given table name.
   *
   * Returns the first non-empty set of instances since the brokers are shared across all table types.
   */
  private Set<String> getBrokerInstances(String tableName) {
    HelixManager helixManager = _pinotHelixResourceManager.getHelixZkManager();
    ExternalView brokerResource = HelixHelper.getExternalViewForResource(helixManager.getClusterManagmentTool(),
        helixManager.getClusterName(), CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    for (String tableWithType : expandToTablesWithType(tableName)) {
      Set<String> instances = brokerResource.getStateMap(tableWithType) != null
          ? brokerResource.getStateMap(tableWithType).keySet() : Collections.emptySet();
      if (!instances.isEmpty()) {
        return new HashSet<>(instances);
      }
    }
    return Collections.emptySet();
  }

  private Set<String> getServerInstances(String tableName, @Nullable PropagationEntityOverrides override) {
    Set<String> instances = new HashSet<>();
    for (String tableWithType : expandToTablesWithType(tableName)) {
      TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableWithType);
      if (tableType == TableType.OFFLINE) {
        // We skip overrides since they are only applicable to REALTIME tables
        String offlineKey = InstancePartitionsUtils.getInstancePartitionsName(tableWithType,
            InstancePartitionsType.OFFLINE.toString());
        Set<String> offlineInstances = getInstancesFromInstancePartitions(offlineKey);
        // Given we allow for entity without table type, it's possible to not find any OFFLINE instances if the
        // table is REALTIME only. However, we have a check above to ensure at least one table type resolves
        // to instances.
        if (offlineInstances != null && !offlineInstances.isEmpty()) {
          instances.addAll(offlineInstances);
        }
      } else if (tableType == TableType.REALTIME) {
        if (override != null) {
          OverrideEntityType overrideEntityType = OverrideEntityType.fromString(override.getEntity());
          Set<String> overrideInstances = getRealtimeInstances(tableWithType, overrideEntityType);
          // If overrides are provided, we must find at least one instance given it is explicitly defined
          if (overrideInstances.isEmpty()) {
            throw new IllegalArgumentException("No instances found for override: " + override + " and table: "
                + tableWithType);
          }
          instances.addAll(overrideInstances);
        } else {
          // No overrides → union CONSUMING + COMPLETED
          Set<String> realtimeInstances = getRealtimeInstances(tableWithType, null);
          if (!realtimeInstances.isEmpty()) {
            instances.addAll(realtimeInstances);
          }
        }
      }
    }
    return instances;
  }

  /**
   * Returns REALTIME instances for the given tableWithType.
   * If {@code type} is null, returns the union of CONSUMING and COMPLETED.
   */
  private Set<String> getRealtimeInstances(String tableWithType, @Nullable OverrideEntityType type) {
    String consumingKey = InstancePartitionsUtils.getInstancePartitionsName(
        tableWithType, InstancePartitionsType.CONSUMING.toString());
    String completedKey = InstancePartitionsUtils.getInstancePartitionsName(
        tableWithType, InstancePartitionsType.COMPLETED.toString());
    if (type == OverrideEntityType.CONSUMING) {
      Set<String> instances = getInstancesFromInstancePartitions(consumingKey);
      return (instances == null) ? Collections.emptySet() : new HashSet<>(instances);
    } else if (type == OverrideEntityType.COMPLETED) {
      Set<String> instances = getInstancesFromInstancePartitions(completedKey);
      return (instances == null) ? Collections.emptySet() : new HashSet<>(instances);
    }

    // Union CONSUMING + COMPLETED
    Set<String> consuming = getInstancesFromInstancePartitions(consumingKey);
    Set<String> completed = getInstancesFromInstancePartitions(completedKey);

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

  private Set<String> getInstancesFromInstancePartitions(String instancePartitionsName) {
    InstancePartitions instancePartitions = InstancePartitionsUtils.fetchInstancePartitions(
        _pinotHelixResourceManager.getPropertyStore(), instancePartitionsName);
    if (instancePartitions == null) {
      return Collections.emptySet();
    }
    Map<String, Integer> instanceToPartitionIdMap = instancePartitions.getInstanceToPartitionIdMap();
    return instanceToPartitionIdMap.keySet();
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
