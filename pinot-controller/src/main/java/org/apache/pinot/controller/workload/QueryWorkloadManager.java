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
package org.apache.pinot.controller.workload;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.pinot.common.messages.QueryWorkloadRefreshMessage;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.workload.scheme.PropagationScheme;
import org.apache.pinot.controller.workload.scheme.PropagationSchemeProvider;
import org.apache.pinot.controller.workload.scheme.PropagationUtils;
import org.apache.pinot.controller.workload.splitter.CostSplitter;
import org.apache.pinot.controller.workload.splitter.DefaultCostSplitter;
import org.apache.pinot.spi.config.workload.InstanceCost;
import org.apache.pinot.spi.config.workload.NodeConfig;
import org.apache.pinot.spi.config.workload.QueryWorkloadConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The PropagationManager class is responsible for propagating the query workload
 * refresh message to the relevant instances based on the node configurations.
 */
public class QueryWorkloadManager {
  public static final Logger LOGGER = LoggerFactory.getLogger(QueryWorkloadManager.class);

  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private PropagationSchemeProvider _propagationSchemeProvider;
  private final CostSplitter _costSplitter;
  private final boolean _enabled;

  public QueryWorkloadManager(PinotHelixResourceManager pinotHelixResourceManager, boolean enabled) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _enabled = enabled;
    _propagationSchemeProvider.init(pinotHelixResourceManager);
    // TODO: To make this configurable once we have multiple cost splitters implementations
    _costSplitter = new DefaultCostSplitter();
  }

  /**
   * Propagate the workload to the relevant instances based on the PropagationScheme
   * @param queryWorkloadConfig The query workload configuration to propagate
   * 1. Resolve the instances based on the node type and propagation scheme
   * 2. Calculate the instance cost for each instance
   * 3. Send the {@link QueryWorkloadRefreshMessage} to the instances
   */
  public void propagateWorkload(QueryWorkloadConfig queryWorkloadConfig) {
    Map<NodeConfig.Type, NodeConfig> nodeConfigs = queryWorkloadConfig.getNodeConfigs();
    String queryWorkloadName = queryWorkloadConfig.getQueryWorkloadName();
    nodeConfigs.forEach((nodeType, nodeConfig) -> {
      // Resolve the instances based on the node type and propagation scheme
      Set<String> instances = resolveInstances(nodeType, nodeConfig);
      if (instances.isEmpty()) {
        String errorMsg = String.format("No instances found for Workload: %s", queryWorkloadName);
        LOGGER.warn(errorMsg);
        return;
      }
      // Calculate the instance cost for each instance
      Map<String, InstanceCost> instanceCostMap = _costSplitter.computeInstanceCostMap(nodeConfig, instances);
      Map<String, QueryWorkloadRefreshMessage> instanceToRefreshMessageMap = instanceCostMap.entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getKey,
                      entry -> new QueryWorkloadRefreshMessage(queryWorkloadName, entry.getValue())));
      // Send the QueryWorkloadRefreshMessage to the instances
      _pinotHelixResourceManager.sendQueryWorkloadRefreshMessage(instanceToRefreshMessageMap);
    });
  }

  /**
   * Propagate the workload for the given table name
   * @param tableName The table name to propagate the workload for, it can be a rawTableName or a tableNameWithType
   * if rawTableName is provided, it will resolve all available tableTypes and propagate the workload for each tableType
   *
   * This method performs the following steps:
   * 1. Find all the helix tags associated with the table
   * 2. Find all the {@link QueryWorkloadConfig} associated with the helix tags
   * 3. Propagate the workload cost for instances associated with the workloads
   */
  public void propagateWorkloadFor(String tableName) {
    if (_enabled) {
      try {
        // Get the helixTags associated with the table
        Set<String> helixTags = PropagationUtils.getHelixTagsForTable(_pinotHelixResourceManager, tableName);
        // Find all workloads associated with the helix tags
        Set<QueryWorkloadConfig> queryWorkloadConfigsForTags
                = PropagationUtils.getQueryWorkloadConfigsForTags(_pinotHelixResourceManager, helixTags);
        // Propagate the workload for each QueryWorkloadConfig
        for (QueryWorkloadConfig queryWorkloadConfig : queryWorkloadConfigsForTags) {
          propagateWorkload(queryWorkloadConfig);
        }
      } catch (Exception e) {
        String errorMsg = String.format("Failed to propagate workload for table: %s", tableName);
        LOGGER.error(errorMsg, e);
        throw new RuntimeException(errorMsg, e);
      }
    }
  }

  /**
   * Get all the workload costs associated with the given instance and node type
   * 1. Find all the helix tags associated with the instance
   * 2. Find all the {@link QueryWorkloadConfig} associated with the helix tags
   * 3. Find the instance associated with the {@link QueryWorkloadConfig} and node type
   *
   * @param instanceName The instance name to get the workload costs for
   * @param nodeType {@link NodeConfig.Type} The node type to get the workload costs for
   * @return A map of workload name to {@link InstanceCost} for the given instance and node type
   */
  public Map<String, InstanceCost> getWorkloadToInstanceCostFor(String instanceName, NodeConfig.Type nodeType) {
    try {
      Map<String, InstanceCost> workloadToInstanceCostMap = new HashMap<>();
      // Find all the helix tags associated with the instance
      Map<String, Set<String>> instanceToHelixTags
          = PropagationUtils.getInstanceToHelixTags(_pinotHelixResourceManager);
      Set<String> helixTags = instanceToHelixTags.get(instanceName);
      // Find all workloads associated with the helix tags
      Set<QueryWorkloadConfig> queryWorkloadConfigsForTags
              = PropagationUtils.getQueryWorkloadConfigsForTags(_pinotHelixResourceManager, helixTags);
      // Calculate the instance cost from each workload
      for (QueryWorkloadConfig queryWorkloadConfig : queryWorkloadConfigsForTags) {
        workloadToInstanceCostMap.computeIfAbsent(queryWorkloadConfig.getQueryWorkloadName(), k -> {
          Set<String> instances = resolveInstances(nodeType, queryWorkloadConfig.getNodeConfigs().get(nodeType));
          NodeConfig nodeConfig = queryWorkloadConfig.getNodeConfigs().get(nodeType);
          return _costSplitter.computeInstanceCost(nodeConfig, instances, instanceName);
        });
      }
      return workloadToInstanceCostMap;
    } catch (Exception e) {
      String errorMsg = String.format("Failed to get workload to instance cost map for instance: %s, nodeType: %s",
          instanceName, nodeType.getJsonValue());
      LOGGER.error(errorMsg, e);
      throw new RuntimeException(errorMsg, e);
    }
  }

  private Set<String> resolveInstances(NodeConfig.Type nodeType, NodeConfig nodeConfig) {
    PropagationScheme propagationScheme =
            _propagationSchemeProvider.getPropagationScheme(nodeConfig.getPropagationScheme().getPropagationType());
    return propagationScheme.resolveInstances(nodeType, nodeConfig);
  }
}
