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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.helix.model.InstanceConfig;
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
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The QueryWorkloadManager is responsible for managing the query workload configuration and propagating/computing
 * the cost to be enforced by relevant instances based on the propagation scheme.
 */
public class QueryWorkloadManager {
  public static final Logger LOGGER = LoggerFactory.getLogger(QueryWorkloadManager.class);

  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final PropagationSchemeProvider _propagationSchemeProvider;
  private final CostSplitter _costSplitter;

  public QueryWorkloadManager(PinotHelixResourceManager pinotHelixResourceManager) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _propagationSchemeProvider = new PropagationSchemeProvider(pinotHelixResourceManager);
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
  public void propagateWorkloadUpdateMessage(QueryWorkloadConfig queryWorkloadConfig) {
    String queryWorkloadName = queryWorkloadConfig.getQueryWorkloadName();
    for (NodeConfig nodeConfig: queryWorkloadConfig.getNodeConfigs()) {
      PropagationScheme propagationScheme = _propagationSchemeProvider.getPropagationScheme(
          nodeConfig.getPropagationScheme().getPropagationType());
      Map<String, InstanceCost> instanceCostMap = propagationScheme.resolveInstanceCostMap(nodeConfig, _costSplitter);
      Map<String, QueryWorkloadRefreshMessage> instanceToRefreshMessageMap = instanceCostMap.entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey, entry -> new QueryWorkloadRefreshMessage(queryWorkloadName,
              QueryWorkloadRefreshMessage.REFRESH_QUERY_WORKLOAD_MSG_SUB_TYPE, entry.getValue())));
      _pinotHelixResourceManager.sendQueryWorkloadRefreshMessage(instanceToRefreshMessageMap);
    }
  }

  /**
   * Propagate delete workload refresh message for the given queryWorkloadConfig
   * @param queryWorkloadConfig The query workload configuration to delete
   * 1. Resolve the instances based on the node type and propagation scheme
   * 2. Send the {@link QueryWorkloadRefreshMessage} with DELETE_QUERY_WORKLOAD_MSG_SUB_TYPE to the instances
   */
  public void propagateDeleteWorkloadMessage(QueryWorkloadConfig queryWorkloadConfig) {
    String queryWorkloadName = queryWorkloadConfig.getQueryWorkloadName();
    for (NodeConfig nodeConfig: queryWorkloadConfig.getNodeConfigs()) {
      Set<String> instances = resolveInstances(nodeConfig);
      if (instances.isEmpty()) {
        String errorMsg = String.format("No instances found for Workload: %s", queryWorkloadName);
        LOGGER.warn(errorMsg);
        continue;
      }
      QueryWorkloadRefreshMessage deleteMessage = new QueryWorkloadRefreshMessage(queryWorkloadName,
          QueryWorkloadRefreshMessage.DELETE_QUERY_WORKLOAD_MSG_SUB_TYPE, new InstanceCost(0, 0));
      Map<String, QueryWorkloadRefreshMessage> instanceToRefreshMessageMap = instances.stream()
          .collect(Collectors.toMap(instance -> instance, instance -> deleteMessage));
      // Send the QueryWorkloadRefreshMessage to the instances
      _pinotHelixResourceManager.sendQueryWorkloadRefreshMessage(instanceToRefreshMessageMap);
    }
  }

  /**
   * Propagate the workload for the given table name, it does fast exits if queryWorkloadConfigs is empty
   * @param tableName The table name to propagate the workload for, it can be a rawTableName or a tableNameWithType
   * if rawTableName is provided, it will resolve all available tableTypes and propagate the workload for each tableType
   *
   * This method performs the following steps:
   * 1. Find all the helix tags associated with the table
   * 2. Find all the {@link QueryWorkloadConfig} associated with the helix tags
   * 3. Propagate the workload cost for instances associated with the workloads
   */
  public void propagateWorkloadFor(String tableName) {
    try {
      List<QueryWorkloadConfig> queryWorkloadConfigs = _pinotHelixResourceManager.getAllQueryWorkloadConfigs();
      if (queryWorkloadConfigs.isEmpty()) {
          return;
      }
      // Get the helixTags associated with the table
      List<String> helixTags = PropagationUtils.getHelixTagsForTable(_pinotHelixResourceManager, tableName);
      // Find all workloads associated with the helix tags
      Set<QueryWorkloadConfig> queryWorkloadConfigsForTags =
          PropagationUtils.getQueryWorkloadConfigsForTags(_pinotHelixResourceManager, helixTags, queryWorkloadConfigs);
      // Propagate the workload for each QueryWorkloadConfig
      for (QueryWorkloadConfig queryWorkloadConfig : queryWorkloadConfigsForTags) {
        propagateWorkloadUpdateMessage(queryWorkloadConfig);
      }
    } catch (Exception e) {
      String errorMsg = String.format("Failed to propagate workload for table: %s", tableName);
      LOGGER.error(errorMsg, e);
      throw new RuntimeException(errorMsg, e);
    }
  }

  /**
   * Get all the workload costs associated with the given instance and node type
   * 1. Find all the helix tags associated with the instance
   * 2. Find all the {@link QueryWorkloadConfig} associated with the helix tags
   * 3. Find the instance associated with the {@link QueryWorkloadConfig} and node type
   *
   * @param instanceName The instance name to get the workload costs for
   * @return A map of workload name to {@link InstanceCost} for the given instance and node type
   */
  public Map<String, InstanceCost> getWorkloadToInstanceCostFor(String instanceName) {
    Map<String, InstanceCost> workloadToInstanceCostMap = new HashMap<>();
    List<QueryWorkloadConfig> queryWorkloadConfigs = _pinotHelixResourceManager.getAllQueryWorkloadConfigs();
    if (queryWorkloadConfigs.isEmpty()) {
      LOGGER.warn("No query workload configs found in zookeeper");
      return workloadToInstanceCostMap;
    }

    // Determine node type from instance name
    NodeConfig.Type nodeType;
    if (InstanceTypeUtils.isServer(instanceName)) {
      nodeType = NodeConfig.Type.SERVER_NODE;
    } else if (InstanceTypeUtils.isBroker(instanceName)) {
      nodeType = NodeConfig.Type.BROKER_NODE;
    } else {
      LOGGER.warn("Unsupported instance type: {}, cannot compute workload costs", instanceName);
      return workloadToInstanceCostMap;
    }

    // Find all helix tags for this instance
    InstanceConfig instanceConfig = _pinotHelixResourceManager.getHelixInstanceConfig(instanceName);
    if (instanceConfig == null) {
      LOGGER.warn("Instance config not found for instance: {}", instanceName);
      return workloadToInstanceCostMap;
    }

    // Filter workloads by the instance's tags
    Set<QueryWorkloadConfig> queryWorkloadConfigsForTags =
        PropagationUtils.getQueryWorkloadConfigsForTags(_pinotHelixResourceManager, instanceConfig.getTags(),
            queryWorkloadConfigs);

    // For each workload, aggregate contributions across all applicable nodeConfigs and costSplits
    for (QueryWorkloadConfig queryWorkloadConfig : queryWorkloadConfigsForTags) {
      for (NodeConfig nodeConfig : queryWorkloadConfig.getNodeConfigs()) {
        if (nodeConfig.getNodeType() == nodeType) {
          Map<String, InstanceCost> instanceCostMap =
              _propagationSchemeProvider.getPropagationScheme(nodeConfig.getPropagationScheme().getPropagationType())
                  .resolveInstanceCostMap(nodeConfig, _costSplitter);
          InstanceCost instanceCost = instanceCostMap.get(instanceName);
          if (instanceCost != null) {
            workloadToInstanceCostMap.put(queryWorkloadConfig.getQueryWorkloadName(), instanceCost);
          } else {
            LOGGER.warn("No instance cost found for instance: {} in workload: {}", instanceName,
                queryWorkloadConfig.getQueryWorkloadName());
          }
          // There should be only one matching nodeConfig (BROKER_NODE or SERVER_NODE) within a workload
          break;
        }
      }
    }
    return workloadToInstanceCostMap;
  }

  private Set<String> resolveInstances(NodeConfig nodeConfig) {
    PropagationScheme propagationScheme =
        _propagationSchemeProvider.getPropagationScheme(nodeConfig.getPropagationScheme().getPropagationType());
    return propagationScheme.resolveInstances(nodeConfig);
  }
}
