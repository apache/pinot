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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.pinot.common.messages.QueryWorkloadRefreshMessage;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.workload.scheme.DefaultPropagationScheme;
import org.apache.pinot.controller.workload.scheme.PropagationUtils;
import org.apache.pinot.controller.workload.scheme.TablePropagationScheme;
import org.apache.pinot.controller.workload.scheme.TenantPropagationScheme;
import org.apache.pinot.controller.workload.splitter.CostSplitter;
import org.apache.pinot.controller.workload.splitter.InstancesInfo;
import org.apache.pinot.spi.config.workload.InstanceCost;
import org.apache.pinot.spi.config.workload.NodeConfig;
import org.apache.pinot.spi.config.workload.PropagationScheme;
import org.apache.pinot.spi.config.workload.QueryWorkloadConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The PropagationManager class is responsible for propagating the query workload
 * refresh message to the relevant instances based on the node configurations.
 */
public class QueryWorkloadManager {
  public static final Logger LOGGER = LoggerFactory. getLogger(QueryWorkloadManager.class);

  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final TablePropagationScheme _tablePropagationScheme;
  private final TenantPropagationScheme _tenantPropagationScheme;
  private final DefaultPropagationScheme _defaultPropagationScheme;
  private final CostSplitter _costSplitter;

  public QueryWorkloadManager(PinotHelixResourceManager pinotHelixResourceManager, CostSplitter costSplitter) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _costSplitter = costSplitter;
    _tablePropagationScheme = new TablePropagationScheme(pinotHelixResourceManager);
    _tenantPropagationScheme = new TenantPropagationScheme(pinotHelixResourceManager);
    _defaultPropagationScheme = new DefaultPropagationScheme(pinotHelixResourceManager);
  }

  /**
   * Propagate the workload to the relevant instances based on the PropagationScheme
   * @param queryWorkloadConfig The query workload configuration to propagate
   */
  public void propagateWorkload(QueryWorkloadConfig queryWorkloadConfig) {
    long startTime = System.currentTimeMillis();
    Map<NodeConfig.Type, NodeConfig> nodeConfigs = queryWorkloadConfig.getNodeConfigs();
    String queryWorkloadName = queryWorkloadConfig.getQueryWorkloadName();
    nodeConfigs.forEach((nodeType, nodeConfig) -> {
      Set<String> instances = resolveInstances(nodeType, nodeConfig);
      if (instances.isEmpty()) {
        String errorMsg = String.format("No instances found for Workload: %s", queryWorkloadName);
        LOGGER.warn(errorMsg);
        System.out.println(errorMsg);
        return;
      }
      long startTimeForCost = System.currentTimeMillis();
      Map<String, InstanceCost> instanceCostMap = _costSplitter.getInstanceCostMap(nodeConfig,
          new InstancesInfo(instances));
      Map<String, QueryWorkloadRefreshMessage> instanceToRefreshMessageMap = instanceCostMap.entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey,
              entry -> new QueryWorkloadRefreshMessage(queryWorkloadName, entry.getValue())));
      long endTimeForCost = System.currentTimeMillis();
      System.out.printf("Query workload cost calculation time %dms for workload: %s%n",
          (endTimeForCost - startTimeForCost), queryWorkloadName);
      _pinotHelixResourceManager.sendQueryWorkloadRefreshMessage(instanceToRefreshMessageMap);
      long endTime = System.currentTimeMillis();
      System.out.printf("Query workload propagation time %dms for workload: %s%n", (endTime - startTime),
          queryWorkloadName);
    });
  }

  /**
   * Propagate the workload for the given table name
   * 1. Find all the helix tags associated with the table
   * 2. Find all the {@link QueryWorkloadConfig} associated with the helix tags
   * 3. Propagate the workload cost for instances associated with the workloads
   *
   * @param tableName The table name to propagate the workload for
   */
  public void propagateWorkloadFor(String tableName) {
    long startTime = System.currentTimeMillis();
    try {
      // Get the helixTags associated with the table
      Set<String> helixTags = PropagationUtils.getHelixTagsForTable(_pinotHelixResourceManager, tableName);
      Set<QueryWorkloadConfig> allqueryWorkloadConfigs = new HashSet<>();
      Map<String, Set<QueryWorkloadConfig>> helixTagsToWorkloadConfigs
          = PropagationUtils.getHelixTagToWorkloadConfigs(_pinotHelixResourceManager);
      // Find all workloads associated with the helix tags
      for (String helix : helixTags) {
        Set<QueryWorkloadConfig> queryWorkloadConfigs = helixTagsToWorkloadConfigs.get(helix);
        if (queryWorkloadConfigs != null) {
          allqueryWorkloadConfigs.addAll(queryWorkloadConfigs);
        }
      }
      for (QueryWorkloadConfig queryWorkloadConfig : allqueryWorkloadConfigs) {
        propagateWorkload(queryWorkloadConfig);
      }
    } catch (Exception e) {
      String errorMsg = String.format("Failed to propagate workload for table: %s", tableName);
      LOGGER.error(errorMsg, e);
      throw new RuntimeException(errorMsg, e);
    }
    long endTime = System.currentTimeMillis();
    System.out.printf("Query workload propagation time %dms ", (endTime - startTime));
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

      // Find all the workloads associated with the helix tags
      Map<String, Set<QueryWorkloadConfig>> helixTagsToWorkloadConfigs
          = PropagationUtils.getHelixTagToWorkloadConfigs(_pinotHelixResourceManager);
      Set<QueryWorkloadConfig> allQueryWorkloadConfigs = new HashSet<>();
      for (String helixTag : helixTags) {
        Set<QueryWorkloadConfig> queryWorkloadConfigs = helixTagsToWorkloadConfigs.get(helixTag);
        if (queryWorkloadConfigs == null) {
          continue;
        }
        allQueryWorkloadConfigs.addAll(queryWorkloadConfigs);
      }
      // Calculate the instance cost from each workload
      for (QueryWorkloadConfig queryWorkloadConfig : allQueryWorkloadConfigs) {
        workloadToInstanceCostMap.computeIfAbsent(queryWorkloadConfig.getQueryWorkloadName(), k -> {
          Set<String> instances = resolveInstances(nodeType, queryWorkloadConfig.getNodeConfigs().get(nodeType));
          NodeConfig nodeConfig = queryWorkloadConfig.getNodeConfigs().get(nodeType);
          return _costSplitter.getInstanceCost(nodeConfig, new InstancesInfo(instances), instanceName);
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
    PropagationScheme.Type propagationType = nodeConfig.getPropagationScheme().getPropagationType();
    Set<String> instances;
    switch (propagationType) {
      case TABLE:
        instances = _tablePropagationScheme.resolveInstances(nodeType, nodeConfig);
        break;
      case TENANT:
        instances = _tenantPropagationScheme.resolveInstances(nodeType, nodeConfig);
        break;
      default:
        instances = _defaultPropagationScheme.resolveInstances(nodeType, nodeConfig);
        break;
    }
    return instances;
  }
}
