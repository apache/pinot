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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.messages.QueryWorkloadRefreshMessage;
import org.apache.pinot.common.utils.config.QueryWorkloadConfigUtils;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.workload.scheme.PropagationScheme;
import org.apache.pinot.controller.workload.scheme.PropagationSchemeProvider;
import org.apache.pinot.controller.workload.scheme.PropagationUtils;
import org.apache.pinot.controller.workload.splitter.CostSplitter;
import org.apache.pinot.controller.workload.splitter.DefaultCostSplitter;
import org.apache.pinot.spi.config.workload.CostSplit;
import org.apache.pinot.spi.config.workload.InstanceCost;
import org.apache.pinot.spi.config.workload.NodeConfig;
import org.apache.pinot.spi.config.workload.QueryWorkloadConfig;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code QueryWorkloadManager} is responsible for managing query workload configurations
 * in a Pinot Helix cluster.
 *
 * <p>
 * It propagates and computes workload costs to be enforced by relevant instances based on
 * the configured propagation scheme. This ensures that workloads can be isolated and resource
 * budgets (CPU and memory) can be enforced consistently across brokers and servers.
 * </p>
 *
 * <p><strong>Responsibilities include:</strong></p>
 * <ul>
 *   <li>Resolving instances based on node type and propagation scheme.</li>
 *   <li>Computing instance costs using a cost split strategy.</li>
 *   <li>Sending {@link QueryWorkloadRefreshMessage} updates to instances with their assigned costs.</li>
 *   <li>Handling workload deletions by propagating delete messages.</li>
 *   <li>Providing lookup APIs for workload costs per instance.</li>
 * </ul>
 */
public class QueryWorkloadManager {
  public static final Logger LOGGER = LoggerFactory.getLogger(QueryWorkloadManager.class);

  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final PropagationSchemeProvider _propagationSchemeProvider;
  private final CostSplitter _costSplitter;

  public QueryWorkloadManager(PinotHelixResourceManager pinotHelixResourceManager) {
    if (pinotHelixResourceManager == null) {
      throw new IllegalArgumentException("PinotHelixResourceManager cannot be null");
    }
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _propagationSchemeProvider = new PropagationSchemeProvider(pinotHelixResourceManager);
    // TODO: To make this configurable once we have multiple cost splitters implementations
    _costSplitter = new DefaultCostSplitter();
  }

  /**
   * Propagates an upsert of a workload's cost configuration to all relevant instances.
   *
   * <p>
   * For each {@link NodeConfig} in the supplied {@link QueryWorkloadConfig}, this method:
   * </p>
   * <ol>
   *   <li>Resolves the {@link PropagationScheme} from the node's configured scheme type.</li>
   *   <li>Computes the per-instance {@link InstanceCost} map using the configured
   *       {@link CostSplitter}.</li>
   *   <li>Sends a {@link QueryWorkloadRefreshMessage} with subtype
   *       {@link QueryWorkloadRefreshMessage#REFRESH_QUERY_WORKLOAD_MSG_SUB_TYPE} to each
   *       instance with its computed cost.</li>
   * </ol>
   *
   * <p>
   * This call is idempotent from the manager's perspective: the same inputs will result in the
   * same set of messages being sent. Instances are expected to apply the new costs immediately.
   * </p>
   *
   * @param queryWorkloadConfig The workload definition (name, node types, budgets, and propagation
   *                            scheme) to propagate.
   */
  public void propagateWorkloadUpdateMessage(QueryWorkloadConfig queryWorkloadConfig) {
    String queryWorkloadName = queryWorkloadConfig.getQueryWorkloadName();
    LOGGER.info("Propagating workload update for: {}", queryWorkloadName);

    for (NodeConfig nodeConfig: queryWorkloadConfig.getNodeConfigs()) {
      try {
        PropagationScheme propagationScheme = _propagationSchemeProvider.getPropagationScheme(
            nodeConfig.getPropagationScheme().getPropagationType());
        // For cost splits with empty cpu or memory cost, distribute the remaining cost evenly among them
        checkAndDistributeEmptyCostSplitsEvenly(nodeConfig);
        Map<String, InstanceCost> instanceCostMap = propagationScheme.resolveInstanceCostMap(nodeConfig, _costSplitter);

        if (instanceCostMap.isEmpty()) {
          LOGGER.warn("No instances found for workload: {} with nodeConfig: {}", queryWorkloadName, nodeConfig);
          continue;
        }

        Map<String, QueryWorkloadRefreshMessage> instanceToRefreshMessageMap = instanceCostMap.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> new QueryWorkloadRefreshMessage(queryWorkloadName,
                QueryWorkloadRefreshMessage.REFRESH_QUERY_WORKLOAD_MSG_SUB_TYPE, entry.getValue())));

        _pinotHelixResourceManager.sendQueryWorkloadRefreshMessage(instanceToRefreshMessageMap);
        LOGGER.info("Successfully propagated workload update for: {} to {} instances", queryWorkloadName,
            instanceCostMap.size());
      } catch (Exception e) {
        String errorMsg = String.format("Failed to propagate workload update for: %s with nodeConfig: %s",
            queryWorkloadName, nodeConfig);
        LOGGER.error(errorMsg, e);
        throw new RuntimeException(errorMsg, e);
      }
    }
  }

  /**
   * Propagates a delete for the given workload to all relevant instances.
   *
   * <p>
   * The method resolves the target instances for each {@link NodeConfig} and sends a
   * {@link QueryWorkloadRefreshMessage} with subtype
   * {@link QueryWorkloadRefreshMessage#DELETE_QUERY_WORKLOAD_MSG_SUB_TYPE}. The payload contains
   * an {@link InstanceCost} of zero, which instructs the instance to remove any local state
   * associated with the workload and stop enforcing costs for it.
   * </p>
   *
   * @param queryWorkloadConfig The workload to delete (only the name and node scoping are used).
   */
  public void propagateDeleteWorkloadMessage(QueryWorkloadConfig queryWorkloadConfig) {
    String queryWorkloadName = queryWorkloadConfig.getQueryWorkloadName();
    LOGGER.info("Propagating workload delete for: {}", queryWorkloadName);

    for (NodeConfig nodeConfig: queryWorkloadConfig.getNodeConfigs()) {
      if (nodeConfig == null) {
        LOGGER.warn("Skipping null NodeConfig for workload delete: {}", queryWorkloadName);
        continue;
      }
      try {
        Set<String> instances = resolveInstances(nodeConfig);
        if (instances.isEmpty()) {
          LOGGER.warn("No instances found for workload delete: {} with nodeConfig: {}", queryWorkloadName, nodeConfig);
          continue;
        }

        QueryWorkloadRefreshMessage deleteMessage = new QueryWorkloadRefreshMessage(queryWorkloadName,
            QueryWorkloadRefreshMessage.DELETE_QUERY_WORKLOAD_MSG_SUB_TYPE, new InstanceCost(0, 0));
        Map<String, QueryWorkloadRefreshMessage> instanceToRefreshMessageMap = instances.stream()
            .collect(Collectors.toMap(instance -> instance, instance -> deleteMessage));

        // Send the QueryWorkloadRefreshMessage to the instances
        _pinotHelixResourceManager.sendQueryWorkloadRefreshMessage(instanceToRefreshMessageMap);
        LOGGER.info("Successfully propagated workload delete for: {} to {} instances", queryWorkloadName,
            instances.size());
      } catch (Exception e) {
        String errorMsg = String.format("Failed to propagate workload delete for: %s with nodeConfig: %s",
            queryWorkloadName, nodeConfig);
        LOGGER.error(errorMsg, e);
        throw new RuntimeException(errorMsg, e);
      }
    }
  }

  /**
   * Propagates workload updates for all workloads that apply to the given table.
   *
   * <p>
   * This helper performs the following:
   * </p>
   * <ol>
   *   <li>Fetches all {@link QueryWorkloadConfig}s from Zookeeper.</li>
   *   <li>Resolves the Helix tags associated with the table (supports raw table names and
   *       type-qualified names).</li>
   *   <li>Filters the workload configs to those whose scope matches the table's tags.</li>
   *   <li>Invokes {@link #propagateWorkloadUpdateMessage(QueryWorkloadConfig)} for each match.</li>
   * </ol>
   *
   * <p>
   * If no workloads are configured, the method returns immediately. Any exception encountered is
   * logged and rethrown as a {@link RuntimeException}.
   * </p>
   *
   * @param tableName The raw or type-qualified table name (e.g., {@code myTable} or
   *                  {@code myTable_OFFLINE}).
   * @throws RuntimeException If propagation fails due to Helix/ZK access or message dispatch
   *                          errors.
   */
  public void propagateWorkloadFor(String tableName) {
    try {
      List<QueryWorkloadConfig> queryWorkloadConfigs = _pinotHelixResourceManager.getAllQueryWorkloadConfigs();
      if (queryWorkloadConfigs.isEmpty()) {
          return;
      }
      // Get the helixTags associated with the table
      List<String> helixTags = PropagationUtils.getHelixTagsForTable(_pinotHelixResourceManager, tableName);
      if (helixTags.isEmpty()) {
        LOGGER.warn("No Helix tags found for table: {}, skipping workload propagation", tableName);
        return;
      }

      // Find all workloads associated with the helix tags
      Set<QueryWorkloadConfig> queryWorkloadConfigsForTags =
          PropagationUtils.getQueryWorkloadConfigsForTags(_pinotHelixResourceManager, helixTags, queryWorkloadConfigs);

      if (queryWorkloadConfigsForTags.isEmpty()) {
        LOGGER.info("No workload configs match table: {}, no propagation needed", tableName);
        return;
      }

      // Propagate the workload for each QueryWorkloadConfig
      int successCount = 0;
      for (QueryWorkloadConfig queryWorkloadConfig : queryWorkloadConfigsForTags) {
        try {
          List<String> errors = QueryWorkloadConfigUtils.validateQueryWorkloadConfig(queryWorkloadConfig);
          if (!errors.isEmpty()) {
            LOGGER.error("Invalid QueryWorkloadConfig: {} for table: {}, errors: {}", queryWorkloadConfig,
                tableName, errors);
            continue;
          }
          propagateWorkloadUpdateMessage(queryWorkloadConfig);
          successCount++;
        } catch (Exception e) {
          LOGGER.error("Failed to propagate workload: {} for table: {}", queryWorkloadConfig.getQueryWorkloadName(),
              tableName, e);
          // Continue with other workloads instead of failing completely
        }
      }
      LOGGER.info("Successfully propagated {} out of {} workloads for table: {}",
          successCount, queryWorkloadConfigsForTags.size(), tableName);
    } catch (Exception e) {
      String errorMsg = String.format("Failed to propagate workload for table: %s", tableName);
      LOGGER.error(errorMsg, e);
      throw new RuntimeException(errorMsg, e);
    }
  }

  /**
   * Computes the effective workload costs for a specific instance.
   *
   * <p>
   * The method infers the node type (broker or server) from the instance name, resolves the
   * instance's Helix tags, and evaluates all configured workloads whose scope includes those tags.
   * For each matching workload, the corresponding {@link InstanceCost} is computed using the
   * workload's {@link PropagationScheme} and the manager's {@link CostSplitter}.
   * </p>
   *
   * <p>
   * If the instance is not a recognized Pinot broker or server, or if its Helix configuration
   * cannot be found, an empty map is returned and a warning is logged.
   * </p>
   *
   * @param instanceName The Helix instance name (e.g., {@code Server_foo_8001} or
   *                     {@code Broker_bar_8099}).
   * @return A map from workload name to {@link InstanceCost} representing the budgets that apply
   *         to the given instance for its role.
   */
  public Map<String, InstanceCost> getWorkloadToInstanceCostFor(String instanceName) {
    LOGGER.debug("Computing workload costs for instance: {}", instanceName);

    Map<String, InstanceCost> workloadToInstanceCostMap = new HashMap<>();

    try {
      List<QueryWorkloadConfig> queryWorkloadConfigs = _pinotHelixResourceManager.getAllQueryWorkloadConfigs();
      if (queryWorkloadConfigs == null || queryWorkloadConfigs.isEmpty()) {
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

      List<String> instanceTags = instanceConfig.getTags();
      if (instanceTags == null || instanceTags.isEmpty()) {
        LOGGER.warn("No tags found for instance: {}, cannot compute workload costs", instanceName);
        return workloadToInstanceCostMap;
      }

      // Filter workloads by the instance's tags
      Set<QueryWorkloadConfig> queryWorkloadConfigsForTags =
          PropagationUtils.getQueryWorkloadConfigsForTags(_pinotHelixResourceManager, instanceTags,
              queryWorkloadConfigs);

      if (queryWorkloadConfigsForTags.isEmpty()) {
        LOGGER.debug("No workload configs match instance: {}", instanceName);
        return workloadToInstanceCostMap;
      }

      // For each workload, aggregate contributions across all applicable nodeConfigs and costSplits
      for (QueryWorkloadConfig queryWorkloadConfig : queryWorkloadConfigsForTags) {
        for (NodeConfig nodeConfig : queryWorkloadConfig.getNodeConfigs()) {
          try {
            if (nodeConfig.getNodeType() == nodeType) {
              List<String> errors = QueryWorkloadConfigUtils.validateQueryWorkloadConfig(queryWorkloadConfig);
              if (!errors.isEmpty()) {
                LOGGER.error("Invalid QueryWorkloadConfig: {} for instance: {}, errors: {}", queryWorkloadConfig,
                    instanceName, errors);
                continue;
              }
              Map<String, InstanceCost> instanceCostMap =
                  _propagationSchemeProvider.getPropagationScheme(nodeConfig.getPropagationScheme()
                          .getPropagationType()).resolveInstanceCostMap(nodeConfig, _costSplitter);

              InstanceCost instanceCost = instanceCostMap.get(instanceName);
              if (instanceCost != null) {
                workloadToInstanceCostMap.put(queryWorkloadConfig.getQueryWorkloadName(), instanceCost);
                LOGGER.info("Found workload cost for instance: {} workload: {} cost: {}",
                    instanceName, queryWorkloadConfig.getQueryWorkloadName(), instanceCost);
              }
              // There should be only one matching nodeConfig (BROKER_NODE or SERVER_NODE) within a workload
              break;
            }
          } catch (Exception e) {
            LOGGER.error("Failed to compute instance cost for instance: {} workload: {}",
                instanceName, queryWorkloadConfig.getQueryWorkloadName(), e);
            // Continue with other workloads instead of failing completely
          }
        }
      }
      LOGGER.info("Computed {} workload costs for instance: {}", workloadToInstanceCostMap.size(), instanceName);
      return workloadToInstanceCostMap;
    } catch (Exception e) {
      String errorMsg = String.format("Failed to compute workload costs for instance: %s", instanceName);
      LOGGER.error(errorMsg, e);
      throw new RuntimeException(errorMsg, e);
    }
  }

  private Set<String> resolveInstances(NodeConfig nodeConfig) {
    PropagationScheme propagationScheme =
        _propagationSchemeProvider.getPropagationScheme(nodeConfig.getPropagationScheme().getPropagationType());
    return propagationScheme.resolveInstances(nodeConfig);
  }

  private void checkAndDistributeEmptyCostSplitsEvenly(NodeConfig nodeConfig) {
    // Check for empty cost splits
    List<CostSplit> costSplits = nodeConfig.getPropagationScheme().getCostSplits();
    List<CostSplit> emptySplits = new ArrayList<>();
    List<CostSplit> nonEmptySplits = new ArrayList<>();
    for (CostSplit costSplit : costSplits) {
      if (costSplit.getCpuCostNs() == null || costSplit.getMemoryCostBytes() == null) {
        emptySplits.add(costSplit);
      } else {
        nonEmptySplits.add(costSplit);
      }
    }
    if (emptySplits.isEmpty()) {
      return;
    }
    // Get remaining cpu and memory cost after removing share of non-empty splits
    long totalCpuCostNs = nodeConfig.getEnforcementProfile().getCpuCostNs();
    long totalMemoryCostBytes = nodeConfig.getEnforcementProfile().getMemoryCostBytes();
    for (CostSplit costSplit : nonEmptySplits) {
      totalCpuCostNs -= costSplit.getCpuCostNs();
      totalMemoryCostBytes -= costSplit.getMemoryCostBytes();
    }
    // Distribute remaining cost equally among empty splits
    int numEmptySplits = emptySplits.size();
    long shareCpuCostNs = totalCpuCostNs / numEmptySplits;
    long shareMemoryCostBytes = totalMemoryCostBytes / numEmptySplits;
    for (CostSplit costSplit : emptySplits) {
      costSplit.setCpuCostNs(shareCpuCostNs);
      costSplit.setMemoryCostBytes(shareMemoryCostBytes);
    }
    // Add empty and non-empty splits back together
    costSplits.clear();
    List<CostSplit> mergedCostSplits = new ArrayList<>();
    mergedCostSplits.addAll(nonEmptySplits);
    mergedCostSplits.addAll(emptySplits);
    // Set merged cost splits back to nodeConfig
    nodeConfig.getPropagationScheme().setCostSplits(mergedCostSplits);
  }
}
