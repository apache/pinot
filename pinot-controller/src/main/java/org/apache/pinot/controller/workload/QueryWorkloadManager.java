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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.ControllerTimer;
import org.apache.pinot.common.utils.config.QueryWorkloadConfigUtils;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.common.workload.WorkloadChangeListener;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.workload.scheme.PropagationScheme;
import org.apache.pinot.controller.workload.scheme.PropagationSchemeProvider;
import org.apache.pinot.controller.workload.scheme.PropagationUtils;
import org.apache.pinot.controller.workload.splitter.CostSplitter;
import org.apache.pinot.controller.workload.splitter.DefaultCostSplitter;
import org.apache.pinot.spi.config.workload.InstanceCost;
import org.apache.pinot.spi.config.workload.NodeConfig;
import org.apache.pinot.spi.config.workload.PropagationEntity;
import org.apache.pinot.spi.config.workload.PropagationEntityOverrides;
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
 *   <li>Sending HTTP refresh requests to instances with their assigned costs.</li>
 *   <li>Handling workload deletions by propagating delete requests.</li>
 *   <li>Providing lookup APIs for workload costs per instance.</li>
 * </ul>
 */
public class QueryWorkloadManager implements WorkloadChangeListener {
  public static final Logger LOGGER = LoggerFactory.getLogger(QueryWorkloadManager.class);

  private static final String WORKLOAD_EXECUTOR_THREAD_NAME_FORMAT = "workload-propagation-%d";
  // Core dependencies for workload management
  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final PropagationSchemeProvider _propagationSchemeProvider;
  private final CostSplitter _costSplitter;
  private final WorkloadPropagationClient _propagationClient;
  // Dedicated executor pool for processing workload propagation
  private final ExecutorService _queryWorkloadExecutor;
  private final ControllerMetrics _controllerMetrics;
  // This controls whether to propagate workloads on any (table/instance) change.
  // TODO: Remove this check once we have fully rolled out query workload configs
  private final boolean _enableInstanceChangePropagation;
  private final boolean _enableBrokerChangePropagation;
  private final long _propagationTimeoutSeconds;


  public QueryWorkloadManager(PinotHelixResourceManager pinotHelixResourceManager,
                              ControllerConf controllerConf, ControllerMetrics controllerMetrics) {
    // Initialize core dependencies
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _propagationSchemeProvider = new PropagationSchemeProvider(pinotHelixResourceManager);
    // TODO: Make cost splitter configurable once we have multiple implementations
    _costSplitter = new DefaultCostSplitter();
    // Initialize executor for workload propagation tasks (high-level operations)
    _queryWorkloadExecutor = createQueryWorkloadExecutor(controllerConf);
    // Initialize propagation client for HTTP communication (creates its own executor for HTTP I/O)
    _propagationClient = new WorkloadPropagationClient(pinotHelixResourceManager, controllerConf, controllerMetrics);
    _enableInstanceChangePropagation = controllerConf.enableInstanceChangePropagation();
    _enableBrokerChangePropagation = controllerConf.enableBrokerChangePropagation();
    _controllerMetrics = controllerMetrics;
    _propagationTimeoutSeconds = controllerConf.getControllerWorkloadPropagationTimeoutSeconds();
    LOGGER.info("Initialized QueryWorkloadManager with instance change propagation: {}, broker change propagation: {}",
        _enableInstanceChangePropagation, _enableBrokerChangePropagation);
  }

  private ExecutorService createQueryWorkloadExecutor(ControllerConf controllerConf) {
    int workloadExecutorThreads = controllerConf.getControllerWorkloadExecutorThreads();
    int queueSize = controllerConf.getControllerWorkloadExecutorQueueSize();
    return new ThreadPoolExecutor(workloadExecutorThreads, workloadExecutorThreads,
        60, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(queueSize),
        new ThreadFactoryBuilder().setNameFormat(WORKLOAD_EXECUTOR_THREAD_NAME_FORMAT).setDaemon(true).build(),
        new ThreadPoolExecutor.AbortPolicy());
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
   *   <li>Sends HTTP refresh requests to each instance with its computed cost.</li>
   * </ol>
   *
   * <p>
   *  This call is atomic to the extent possible: if any error occurs during estimating the target instances
   *  and their cost. The entire propagation is aborted and no partial updates are sent to any instances. However,
   *  if any error occurs during sending the HTTP requests, we do retry the propagation up to a configurable number of
   *  times and if it still fails, we don't send delete requests to instances that were successfully updated.
   * </p>
   *
   * @param queryWorkloadConfig The workload definition (name, node types, budgets, and propagation
   *                            scheme) to propagate.
   */
  public void propagateWorkloadUpdateMessage(QueryWorkloadConfig queryWorkloadConfig) {
    String queryWorkloadName = queryWorkloadConfig.getQueryWorkloadName();
    LOGGER.info("Propagating workload update for: {}", queryWorkloadName);
    long startTime = System.currentTimeMillis();
    _controllerMetrics.addMeteredGlobalValue(ControllerMeter.QUERY_WORKLOAD_PROPAGATION_COUNT, 1L);
    try {
      // Run this inside the dedicated executor to ensure that the propagation overhead can be controlled
      CompletableFuture.runAsync(() -> {
        Map<String, InstanceCost> workloadInstanceCostMap = new HashMap<>();
        for (NodeConfig nodeConfig: queryWorkloadConfig.getNodeConfigs()) {
          resolveInstanceCostMap(nodeConfig, workloadInstanceCostMap);
        }
        Map<String, QueryWorkloadRequest> instanceToRefreshRequestMap = workloadInstanceCostMap.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> new QueryWorkloadRequest(
                queryWorkloadName, entry.getValue())));
        // Sends the message only after all nodeConfigs are processed successfully
        // TODO: See if we also need to send a delete message message to entities that were previously targeted but
        //  are no longer targeted by the updated workload config.
        _propagationClient.sendQueryWorkloadMessage(instanceToRefreshRequestMap);
        LOGGER.info("Successfully propagated workload update for: {} to {} instances", queryWorkloadName,
            instanceToRefreshRequestMap.size());
      }, _queryWorkloadExecutor).get(_propagationTimeoutSeconds, TimeUnit.SECONDS);
    } catch (Exception e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.QUERY_WORKLOAD_PROPAGATION_ERROR, 1L);
      if (e instanceof TimeoutException) {
        LOGGER.error("Workload propagation timed out after {} seconds for: {}", _propagationTimeoutSeconds,
            queryWorkloadName);
        throw new RuntimeException("Workload propagation timed out for: " + queryWorkloadName);
      } else if (e instanceof RejectedExecutionException) {
        _controllerMetrics.addMeteredGlobalValue(ControllerMeter.QUERY_WORKLOAD_REQUEST_DROPPED, 1L);
        LOGGER.error("Workload propagation queue full - request dropped for: {}", queryWorkloadName);
        throw new RuntimeException("Workload propagation queue full for: " + queryWorkloadName, e);
      } else {
        LOGGER.error("Workload propagation failed for: {}", queryWorkloadName, e);
        throw new RuntimeException("Workload propagation failed for: " + queryWorkloadName, e);
      }
    } finally {
      long duration = System.currentTimeMillis() - startTime;
      _controllerMetrics.addTimedValue(ControllerTimer.QUERY_WORKLOAD_PROPAGATE_TIME_MS, duration,
          TimeUnit.MILLISECONDS);
    }
  }

  private void resolveInstanceCostMap(NodeConfig nodeConfig, Map<String, InstanceCost> instanceCostMap) {
    PropagationScheme propagationScheme = _propagationSchemeProvider.getPropagationScheme(
        nodeConfig.getPropagationScheme().getPropagationType());
    for (PropagationEntity entity : nodeConfig.getPropagationScheme().getPropagationEntities()) {
      if (entity.getOverrides() != null && propagationScheme.isOverrideSupported(entity)) {
        List<PropagationEntityOverrides> overrides = entity.getOverrides();
        // Apply each override separately and aggregate the instance costs
        for (PropagationEntityOverrides override : overrides) {
          resolveAndAggregateInstanceCosts(propagationScheme, entity, override, nodeConfig.getNodeType(),
              instanceCostMap);
        }
      } else {
        resolveAndAggregateInstanceCosts(propagationScheme, entity, null, nodeConfig.getNodeType(),
            instanceCostMap);
      }
    }
  }

  private void resolveAndAggregateInstanceCosts(PropagationScheme propagationScheme,
                                                PropagationEntity entity, PropagationEntityOverrides override,
                                                NodeConfig.Type nodeType,
                                                Map<String, InstanceCost> workloadInstanceCostMap) {
    Set<String> instances = propagationScheme.resolveInstances(entity, nodeType, override);
    Map<String, InstanceCost> entityInstanceCostMap = _costSplitter.computeInstanceCostMap(entity.getCpuCostNs(),
        entity.getMemoryCostBytes(), instances);
    PropagationUtils.mergeCosts(workloadInstanceCostMap, entityInstanceCostMap);
  }

  /**
   * Propagates a delete for the given workload to all relevant instances.
   *
   * <p>
   * The method resolves the target instances for each {@link NodeConfig} and sends HTTP
   * delete requests to instruct instances to remove local state associated with the workload
   * and stop enforcing costs for it.
   * </p>
   *
   * @param queryWorkloadConfig The workload to delete (only the name and node scoping are used).
   */
  public void propagateDeleteWorkloadMessage(QueryWorkloadConfig queryWorkloadConfig) {
    String queryWorkloadName = queryWorkloadConfig.getQueryWorkloadName();
    LOGGER.info("Propagating workload delete for: {}", queryWorkloadName);
    long startTime = System.currentTimeMillis();
    _controllerMetrics.addMeteredGlobalValue(ControllerMeter.QUERY_WORKLOAD_PROPAGATION_COUNT, 1L);
    try {
      // Run this inside the dedicated executor to ensure that the propagation overhead can be controlled
      CompletableFuture.runAsync(() -> {
        Map<String, QueryWorkloadRequest> instanceToDeleteRequestMap = new HashMap<>();
        for (NodeConfig nodeConfig : queryWorkloadConfig.getNodeConfigs()) {
          Set<String> instances = resolveInstances(nodeConfig);
          if (instances.isEmpty()) {
            LOGGER.warn("No instances found for workload delete: {} with nodeConfig: {}", queryWorkloadName,
                nodeConfig);
            continue;
          }
          QueryWorkloadRequest deleteRequest = new QueryWorkloadRequest(queryWorkloadName, null);
          instanceToDeleteRequestMap.putAll(instances.stream()
              .collect(Collectors.toMap(instance -> instance, instance -> deleteRequest)));
        }
        _propagationClient.sendQueryWorkloadMessage(instanceToDeleteRequestMap);
        LOGGER.info("Successfully propagated workload delete for: {} to {} instances", queryWorkloadName,
            instanceToDeleteRequestMap.size());
      }, _queryWorkloadExecutor).get(_propagationTimeoutSeconds, TimeUnit.SECONDS);
    } catch (Exception e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.QUERY_WORKLOAD_PROPAGATION_ERROR, 1L);
      if (e instanceof TimeoutException) {
        LOGGER.error("Workload delete propagation timed out after {} seconds for: {}", _propagationTimeoutSeconds,
            queryWorkloadName);
        throw new RuntimeException("Workload delete propagation timed out for: " + queryWorkloadName);
      } else if (e instanceof RejectedExecutionException) {
        _controllerMetrics.addMeteredGlobalValue(ControllerMeter.QUERY_WORKLOAD_REQUEST_DROPPED, 1L);
        LOGGER.error("Workload delete propagation queue full - request dropped for: {}", queryWorkloadName, e);
        throw new RuntimeException("Workload delete propagation queue full for: " + queryWorkloadName);
      } else {
        LOGGER.error("Workload delete propagation failed for: {}", queryWorkloadName, e);
        throw new RuntimeException("Workload delete propagation failed for: " + queryWorkloadName);
      }
    } finally {
      long duration = System.currentTimeMillis() - startTime;
      _controllerMetrics.addTimedValue(ControllerTimer.QUERY_WORKLOAD_PROPAGATE_TIME_MS, duration,
          TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Propagates workload configurations for multiple tables efficiently.
   * Deduplicates workloads that apply to multiple tables to avoid redundant propagation.
   *
   * @param tableNames List of table names
   * @param nodeType The node type (BROKER_NODE, SERVER_NODE, or null for both)
   */
  public void propagateWorkloadForTables(Set<String> tableNames, @Nullable NodeConfig.Type nodeType) {
    if (nodeType == null) {
      // Propagate to both broker and server nodes
      propagateWorkloadForTables(tableNames, NodeConfig.Type.BROKER_NODE);
      propagateWorkloadForTables(tableNames, NodeConfig.Type.SERVER_NODE);
      return;
    }
    // Collect all unique helix tags from all tables
    Set<String> allHelixTags = new HashSet<>();
    for (String tableName : tableNames) {
      try {
        Set<String> helixTags = PropagationUtils.getHelixTagsForTable(_pinotHelixResourceManager, tableName, nodeType);
        allHelixTags.addAll(helixTags);
      } catch (Exception e) {
        LOGGER.error("Failed to get helix tags for table: {}", tableName, e);
      }
    }
    if (allHelixTags.isEmpty()) {
      LOGGER.info("No helix tags found for tables: {}", tableNames);
      return;
    }
    propagateWorkloadForHelixTags(allHelixTags);
  }

  /**
   * Common helper method to propagate workload configurations based on Helix tags.
   * Batches multiple workloads going to the same instance into a single message.
   *
   * @param helixTags Set of Helix tags to filter workload configs
   */
  private void propagateWorkloadForHelixTags(Set<String> helixTags) {
    List<QueryWorkloadConfig> queryWorkloadConfigs = _pinotHelixResourceManager.getAllQueryWorkloadConfigs();
    if (queryWorkloadConfigs.isEmpty()) {
      return;
    }
    // Find all workloads associated with the helix tags
    Set<QueryWorkloadConfig> queryWorkloadConfigsForTags =
        PropagationUtils.getQueryWorkloadConfigsForTags(_pinotHelixResourceManager, helixTags, queryWorkloadConfigs);

    if (queryWorkloadConfigsForTags.isEmpty()) {
      LOGGER.info("No workload configs match {}, no propagation needed", helixTags);
      return;
    }
    // Build a map of instance -> (set of workloadNames -> instanceCost) to batch workloads per instance
    Map<String, Map<String, InstanceCost>> instanceToWorkloadCostMap = new HashMap<>();
    int successCount = 0;
    for (QueryWorkloadConfig queryWorkloadConfig : queryWorkloadConfigsForTags) {
      try {
        String queryWorkloadName = queryWorkloadConfig.getQueryWorkloadName();
        Map<String, InstanceCost> workloadInstanceCostMap = new HashMap<>();
        for (NodeConfig nodeConfig: queryWorkloadConfig.getNodeConfigs()) {
          resolveInstanceCostMap(nodeConfig, workloadInstanceCostMap);
        }
        // Group by instance
        for (Map.Entry<String, InstanceCost> entry : workloadInstanceCostMap.entrySet()) {
          String instanceName = entry.getKey();
          instanceToWorkloadCostMap.computeIfAbsent(instanceName, k -> new HashMap<>())
              .put(queryWorkloadName, entry.getValue());
        }
        successCount++;
      } catch (Exception e) {
        LOGGER.error("Error processing workload config: {} for {}", queryWorkloadConfig.getQueryWorkloadName(),
            helixTags, e);
      }
    }
    // Convert to refresh requests and send
    Map<String, QueryWorkloadRequest> instanceToRefreshRequestMap = new HashMap<>();
    for (Map.Entry<String, Map<String, InstanceCost>> entry : instanceToWorkloadCostMap.entrySet()) {
      instanceToRefreshRequestMap.put(entry.getKey(), new QueryWorkloadRequest(entry.getValue(), true));
    }
    // Send all requests
    if (!instanceToRefreshRequestMap.isEmpty()) {
      _propagationClient.sendQueryWorkloadMessage(instanceToRefreshRequestMap);
      LOGGER.info("Successfully propagated {} workloads for {} to {} instances", successCount, helixTags,
          instanceToRefreshRequestMap.size());
    } else {
      LOGGER.info("No instances to propagate workloads for {}", helixTags);
    }
  }

  /**
   * Computes the workload-to-cost mapping for a specific instance. This is used called by the broker/server
   * during startup to load its assigned workloads and budgets.
   *
   * <p>
   * This method iterates through all {@link QueryWorkloadConfig}s stored in Zookeeper and
   * determines which workloads apply to the given instance. For each applicable workload, it
   * computes the {@link InstanceCost} (CPU and memory budgets) assigned to that instance. The
   * computation is based on the workload's {@link PropagationScheme} and the manager's
   * {@link CostSplitter}.
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
    long startTime = System.currentTimeMillis();
    _controllerMetrics.addMeteredGlobalValue(ControllerMeter.QUERY_WORKLOAD_COMPUTE_INSTANCE_COST_COUNT, 1L);
    try {
      // Run this inside the dedicated executor to ensure that computation overhead can be controlled
      return CompletableFuture.supplyAsync(() -> {
        Map<String, InstanceCost> workloadToInstanceCostMap = new HashMap<>();
        InstanceConfig instanceConfig = _pinotHelixResourceManager.getHelixInstanceConfig(instanceName);
        if (instanceConfig == null) {
          LOGGER.warn("InstanceConfig not found for instance: {}", instanceName);
          return workloadToInstanceCostMap;
        }
        List<String> helixTags = instanceConfig.getTags();
        if (helixTags.isEmpty()) {
          LOGGER.warn("No helix tags found for instance: {}", instanceName);
          return workloadToInstanceCostMap;
        }
        List<QueryWorkloadConfig> queryWorkloadConfigs = _pinotHelixResourceManager.getAllQueryWorkloadConfigs();
        if (queryWorkloadConfigs.isEmpty()) {
          return workloadToInstanceCostMap;
        }
        // Filter to only workloads that match this instance's tags
        Set<QueryWorkloadConfig> relevantWorkloadConfigs =
            PropagationUtils.getQueryWorkloadConfigsForTags(_pinotHelixResourceManager, new HashSet<>(helixTags),
                queryWorkloadConfigs);
        // Determine node type from instance name
        NodeConfig.Type nodeType;
        if (InstanceTypeUtils.isServer(instanceName)) {
          nodeType = NodeConfig.Type.SERVER_NODE;
        } else if (InstanceTypeUtils.isBroker(instanceName)) {
          nodeType = NodeConfig.Type.BROKER_NODE;
        } else {
          LOGGER.warn("Instance {} is neither a server nor a broker", instanceName);
          return workloadToInstanceCostMap;
        }
        // Iterate through relevant workloads and compute cost for this instance
        for (QueryWorkloadConfig queryWorkloadConfig : relevantWorkloadConfigs) {
          try {
            List<String> errors = QueryWorkloadConfigUtils.validateQueryWorkloadConfig(queryWorkloadConfig);
            if (!errors.isEmpty()) {
              LOGGER.warn("Invalid QueryWorkloadConfig: {}, errors: {}", queryWorkloadConfig, errors);
              continue;
            }
            String queryWorkloadName = queryWorkloadConfig.getQueryWorkloadName();
            for (NodeConfig nodeConfig : queryWorkloadConfig.getNodeConfigs()) {
              if (nodeConfig.getNodeType() != nodeType) {
                continue;
              }
              Map<String, InstanceCost> instanceCostMap = new HashMap<>();
              resolveInstanceCostMap(nodeConfig, instanceCostMap);
              InstanceCost instanceCost = instanceCostMap.get(instanceName);
              if (instanceCost != null) {
                workloadToInstanceCostMap.put(queryWorkloadName, instanceCost);
                break;
              }
            }
          } catch (Exception e) {
            _controllerMetrics.addMeteredGlobalValue(ControllerMeter.QUERY_WORKLOAD_COMPUTE_INSTANCE_COST_ERROR, 1L);
            LOGGER.error("Error computing cost for workload: {}", queryWorkloadConfig.getQueryWorkloadName(), e);
          }
        }
        LOGGER.info("Computed {} workload costs for instance: {}", workloadToInstanceCostMap.size(), instanceName);
        return workloadToInstanceCostMap;
      }, _queryWorkloadExecutor).get(_propagationTimeoutSeconds, TimeUnit.SECONDS);
    } catch (Exception e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.QUERY_WORKLOAD_COMPUTE_INSTANCE_COST_ERROR, 1L);
      if (e instanceof TimeoutException) {
        LOGGER.error("Workload cost computation timed out after {} seconds for instance: {}",
            _propagationTimeoutSeconds, instanceName);
        throw new RuntimeException("Workload cost computation timed out for: " + instanceName);
      } else if (e instanceof RejectedExecutionException) {
        _controllerMetrics.addMeteredGlobalValue(ControllerMeter.QUERY_WORKLOAD_REQUEST_DROPPED, 1L);
        LOGGER.error("Workload cost computation queue full - request dropped for instance: {}", instanceName);
        throw new RuntimeException("Workload cost computation queue full for: " + instanceName);
      } else {
        LOGGER.error("Failed to compute workload costs for instance: {}", instanceName, e);
        throw new RuntimeException("Failed to compute workload costs for: " + instanceName, e);
      }
    } finally {
      long duration = System.currentTimeMillis() - startTime;
      _controllerMetrics.addTimedValue(ControllerTimer.QUERY_WORKLOAD_COMPUTE_INSTANCE_COST_TIME_MS, duration,
          TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Propagates workload configurations for a specific tenant.
   *
   * <p>
   * This method identifies all workload configurations that are associated with the specified
   * tenant and propagates them to the relevant instances. The tenant name is resolved to its
   * corresponding Helix tags (broker, offline server, and realtime server tags), and all
   * workloads whose propagation scope matches these tags are propagated.
   * </p>
   *
   * <p>
   * The propagation process:
   * </p>
   * <ol>
   *   <li>Resolves the Helix tags associated with the tenant (broker, offline, realtime).</li>
   *   <li>Filters the workload configs to those whose scope matches the tenant's tags.</li>
   *   <li>Invokes {@link #propagateWorkloadUpdateMessage(QueryWorkloadConfig)} for each match.</li>
   * </ol>
   *
   * <p>
   * If no workloads are configured, the method returns immediately. Any exception encountered is
   * logged but does not cause the method to fail completely.
   * </p>
   *
   * @param tenantName The tenant name (e.g., {@code DefaultTenant}).
   */
  public void propagateWorkloadForTenant(String tenantName) {
    NodeConfig.Type nodeType = null;
    if (TagNameUtils.isBrokerTag(tenantName)) {
      nodeType = NodeConfig.Type.BROKER_NODE;
    } else if (TagNameUtils.isServerTag(tenantName)) {
      nodeType = NodeConfig.Type.SERVER_NODE;
    }
    Set<String> helixTags = PropagationUtils.getHelixTagsForTenant(tenantName, nodeType);
    propagateWorkloadForHelixTags(helixTags);
  }

  private Set<String> resolveInstances(NodeConfig nodeConfig) {
    PropagationScheme propagationScheme =
        _propagationSchemeProvider.getPropagationScheme(nodeConfig.getPropagationScheme().getPropagationType());
    Set<String> instances = new HashSet<>();
    for (PropagationEntity entity : nodeConfig.getPropagationScheme().getPropagationEntities()) {
      if (entity.getOverrides() != null && propagationScheme.isOverrideSupported(entity)) {
        List<PropagationEntityOverrides> overrides = entity.getOverrides();
        // Apply each override separately and aggregate the instances
        for (PropagationEntityOverrides override : overrides) {
          instances.addAll(propagationScheme.resolveInstances(entity, nodeConfig.getNodeType(), override));
        }
      } else {
        instances.addAll(propagationScheme.resolveInstances(entity, nodeConfig.getNodeType(), null));
      }
    }
    return instances;
  }

  @Override
  public void onInstancePartitionsChanged(InstancePartitions instancePartitions) {
    _controllerMetrics.addMeteredGlobalValue(ControllerMeter.QUERY_WORKLOAD_LISTENER_CHANGES_COUNT, 1L);
    if (!_enableInstanceChangePropagation) {
      return;
    }
    String instancePartitionsName = instancePartitions.getInstancePartitionsName();
    LOGGER.info("Instance partitions changed for: {}, triggering workload propagation", instancePartitionsName);
    try {
      Set<String> helixTags = getHelixTagsFromInstancePartitions(instancePartitions);
      if (!helixTags.isEmpty()) {
        CompletableFuture.runAsync(() -> propagateWorkloadForHelixTags(helixTags), _queryWorkloadExecutor)
            .exceptionally(ex -> {
              LOGGER.error("Error propagating workload for instance partitions: {}", instancePartitionsName, ex);
              return null;
            });
      }
    } catch (Exception e) {
      if (e instanceof RejectedExecutionException) {
        _controllerMetrics.addMeteredGlobalValue(ControllerMeter.QUERY_WORKLOAD_REQUEST_DROPPED, 1L);
        LOGGER.warn("Workload propagation queue full - cannot propagate for instance partitions change: {}. ",
            instancePartitionsName);
      } else {
        LOGGER.warn("Error handling instance partitions change for: {}", instancePartitionsName, e);
      }
    }
  }

  private Set<String> getHelixTagsFromInstancePartitions(InstancePartitions instancePartitions) {
    Set<String> helixTags = new HashSet<>();
    Set<String> instances = instancePartitions.getInstanceToPartitionIdMap().keySet();
    for (String instanceName : instances) {
      try {
        InstanceConfig instanceConfig = _pinotHelixResourceManager.getHelixInstanceConfig(instanceName);
        if (instanceConfig != null) {
          List<String> tags = instanceConfig.getTags();
          helixTags.addAll(tags);
          // TODO: This assumes that all instances in the instance partitions have the same helix tag types. See if
          // can remove this assumption. The easiest is querying all instances but maybe expensive.
          break;
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to get instance config for: {}", instanceName, e);
      }
    }
    return helixTags;
  }

  @Override
  public void onBrokerResourceChanged(@Nullable List<String> tablesAdded, @Nullable List<String> tablesRemoved) {
    try {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.QUERY_WORKLOAD_LISTENER_CHANGES_COUNT, 1L);
      if ((tablesAdded == null && tablesRemoved == null) || !_enableBrokerChangePropagation) {
        return;
      }
      LOGGER.info("Broker resource changed - tables added: {}, removed: {}, triggering workload propagation",
          tablesAdded, tablesRemoved);
      Set<String> allTables = new HashSet<>();
      if (tablesAdded != null) {
        allTables.addAll(tablesAdded);
      }
      if (tablesRemoved != null) {
        allTables.addAll(tablesRemoved);
      }
      CompletableFuture.runAsync(() ->
          propagateWorkloadForTables(allTables, NodeConfig.Type.BROKER_NODE), _queryWorkloadExecutor);
    } catch (Exception e) {
      if (e instanceof RejectedExecutionException) {
        _controllerMetrics.addMeteredGlobalValue(ControllerMeter.QUERY_WORKLOAD_REQUEST_DROPPED, 1L);
        LOGGER.warn("Workload propagation queue full - cannot propagate for broker resource change: "
            + "tables added: {}, removed: {}.", tablesAdded, tablesRemoved);
      } else {
        LOGGER.warn("Error handling broker resource change for tables added: {}, removed: {}",
            tablesAdded, tablesRemoved, e);
      }
    }
  }
}
