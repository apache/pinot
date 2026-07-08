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
package org.apache.pinot.query.routing;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.calcite.rel.rules.ImmutableTableOptions;
import org.apache.pinot.calcite.rel.rules.TableOptions;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.routing.LogicalTableRouteInfo;
import org.apache.pinot.core.routing.LogicalTableRouteProvider;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.SegmentsToQuery;
import org.apache.pinot.core.routing.TablePartitionReplicatedServersInfo;
import org.apache.pinot.core.routing.TableRouteInfo;
import org.apache.pinot.core.routing.timeboundary.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.physical.DispatchablePlanContext;
import org.apache.pinot.query.planner.physical.DispatchablePlanMetadata;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code WorkerManager} manages stage to worker assignment.
 *
 * <p>It contains the logic to assign worker to a particular stages. If it is a leaf stage the logic fallback to
 * how Pinot server assigned server and server-segment mapping.
 */
public class WorkerManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerManager.class);
  private static final Random RANDOM = new Random();
  // default shuffle method in v2
  private static final String DEFAULT_SHUFFLE_PARTITION_FUNCTION = "AbsHashCodeSum";
  // default table partition function if not specified in hint
  private static final String DEFAULT_TABLE_PARTITION_FUNCTION = "Murmur";

  private final String _instanceId;
  private final String _hostName;
  private final int _port;
  private final RoutingManager _routingManager;

  public WorkerManager(String instanceId, String hostName, int port, RoutingManager routingManager) {
    _instanceId = instanceId;
    _hostName = hostName;
    _port = port;
    _routingManager = routingManager;
  }

  public String getInstanceId() {
    return _instanceId;
  }

  public String getHostName() {
    return _hostName;
  }

  public int getPort() {
    return _port;
  }

  public RoutingManager getRoutingManager() {
    return _routingManager;
  }

  public void assignWorkers(PlanFragment rootFragment, DispatchablePlanContext context) {
    // ROOT stage doesn't have a QueryServer as it is strictly only reducing results, so here we simply assign the
    // worker instance with identical server/mailbox port number.
    DispatchablePlanMetadata metadata = context.getDispatchablePlanMetadataMap().get(0);
    metadata.setWorkerIdToServerInstanceMap(
        Map.of(0, new QueryServerInstance(_instanceId, _hostName, _port, _port)));

    // Two-pass assignment: leaf stages must be assigned first so that the candidate server information
    // (_nonLookupTables or _leafServerInstances) is fully populated before intermediate stages use it.
    // Without this, literal-only stages (e.g. UNION ALL of constants) that are traversed before any table scan
    // would see an empty candidate set and fall back to all enabled servers across all tenants.
    for (PlanFragment child : rootFragment.getChildren()) {
      assignWorkersToNonRootFragment(child, context, true);
    }
    for (PlanFragment child : rootFragment.getChildren()) {
      assignWorkersToNonRootFragment(child, context, false);
    }
  }

  /// Post-order traversal that assigns workers to either leaf or intermediate fragments.
  /// @param leafOnly when true, only leaf fragments are assigned; when false, only intermediate fragments are assigned
  private void assignWorkersToNonRootFragment(PlanFragment fragment, DispatchablePlanContext context,
      boolean leafOnly) {
    List<PlanFragment> children = fragment.getChildren();
    for (PlanFragment child : children) {
      assignWorkersToNonRootFragment(child, context, leafOnly);
    }
    Map<Integer, DispatchablePlanMetadata> metadataMap = context.getDispatchablePlanMetadataMap();
    DispatchablePlanMetadata metadata = metadataMap.get(fragment.getFragmentId());
    boolean isLeaf = isLeafPlan(metadata);
    if (leafOnly != isLeaf) {
      return;
    }
    if (isLeaf) {
      // TODO: Revisit this logic and see if we can generalize this
      // For LOOKUP join, join is leaf stage because there is no exchange added to the right side of the join. When we
      // find a single local exchange child in the leaf stage, assign workers based on the local exchange child.
      if (isLookupJoin(children)) {
        DispatchablePlanMetadata childMetadata = metadataMap.get(children.get(0).getFragmentId());
        Map<Integer, QueryServerInstance> workerIdToServerInstanceMap = assignWorkersForLocalExchange(childMetadata);
        metadata.setWorkerIdToServerInstanceMap(workerIdToServerInstanceMap);
        metadata.setPartitionFunction(childMetadata.getPartitionFunction());
        // Fake a segments map so that the worker can be correctly identified as leaf stage
        Map<String, List<String>> segmentsMap = Map.of(TableType.OFFLINE.name(), List.of());
        Map<Integer, Map<String, List<String>>> workerIdToSegmentsMap =
            Maps.newHashMapWithExpectedSize(workerIdToServerInstanceMap.size());
        for (Integer workerId : workerIdToServerInstanceMap.keySet()) {
          workerIdToSegmentsMap.put(workerId, segmentsMap);
        }
        metadata.setWorkerIdToSegmentsMap(workerIdToSegmentsMap);
      } else {
        assignWorkersToLeafFragment(fragment, context);
      }
    } else {
      assignWorkersToIntermediateFragment(fragment, context);
    }
  }

  private boolean isLookupJoin(List<PlanFragment> children) {
    if (children.size() != 1) {
      return false;
    }
    PlanNode planNode = children.get(0).getFragmentRoot();
    if (!(planNode instanceof MailboxSendNode)) {
      return false;
    }
    MailboxSendNode mailboxSendNode = (MailboxSendNode) planNode;
    // NOTE: Exclude colocated semi-join which also contains a single SINGLETON exchange.
    return mailboxSendNode.getDistributionType() == RelDistribution.Type.SINGLETON
        && mailboxSendNode.getExchangeType() != PinotRelExchangeType.PIPELINE_BREAKER;
  }

  private boolean isLocalExchange(PlanFragment fragment, DispatchablePlanContext context) {
    PlanNode planNode = fragment.getFragmentRoot();
    if (planNode instanceof MailboxSendNode
        && ((MailboxSendNode) planNode).getDistributionType() == RelDistribution.Type.SINGLETON) {
      DispatchablePlanMetadata dispatchablePlanMetadata =
          context.getDispatchablePlanMetadataMap().get(fragment.getFragmentId());
      // NOTE: Do not count replicated table as local exchange because it needs to follow the worker assignment for its
      //       peer node.
      return dispatchablePlanMetadata.getReplicatedSegments() == null;
    }
    return false;
  }

  private Map<Integer, QueryServerInstance> assignWorkersForLocalExchange(DispatchablePlanMetadata childMetadata) {
    int partitionParallelism = childMetadata.getPartitionParallelism();
    Map<Integer, QueryServerInstance> childWorkerIdToServerInstanceMap = childMetadata.getWorkerIdToServerInstanceMap();
    if (partitionParallelism == 1) {
      return childWorkerIdToServerInstanceMap;
    } else {
      // Create multiple intermediate stage workers on the same instance for each worker in the child
      int numChildWorkers = childWorkerIdToServerInstanceMap.size();
      Map<Integer, QueryServerInstance> workerIdToServerInstanceMap =
          Maps.newHashMapWithExpectedSize(numChildWorkers * partitionParallelism);
      int workerId = 0;
      for (int i = 0; i < numChildWorkers; i++) {
        QueryServerInstance serverInstance = childWorkerIdToServerInstanceMap.get(i);
        for (int j = 0; j < partitionParallelism; j++) {
          workerIdToServerInstanceMap.put(workerId++, serverInstance);
        }
      }
      return workerIdToServerInstanceMap;
    }
  }

  private static boolean isLeafPlan(DispatchablePlanMetadata metadata) {
    return metadata.getScannedTables().size() == 1;
  }

  // --------------------------------------------------------------------------
  // Intermediate stage assign logic
  // --------------------------------------------------------------------------
  protected void assignWorkersToIntermediateFragment(PlanFragment fragment, DispatchablePlanContext context) {
    List<PlanFragment> children = fragment.getChildren();
    Map<Integer, DispatchablePlanMetadata> metadataMap = context.getDispatchablePlanMetadataMap();
    DispatchablePlanMetadata metadata = metadataMap.get(fragment.getFragmentId());

    if (context.getTableNames().isEmpty()) {
      // For constant expression query (no table is accessed), assign it to a random routable server so we don't pick
      // a server that's been excluded from routing by the FailureDetector.
      // TODO: Consider short-circuiting it and directly calculating the result on broker.

      Collection<ServerInstance> serverInstances = _routingManager.getRoutableServerInstanceMap().values();
      int numServers = serverInstances.size();
      if (numServers == 0) {
        LOGGER.error("[RequestId: {}] No server instance found for constant expression query", context.getRequestId());
        throw new IllegalStateException("No server instance found for constant expression query");
      }
      int index = RANDOM.nextInt(numServers);
      Iterator<ServerInstance> iterator = serverInstances.iterator();
      for (int i = 0; i < index; i++) {
        iterator.next();
      }
      metadata.setWorkerIdToServerInstanceMap(Map.of(0, new QueryServerInstance(iterator.next())));
      return;
    }

    if (isPrePartitionAssignment(children, metadataMap)) {
      // If all the children are pre-partitioned the same way, use local exchange.
      DispatchablePlanMetadata firstChildMetadata = metadataMap.get(children.get(0).getFragmentId());
      metadata.setWorkerIdToServerInstanceMap(assignWorkersForLocalExchange(firstChildMetadata));
      metadata.setPartitionFunction(firstChildMetadata.getPartitionFunction());
      return;
    }

    if (metadata.isRequiresSingletonInstance()) {
      // When singleton instance is required, assign it to a random candidate server.
      List<QueryServerInstance> candidateServers = getCandidateServers(context);
      metadata.setWorkerIdToServerInstanceMap(Map.of(0, candidateServers.get(RANDOM.nextInt(candidateServers.size()))));
      return;
    }

    // Assign workers for local exchange if there is one
    DispatchablePlanMetadata localExchangeChildMetadata = null;
    Map<Integer, QueryServerInstance> workerIdToServerInstanceMap = null;
    for (PlanFragment child : children) {
      if (isLocalExchange(child, context)) {
        Preconditions.checkState(localExchangeChildMetadata == null, "Found multiple local exchanges in the children");
        localExchangeChildMetadata = metadataMap.get(child.getFragmentId());
        workerIdToServerInstanceMap = assignWorkersForLocalExchange(localExchangeChildMetadata);
      }
    }

    // If there is no local exchange, assign workers to the servers hosting the tables
    List<QueryServerInstance> candidateServers = null;
    if (workerIdToServerInstanceMap == null) {
      candidateServers = getCandidateServers(context);
      // Sort to ensure deterministic worker ID assignment across stages.
      // This is critical for pre-partitioned exchanges where worker ID N on one stage should to the same physical
      // server as worker ID N on another stage.
      candidateServers.sort(Comparator.comparing(QueryServerInstance::getInstanceId));
      int stageParallelism = Integer.parseInt(
          context.getPlannerContext().getOptions().getOrDefault(QueryOptionKey.STAGE_PARALLELISM, "1"));
      workerIdToServerInstanceMap = Maps.newHashMapWithExpectedSize(candidateServers.size() * stageParallelism);
      int workerId = 0;
      if (stageParallelism == 1) {
        for (QueryServerInstance serverInstance : candidateServers) {
          workerIdToServerInstanceMap.put(workerId++, serverInstance);
        }
      } else {
        for (QueryServerInstance serverInstance : candidateServers) {
          for (int i = 0; i < stageParallelism; i++) {
            workerIdToServerInstanceMap.put(workerId++, serverInstance);
          }
        }
      }
    }

    // Handle replicated leaf stage, fill worker ids based on the assignment from:
    // - Local exchange peer if exists
    // - Intermediate stage workers
    // Do not include workers resulted from parallelism to reduce repeated work
    for (PlanFragment child : children) {
      DispatchablePlanMetadata childMetadata = metadataMap.get(child.getFragmentId());
      Map<String, List<String>> replicatedSegments = childMetadata.getReplicatedSegments();
      if (replicatedSegments != null) {
        // Fill worker ids for the replicated
        assert
            childMetadata.getWorkerIdToServerInstanceMap() == null && childMetadata.getWorkerIdToSegmentsMap() == null;
        Map<Integer, QueryServerInstance> childWorkerIdToServerInstanceMap;
        Map<Integer, Map<String, List<String>>> childWorkerIdToSegmentsMap;
        if (localExchangeChildMetadata != null) {
          childWorkerIdToServerInstanceMap = localExchangeChildMetadata.getWorkerIdToServerInstanceMap();
          childWorkerIdToSegmentsMap = Maps.newHashMapWithExpectedSize(childWorkerIdToServerInstanceMap.size());
          for (Integer workerId : childWorkerIdToServerInstanceMap.keySet()) {
            childWorkerIdToSegmentsMap.put(workerId, replicatedSegments);
          }
        } else {
          List<QueryServerInstance> replicatedLeafServers =
              getCandidateServersForReplicatedLeaf(context, candidateServers);
          int numWorkers = replicatedLeafServers.size();
          childWorkerIdToServerInstanceMap = Maps.newHashMapWithExpectedSize(numWorkers);
          childWorkerIdToSegmentsMap = Maps.newHashMapWithExpectedSize(numWorkers);
          for (int workerId = 0; workerId < numWorkers; workerId++) {
            childWorkerIdToServerInstanceMap.put(workerId, replicatedLeafServers.get(workerId));
            childWorkerIdToSegmentsMap.put(workerId, replicatedSegments);
          }
        }
        childMetadata.setWorkerIdToServerInstanceMap(childWorkerIdToServerInstanceMap);
        childMetadata.setWorkerIdToSegmentsMap(childWorkerIdToSegmentsMap);
      }
    }

    metadata.setWorkerIdToServerInstanceMap(workerIdToServerInstanceMap);
    if (localExchangeChildMetadata != null) {
      metadata.setPartitionFunction(localExchangeChildMetadata.getPartitionFunction());
    } else {
      metadata.setPartitionFunction(DEFAULT_SHUFFLE_PARTITION_FUNCTION);
    }
  }

  private boolean isPrePartitionAssignment(List<PlanFragment> children,
      Map<Integer, DispatchablePlanMetadata> metadataMap) {
    if (children.isEmpty()) {
      return false;
    }
    // Now, is all children needs to be pre-partitioned by the same function and size to allow pre-partition assignment
    // TODO:
    //   1. When partition function is allowed to be configured in exchange we can relax this condition
    //   2. Pick the most colocate assignment instead of picking the first children
    String partitionFunction = null;
    int partitionCount = 0;
    for (PlanFragment child : children) {
      DispatchablePlanMetadata childMetadata = metadataMap.get(child.getFragmentId());
      if (!childMetadata.isPrePartitioned()) {
        return false;
      }
      if (partitionFunction == null) {
        partitionFunction = childMetadata.getPartitionFunction();
      } else if (!partitionFunction.equalsIgnoreCase(childMetadata.getPartitionFunction())) {
        return false;
      }
      Map<Integer, QueryServerInstance> workerIdToServerInstanceMap = childMetadata.getWorkerIdToServerInstanceMap();
      if (workerIdToServerInstanceMap == null) {
        return false;
      }
      int childComputedPartitionCount = workerIdToServerInstanceMap.size() * childMetadata.getPartitionParallelism();
      if (partitionCount == 0) {
        partitionCount = childComputedPartitionCount;
      } else if (childComputedPartitionCount != partitionCount) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns the servers serving any segment of the tables in the query.
   */
  protected List<QueryServerInstance> getCandidateServers(DispatchablePlanContext context) {
    List<QueryServerInstance> candidateServers;
    if (context.isUseLeafServerForIntermediateStage()) {
      Set<QueryServerInstance> leafServerInstances = context.getLeafServerInstances();
      if (leafServerInstances.isEmpty()) {
        // Fall back to use all routable servers if no leaf server is found (e.g., when querying an empty table).
        // Routable excludes servers removed from routing by the FailureDetector.
        LOGGER.warn("[RequestId: {}] No leaf server found with useLeafServerForIntermediateStage enabled, "
            + "falling back to all routable servers", context.getRequestId());
        Map<String, ServerInstance> routableServerInstanceMap = _routingManager.getRoutableServerInstanceMap();
        candidateServers = new ArrayList<>(routableServerInstanceMap.size());
        for (ServerInstance serverInstance : routableServerInstanceMap.values()) {
          candidateServers.add(new QueryServerInstance(serverInstance));
        }
        if (candidateServers.isEmpty()) {
          LOGGER.error("[RequestId: {}] No server instance found for intermediate stage", context.getRequestId());
          throw new IllegalStateException("No server instance found for intermediate stage");
        }
      } else {
        candidateServers = new ArrayList<>(leafServerInstances);
      }
    } else {
      candidateServers = getCandidateServersPerTables(context);
    }
    return candidateServers;
  }

  protected List<QueryServerInstance> getCandidateServersPerTables(DispatchablePlanContext context) {
    Set<String> nonLookupTables = context.getNonLookupTables();
    assert !nonLookupTables.isEmpty();
    Set<String> servers = new HashSet<>();
    for (String tableName : nonLookupTables) {
      TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
      if (tableType == null) {
        Set<String> offlineTableServers = _routingManager.getServingInstances(
            TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(tableName));
        if (offlineTableServers != null) {
          servers.addAll(offlineTableServers);
        }
        Set<String> realtimeTableServers = _routingManager.getServingInstances(
            TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(tableName));
        if (realtimeTableServers != null) {
          servers.addAll(realtimeTableServers);
        }
      } else {
        Set<String> tableServers = _routingManager.getServingInstances(tableName);
        if (tableServers != null) {
          servers.addAll(tableServers);
        }
      }
    }
    // Use the routable server map so that FailureDetector-excluded servers are filtered out from both the fallback and
    // the per-table lookup paths. The {@code servers} set is already filtered via per-table InstanceSelector, but the
    // routable map narrows the fallback path too.
    Map<String, ServerInstance> routableServerInstanceMap = _routingManager.getRoutableServerInstanceMap();
    List<QueryServerInstance> candidateServers;
    if (servers.isEmpty()) {
      // Fall back to use all routable servers if no server is found for the tables.
      // TODO: Revisit if we should throw an exception instead.
      LOGGER.warn("[RequestId: {}] No server instance found for intermediate stage for tables: {}, "
          + "falling back to all routable servers", context.getRequestId(), nonLookupTables);
      candidateServers = new ArrayList<>(routableServerInstanceMap.size());
      for (ServerInstance serverInstance : routableServerInstanceMap.values()) {
        candidateServers.add(new QueryServerInstance(serverInstance));
      }
    } else {
      candidateServers = new ArrayList<>(servers.size());
      for (String server : servers) {
        ServerInstance serverInstance = routableServerInstanceMap.get(server);
        if (serverInstance != null) {
          candidateServers.add(new QueryServerInstance(serverInstance));
        }
      }
    }
    if (candidateServers.isEmpty()) {
      LOGGER.error("[RequestId: {}] No server instance found for intermediate stage for tables: {}",
          context.getRequestId(), nonLookupTables);
      throw new IllegalStateException("No server instance found for intermediate stage for tables: " + nonLookupTables);
    }
    return candidateServers;
  }

  /**
   * Returns the instances to assign to replicated leaf stage children when there is no local exchange peer. By default,
   * uses the same candidates as the intermediate stage.
   *
   * <p>Subclasses can override to use different instances for replicated leaf stages (e.g., when intermediate stages
   * run on non-server instances that cannot scan segments).</p>
   */
  protected List<QueryServerInstance> getCandidateServersForReplicatedLeaf(DispatchablePlanContext context,
      List<QueryServerInstance> intermediateStageWorkers) {
    return intermediateStageWorkers;
  }

  private void assignWorkersToLeafFragment(PlanFragment fragment, DispatchablePlanContext context) {
    DispatchablePlanMetadata metadata = context.getDispatchablePlanMetadataMap().get(fragment.getFragmentId());

    if (!context.isUseLeafServerForIntermediateStage()) {
      context.getNonLookupTables().add(metadata.getScannedTables().get(0));
    }

    Map<String, String> tableOptions = metadata.getTableOptions();
    if (tableOptions != null) {
      if (Boolean.parseBoolean(tableOptions.get(PinotHintOptions.TableHintOptions.IS_REPLICATED))) {
        setSegmentsForReplicatedLeafFragment(metadata, context);
        return;
      }

      String partitionParallelismStr = tableOptions.get(PinotHintOptions.TableHintOptions.PARTITION_PARALLELISM);
      int partitionParallelism = partitionParallelismStr != null ? Integer.parseInt(partitionParallelismStr) : 1;
      Preconditions.checkState(partitionParallelism > 0, "'%s' must be positive: %s, got: %s",
          PinotHintOptions.TableHintOptions.PARTITION_PARALLELISM, partitionParallelism);
      metadata.setPartitionParallelism(partitionParallelism);

      String partitionKey = tableOptions.get(PinotHintOptions.TableHintOptions.PARTITION_KEY);
      if (partitionKey != null) {
        // Broker pruning: build a filter-bearing routing query (null when disabled/unsupported) so the partitioned
        // assignment can drop partitions with no matching segments. Reuses the same gate as the non-partitioned path.
        // Skip pre-partitioned leaves up front: pruning is disabled for them (see computePartitionsToKeep), so don't
        // spend planning time building the routing query, e.g. for colocated-join leaves.
        PinotQuery routingPinotQuery = metadata.isPrePartitioned() ? null
            : extractRoutingQuery(fragment.getFragmentRoot(), metadata.getScannedTables().get(0), context);
        assignWorkersToPartitionedLeafFragment(metadata, context, partitionKey, tableOptions, routingPinotQuery);
        updateContextForLeafStage(metadata, context);
        return;
      }
    }

    if (metadata.getLogicalTableRouteInfo() != null) {
      assignWorkersToNonPartitionedLeafFragmentForLogicalTable(fragment, metadata, context);
    } else {
      assignWorkersToNonPartitionedLeafFragment(fragment, metadata, context);
    }
    updateContextForLeafStage(metadata, context);
  }

  private void updateContextForLeafStage(DispatchablePlanMetadata metadata, DispatchablePlanContext context) {
    filterLeafStageSegments(context, metadata);
    if (context.isUseLeafServerForIntermediateStage()) {
      Map<Integer, QueryServerInstance> workerIdToServerInstanceMap = metadata.getWorkerIdToServerInstanceMap();
      assert workerIdToServerInstanceMap != null;
      context.getLeafServerInstances().addAll(workerIdToServerInstanceMap.values());
    }
    // Track empty leaf stage for short-circuit detection.
    // The replicated path returns early above and is excluded: replicated leaves
    // broadcast segments to all servers rather than populating workerIdToServerInstanceMap.
    context.recordLeafStageAssigned();
    if (metadata.getWorkerIdToServerInstanceMap().isEmpty()) {
      context.recordLeafStageEmpty();
    }
  }

  // --------------------------------------------------------------------------
  // Non-partitioned leaf stage assignment
  // --------------------------------------------------------------------------
  private void assignWorkersToNonPartitionedLeafFragment(PlanFragment fragment, DispatchablePlanMetadata metadata,
      DispatchablePlanContext context) {
    String tableName = metadata.getScannedTables().get(0);
    PinotQuery routingPinotQuery = extractRoutingQuery(fragment.getFragmentRoot(), tableName, context);
    // When broker pruning is enabled, routingPinotQuery carries the leaf stage filter so that segment pruners can
    // eliminate segments. When disabled (null), fall back to an unfiltered SELECT * routing request.
    Map<String, RoutingTable> routingTableMap = routingPinotQuery != null
        ? getRoutingTable(routingPinotQuery, context.getRequestId())
        : getRoutingTable(tableName, context.getRequestId(), context.getPlannerContext().getOptions());
    Preconditions.checkState(!routingTableMap.isEmpty(), "Unable to find routing entries for table: %s", tableName);

    // acquire time boundary info if it is a hybrid table.
    if (routingTableMap.size() > 1) {
      TimeBoundaryInfo timeBoundaryInfo = _routingManager.getTimeBoundaryInfo(
          TableNameBuilder.OFFLINE.tableNameWithType(TableNameBuilder.extractRawTableName(tableName)));
      if (timeBoundaryInfo != null) {
        metadata.setTimeBoundaryInfo(timeBoundaryInfo);
      } else {
        // remove offline table routing if no time boundary info is acquired.
        routingTableMap.remove(TableType.OFFLINE.name());
      }
    }

    // extract all the instances associated to each table type
    Map<ServerInstance, Map<String, List<String>>> serverInstanceToSegmentsMap = new HashMap<>();
    for (Map.Entry<String, RoutingTable> routingEntry : routingTableMap.entrySet()) {
      String tableType = routingEntry.getKey();
      RoutingTable routingTable = routingEntry.getValue();
      // for each server instance, attach all table types and their associated segment list.
      Map<ServerInstance, SegmentsToQuery> segmentsMap = routingTable.getServerInstanceToSegmentsMap();
      for (Map.Entry<ServerInstance, SegmentsToQuery> serverEntry : segmentsMap.entrySet()) {
        Map<String, List<String>> tableTypeToSegmentListMap =
            serverInstanceToSegmentsMap.computeIfAbsent(serverEntry.getKey(), k -> new HashMap<>());
        // TODO: support optional segments for multi-stage engine.
        Preconditions.checkState(tableTypeToSegmentListMap.put(tableType, serverEntry.getValue().getSegments()) == null,
            "Entry for server {} and table type: {} already exist!", serverEntry.getKey(), tableType);
      }

      // attach unavailable segments to metadata
      if (!routingTable.getUnavailableSegments().isEmpty()) {
        metadata.addUnavailableSegments(tableName, routingTable.getUnavailableSegments());
      }
      if (routingPinotQuery != null) {
        context.addNumSegmentsPrunedByBroker(routingTable.getNumPrunedSegments());
      }
    }
    // Sort server instances to ensure deterministic worker ID assignment.
    // This is critical for pre-partitioned exchanges where worker ID N on one stage
    // must map to the same physical server as worker ID N on another stage.
    List<Map.Entry<ServerInstance, Map<String, List<String>>>> sortedServerInstanceToSegmentsMap =
        new ArrayList<>(serverInstanceToSegmentsMap.entrySet());
    sortedServerInstanceToSegmentsMap.sort(Comparator.comparing(entry -> entry.getKey().getInstanceId()));

    // Assign 1 worker per server
    int numWorkers = sortedServerInstanceToSegmentsMap.size();
    Map<Integer, QueryServerInstance> workerIdToServerInstanceMap = Maps.newHashMapWithExpectedSize(numWorkers);
    Map<Integer, Map<String, List<String>>> workerIdToSegmentsMap = Maps.newHashMapWithExpectedSize(numWorkers);

    for (int workerId = 0; workerId < numWorkers; workerId++) {
      Map.Entry<ServerInstance, Map<String, List<String>>> serverEntry =
          sortedServerInstanceToSegmentsMap.get(workerId);
      QueryServerInstance server = new QueryServerInstance(serverEntry.getKey());
      Map<String, List<String>> segmentsMap = serverEntry.getValue();

      workerIdToServerInstanceMap.put(workerId, server);
      workerIdToSegmentsMap.put(workerId, segmentsMap);
    }

    metadata.setWorkerIdToServerInstanceMap(workerIdToServerInstanceMap);
    metadata.setWorkerIdToSegmentsMap(workerIdToSegmentsMap);
  }

  /**
   * Acquire routing table for items listed in {@link TableScanNode}. Creates a bare {@code SELECT *} broker request
   * with no filter, so no broker-side segment pruning occurs.
   *
   * @param tableName table name with or without type suffix.
   * @return keyed-map from table type(s) to routing table(s).
   */
  private Map<String, RoutingTable> getRoutingTable(String tableName, long requestId) {
    return getRoutingTable(tableName, requestId, Map.of());
  }

  private Map<String, RoutingTable> getRoutingTable(String tableName, long requestId,
      Map<String, String> queryOptions) {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType == null) {
      // Raw table name
      Map<String, RoutingTable> routingTableMap = new HashMap<>(4);
      RoutingTable offlineRoutingTable =
          getRoutingTableHelper(TableNameBuilder.OFFLINE.tableNameWithType(tableName), requestId, queryOptions);
      if (offlineRoutingTable != null) {
        routingTableMap.put(TableType.OFFLINE.name(), offlineRoutingTable);
      }
      RoutingTable realtimeRoutingTable =
          getRoutingTableHelper(TableNameBuilder.REALTIME.tableNameWithType(tableName), requestId, queryOptions);
      if (realtimeRoutingTable != null) {
        routingTableMap.put(TableType.REALTIME.name(), realtimeRoutingTable);
      }
      return routingTableMap;
    } else {
      // Table name with type
      RoutingTable routingTable = getRoutingTableHelper(tableName, requestId, queryOptions);
      return routingTable != null ? Map.of(tableType.name(), routingTable) : Map.of();
    }
  }

  /**
   * Acquire routing table using a pre-built {@link PinotQuery} that carries filter expressions for segment pruning.
   * Unlike {@link #getRoutingTable(String, long)} which creates a bare {@code SELECT *} broker request,
   * this overload forwards the filter to the routing manager so broker-side segment pruners can eliminate
   * segments before dispatching to servers.
   *
   * @param pinotQuery the routing query with filter expressions for pruning. Table name may be raw or typed.
   * @return keyed-map from table type(s) to routing table(s).
   */
  private Map<String, RoutingTable> getRoutingTable(PinotQuery pinotQuery, long requestId) {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(pinotQuery.getDataSource().getTableName());
    if (tableType == null) {
      Map<String, RoutingTable> routingTableMap = new HashMap<>(4);
      RoutingTable offlineRoutingTable = getRoutingTableHelper(pinotQuery, requestId, TableType.OFFLINE);
      if (offlineRoutingTable != null) {
        routingTableMap.put(TableType.OFFLINE.name(), offlineRoutingTable);
      }
      RoutingTable realtimeRoutingTable = getRoutingTableHelper(pinotQuery, requestId, TableType.REALTIME);
      if (realtimeRoutingTable != null) {
        routingTableMap.put(TableType.REALTIME.name(), realtimeRoutingTable);
      }
      return routingTableMap;
    } else {
      RoutingTable routingTable = getRoutingTableHelper(pinotQuery, requestId);
      return routingTable != null ? Map.of(tableType.name(), routingTable) : Map.of();
    }
  }

  /**
   * Builds a {@link PinotQuery} from the leaf stage tree for broker-side segment pruning on the logical planner path.
   * Returns {@code null} if broker pruning is disabled or the leaf stage shape is unsupported.
   */
  @Nullable
  private PinotQuery extractRoutingQuery(PlanNode leafStageRoot, String tableName, DispatchablePlanContext context) {
    boolean defaultLogicalPlannerUseBrokerPruning =
        context.getPlannerContext().getEnvConfig().defaultLogicalPlannerUseBrokerPruning();
    boolean useBrokerPruning = QueryOptionsUtils.isUseBrokerPruning(
        context.getPlannerContext().getOptions(), defaultLogicalPlannerUseBrokerPruning);
    if (!useBrokerPruning) {
      return null;
    }
    try {
      PinotQuery pinotQuery = PlanNodeRoutingQueryBuilder.createPinotQueryForRouting(tableName, leafStageRoot, false);
      Map<String, String> queryOptions = context.getPlannerContext().getOptions();
      if (MapUtils.isNotEmpty(queryOptions)) {
        pinotQuery.setQueryOptions(new HashMap<>(queryOptions));
      }
      return pinotQuery;
    } catch (RuntimeException e) {
      LOGGER.warn("Broker pruning skipped for table {} due to unsupported leaf stage shape: {}",
          tableName, e.getMessage());
      return null;
    }
  }

  @Nullable
  private RoutingTable getRoutingTableHelper(String tableNameWithType, long requestId,
      Map<String, String> queryOptions) {
    BrokerRequest brokerRequest =
        CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM \"" + tableNameWithType + "\"");
    if (MapUtils.isNotEmpty(queryOptions) && brokerRequest.isSetPinotQuery()) {
      // Ensure query options (e.g. sampler) are visible to routing selection.
      brokerRequest.getPinotQuery().setQueryOptions(new HashMap<>(queryOptions));
    }
    return _routingManager.getRoutingTable(brokerRequest, requestId);
  }

  @Nullable
  private RoutingTable getRoutingTableHelper(PinotQuery pinotQuery, long requestId) {
    return _routingManager.getRoutingTable(CalciteSqlCompiler.convertToBrokerRequest(pinotQuery), requestId);
  }

  @Nullable
  private RoutingTable getRoutingTableHelper(PinotQuery pinotQuery, long requestId, TableType tableType) {
    PinotQuery copy = pinotQuery.deepCopy();
    copy.getDataSource().setTableName(TableNameBuilder.forType(tableType).tableNameWithType(
        TableNameBuilder.extractRawTableName(pinotQuery.getDataSource().getTableName())));
    return getRoutingTableHelper(copy, requestId);
  }

  // --------------------------------------------------------------------------
  // Replicated non-partitioned leaf stage assignment
  // --------------------------------------------------------------------------
  private void setSegmentsForReplicatedLeafFragment(DispatchablePlanMetadata metadata,
      DispatchablePlanContext context) {
    String tableName = metadata.getScannedTables().get(0);
    Map<String, List<String>> segmentsMap = getSegments(tableName, context.getPlannerContext().getOptions());
    Preconditions.checkState(!segmentsMap.isEmpty(), "Unable to find segments for table: %s", tableName);

    // Acquire time boundary info if it is a hybrid table.
    if (segmentsMap.size() > 1) {
      TimeBoundaryInfo timeBoundaryInfo = _routingManager.getTimeBoundaryInfo(
          TableNameBuilder.OFFLINE.tableNameWithType(TableNameBuilder.extractRawTableName(tableName)));
      if (timeBoundaryInfo != null) {
        metadata.setTimeBoundaryInfo(timeBoundaryInfo);
      } else {
        // Remove offline table segments if no time boundary info is acquired.
        segmentsMap.remove(TableType.OFFLINE.name());
      }
    }

    // TODO: Support unavailable segments and optional segments for replicated leaf stage
    metadata.setReplicatedSegments(segmentsMap);
    filterReplicatedLeafStageSegments(context, metadata);
  }

  /** Extension point to filter the non-replicated leaf-stage per-worker segment assignment; no-op by default. */
  protected void filterLeafStageSegments(DispatchablePlanContext context, DispatchablePlanMetadata metadata) {
  }

  /** Extension point to filter the replicated leaf-stage segments; no-op by default. */
  protected void filterReplicatedLeafStageSegments(DispatchablePlanContext context, DispatchablePlanMetadata metadata) {
  }

  /**
   * Returns the segments for the given table, keyed by table type.
   * TODO: It doesn't handle unavailable segments.
   */
  private Map<String, List<String>> getSegments(String tableName, Map<String, String> queryOptions) {
    String samplerName = MapUtils.isNotEmpty(queryOptions) ? QueryOptionsUtils.getTableSampler(queryOptions) : null;
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType == null) {
      // Raw table name
      Map<String, List<String>> segmentsMap = new HashMap<>(4);
      List<String> offlineSegments =
          setSegmentsHelper(TableNameBuilder.OFFLINE.tableNameWithType(tableName), samplerName);
      if (CollectionUtils.isNotEmpty(offlineSegments)) {
        segmentsMap.put(TableType.OFFLINE.name(), offlineSegments);
      }
      List<String> realtimeSegments =
          setSegmentsHelper(TableNameBuilder.REALTIME.tableNameWithType(tableName), samplerName);
      if (CollectionUtils.isNotEmpty(realtimeSegments)) {
        segmentsMap.put(TableType.REALTIME.name(), realtimeSegments);
      }
      return segmentsMap;
    } else {
      // Table name with type
      List<String> segments = setSegmentsHelper(tableName, samplerName);
      return CollectionUtils.isNotEmpty(segments) ? Map.of(tableType.name(), segments) : Map.of();
    }
  }

  @Nullable
  private List<String> setSegmentsHelper(String tableNameWithType, @Nullable String samplerName) {
    return _routingManager.getSegments(
        CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM \"" + tableNameWithType + "\""), samplerName);
  }

  private void assignWorkersToNonPartitionedLeafFragmentForLogicalTable(PlanFragment fragment,
      DispatchablePlanMetadata metadata, DispatchablePlanContext context) {
    LogicalTableRouteInfo logicalTableRouteInfo = metadata.getLogicalTableRouteInfo();
    Preconditions.checkNotNull(logicalTableRouteInfo);
    LogicalTableRouteProvider tableRouteProvider = new LogicalTableRouteProvider();
    tableRouteProvider.fillRouteMetadata(logicalTableRouteInfo, _routingManager);
    if (logicalTableRouteInfo.getTimeBoundaryInfo() != null) {
      metadata.setTimeBoundaryInfo(logicalTableRouteInfo.getTimeBoundaryInfo());
    }
    Map<String, String> queryOptions = context.getPlannerContext().getOptions();

    // Broker pruning: build a filter-bearing routing query (null when disabled/unsupported). When non-null, each
    // physical table's route below carries the filter so segment pruners can eliminate segments; when null, fall back
    // to an unfiltered SELECT * per table type. The physical route resolution ignores the data source table name (it
    // uses the physical table names), so we set the typed logical name here purely to build a valid broker request.
    String rawTableName = TableNameBuilder.extractRawTableName(
        logicalTableRouteInfo.hasOffline() ? logicalTableRouteInfo.getOfflineTableName()
            : logicalTableRouteInfo.getRealtimeTableName());
    PinotQuery routingPinotQuery = extractRoutingQuery(fragment.getFragmentRoot(), rawTableName, context);

    BrokerRequest offlineBrokerRequest = logicalTableRouteInfo.hasOffline() ? buildLogicalTableRoutingBrokerRequest(
        logicalTableRouteInfo.getOfflineTableName(), routingPinotQuery, queryOptions) : null;
    BrokerRequest realtimeBrokerRequest = logicalTableRouteInfo.hasRealtime() ? buildLogicalTableRoutingBrokerRequest(
        logicalTableRouteInfo.getRealtimeTableName(), routingPinotQuery, queryOptions) : null;

    tableRouteProvider.calculateRoutes(logicalTableRouteInfo, _routingManager, offlineBrokerRequest,
        realtimeBrokerRequest, context.getRequestId());
    if (routingPinotQuery != null) {
      context.addNumSegmentsPrunedByBroker(logicalTableRouteInfo.getNumPrunedSegmentsTotal());
    }

    assignTableSegmentsToWorkers(logicalTableRouteInfo, metadata);
  }

  /**
   * Builds the routing {@link BrokerRequest} for one physical table type of a logical table. When {@code
   * routingPinotQuery} is non-null it carries the leaf-stage filter so segment pruners can eliminate segments;
   * otherwise a bare {@code SELECT *} is used (no pruning). The data source is set to {@code logicalTableNameWithType}
   * (the typed logical table name; physical table names are resolved later by the route provider).
   *
   * <p>The given {@code routingPinotQuery} is not modified: it is deep-copied before the table name is rewritten, so
   * the same instance can be reused to build both the offline and realtime requests.
   */
  @VisibleForTesting
  static BrokerRequest buildLogicalTableRoutingBrokerRequest(String logicalTableNameWithType,
      @Nullable PinotQuery routingPinotQuery, Map<String, String> queryOptions) {
    BrokerRequest brokerRequest;
    if (routingPinotQuery != null) {
      PinotQuery pinotQuery = routingPinotQuery.deepCopy();
      pinotQuery.getDataSource().setTableName(logicalTableNameWithType);
      brokerRequest = CalciteSqlCompiler.convertToBrokerRequest(pinotQuery);
    } else {
      brokerRequest = CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM \"" + logicalTableNameWithType + "\"");
    }
    if (MapUtils.isNotEmpty(queryOptions) && brokerRequest.isSetPinotQuery()) {
      brokerRequest.getPinotQuery().setQueryOptions(new HashMap<>(queryOptions));
    }
    return brokerRequest;
  }

  private static void assignTableSegmentsToWorkers(LogicalTableRouteInfo logicalTableRouteInfo,
      DispatchablePlanMetadata metadata) {
    Map<ServerInstance, Map<String, List<String>>> serverInstanceToLogicalSegmentsMap =
        new HashMap<>();

    if (logicalTableRouteInfo.getOfflineTables() != null) {
      for (TableRouteInfo physicalTableRoute : logicalTableRouteInfo.getOfflineTables()) {
        // Routing table maybe null if no routing table is found OR there are no segments.
        if (physicalTableRoute.getOfflineRoutingTable() != null) {
          transferToServerInstanceLogicalSegmentsMap(physicalTableRoute.getOfflineTableName(),
              physicalTableRoute.getOfflineRoutingTable(), serverInstanceToLogicalSegmentsMap);
        }
      }
    }

    if (logicalTableRouteInfo.getRealtimeTables() != null) {
      for (TableRouteInfo physicalTableRoute : logicalTableRouteInfo.getRealtimeTables()) {
        // Routing table maybe null if no routing table is found OR there are no segments.
        if (physicalTableRoute.getRealtimeRoutingTable() != null) {
          transferToServerInstanceLogicalSegmentsMap(physicalTableRoute.getRealtimeTableName(),
              physicalTableRoute.getRealtimeRoutingTable(), serverInstanceToLogicalSegmentsMap);
        }
      }
    }

    // Sort server instances to ensure deterministic worker ID assignment.
    // This is critical for pre-partitioned exchanges where worker ID N on one stage
    // must map to the same physical server as worker ID N on another stage.
    List<Map.Entry<ServerInstance, Map<String, List<String>>>> sortedServerInstanceToSegmentsMap =
        new ArrayList<>(serverInstanceToLogicalSegmentsMap.entrySet());
    sortedServerInstanceToSegmentsMap.sort(Comparator.comparing(entry -> entry.getKey().getInstanceId()));

    // Assign 1 worker per server
    int numWorkers = sortedServerInstanceToSegmentsMap.size();
    Map<Integer, QueryServerInstance> workerIdToServerInstanceMap = Maps.newHashMapWithExpectedSize(numWorkers);
    Map<Integer, Map<String, List<String>>> workerIdToLogicalTableSegmentsMap =
        Maps.newHashMapWithExpectedSize(numWorkers);

    for (int workerId = 0; workerId < numWorkers; workerId++) {
      Map.Entry<ServerInstance, Map<String, List<String>>> serverEntry =
          sortedServerInstanceToSegmentsMap.get(workerId);
      QueryServerInstance server = new QueryServerInstance(serverEntry.getKey());
      Map<String, List<String>> segmentsMap = serverEntry.getValue();

      workerIdToServerInstanceMap.put(workerId, server);
      workerIdToLogicalTableSegmentsMap.put(workerId, segmentsMap);
    }

    metadata.setWorkerIdToServerInstanceMap(workerIdToServerInstanceMap);
    metadata.setWorkerIdToTableSegmentsMap(workerIdToLogicalTableSegmentsMap);
  }

  private static void transferToServerInstanceLogicalSegmentsMap(String physicalTableName,
      Map<ServerInstance, SegmentsToQuery> segmentsMap,
      Map<ServerInstance, Map<String, List<String>>> serverInstanceToLogicalSegmentsMap) {
    for (Map.Entry<ServerInstance, SegmentsToQuery> serverEntry : segmentsMap.entrySet()) {
      Map<String, List<String>> tableNameToSegmentsMap =
          serverInstanceToLogicalSegmentsMap.computeIfAbsent(serverEntry.getKey(), k -> new HashMap<>());
      // TODO: support optional segments for multi-stage engine.
      Preconditions.checkState(
          tableNameToSegmentsMap.put(physicalTableName, serverEntry.getValue().getSegments()) == null,
          "Entry for server {} and physical table: {} already exist!", serverEntry.getKey(), physicalTableName);
    }
  }

  // --------------------------------------------------------------------------
  // Partitioned leaf stage assignment
  // --------------------------------------------------------------------------
  private void assignWorkersToPartitionedLeafFragment(DispatchablePlanMetadata metadata,
      DispatchablePlanContext context, String partitionKey, Map<String, String> tableOptions,
      @Nullable PinotQuery routingPinotQuery) {
    // when partition key exist, we assign workers for leaf-stage in partitioned fashion.

    String numPartitionsStr = tableOptions.get(PinotHintOptions.TableHintOptions.PARTITION_SIZE);
    Preconditions.checkState(numPartitionsStr != null, "'%s' must be provided for partition key: %s",
        PinotHintOptions.TableHintOptions.PARTITION_SIZE, partitionKey);
    int numWorkers = Integer.parseInt(numPartitionsStr);
    Preconditions.checkState(numWorkers > 0, "'%s' must be positive, got: %s",
        PinotHintOptions.TableHintOptions.PARTITION_SIZE, numWorkers);

    String partitionFunction = tableOptions.getOrDefault(PinotHintOptions.TableHintOptions.PARTITION_FUNCTION,
        DEFAULT_TABLE_PARTITION_FUNCTION);

    String tableName = metadata.getScannedTables().get(0);
    // calculates the partition table info using the routing manager
    PartitionTableInfo partitionTableInfo = calculatePartitionTableInfo(tableName);
    // verifies that the partition table obtained from routing manager is compatible with the hint options
    checkPartitionInfoMap(partitionTableInfo, tableName, partitionKey, partitionFunction, numWorkers);

    PartitionInfo[] partitionInfoMap = partitionTableInfo._partitionInfoMap;
    int numPartitions = partitionInfoMap.length;
    assert numPartitions % numWorkers == 0;
    int numPartitionsPerWorker = numPartitions / numWorkers;

    // Broker pruning: the partitions to keep (null means keep all). Partitions absent from the set are skipped below.
    Set<Integer> partitionsToKeep =
        computePartitionsToKeep(routingPinotQuery, metadata, context.getRequestId(), partitionInfoMap);
    if (partitionsToKeep != null) {
      long numSegmentsPrunedByBroker = countPrunedSegments(partitionInfoMap, partitionsToKeep);
      if (numSegmentsPrunedByBroker > 0) {
        context.addNumSegmentsPrunedByBroker(numSegmentsPrunedByBroker);
      }
    }

    Map<Integer, QueryServerInstance> workerIdToServerInstanceMap = new HashMap<>();
    Map<Integer, Map<String, List<String>>> workerIdToSegmentsMap = new HashMap<>();
    if (numPartitionsPerWorker == 1) {
      assignOnePartitionPerWorker(tableName, context.getRequestId(), partitionInfoMap, partitionsToKeep,
          _routingManager.getEnabledServerInstanceMap(), workerIdToServerInstanceMap, workerIdToSegmentsMap);
    } else {
      assignMultiplePartitionsPerWorker(tableName, context.getRequestId(), numPartitionsPerWorker, partitionInfoMap,
          partitionsToKeep, _routingManager.getEnabledServerInstanceMap(), workerIdToServerInstanceMap,
          workerIdToSegmentsMap);
    }
    metadata.setWorkerIdToServerInstanceMap(workerIdToServerInstanceMap);
    metadata.setWorkerIdToSegmentsMap(workerIdToSegmentsMap);
    metadata.setTimeBoundaryInfo(partitionTableInfo._timeBoundaryInfo);
    metadata.setPartitionFunction(partitionFunction);
  }

  /**
   * Broker pruning for the partitioned leaf path. Returns the set of partition ids that still have at least one segment
   * matching the query filter, or {@code null} to keep all partitions.
   *
   * <p>Returns {@code null} (no pruning) when any of the following hold:
   * <ul>
   *   <li>broker pruning is disabled or the leaf shape is unsupported (the routing query is {@code null}), or there is
   *   no filter to prune with;</li>
   *   <li>the leaf feeds a pre-partitioned (1-to-1 direct) exchange -- dropping/compacting workers would misalign
   *   sender/receiver worker ids in {@code MailboxAssignmentVisitor}. A non-pre-partitioned leaf is shuffled via
   *   {@code connectWorkers}, which re-hashes across any worker count, so pruning is safe there;</li>
   *   <li>routing fails (pruning is best-effort);</li>
   *   <li>every partition would be pruned -- an empty worker map would break exchanges in a multi-leaf plan (the
   *   all-leaves-empty short-circuit does not fire for a partially-empty plan), and the server-side filter still
   *   yields the correct empty result unpruned.</li>
   * </ul>
   *
   * <p>Partition survival is decided by routing the filter-bearing query through the {@link RoutingManager} (the same
   * mechanism the non-partitioned path uses), so the segment-level pruners judge survival using each segment's own
   * partition metadata. This is correct for every partition function and configuration, unlike recomputing the
   * partition id from the table-level function name (which lacks the per-segment function config). A partition is
   * dropped only when every one of its segments was pruned; a segment that merely became unavailable keeps its
   * partition alive so matching data is never silently dropped.
   *
   * <p>Note that pruning here is partition-level, not segment-level: a surviving partition dispatches all of its
   * segments, including ones the pruners eliminated (the server-side pruners drop those again cheaply). This keeps
   * surviving partitions' assignments identical to the unpruned path -- the only behavioral delta is dropped
   * workers -- at the cost of a lower pruning ceiling than the non-partitioned path for partitions with mixed-match
   * segments. Segment-level pruning within surviving partitions is a possible follow-up.
   */
  @Nullable
  private Set<Integer> computePartitionsToKeep(@Nullable PinotQuery routingPinotQuery,
      DispatchablePlanMetadata metadata, long requestId, PartitionInfo[] partitionInfoMap) {
    if (routingPinotQuery == null || routingPinotQuery.getFilterExpression() == null || metadata.isPrePartitioned()) {
      return null;
    }
    Map<String, RoutingTable> routingTableMap;
    try {
      routingTableMap = getRoutingTable(routingPinotQuery, requestId);
    } catch (RuntimeException e) {
      // Pruning is best-effort: never fail a query that would otherwise route successfully unpruned.
      LOGGER.warn("Broker pruning skipped for partitioned table {} due to routing failure: {}",
          routingPinotQuery.getDataSource().getTableName(), e.getMessage());
      return null;
    }
    if (routingTableMap.isEmpty()) {
      return null;
    }
    Set<String> matchedSegments = new HashSet<>();
    for (RoutingTable routingTable : routingTableMap.values()) {
      for (SegmentsToQuery segmentsToQuery : routingTable.getServerInstanceToSegmentsMap().values()) {
        matchedSegments.addAll(segmentsToQuery.getSegments());
      }
      // Keep a partition alive if any of its segments is merely unavailable (rather than pruned) so we never drop data.
      matchedSegments.addAll(routingTable.getUnavailableSegments());
    }
    Set<Integer> partitionsToKeep = new HashSet<>();
    for (int i = 0; i < partitionInfoMap.length; i++) {
      PartitionInfo partitionInfo = partitionInfoMap[i];
      if (partitionInfo != null && (containsAny(partitionInfo._offlineSegments, matchedSegments) || containsAny(
          partitionInfo._realtimeSegments, matchedSegments))) {
        partitionsToKeep.add(i);
      }
    }
    // If everything would be pruned, keep all partitions to avoid an empty worker map (see the javadoc above).
    return partitionsToKeep.isEmpty() ? null : partitionsToKeep;
  }

  private static boolean containsAny(@Nullable List<String> segments, Set<String> matchedSegments) {
    if (segments != null) {
      for (String segment : segments) {
        if (matchedSegments.contains(segment)) {
          return true;
        }
      }
    }
    return false;
  }

  /// Counts the segments in partitions dropped by broker pruning (those absent from {@code partitionsToKeep}).
  private static long countPrunedSegments(PartitionInfo[] partitionInfoMap, Set<Integer> partitionsToKeep) {
    long numPrunedSegments = 0;
    for (int i = 0; i < partitionInfoMap.length; i++) {
      PartitionInfo partitionInfo = partitionInfoMap[i];
      if (partitionInfo != null && !partitionsToKeep.contains(i)) {
        numPrunedSegments += CollectionUtils.size(partitionInfo._offlineSegments)
            + CollectionUtils.size(partitionInfo._realtimeSegments);
      }
    }
    return numPrunedSegments;
  }

  /// Pick one worker per partition for partitioned leaf stage. When {@code partitionsToKeep} is non-null (broker
  /// pruning is active), partitions absent from the set are pruned by the query filter and skipped.
  private void assignOnePartitionPerWorker(String tableName, long requestId, PartitionInfo[] partitionInfoMap,
      @Nullable Set<Integer> partitionsToKeep, Map<String, ServerInstance> enabledServerInstanceMap,
      Map<Integer, QueryServerInstance> workerIdToServerInstanceMap,
      Map<Integer, Map<String, List<String>>> workerIdToSegmentsMap) {
    int numPartitions = partitionInfoMap.length;
    int workerId = 0;
    for (int i = 0; i < numPartitions; i++) {
      // Skip partitions pruned by the broker filter. Empty partitions are never in partitionsToKeep, so under pruning
      // they are skipped here too; the precondition below only fires when pruning is inactive (partitionsToKeep null).
      if (partitionsToKeep != null && !partitionsToKeep.contains(i)) {
        continue;
      }
      PartitionInfo partitionInfo = partitionInfoMap[i];
      // TODO: Currently we don't support the case when a partition doesn't contain any segment. The reason is that
      //       the leaf stage won't be able to directly return empty response.
      Preconditions.checkState(partitionInfo != null, "Failed to find any segment for table: %s, partition: %s",
          tableName, i);
      // NOTE: Pick worker based on the request id plus the partition id (not a running counter) so that the same worker
      //       is picked across different table scans when the segments for the same partition are colocated, and so
      //       that skipping pruned partitions does not shift the server assignment of the surviving ones.
      ServerInstance serverInstance =
          pickEnabledServer(partitionInfo._fullyReplicatedServers, enabledServerInstanceMap, requestId + i);
      Preconditions.checkState(serverInstance != null,
          "Failed to find enabled fully replicated server for table: %s, partition: %s", tableName, i);
      workerIdToServerInstanceMap.put(workerId, new QueryServerInstance(serverInstance));
      workerIdToSegmentsMap.put(workerId,
          getSegmentsMap(partitionInfo._offlineSegments, partitionInfo._realtimeSegments));
      workerId++;
    }
  }

  /// Round-robin partitions to workers, where each worker gets numPartitionsPerWorker partitions. This setup works only
  /// if all segments for these partitions are assigned to the same group of servers. This is useful when user wants to
  /// colocate tables with different partition count, but same partition function.
  /// E.g. when there are 16 partitions for table A and 4 partitions for table B, we may assign 16 partitions for table
  /// A to 4 workers, where partition 0, 4, 8, 12 goes to worker 0, partition 1, 5, 9, 13 goes to worker 1, etc.
  private void assignMultiplePartitionsPerWorker(String tableName, long requestId, int numPartitionsPerWorker,
      PartitionInfo[] partitionInfoMap, @Nullable Set<Integer> partitionsToKeep,
      Map<String, ServerInstance> enabledServerInstanceMap,
      Map<Integer, QueryServerInstance> workerIdToServerInstanceMap,
      Map<Integer, Map<String, List<String>>> workerIdToSegmentsMap) {
    int numPartitions = partitionInfoMap.length;
    assert numPartitions % numPartitionsPerWorker == 0;
    int numWorkers = numPartitions / numPartitionsPerWorker;
    int workerId = 0;
    for (int i = 0; i < numWorkers; i++) {
      Set<String> fullyReplicatedServers = null;
      List<String> offlineSegments = null;
      List<String> realtimeSegments = null;
      for (int j = i; j < numPartitions; j += numWorkers) {
        if (partitionsToKeep != null && !partitionsToKeep.contains(j)) {
          // Partition pruned by the broker filter.
          continue;
        }
        PartitionInfo partitionInfo = partitionInfoMap[j];
        if (partitionInfo == null) {
          continue;
        }
        if (fullyReplicatedServers == null) {
          fullyReplicatedServers = new HashSet<>(partitionInfo._fullyReplicatedServers);
        } else {
          fullyReplicatedServers.retainAll(partitionInfo._fullyReplicatedServers);
        }
        if (partitionInfo._offlineSegments != null) {
          if (offlineSegments == null) {
            offlineSegments = new ArrayList<>(partitionInfo._offlineSegments);
          } else {
            offlineSegments.addAll(partitionInfo._offlineSegments);
          }
        }
        if (partitionInfo._realtimeSegments != null) {
          if (realtimeSegments == null) {
            realtimeSegments = new ArrayList<>(partitionInfo._realtimeSegments);
          } else {
            realtimeSegments.addAll(partitionInfo._realtimeSegments);
          }
        }
      }
      // Without broker pruning we don't support a worker whose partitions all lack segments, because the leaf stage
      // can't directly return an empty response. With pruning active a fully-pruned worker is legitimate and skipped.
      if (fullyReplicatedServers == null) {
        Preconditions.checkState(partitionsToKeep != null,
            "Failed to find any segment for table: %s, worker: %s, partitions per worker: %s", tableName, i,
            numPartitionsPerWorker);
        continue;
      }
      // NOTE: Pick worker based on the request id plus the worker index (not a running counter) so that the same worker
      //       is picked across different table scans when the segments for the same partition are colocated, and so
      //       that skipping fully-pruned workers does not shift the server assignment of the surviving ones.
      ServerInstance serverInstance =
          pickEnabledServer(fullyReplicatedServers, enabledServerInstanceMap, requestId + i);
      Preconditions.checkState(serverInstance != null,
          "Failed to find enabled fully replicated server for table: %s, worker: %s, partitions per worker: %s",
          tableName, i, numPartitionsPerWorker);
      workerIdToServerInstanceMap.put(workerId, new QueryServerInstance(serverInstance));
      workerIdToSegmentsMap.put(workerId, getSegmentsMap(offlineSegments, realtimeSegments));
      workerId++;
    }
  }

  @Nullable
  public TableOptions inferTableOptions(String tableName) {
    try {
      PartitionTableInfo partitionTableInfo = calculatePartitionTableInfo(tableName);
      return ImmutableTableOptions.builder()
          .partitionKey(partitionTableInfo._partitionKey)
          .partitionFunction(partitionTableInfo._partitionFunction)
          .partitionSize(partitionTableInfo._partitionInfoMap.length)
          .build();
    } catch (IllegalStateException e) {
      return null;
    }
  }

  private PartitionTableInfo calculatePartitionTableInfo(String tableName) {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType == null) {
      String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
      String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
      boolean offlineRoutingExists = _routingManager.routingExists(offlineTableName);
      boolean realtimeRoutingExists = _routingManager.routingExists(realtimeTableName);
      Preconditions.checkState(offlineRoutingExists || realtimeRoutingExists, "Routing doesn't exist for table: %s",
          tableName);

      if (offlineRoutingExists && realtimeRoutingExists) {
        TablePartitionReplicatedServersInfo offlineTpi = _routingManager.getTablePartitionReplicatedServersInfo(
            offlineTableName);
        Preconditions.checkState(offlineTpi != null, "Failed to find table partition info for table: %s",
            offlineTableName);
        TablePartitionReplicatedServersInfo realtimeTpi = _routingManager.getTablePartitionReplicatedServersInfo(
            realtimeTableName);
        Preconditions.checkState(realtimeTpi != null, "Failed to find table partition info for table: %s",
            realtimeTableName);
        // For hybrid table, find the common servers for each partition
        TimeBoundaryInfo timeBoundaryInfo = _routingManager.getTimeBoundaryInfo(offlineTableName);
        // Ignore OFFLINE side when time boundary info is unavailable
        if (timeBoundaryInfo == null) {
          return PartitionTableInfo.fromTablePartitionInfo(realtimeTpi, TableType.REALTIME);
        }

        verifyCompatibility(offlineTpi, realtimeTpi);

        TablePartitionReplicatedServersInfo.PartitionInfo[] offlinePartitionInfoMap = offlineTpi.getPartitionInfoMap();
        TablePartitionReplicatedServersInfo.PartitionInfo[] realtimePartitionInfoMap
            = realtimeTpi.getPartitionInfoMap();

        int numPartitions = offlineTpi.getNumPartitions();
        PartitionInfo[] partitionInfoMap = new PartitionInfo[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
          TablePartitionReplicatedServersInfo.PartitionInfo offlinePartitionInfo = offlinePartitionInfoMap[i];
          TablePartitionReplicatedServersInfo.PartitionInfo realtimePartitionInfo = realtimePartitionInfoMap[i];
          if (offlinePartitionInfo == null && realtimePartitionInfo == null) {
            continue;
          }
          if (offlinePartitionInfo == null) {
            partitionInfoMap[i] =
                new PartitionInfo(realtimePartitionInfo._fullyReplicatedServers, null, realtimePartitionInfo._segments);
            continue;
          }
          if (realtimePartitionInfo == null) {
            partitionInfoMap[i] =
                new PartitionInfo(offlinePartitionInfo._fullyReplicatedServers, offlinePartitionInfo._segments, null);
            continue;
          }
          Set<String> fullyReplicatedServers = new HashSet<>(offlinePartitionInfo._fullyReplicatedServers);
          fullyReplicatedServers.retainAll(realtimePartitionInfo._fullyReplicatedServers);
          Preconditions.checkState(!fullyReplicatedServers.isEmpty(),
              "Failed to find fully replicated server for partition: %s in hybrid table: %s", i, tableName);
          partitionInfoMap[i] = new PartitionInfo(fullyReplicatedServers, offlinePartitionInfo._segments,
              realtimePartitionInfo._segments);
        }
        return new PartitionTableInfo(offlineTpi.getPartitionColumn(), offlineTpi.getPartitionFunctionName(),
            partitionInfoMap, timeBoundaryInfo);
      } else if (offlineRoutingExists) {
        return getOfflinePartitionTableInfo(offlineTableName);
      } else {
        return getRealtimePartitionTableInfo(realtimeTableName);
      }
    } else {
      if (tableType == TableType.OFFLINE) {
        return getOfflinePartitionTableInfo(tableName);
      } else {
        return getRealtimePartitionTableInfo(tableName);
      }
    }
  }

  private static void verifyCompatibility(TablePartitionReplicatedServersInfo offlineTpi,
      TablePartitionReplicatedServersInfo realtimeTpi)
      throws IllegalArgumentException {
    Preconditions.checkState(offlineTpi.getPartitionColumn().equals(realtimeTpi.getPartitionColumn()),
        "Partition column mismatch for hybrid table %s: %s offline vs %s online", offlineTpi.getTableNameWithType(),
        offlineTpi.getPartitionColumn(), realtimeTpi.getPartitionColumn());
    Preconditions.checkState(offlineTpi.getNumPartitions() == realtimeTpi.getNumPartitions(),
        "Partition size mismatch for hybrid table %s: %s offline vs %s online", offlineTpi.getTableNameWithType(),
        offlineTpi.getNumPartitions(), realtimeTpi.getNumPartitions());
    Preconditions.checkState(
        offlineTpi.getPartitionFunctionName().equalsIgnoreCase(realtimeTpi.getPartitionFunctionName()),
        "Partition function mismatch for hybrid table %s: %s offline vs %s online", offlineTpi.getTableNameWithType(),
        offlineTpi.getPartitionFunctionName(), realtimeTpi.getPartitionFunctionName());
  }

  /**
   * Verifies that the partition info maps from the table partition info are compatible with the information supplied
   * as arguments.
   */
  private void checkPartitionInfoMap(PartitionTableInfo partitionTableInfo, String tableNameWithType,
      String partitionKey, String partitionFunction, int numPartitions) {
    Preconditions.checkState(partitionTableInfo._partitionKey.equals(partitionKey),
        "Partition key: %s does not match partition column: %s for table: %s", partitionKey,
        partitionTableInfo._partitionKey, tableNameWithType);
    Preconditions.checkState(partitionTableInfo._partitionFunction.equalsIgnoreCase(partitionFunction),
        "Partition function mismatch (hint: %s, table: %s) for table %s", partitionFunction,
        partitionTableInfo._partitionFunction, tableNameWithType);
    Preconditions.checkState(partitionTableInfo._partitionInfoMap.length % numPartitions == 0,
        "Partition size mismatch (hint: %s, table: %s) for table: %s, actual partition size must be multiple of "
            + "hinted partition size", numPartitions, partitionTableInfo._partitionInfoMap.length, tableNameWithType);
  }

  private PartitionTableInfo getOfflinePartitionTableInfo(String offlineTableName) {
    TablePartitionReplicatedServersInfo offlineTpi = _routingManager.getTablePartitionReplicatedServersInfo(
        offlineTableName);
    Preconditions.checkState(offlineTpi != null, "Failed to find table partition info for table: %s", offlineTableName);
    return PartitionTableInfo.fromTablePartitionInfo(offlineTpi, TableType.OFFLINE);
  }

  private PartitionTableInfo getRealtimePartitionTableInfo(String realtimeTableName) {
    TablePartitionReplicatedServersInfo realtimeTpi = _routingManager.getTablePartitionReplicatedServersInfo(
        realtimeTableName);
    Preconditions.checkState(realtimeTpi != null, "Failed to find table partition info for table: %s",
        realtimeTableName);
    return PartitionTableInfo.fromTablePartitionInfo(realtimeTpi, TableType.REALTIME);
  }

  private static class PartitionTableInfo {
    final String _partitionKey;
    final String _partitionFunction;
    final PartitionInfo[] _partitionInfoMap;
    @Nullable
    final TimeBoundaryInfo _timeBoundaryInfo;

    PartitionTableInfo(String partitionKey, String partitionFunction, PartitionInfo[] partitionInfoMap,
        @Nullable TimeBoundaryInfo timeBoundaryInfo) {
      _partitionKey = partitionKey;
      _partitionFunction = partitionFunction;
      _partitionInfoMap = partitionInfoMap;
      _timeBoundaryInfo = timeBoundaryInfo;
    }

    static PartitionTableInfo fromTablePartitionInfo(
        TablePartitionReplicatedServersInfo tablePartitionReplicatedServersInfo,
        TableType tableType) {
      if (!tablePartitionReplicatedServersInfo.getSegmentsWithInvalidPartition().isEmpty()) {
        throw new IllegalStateException(
            "Find " + tablePartitionReplicatedServersInfo.getSegmentsWithInvalidPartition().size()
                + " segments with invalid partition");
      }

      int numPartitions = tablePartitionReplicatedServersInfo.getNumPartitions();
      TablePartitionReplicatedServersInfo.PartitionInfo[] tablePartitionInfoMap = tablePartitionReplicatedServersInfo
          .getPartitionInfoMap();
      PartitionInfo[] workerPartitionInfoMap = new PartitionInfo[numPartitions];
      for (int i = 0; i < numPartitions; i++) {
        TablePartitionReplicatedServersInfo.PartitionInfo partitionInfo = tablePartitionInfoMap[i];
        if (partitionInfo != null) {
          switch (tableType) {
            case OFFLINE:
              workerPartitionInfoMap[i] =
                  new PartitionInfo(partitionInfo._fullyReplicatedServers, partitionInfo._segments, null);
              break;
            case REALTIME:
              workerPartitionInfoMap[i] =
                  new PartitionInfo(partitionInfo._fullyReplicatedServers, null, partitionInfo._segments);
              break;
            default:
              throw new IllegalStateException("Unsupported table type: " + tableType);
          }
        }
      }
      return new PartitionTableInfo(tablePartitionReplicatedServersInfo.getPartitionColumn(),
          tablePartitionReplicatedServersInfo.getPartitionFunctionName(), workerPartitionInfoMap, null);
    }
  }

  private static class PartitionInfo {
    final Set<String> _fullyReplicatedServers;
    final List<String> _offlineSegments;
    final List<String> _realtimeSegments;

    PartitionInfo(Set<String> fullyReplicatedServers, @Nullable List<String> offlineSegments,
        @Nullable List<String> realtimeSegments) {
      _fullyReplicatedServers = fullyReplicatedServers;
      _offlineSegments = offlineSegments;
      _realtimeSegments = realtimeSegments;
    }
  }

  /**
   * Picks an enabled server deterministically based on the given index to pick.
   */
  @Nullable
  private static ServerInstance pickEnabledServer(Set<String> candidates,
      Map<String, ServerInstance> enabledServerInstanceMap, long indexToPick) {
    int numCandidates = candidates.size();
    if (numCandidates == 0) {
      return null;
    }
    if (numCandidates == 1) {
      return enabledServerInstanceMap.get(candidates.iterator().next());
    }
    List<String> candidateList = new ArrayList<>(candidates);
    candidateList.sort(null);
    int startIndex = (int) ((indexToPick & Long.MAX_VALUE) % numCandidates);
    for (int i = 0; i < numCandidates; i++) {
      String server = candidateList.get((startIndex + i) % numCandidates);
      ServerInstance serverInstance = enabledServerInstanceMap.get(server);
      if (serverInstance != null) {
        return serverInstance;
      }
    }
    return null;
  }

  private static Map<String, List<String>> getSegmentsMap(@Nullable List<String> offlineSegments,
      @Nullable List<String> realtimeSegments) {
    if (offlineSegments != null) {
      if (realtimeSegments != null) {
        return Map.of(TableType.OFFLINE.name(), offlineSegments, TableType.REALTIME.name(), realtimeSegments);
      } else {
        return Map.of(TableType.OFFLINE.name(), offlineSegments);
      }
    } else {
      Preconditions.checkState(realtimeSegments != null, "Both offline and realtime segments are null");
      return Map.of(TableType.REALTIME.name(), realtimeSegments);
    }
  }
}
