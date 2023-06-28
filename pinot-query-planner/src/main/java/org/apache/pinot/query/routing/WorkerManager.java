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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.TablePartitionInfo;
import org.apache.pinot.core.routing.TablePartitionInfo.PartitionInfo;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.physical.DispatchablePlanContext;
import org.apache.pinot.query.planner.physical.DispatchablePlanMetadata;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
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

  private final String _hostName;
  private final int _port;
  private final RoutingManager _routingManager;

  public WorkerManager(String hostName, int port, RoutingManager routingManager) {
    _hostName = hostName;
    _port = port;
    _routingManager = routingManager;
  }

  public void assignWorkers(PlanFragment rootFragment, DispatchablePlanContext context) {
    // ROOT stage doesn't have a QueryServer as it is strictly only reducing results, so here we simply assign the
    // worker instance with identical server/mailbox port number.
    DispatchablePlanMetadata metadata = context.getDispatchablePlanMetadataMap().get(0);
    metadata.setServerInstanceToWorkerIdMap(
        Collections.singletonMap(new QueryServerInstance(_hostName, _port, _port), Collections.singletonList(0)));
    metadata.setTotalWorkerCount(1);
    for (PlanFragment child : rootFragment.getChildren()) {
      assignWorkersToNonRootFragment(child, context);
    }
  }

  private void assignWorkersToNonRootFragment(PlanFragment fragment, DispatchablePlanContext context) {
    if (isLeafPlan(context.getDispatchablePlanMetadataMap().get(fragment.getFragmentId()))) {
      assignWorkersToLeafFragment(fragment, context);
    } else {
      assignWorkersToIntermediateFragment(fragment, context);
    }
  }

  // TODO: Find a better way to determine whether a stage is leaf stage or intermediary. We could have query plans that
  //       process table data even in intermediary stages.
  private static boolean isLeafPlan(DispatchablePlanMetadata metadata) {
    return metadata.getScannedTables().size() == 1;
  }

  private void assignWorkersToLeafFragment(PlanFragment fragment, DispatchablePlanContext context) {
    DispatchablePlanMetadata metadata = context.getDispatchablePlanMetadataMap().get(fragment.getFragmentId());
    // table scan stage, need to attach server as well as segment info for each physical table type.
    List<String> scannedTables = metadata.getScannedTables();
    String logicalTableName = scannedTables.get(0);
    Map<String, RoutingTable> routingTableMap = getRoutingTable(logicalTableName, context.getRequestId());
    if (routingTableMap.size() == 0) {
      throw new IllegalArgumentException("Unable to find routing entries for table: " + logicalTableName);
    }
    // acquire time boundary info if it is a hybrid table.
    if (routingTableMap.size() > 1) {
      TimeBoundaryInfo timeBoundaryInfo = _routingManager.getTimeBoundaryInfo(
          TableNameBuilder.forType(TableType.OFFLINE)
              .tableNameWithType(TableNameBuilder.extractRawTableName(logicalTableName)));
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
      for (Map.Entry<ServerInstance, List<String>> serverEntry : routingTable.getServerInstanceToSegmentsMap()
          .entrySet()) {
        serverInstanceToSegmentsMap.putIfAbsent(serverEntry.getKey(), new HashMap<>());
        Map<String, List<String>> tableTypeToSegmentListMap = serverInstanceToSegmentsMap.get(serverEntry.getKey());
        Preconditions.checkState(tableTypeToSegmentListMap.put(tableType, serverEntry.getValue()) == null,
            "Entry for server {} and table type: {} already exist!", serverEntry.getKey(), tableType);
      }
    }
    int globalIdx = 0;
    Map<QueryServerInstance, List<Integer>> serverInstanceToWorkerIdMap = new HashMap<>();
    Map<Integer, Map<String, List<String>>> workerIdToSegmentsMap = new HashMap<>();
    for (Map.Entry<ServerInstance, Map<String, List<String>>> entry : serverInstanceToSegmentsMap.entrySet()) {
      QueryServerInstance queryServerInstance = new QueryServerInstance(entry.getKey());
      serverInstanceToWorkerIdMap.put(queryServerInstance, Collections.singletonList(globalIdx));
      workerIdToSegmentsMap.put(globalIdx, entry.getValue());
      globalIdx++;
    }
    metadata.setServerInstanceToWorkerIdMap(serverInstanceToWorkerIdMap);
    metadata.setWorkerIdToSegmentsMap(workerIdToSegmentsMap);
    metadata.setTotalWorkerCount(globalIdx);

    // NOTE: For pipeline breaker, leaf fragment can also have children
    for (PlanFragment child : fragment.getChildren()) {
      assignWorkersToNonRootFragment(child, context);
    }
  }

  /**
   * Acquire routing table for items listed in {@link org.apache.pinot.query.planner.plannode.TableScanNode}.
   *
   * @param logicalTableName it can either be a hybrid table name or a physical table name with table type.
   * @return keyed-map from table type(s) to routing table(s).
   */
  private Map<String, RoutingTable> getRoutingTable(String logicalTableName, long requestId) {
    String rawTableName = TableNameBuilder.extractRawTableName(logicalTableName);
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(logicalTableName);
    Map<String, RoutingTable> routingTableMap = new HashMap<>();
    RoutingTable routingTable;
    if (tableType == null) {
      routingTable = getRoutingTable(rawTableName, TableType.OFFLINE, requestId);
      if (routingTable != null) {
        routingTableMap.put(TableType.OFFLINE.name(), routingTable);
      }
      routingTable = getRoutingTable(rawTableName, TableType.REALTIME, requestId);
      if (routingTable != null) {
        routingTableMap.put(TableType.REALTIME.name(), routingTable);
      }
    } else {
      routingTable = getRoutingTable(logicalTableName, tableType, requestId);
      if (routingTable != null) {
        routingTableMap.put(tableType.name(), routingTable);
      }
    }
    return routingTableMap;
  }

  private RoutingTable getRoutingTable(String tableName, TableType tableType, long requestId) {
    String tableNameWithType =
        TableNameBuilder.forType(tableType).tableNameWithType(TableNameBuilder.extractRawTableName(tableName));
    return _routingManager.getRoutingTable(
        CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM " + tableNameWithType), requestId);
  }

  private void assignWorkersToIntermediateFragment(PlanFragment fragment, DispatchablePlanContext context) {
    if (isColocatedJoin(fragment.getFragmentRoot())) {
      // TODO: Make it more general so that it can be used for other partitioned cases (e.g. group-by, window function)
      try {
        assignWorkersForColocatedJoin(fragment, context);
        return;
      } catch (Exception e) {
        LOGGER.warn("[RequestId: {}] Caught exception while assigning workers for colocated join, "
            + "falling back to regular worker assignment", context.getRequestId(), e);
      }
    }

    // If the query has more than one table, it is possible that the tables could be hosted on different tenants.
    // The intermediate stage will be processed on servers randomly picked from the tenants belonging to either or
    // all of the tables in the query.
    // TODO: actually make assignment strategy decisions for intermediate stages
    List<ServerInstance> serverInstances;
    Set<String> tableNames = context.getTableNames();
    if (tableNames.size() == 0) {
      // TODO: Short circuit it when no table needs to be scanned
      // This could be the case from queries that don't actually fetch values from the tables. In such cases the
      // routing need not be tenant aware.
      // Eg: SELECT 1 AS one FROM select_having_expression_test_test_having HAVING 1 > 2;
      serverInstances = new ArrayList<>(_routingManager.getEnabledServerInstanceMap().values());
    } else {
      serverInstances = fetchServersForIntermediateStage(tableNames);
    }
    DispatchablePlanMetadata metadata = context.getDispatchablePlanMetadataMap().get(fragment.getFragmentId());
    Map<String, String> options = context.getPlannerContext().getOptions();
    int stageParallelism = Integer.parseInt(options.getOrDefault(QueryOptionKey.STAGE_PARALLELISM, "1"));
    if (metadata.isRequiresSingletonInstance()) {
      // require singleton should return a single global worker ID with 0;
      ServerInstance serverInstance = serverInstances.get(RANDOM.nextInt(serverInstances.size()));
      metadata.setServerInstanceToWorkerIdMap(
          Collections.singletonMap(new QueryServerInstance(serverInstance), Collections.singletonList(0)));
      metadata.setTotalWorkerCount(1);
    } else {
      Map<QueryServerInstance, List<Integer>> serverInstanceToWorkerIdMap = new HashMap<>();
      int nextWorkerId = 0;
      for (ServerInstance serverInstance : serverInstances) {
        List<Integer> workerIds = new ArrayList<>();
        for (int i = 0; i < stageParallelism; i++) {
          workerIds.add(nextWorkerId++);
        }
        serverInstanceToWorkerIdMap.put(new QueryServerInstance(serverInstance), workerIds);
      }
      metadata.setServerInstanceToWorkerIdMap(serverInstanceToWorkerIdMap);
      metadata.setTotalWorkerCount(nextWorkerId);
    }

    for (PlanFragment child : fragment.getChildren()) {
      assignWorkersToNonRootFragment(child, context);
    }
  }

  private boolean isColocatedJoin(PlanNode planNode) {
    if (planNode instanceof JoinNode) {
      return ((JoinNode) planNode).isColocatedJoin();
    }
    for (PlanNode child : planNode.getInputs()) {
      if (isColocatedJoin(child)) {
        return true;
      }
    }
    return false;
  }

  private void assignWorkersForColocatedJoin(PlanFragment fragment, DispatchablePlanContext context) {
    List<PlanFragment> children = fragment.getChildren();
    Preconditions.checkArgument(children.size() == 2, "Expecting 2 children, find: %s", children.size());
    PlanFragment leftFragment = children.get(0);
    PlanFragment rightFragment = children.get(1);
    Map<Integer, DispatchablePlanMetadata> metadataMap = context.getDispatchablePlanMetadataMap();
    // TODO: Support multi-level colocated join (more than 2 tables colocated)
    DispatchablePlanMetadata leftMetadata = metadataMap.get(leftFragment.getFragmentId());
    Preconditions.checkArgument(isLeafPlan(leftMetadata), "Left side is not leaf");
    DispatchablePlanMetadata rightMetadata = metadataMap.get(rightFragment.getFragmentId());
    Preconditions.checkArgument(isLeafPlan(rightMetadata), "Right side is not leaf");

    String leftTable = leftMetadata.getScannedTables().get(0);
    String rightTable = rightMetadata.getScannedTables().get(0);
    ColocatedTableInfo leftColocatedTableInfo = getColocatedTableInfo(leftTable);
    ColocatedTableInfo rightColocatedTableInfo = getColocatedTableInfo(rightTable);
    ColocatedPartitionInfo[] leftPartitionInfoMap = leftColocatedTableInfo._partitionInfoMap;
    ColocatedPartitionInfo[] rightPartitionInfoMap = rightColocatedTableInfo._partitionInfoMap;
    // TODO: Support colocated join when both side have different number of partitions (e.g. left: 8, right: 16)
    int numPartitions = leftPartitionInfoMap.length;
    Preconditions.checkState(numPartitions == rightPartitionInfoMap.length,
        "Got different number of partitions in left table: %s (%s) and right table: %s (%s)", leftTable, numPartitions,
        rightTable, rightPartitionInfoMap.length);

    // Pick one server per partition
    int nextWorkerId = 0;
    Map<QueryServerInstance, List<Integer>> serverInstanceToWorkerIdMap = new HashMap<>();
    Map<Integer, Map<String, List<String>>> leftWorkerIdToSegmentsMap = new HashMap<>();
    Map<Integer, Map<String, List<String>>> rightWorkerIdToSegmentsMap = new HashMap<>();
    Map<String, ServerInstance> enabledServerInstanceMap = _routingManager.getEnabledServerInstanceMap();
    for (int i = 0; i < numPartitions; i++) {
      ColocatedPartitionInfo leftPartitionInfo = leftPartitionInfoMap[i];
      ColocatedPartitionInfo rightPartitionInfo = rightPartitionInfoMap[i];
      if (leftPartitionInfo == null && rightPartitionInfo == null) {
        continue;
      }
      // TODO: Currently we don't support the case when for a partition only one side has segments. The reason is that
      //       the leaf stage won't be able to directly return empty response.
      Preconditions.checkState(leftPartitionInfo != null && rightPartitionInfo != null,
          "One side doesn't have any segment for partition: %s", i);
      Set<String> candidates = new HashSet<>(leftPartitionInfo._fullyReplicatedServers);
      candidates.retainAll(rightPartitionInfo._fullyReplicatedServers);
      ServerInstance serverInstance = pickRandomEnabledServer(candidates, enabledServerInstanceMap);
      Preconditions.checkState(serverInstance != null,
          "Failed to find enabled fully replicated server for partition: %s in table: %s and %s", i, leftTable,
          rightTable);
      QueryServerInstance queryServerInstance = new QueryServerInstance(serverInstance);
      int workerId = nextWorkerId++;
      serverInstanceToWorkerIdMap.computeIfAbsent(queryServerInstance, k -> new ArrayList<>()).add(workerId);
      leftWorkerIdToSegmentsMap.put(workerId, getSegmentsMap(leftPartitionInfo));
      rightWorkerIdToSegmentsMap.put(workerId, getSegmentsMap(rightPartitionInfo));
    }

    DispatchablePlanMetadata joinMetadata = metadataMap.get(fragment.getFragmentId());
    joinMetadata.setServerInstanceToWorkerIdMap(serverInstanceToWorkerIdMap);
    joinMetadata.setTotalWorkerCount(nextWorkerId);

    leftMetadata.setServerInstanceToWorkerIdMap(serverInstanceToWorkerIdMap);
    leftMetadata.setWorkerIdToSegmentsMap(leftWorkerIdToSegmentsMap);
    leftMetadata.setTotalWorkerCount(nextWorkerId);
    leftMetadata.setTimeBoundaryInfo(leftColocatedTableInfo._timeBoundaryInfo);

    rightMetadata.setServerInstanceToWorkerIdMap(serverInstanceToWorkerIdMap);
    rightMetadata.setWorkerIdToSegmentsMap(rightWorkerIdToSegmentsMap);
    rightMetadata.setTotalWorkerCount(nextWorkerId);
    rightMetadata.setTimeBoundaryInfo(rightColocatedTableInfo._timeBoundaryInfo);

    // NOTE: For pipeline breaker, leaf fragment can also have children
    for (PlanFragment child : leftFragment.getChildren()) {
      assignWorkersToNonRootFragment(child, context);
    }
    for (PlanFragment child : rightFragment.getChildren()) {
      assignWorkersToNonRootFragment(child, context);
    }
  }

  private ColocatedTableInfo getColocatedTableInfo(String tableName) {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType == null) {
      String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
      String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
      boolean offlineRoutingExists = _routingManager.routingExists(offlineTableName);
      boolean realtimeRoutingExists = _routingManager.routingExists(realtimeTableName);
      Preconditions.checkState(offlineRoutingExists || realtimeRoutingExists, "Routing doesn't exist for table: %s",
          tableName);
      if (offlineRoutingExists && realtimeRoutingExists) {
        // For hybrid table, find the common servers for each partition
        TimeBoundaryInfo timeBoundaryInfo = _routingManager.getTimeBoundaryInfo(offlineTableName);
        // Ignore OFFLINE side when time boundary info is unavailable
        if (timeBoundaryInfo == null) {
          return getRealtimeColocatedTableInfo(realtimeTableName);
        }
        PartitionInfo[] offlinePartitionInfoMap = getTablePartitionInfo(offlineTableName).getPartitionInfoMap();
        PartitionInfo[] realtimePartitionInfoMap = getTablePartitionInfo(realtimeTableName).getPartitionInfoMap();
        int numPartitions = offlinePartitionInfoMap.length;
        Preconditions.checkState(numPartitions == realtimePartitionInfoMap.length,
            "Got different number of partitions in OFFLINE side: %s and REALTIME side: %s of table: %s", numPartitions,
            realtimePartitionInfoMap.length, tableName);
        ColocatedPartitionInfo[] colocatedPartitionInfoMap = new ColocatedPartitionInfo[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
          PartitionInfo offlinePartitionInfo = offlinePartitionInfoMap[i];
          PartitionInfo realtimePartitionInfo = realtimePartitionInfoMap[i];
          if (offlinePartitionInfo == null && realtimePartitionInfo == null) {
            continue;
          }
          if (offlinePartitionInfo == null) {
            colocatedPartitionInfoMap[i] =
                new ColocatedPartitionInfo(realtimePartitionInfo._fullyReplicatedServers, null,
                    realtimePartitionInfo._segments);
            continue;
          }
          if (realtimePartitionInfo == null) {
            colocatedPartitionInfoMap[i] =
                new ColocatedPartitionInfo(offlinePartitionInfo._fullyReplicatedServers, offlinePartitionInfo._segments,
                    null);
            continue;
          }
          Set<String> fullyReplicatedServers = new HashSet<>(offlinePartitionInfo._fullyReplicatedServers);
          fullyReplicatedServers.retainAll(realtimePartitionInfo._fullyReplicatedServers);
          Preconditions.checkState(!fullyReplicatedServers.isEmpty(),
              "Failed to find fully replicated server for partition: %s in hybrid table: %s", i, tableName);
          colocatedPartitionInfoMap[i] =
              new ColocatedPartitionInfo(fullyReplicatedServers, offlinePartitionInfo._segments,
                  realtimePartitionInfo._segments);
        }
        return new ColocatedTableInfo(colocatedPartitionInfoMap, timeBoundaryInfo);
      } else if (offlineRoutingExists) {
        return getOfflineColocatedTableInfo(offlineTableName);
      } else {
        return getRealtimeColocatedTableInfo(realtimeTableName);
      }
    } else {
      if (tableType == TableType.OFFLINE) {
        return getOfflineColocatedTableInfo(tableName);
      } else {
        return getRealtimeColocatedTableInfo(tableName);
      }
    }
  }

  private TablePartitionInfo getTablePartitionInfo(String tableNameWithType) {
    TablePartitionInfo tablePartitionInfo = _routingManager.getTablePartitionInfo(tableNameWithType);
    Preconditions.checkState(tablePartitionInfo != null, "Failed to find table partition info for table: %s",
        tableNameWithType);
    Preconditions.checkState(tablePartitionInfo.getSegmentsWithInvalidPartition().isEmpty(),
        "Find %s segments with invalid partition for table: %s",
        tablePartitionInfo.getSegmentsWithInvalidPartition().size(), tableNameWithType);
    return tablePartitionInfo;
  }

  private ColocatedTableInfo getOfflineColocatedTableInfo(String offlineTableName) {
    PartitionInfo[] partitionInfoMap = getTablePartitionInfo(offlineTableName).getPartitionInfoMap();
    int numPartitions = partitionInfoMap.length;
    ColocatedPartitionInfo[] colocatedPartitionInfoMap = new ColocatedPartitionInfo[numPartitions];
    for (int i = 0; i < numPartitions; i++) {
      PartitionInfo partitionInfo = partitionInfoMap[i];
      if (partitionInfo != null) {
        colocatedPartitionInfoMap[i] =
            new ColocatedPartitionInfo(partitionInfo._fullyReplicatedServers, partitionInfo._segments, null);
      }
    }
    return new ColocatedTableInfo(colocatedPartitionInfoMap, null);
  }

  private ColocatedTableInfo getRealtimeColocatedTableInfo(String realtimeTableName) {
    PartitionInfo[] partitionInfoMap = getTablePartitionInfo(realtimeTableName).getPartitionInfoMap();
    int numPartitions = partitionInfoMap.length;
    ColocatedPartitionInfo[] colocatedPartitionInfoMap = new ColocatedPartitionInfo[numPartitions];
    for (int i = 0; i < numPartitions; i++) {
      PartitionInfo partitionInfo = partitionInfoMap[i];
      if (partitionInfo != null) {
        colocatedPartitionInfoMap[i] =
            new ColocatedPartitionInfo(partitionInfo._fullyReplicatedServers, null, partitionInfo._segments);
      }
    }
    return new ColocatedTableInfo(colocatedPartitionInfoMap, null);
  }

  private static class ColocatedTableInfo {
    final ColocatedPartitionInfo[] _partitionInfoMap;
    final TimeBoundaryInfo _timeBoundaryInfo;

    ColocatedTableInfo(ColocatedPartitionInfo[] partitionInfoMap, @Nullable TimeBoundaryInfo timeBoundaryInfo) {
      _partitionInfoMap = partitionInfoMap;
      _timeBoundaryInfo = timeBoundaryInfo;
    }
  }

  private static class ColocatedPartitionInfo {
    final Set<String> _fullyReplicatedServers;
    final List<String> _offlineSegments;
    final List<String> _realtimeSegments;

    public ColocatedPartitionInfo(Set<String> fullyReplicatedServers, @Nullable List<String> offlineSegments,
        @Nullable List<String> realtimeSegments) {
      _fullyReplicatedServers = fullyReplicatedServers;
      _offlineSegments = offlineSegments;
      _realtimeSegments = realtimeSegments;
    }
  }

  @Nullable
  private static ServerInstance pickRandomEnabledServer(Set<String> candidates,
      Map<String, ServerInstance> enabledServerInstanceMap) {
    if (candidates.isEmpty()) {
      return null;
    }
    String[] servers = candidates.toArray(new String[0]);
    ArrayUtils.shuffle(servers, RANDOM);
    for (String server : servers) {
      ServerInstance serverInstance = enabledServerInstanceMap.get(server);
      if (serverInstance != null) {
        return serverInstance;
      }
    }
    return null;
  }

  private static Map<String, List<String>> getSegmentsMap(ColocatedPartitionInfo partitionInfo) {
    Map<String, List<String>> segmentsMap = new HashMap<>();
    if (partitionInfo._offlineSegments != null) {
      segmentsMap.put(TableType.OFFLINE.name(), partitionInfo._offlineSegments);
    }
    if (partitionInfo._realtimeSegments != null) {
      segmentsMap.put(TableType.REALTIME.name(), partitionInfo._realtimeSegments);
    }
    return segmentsMap;
  }

  private List<ServerInstance> fetchServersForIntermediateStage(Set<String> tableNames) {
    Set<ServerInstance> serverInstances = new HashSet<>();
    for (String tableName : tableNames) {
      TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
      if (tableType == null) {
        String offlineTableName = TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(tableName);
        serverInstances.addAll(_routingManager.getEnabledServersForTableTenant(offlineTableName).values());
        String realtimeTableName = TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(tableName);
        serverInstances.addAll(_routingManager.getEnabledServersForTableTenant(realtimeTableName).values());
      } else {
        serverInstances.addAll(_routingManager.getEnabledServersForTableTenant(tableName).values());
      }
    }
    return new ArrayList<>(serverInstances);
  }
}
