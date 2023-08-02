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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.rel.hint.PinotHintOptions;
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
    // NOTE: For pipeline breaker, leaf fragment can also have children
    for (PlanFragment child : fragment.getChildren()) {
      assignWorkersToNonRootFragment(child, context);
    }

    DispatchablePlanMetadata metadata = context.getDispatchablePlanMetadataMap().get(fragment.getFragmentId());
    Map<String, String> tableOptions = metadata.getTableOptions();
    String partitionKey = null;
    int numPartitions = 0;
    if (tableOptions != null) {
      partitionKey = tableOptions.get(PinotHintOptions.TableHintOptions.PARTITION_KEY);
      String partitionSize = tableOptions.get(PinotHintOptions.TableHintOptions.PARTITION_SIZE);
      if (partitionSize != null) {
        numPartitions = Integer.parseInt(partitionSize);
      }
    }
    if (partitionKey == null) {
      assignWorkersToNonPartitionedLeafFragment(metadata, context);
    } else {
      Preconditions.checkState(numPartitions > 0, "'%s' must be provided for partition key: %s",
          PinotHintOptions.TableHintOptions.PARTITION_SIZE, partitionKey);
      assignWorkersToPartitionedLeafFragment(metadata, context, partitionKey, numPartitions);
    }
  }

  private void assignWorkersToNonPartitionedLeafFragment(DispatchablePlanMetadata metadata,
      DispatchablePlanContext context) {
    String tableName = metadata.getScannedTables().get(0);
    Map<String, RoutingTable> routingTableMap = getRoutingTable(tableName, context.getRequestId());
    Preconditions.checkState(!routingTableMap.isEmpty(), "Unable to find routing entries for table: %s", tableName);

    // acquire time boundary info if it is a hybrid table.
    if (routingTableMap.size() > 1) {
      TimeBoundaryInfo timeBoundaryInfo = _routingManager.getTimeBoundaryInfo(
          TableNameBuilder.forType(TableType.OFFLINE)
              .tableNameWithType(TableNameBuilder.extractRawTableName(tableName)));
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

      // attach unavailable segments to metadata
      if (!routingTable.getUnavailableSegments().isEmpty()) {
        metadata.addTableToUnavailableSegmentsMap(tableName, routingTable.getUnavailableSegments());
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
  }

  /**
   * Acquire routing table for items listed in {@link TableScanNode}.
   *
   * @param tableName table name with or without type suffix.
   * @return keyed-map from table type(s) to routing table(s).
   */
  private Map<String, RoutingTable> getRoutingTable(String tableName, long requestId) {
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
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
      routingTable = getRoutingTable(tableName, tableType, requestId);
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

  private void assignWorkersToPartitionedLeafFragment(DispatchablePlanMetadata metadata,
      DispatchablePlanContext context, String partitionKey, int numPartitions) {
    String tableName = metadata.getScannedTables().get(0);
    ColocatedTableInfo colocatedTableInfo = getColocatedTableInfo(tableName, partitionKey, numPartitions);

    // Pick one server per partition
    // NOTE: Pick server based on the request id so that the same server is picked across different table scan when the
    //       segments for the same partition is colocated
    long indexToPick = context.getRequestId();
    ColocatedPartitionInfo[] partitionInfoMap = colocatedTableInfo._partitionInfoMap;
    int nextWorkerId = 0;
    Map<QueryServerInstance, List<Integer>> serverInstanceToWorkerIdMap = new HashMap<>();
    Map<Integer, Map<String, List<String>>> workerIdToSegmentsMap = new HashMap<>();
    Map<String, ServerInstance> enabledServerInstanceMap = _routingManager.getEnabledServerInstanceMap();
    for (int i = 0; i < numPartitions; i++) {
      ColocatedPartitionInfo partitionInfo = partitionInfoMap[i];
      // TODO: Currently we don't support the case when a partition doesn't contain any segment. The reason is that the
      //       leaf stage won't be able to directly return empty response.
      Preconditions.checkState(partitionInfo != null, "Failed to find any segment for table: %s, partition: %s",
          tableName, i);
      ServerInstance serverInstance =
          pickEnabledServer(partitionInfo._fullyReplicatedServers, enabledServerInstanceMap, indexToPick++);
      Preconditions.checkState(serverInstance != null,
          "Failed to find enabled fully replicated server for table: %s, partition: %s in table: %s", tableName, i);
      QueryServerInstance queryServerInstance = new QueryServerInstance(serverInstance);
      int workerId = nextWorkerId++;
      serverInstanceToWorkerIdMap.computeIfAbsent(queryServerInstance, k -> new ArrayList<>()).add(workerId);
      workerIdToSegmentsMap.put(workerId, getSegmentsMap(partitionInfo));
    }

    metadata.setServerInstanceToWorkerIdMap(serverInstanceToWorkerIdMap);
    metadata.setWorkerIdToSegmentsMap(workerIdToSegmentsMap);
    metadata.setTotalWorkerCount(nextWorkerId);
    metadata.setTimeBoundaryInfo(colocatedTableInfo._timeBoundaryInfo);
    metadata.setPartitionedTableScan(true);
  }

  private void assignWorkersToIntermediateFragment(PlanFragment fragment, DispatchablePlanContext context) {
    List<PlanFragment> children = fragment.getChildren();
    for (PlanFragment child : children) {
      assignWorkersToNonRootFragment(child, context);
    }

    Map<Integer, DispatchablePlanMetadata> metadataMap = context.getDispatchablePlanMetadataMap();
    DispatchablePlanMetadata metadata = metadataMap.get(fragment.getFragmentId());

    // If the first child is partitioned table scan, use the same worker assignment to avoid shuffling data
    // TODO: Introduce a hint to control this
    if (children.size() > 0) {
      DispatchablePlanMetadata firstChildMetadata = metadataMap.get(children.get(0).getFragmentId());
      if (firstChildMetadata.isPartitionedTableScan()) {
        metadata.setServerInstanceToWorkerIdMap(firstChildMetadata.getServerInstanceToWorkerIdMap());
        metadata.setTotalWorkerCount(firstChildMetadata.getTotalWorkerCount());
        return;
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
    if (serverInstances.isEmpty()) {
      LOGGER.error("[RequestId: {}] No server instance found for intermediate stage for tables: {}",
          context.getRequestId(), tableNames);
      throw new IllegalStateException(
          "No server instance found for intermediate stage for tables: " + Arrays.toString(tableNames.toArray()));
    }
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
  }

  private ColocatedTableInfo getColocatedTableInfo(String tableName, String partitionKey, int numPartitions) {
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
          return getRealtimeColocatedTableInfo(realtimeTableName, partitionKey, numPartitions);
        }
        PartitionInfo[] offlinePartitionInfoMap =
            getTablePartitionInfo(offlineTableName, partitionKey, numPartitions).getPartitionInfoMap();
        PartitionInfo[] realtimePartitionInfoMap =
            getTablePartitionInfo(realtimeTableName, partitionKey, numPartitions).getPartitionInfoMap();
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
        return getOfflineColocatedTableInfo(offlineTableName, partitionKey, numPartitions);
      } else {
        return getRealtimeColocatedTableInfo(realtimeTableName, partitionKey, numPartitions);
      }
    } else {
      if (tableType == TableType.OFFLINE) {
        return getOfflineColocatedTableInfo(tableName, partitionKey, numPartitions);
      } else {
        return getRealtimeColocatedTableInfo(tableName, partitionKey, numPartitions);
      }
    }
  }

  private TablePartitionInfo getTablePartitionInfo(String tableNameWithType, String partitionKey, int numPartitions) {
    TablePartitionInfo tablePartitionInfo = _routingManager.getTablePartitionInfo(tableNameWithType);
    Preconditions.checkState(tablePartitionInfo != null, "Failed to find table partition info for table: %s",
        tableNameWithType);
    Preconditions.checkState(tablePartitionInfo.getPartitionColumn().equals(partitionKey),
        "Partition key: %s does not match partition column: %s for table: %s", partitionKey,
        tablePartitionInfo.getPartitionColumn(), tableNameWithType);
    Preconditions.checkState(tablePartitionInfo.getNumPartitions() == numPartitions,
        "Partition size mismatch (hint: %s, table: %s) for table: %s", numPartitions,
        tablePartitionInfo.getNumPartitions(), tableNameWithType);
    Preconditions.checkState(tablePartitionInfo.getSegmentsWithInvalidPartition().isEmpty(),
        "Find %s segments with invalid partition for table: %s",
        tablePartitionInfo.getSegmentsWithInvalidPartition().size(), tableNameWithType);
    return tablePartitionInfo;
  }

  private ColocatedTableInfo getOfflineColocatedTableInfo(String offlineTableName, String partitionKey,
      int numPartitions) {
    PartitionInfo[] partitionInfoMap =
        getTablePartitionInfo(offlineTableName, partitionKey, numPartitions).getPartitionInfoMap();
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

  private ColocatedTableInfo getRealtimeColocatedTableInfo(String realtimeTableName, String partitionKey,
      int numPartitions) {
    PartitionInfo[] partitionInfoMap =
        getTablePartitionInfo(realtimeTableName, partitionKey, numPartitions).getPartitionInfoMap();
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
    String[] servers = candidates.toArray(new String[0]);
    ArrayUtils.shuffle(servers, RANDOM);
    for (int i = 0; i < numCandidates; i++) {
      String server = candidateList.get((startIndex + i) % numCandidates);
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
