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
import org.apache.commons.lang3.tuple.Pair;
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
  private static final String DEFAULT_PARTITION_FUNCTION = "Hashcode";

  private final String _hostName;
  private final int _port;
  private final RoutingManager _routingManager;
  private final String _defaultPartitionFunc;

  public WorkerManager(String hostName, int port, RoutingManager routingManager) {
    this(hostName, port, routingManager, DEFAULT_PARTITION_FUNCTION);
  }

  public WorkerManager(String hostName, int port, RoutingManager routingManager, String defaultPartitionFunc) {
    _hostName = hostName;
    _port = port;
    _routingManager = routingManager;
    _defaultPartitionFunc = defaultPartitionFunc;
  }

  public void assignWorkers(PlanFragment rootFragment, DispatchablePlanContext context) {
    // ROOT stage doesn't have a QueryServer as it is strictly only reducing results, so here we simply assign the
    // worker instance with identical server/mailbox port number.
    DispatchablePlanMetadata metadata = context.getDispatchablePlanMetadataMap().get(0);
    metadata.setWorkerIdToServerInstanceMap(
        Collections.singletonMap(0, new QueryServerInstance(_hostName, _port, _port)));
    for (PlanFragment child : rootFragment.getChildren()) {
      assignWorkersToNonRootFragment(child, context);
    }
  }

  private void assignWorkersToNonRootFragment(PlanFragment fragment, DispatchablePlanContext context) {
    for (PlanFragment child : fragment.getChildren()) {
      assignWorkersToNonRootFragment(child, context);
    }
    if (isLeafPlan(context.getDispatchablePlanMetadataMap().get(fragment.getFragmentId()))) {
      assignWorkersToLeafFragment(fragment, context);
    } else {
      assignWorkersToIntermediateFragment(fragment, context);
    }
  }

  private static boolean isLeafPlan(DispatchablePlanMetadata metadata) {
    return metadata.getScannedTables().size() == 1;
  }

  // --------------------------------------------------------------------------
  // Intermediate stage assign logic
  // --------------------------------------------------------------------------
  private void assignWorkersToIntermediateFragment(PlanFragment fragment, DispatchablePlanContext context) {
    Map<Integer, DispatchablePlanMetadata> metadataMap = context.getDispatchablePlanMetadataMap();
    DispatchablePlanMetadata metadata = metadataMap.get(fragment.getFragmentId());

    // If the first child is partitioned and can be inherent from this intermediate stage, use the same worker
    // assignment to avoid shuffling data.
    // When partition parallelism is configured,
    // 1. create multiple intermediate stage workers on the same instance for each worker in the first child if the
    //    first child is a table scan. this is b/c we cannot pre-config parallelism on leaf stage thus needs fan-out.
    // 2. ignore partition parallelism when first child is NOT table scan b/c it would've done fan-out already.
    List<PlanFragment> children = fragment.getChildren();
    DispatchablePlanMetadata firstChildMetadata = children.isEmpty() ? null
        : metadataMap.get(children.get(0).getFragmentId());
    if (firstChildMetadata != null && firstChildMetadata.isPrePartitioned()) {
      int partitionParallelism = firstChildMetadata.getPartitionParallelism();
      Map<Integer, QueryServerInstance> childWorkerIdToServerInstanceMap =
          firstChildMetadata.getWorkerIdToServerInstanceMap();
      if (partitionParallelism == 1 || firstChildMetadata.getScannedTables().size() == 0) {
        metadata.setWorkerIdToServerInstanceMap(childWorkerIdToServerInstanceMap);
      } else {
        int numChildWorkers = childWorkerIdToServerInstanceMap.size();
        Map<Integer, QueryServerInstance> workerIdToServerInstanceMap = new HashMap<>();
        int workerId = 0;
        for (int i = 0; i < numChildWorkers; i++) {
          QueryServerInstance serverInstance = childWorkerIdToServerInstanceMap.get(i);
          for (int j = 0; j < partitionParallelism; j++) {
            workerIdToServerInstanceMap.put(workerId++, serverInstance);
          }
        }
        metadata.setWorkerIdToServerInstanceMap(workerIdToServerInstanceMap);
      }
      metadata.setPartitionFunction(firstChildMetadata.getPartitionFunction());
    } else {
      // If the query has more than one table, it is possible that the tables could be hosted on different tenants.
      // The intermediate stage will be processed on servers randomly picked from the tenants belonging to either or
      // all of the tables in the query.
      // TODO: actually make assignment strategy decisions for intermediate stages
      List<ServerInstance> serverInstances = assignServerInstances(context);
      if (metadata.isRequiresSingletonInstance()) {
        // require singleton should return a single global worker ID with 0;
        metadata.setWorkerIdToServerInstanceMap(Collections.singletonMap(0,
            new QueryServerInstance(serverInstances.get(RANDOM.nextInt(serverInstances.size())))));
      } else {
        Map<String, String> options = context.getPlannerContext().getOptions();
        int stageParallelism = Integer.parseInt(options.getOrDefault(QueryOptionKey.STAGE_PARALLELISM, "1"));
        Map<Integer, QueryServerInstance> workerIdToServerInstanceMap = new HashMap<>();
        int workerId = 0;
        for (ServerInstance serverInstance : serverInstances) {
          QueryServerInstance queryServerInstance = new QueryServerInstance(serverInstance);
          for (int i = 0; i < stageParallelism; i++) {
            workerIdToServerInstanceMap.put(workerId++, queryServerInstance);
          }
        }
        metadata.setWorkerIdToServerInstanceMap(workerIdToServerInstanceMap);
      }
    }
  }

  private List<ServerInstance> assignServerInstances(DispatchablePlanContext context) {
    List<ServerInstance> serverInstances;
    Set<String> tableNames = context.getTableNames();
    Map<String, ServerInstance> enabledServerInstanceMap = _routingManager.getEnabledServerInstanceMap();
    if (tableNames.size() == 0) {
      // TODO: Short circuit it when no table needs to be scanned
      // This could be the case from queries that don't actually fetch values from the tables. In such cases the
      // routing need not be tenant aware.
      // Eg: SELECT 1 AS one FROM select_having_expression_test_test_having HAVING 1 > 2;
      serverInstances = new ArrayList<>(enabledServerInstanceMap.values());
    } else {
      Set<String> servers = new HashSet<>();
      for (String tableName : tableNames) {
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
      if (servers.isEmpty()) {
        // fall back to use all enabled servers if no server is found for the tables
        serverInstances = new ArrayList<>(enabledServerInstanceMap.values());
      } else {
        serverInstances = new ArrayList<>(servers.size());
        for (String server : servers) {
          ServerInstance serverInstance = enabledServerInstanceMap.get(server);
          if (serverInstance != null) {
            serverInstances.add(serverInstance);
          }
        }
      }
    }
    if (serverInstances.isEmpty()) {
      LOGGER.error("[RequestId: {}] No server instance found for intermediate stage for tables: {}",
          context.getRequestId(), tableNames);
      throw new IllegalStateException(
          "No server instance found for intermediate stage for tables: " + Arrays.toString(tableNames.toArray()));
    }
    return serverInstances;
  }

  private void assignWorkersToLeafFragment(PlanFragment fragment, DispatchablePlanContext context) {
    DispatchablePlanMetadata metadata = context.getDispatchablePlanMetadataMap().get(fragment.getFragmentId());
    Map<String, String> tableOptions = metadata.getTableOptions();
    String partitionKey =
        tableOptions != null ? tableOptions.get(PinotHintOptions.TableHintOptions.PARTITION_KEY) : null;
    if (partitionKey == null) {
      // when partition key is not given, we do not partition assign workers for leaf-stage.
      assignWorkersToNonPartitionedLeafFragment(metadata, context);
    } else {
      assignWorkersToPartitionedLeafFragment(metadata, context, partitionKey, tableOptions);
    }
  }

  // --------------------------------------------------------------------------
  // Non-partitioned leaf stage assignment
  // --------------------------------------------------------------------------
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
      Map<ServerInstance, Pair<List<String>, List<String>>> segmentsMap = routingTable.getServerInstanceToSegmentsMap();
      for (Map.Entry<ServerInstance, Pair<List<String>, List<String>>> serverEntry : segmentsMap.entrySet()) {
        Map<String, List<String>> tableTypeToSegmentListMap =
            serverInstanceToSegmentsMap.computeIfAbsent(serverEntry.getKey(), k -> new HashMap<>());
        // TODO: support optional segments for multi-stage engine.
        Preconditions.checkState(tableTypeToSegmentListMap.put(tableType, serverEntry.getValue().getLeft()) == null,
            "Entry for server {} and table type: {} already exist!", serverEntry.getKey(), tableType);
      }

      // attach unavailable segments to metadata
      if (!routingTable.getUnavailableSegments().isEmpty()) {
        metadata.addUnavailableSegments(tableName, routingTable.getUnavailableSegments());
      }
    }
    int workerId = 0;
    Map<Integer, QueryServerInstance> workerIdToServerInstanceMap = new HashMap<>();
    Map<Integer, Map<String, List<String>>> workerIdToSegmentsMap = new HashMap<>();
    for (Map.Entry<ServerInstance, Map<String, List<String>>> entry : serverInstanceToSegmentsMap.entrySet()) {
      workerIdToServerInstanceMap.put(workerId, new QueryServerInstance(entry.getKey()));
      workerIdToSegmentsMap.put(workerId, entry.getValue());
      workerId++;
    }
    metadata.setWorkerIdToServerInstanceMap(workerIdToServerInstanceMap);
    metadata.setWorkerIdToSegmentsMap(workerIdToSegmentsMap);
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

  // --------------------------------------------------------------------------
  // Partitioned leaf stage assignment
  // --------------------------------------------------------------------------
  private void assignWorkersToPartitionedLeafFragment(DispatchablePlanMetadata metadata,
      DispatchablePlanContext context, String partitionKey, Map<String, String> tableOptions) {
    // when partition key exist, we assign workers for leaf-stage in partitioned fashion.

    String numPartitionsStr = tableOptions.get(PinotHintOptions.TableHintOptions.PARTITION_SIZE);
    Preconditions.checkState(numPartitionsStr != null, "'%s' must be provided for partition key: %s",
        PinotHintOptions.TableHintOptions.PARTITION_SIZE, partitionKey);
    int numPartitions = Integer.parseInt(numPartitionsStr);
    Preconditions.checkState(numPartitions > 0, "'%s' must be positive, got: %s",
        PinotHintOptions.TableHintOptions.PARTITION_SIZE, numPartitions);

    // TODO: make enum of partition functions that are supported.
    String partitionFunction = tableOptions.getOrDefault(PinotHintOptions.TableHintOptions.PARTITION_FUNCTION,
        _defaultPartitionFunc);

    String partitionParallelismStr = tableOptions.get(PinotHintOptions.TableHintOptions.PARTITION_PARALLELISM);
    int partitionParallelism = partitionParallelismStr != null ? Integer.parseInt(partitionParallelismStr) : 1;
    Preconditions.checkState(partitionParallelism > 0, "'%s' must be positive: %s, got: %s",
        PinotHintOptions.TableHintOptions.PARTITION_PARALLELISM, partitionParallelism);

    String tableName = metadata.getScannedTables().get(0);
    ColocatedTableInfo colocatedTableInfo =
        getColocatedTableInfo(tableName, partitionKey, numPartitions, partitionFunction);

    // Pick one server per partition
    // NOTE: Pick server based on the request id so that the same server is picked across different table scan when the
    //       segments for the same partition is colocated
    long indexToPick = context.getRequestId();
    ColocatedPartitionInfo[] partitionInfoMap = colocatedTableInfo._partitionInfoMap;
    int workerId = 0;
    Map<Integer, QueryServerInstance> workedIdToServerInstanceMap = new HashMap<>();
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
          "Failed to find enabled fully replicated server for table: %s, partition: %s", tableName, i);
      workedIdToServerInstanceMap.put(workerId, new QueryServerInstance(serverInstance));
      workerIdToSegmentsMap.put(workerId, getSegmentsMap(partitionInfo));
      workerId++;
    }
    metadata.setWorkerIdToServerInstanceMap(workedIdToServerInstanceMap);
    metadata.setWorkerIdToSegmentsMap(workerIdToSegmentsMap);
    metadata.setTimeBoundaryInfo(colocatedTableInfo._timeBoundaryInfo);
    metadata.setPartitionFunction(partitionFunction);
    metadata.setPartitionParallelism(partitionParallelism);
  }

  private ColocatedTableInfo getColocatedTableInfo(String tableName, String partitionKey, int numPartitions,
      String partitionFunction) {
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
          return getRealtimeColocatedTableInfo(realtimeTableName, partitionKey, numPartitions, partitionFunction);
        }
        PartitionInfo[] offlinePartitionInfoMap = getTablePartitionInfo(offlineTableName, partitionKey, numPartitions,
            partitionFunction).getPartitionInfoMap();
        PartitionInfo[] realtimePartitionInfoMap = getTablePartitionInfo(realtimeTableName, partitionKey, numPartitions,
            partitionFunction).getPartitionInfoMap();
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
        return getOfflineColocatedTableInfo(offlineTableName, partitionKey, numPartitions, partitionFunction);
      } else {
        return getRealtimeColocatedTableInfo(realtimeTableName, partitionKey, numPartitions, partitionFunction);
      }
    } else {
      if (tableType == TableType.OFFLINE) {
        return getOfflineColocatedTableInfo(tableName, partitionKey, numPartitions, partitionFunction);
      } else {
        return getRealtimeColocatedTableInfo(tableName, partitionKey, numPartitions, partitionFunction);
      }
    }
  }

  private TablePartitionInfo getTablePartitionInfo(String tableNameWithType, String partitionKey, int numPartitions,
      String partitionFunction) {
    TablePartitionInfo tablePartitionInfo = _routingManager.getTablePartitionInfo(tableNameWithType);
    Preconditions.checkState(tablePartitionInfo != null, "Failed to find table partition info for table: %s",
        tableNameWithType);
    Preconditions.checkState(tablePartitionInfo.getPartitionColumn().equals(partitionKey),
        "Partition key: %s does not match partition column: %s for table: %s", partitionKey,
        tablePartitionInfo.getPartitionColumn(), tableNameWithType);
    Preconditions.checkState(tablePartitionInfo.getNumPartitions() == numPartitions,
        "Partition size mismatch (hint: %s, table: %s) for table: %s", numPartitions,
        tablePartitionInfo.getNumPartitions(), tableNameWithType);
    Preconditions.checkState(tablePartitionInfo.getPartitionFunctionName().equals(partitionFunction),
        "Partition function mismatch (hint: %s, table: %s) for table %s", partitionFunction,
        tablePartitionInfo.getPartitionFunctionName(), tableNameWithType);
    Preconditions.checkState(tablePartitionInfo.getSegmentsWithInvalidPartition().isEmpty(),
        "Find %s segments with invalid partition for table: %s",
        tablePartitionInfo.getSegmentsWithInvalidPartition().size(), tableNameWithType);
    return tablePartitionInfo;
  }

  private ColocatedTableInfo getOfflineColocatedTableInfo(String offlineTableName, String partitionKey,
      int numPartitions, String partitionFunction) {
    PartitionInfo[] partitionInfoMap =
        getTablePartitionInfo(offlineTableName, partitionKey, numPartitions, partitionFunction).getPartitionInfoMap();
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
      int numPartitions, String partitionFunction) {
    PartitionInfo[] partitionInfoMap =
        getTablePartitionInfo(realtimeTableName, partitionKey, numPartitions, partitionFunction).getPartitionInfoMap();
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

    ColocatedPartitionInfo(Set<String> fullyReplicatedServers, @Nullable List<String> offlineSegments,
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
}
