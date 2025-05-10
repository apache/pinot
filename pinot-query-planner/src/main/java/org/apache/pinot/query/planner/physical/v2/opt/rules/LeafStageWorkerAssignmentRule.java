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
package org.apache.pinot.query.planner.physical.v2.opt.rules;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.ServerRouteInfo;
import org.apache.pinot.core.routing.TablePartitionInfo;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.apache.pinot.query.planner.physical.v2.HashDistributionDesc;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.PinotDataDistribution;
import org.apache.pinot.query.planner.physical.v2.TableScanMetadata;
import org.apache.pinot.query.planner.physical.v2.mapping.DistMappingGenerator;
import org.apache.pinot.query.planner.physical.v2.mapping.PinotDistMapping;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalTableScan;
import org.apache.pinot.query.planner.physical.v2.opt.PRelOptRule;
import org.apache.pinot.query.planner.physical.v2.opt.PRelOptRuleCall;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * <h1>Overview</h1>
 * Assigns workers to all PRelNodes that are part of the leaf stage as determined by {@link PRelNode#isLeafStage()}.
 * The workers are mainly determined by the Table Scan, unless filter based server pruning is enabled.
 * <h1>Current Features</h1>
 * <ul>
 *   <li>
 *     Automatically detects partitioning and adds that information to PinotDataDistribution. This will be used
 *     in subsequent worker assignment steps to simplify Exchange.
 *   </li>
 * </ul>
 * <h1>Planned / Upcoming Features</h1>
 * <ul>
 *   <li>Support for look-up join.</li>
 *   <li>Support for partition parallelism and the colocated join hint. See F2 in #15455.</li>
 *   <li>Support for Hybrid Tables for automatic partitioning inference.</li>
 *   <li>Server pruning based on filter predicates.</li>
 * </ul>
 */
public class LeafStageWorkerAssignmentRule extends PRelOptRule {
  private static final Logger LOGGER = LoggerFactory.getLogger(LeafStageWorkerAssignmentRule.class);
  private final TableCache _tableCache;
  private final RoutingManager _routingManager;
  private final PhysicalPlannerContext _physicalPlannerContext;

  public LeafStageWorkerAssignmentRule(PhysicalPlannerContext physicalPlannerContext, TableCache tableCache) {
    _routingManager = physicalPlannerContext.getRoutingManager();
    _physicalPlannerContext = physicalPlannerContext;
    _tableCache = tableCache;
  }

  @Override
  public boolean matches(PRelOptRuleCall call) {
    return call._currentNode.isLeafStage();
  }

  @Override
  public PRelNode onMatch(PRelOptRuleCall call) {
    if (call._currentNode.unwrap() instanceof TableScan) {
      return assignTableScan((PhysicalTableScan) call._currentNode, _physicalPlannerContext.getRequestId());
    }
    PRelNode currentNode = call._currentNode;
    Preconditions.checkState(currentNode.isLeafStage(), "Leaf stage worker assignment called for non-leaf stage node:"
        + " %s", currentNode);
    PinotDistMapping mapping = DistMappingGenerator.compute(currentNode.getPRelInput(0).unwrap(),
        currentNode.unwrap(), null);
    PinotDataDistribution derivedDistribution = currentNode.getPRelInput(0).getPinotDataDistributionOrThrow()
        .apply(mapping);
    return currentNode.with(currentNode.getPRelInputs(), derivedDistribution);
  }

  private PhysicalTableScan assignTableScan(PhysicalTableScan tableScan, long requestId) {
    // Step-1: Init tableName, table options, routing table and time boundary info.
    String tableName = Objects.requireNonNull(getActualTableName(tableScan), "Table not found");
    Map<String, String> tableOptions = getTableOptions(tableScan.getHints());
    Map<String, RoutingTable> routingTableMap = getRoutingTable(tableName, requestId);
    Preconditions.checkState(!routingTableMap.isEmpty(), "Unable to find routing entries for table: %s", tableName);
    // acquire time boundary info if it is a hybrid table.
    TimeBoundaryInfo timeBoundaryInfo = null;
    if (routingTableMap.size() > 1) {
      timeBoundaryInfo = _routingManager.getTimeBoundaryInfo(
          TableNameBuilder.forType(TableType.OFFLINE)
              .tableNameWithType(TableNameBuilder.extractRawTableName(tableName)));
      if (timeBoundaryInfo == null) {
        // remove offline table routing if no time boundary info is acquired.
        routingTableMap.remove(TableType.OFFLINE.name());
      }
    }
    // Step-2: Compute instance to segments map and unavailable segments.
    Map<String, Set<String>> segmentUnavailableMap = new HashMap<>();
    InstanceIdToSegments instanceIdToSegments = new InstanceIdToSegments();
    for (Map.Entry<String, RoutingTable> routingEntry : routingTableMap.entrySet()) {
      String tableType = routingEntry.getKey();
      RoutingTable routingTable = routingEntry.getValue();
      Map<String, List<String>> currentSegmentsMap = new HashMap<>();
      Map<ServerInstance, ServerRouteInfo> tmp = routingTable.getServerInstanceToSegmentsMap();
      for (Map.Entry<ServerInstance, ServerRouteInfo> serverEntry : tmp.entrySet()) {
        String instanceId = serverEntry.getKey().getInstanceId();
        Preconditions.checkState(currentSegmentsMap.put(instanceId, serverEntry.getValue().getSegments()) == null,
            "Entry for server %s and table type: %s already exist!", serverEntry.getKey(), tableType);
        _physicalPlannerContext.getInstanceIdToQueryServerInstance().computeIfAbsent(instanceId,
            (ignore) -> new QueryServerInstance(serverEntry.getKey()));
      }
      if (tableType.equalsIgnoreCase(TableType.OFFLINE.name())) {
        instanceIdToSegments._offlineTableSegmentsMap = currentSegmentsMap;
      } else {
        instanceIdToSegments._realtimeTableSegmentsMap = currentSegmentsMap;
      }
      if (!routingTable.getUnavailableSegments().isEmpty()) {
        // Set unavailable segments in context, keyed by PRelNode ID.
        segmentUnavailableMap.put(TableNameBuilder.forType(TableType.valueOf(tableName)).tableNameWithType(tableName),
            new HashSet<>(routingTable.getUnavailableSegments()));
      }
    }
    List<String> fieldNames = tableScan.getRowType().getFieldNames();
    Map<String, TablePartitionInfo> tablePartitionInfoMap = calculateTablePartitionInfo(tableName,
        routingTableMap.keySet());
    TableScanWorkerAssignmentResult workerAssignmentResult = assignTableScan(tableName, fieldNames,
        instanceIdToSegments, tablePartitionInfoMap);
    TableScanMetadata metadata = new TableScanMetadata(Set.of(tableName), workerAssignmentResult._workerIdToSegmentsMap,
        tableOptions, segmentUnavailableMap, timeBoundaryInfo);
    return tableScan.with(workerAssignmentResult._pinotDataDistribution, metadata);
  }

  /**
   * Assigns workers for the table-scan node, automatically detecting table partitioning whenever possible. The
   * arguments to this method are minimal to facilitate unit-testing.
   */
  @VisibleForTesting
  static TableScanWorkerAssignmentResult assignTableScan(String tableName, List<String> fieldNames,
      InstanceIdToSegments instanceIdToSegments, Map<String, TablePartitionInfo> tpiMap) {
    Set<String> tableTypes = instanceIdToSegments.getActiveTableTypes();
    Set<String> partitionedTableTypes = tableTypes.stream().filter(tpiMap::containsKey).collect(Collectors.toSet());
    Preconditions.checkState(!tableTypes.isEmpty(), "No routing entry for offline or realtime type");
    if (tableTypes.equals(partitionedTableTypes)) {
      if (partitionedTableTypes.size() == 1) {
        // Attempt partitioned distribution
        String tableType = partitionedTableTypes.iterator().next();
        String tableNameWithType = TableNameBuilder.forType(TableType.valueOf(tableType)).tableNameWithType(tableName);
        TableScanWorkerAssignmentResult assignmentResult = attemptPartitionedDistribution(tableNameWithType,
            fieldNames, instanceIdToSegments.getSegmentsMap(TableType.valueOf(tableType)), tpiMap.get(tableType));
        if (assignmentResult != null) {
          return assignmentResult;
        }
      } else {
        // TODO(mse-physical): Support automatic partitioned dist for hybrid tables.
        LOGGER.warn("Automatic Partitioned Distribution not supported for Hybrid Tables yet");
      }
    }
    // For each server, we want to know the segments for each table-type.
    Map<String, Map<String, List<String>>> instanceIdToTableTypeToSegmentsMap = new HashMap<>();
    for (String tableType : tableTypes) {
      Map<String, List<String>> segmentsMap = instanceIdToSegments.getSegmentsMap(TableType.valueOf(tableType));
      Preconditions.checkNotNull(segmentsMap, "Unexpected null segments map in leaf worker assignment");
      for (Map.Entry<String, List<String>> entry : segmentsMap.entrySet()) {
        String instanceId = entry.getKey();
        List<String> segments = entry.getValue();
        instanceIdToTableTypeToSegmentsMap.computeIfAbsent(instanceId, k -> new HashMap<>())
            .put(tableType, segments);
      }
    }
    // For each server, assign one worker each.
    Map<Integer, Map<String, List<String>>> workerIdToSegmentsMap = new HashMap<>();
    List<String> workers = new ArrayList<>();
    for (Map.Entry<String, Map<String, List<String>>> entry : instanceIdToTableTypeToSegmentsMap.entrySet()) {
      String instanceId = entry.getKey();
      for (var tableTypeAndSegments : entry.getValue().entrySet()) {
        String tableType = tableTypeAndSegments.getKey();
        List<String> segments = tableTypeAndSegments.getValue();
        workerIdToSegmentsMap.computeIfAbsent(workers.size(), (x) -> new HashMap<>()).put(tableType, segments);
      }
      workers.add(String.format("%s@%s", workers.size(), instanceId));
    }
    RelDistribution.Type distType = workers.size() == 1 ? RelDistribution.Type.SINGLETON
        : RelDistribution.Type.RANDOM_DISTRIBUTED;
    PinotDataDistribution pinotDataDistribution = new PinotDataDistribution(distType, workers, workers.hashCode(),
        null, null);
    return new TableScanWorkerAssignmentResult(pinotDataDistribution, workerIdToSegmentsMap);
  }

  /**
   * Tries to assign workers for the table-scan node to generate a partitioned data distribution. If this is not
   * possible, we simply return null.
   */
  @Nullable
  @VisibleForTesting
  static TableScanWorkerAssignmentResult attemptPartitionedDistribution(String tableNameWithType,
      List<String> fieldNames, Map<String, List<String>> instanceIdToSegmentsMap,
      @Nullable TablePartitionInfo tablePartitionInfo) {
    if (tablePartitionInfo == null) {
      return null;
    }
    if (CollectionUtils.isNotEmpty(tablePartitionInfo.getSegmentsWithInvalidPartition())) {
      LOGGER.warn("Table {} has {} segments with invalid partition info. Will assume un-partitioned distribution",
          tableNameWithType, tablePartitionInfo.getSegmentsWithInvalidPartition().size());
      return null;
    }
    String tableType =
        Objects.requireNonNull(TableNameBuilder.getTableTypeFromTableName(tableNameWithType),
            "Illegal state: expected table name with type").toString();
    int numPartitions = tablePartitionInfo.getNumPartitions();
    int keyIndex = fieldNames.indexOf(tablePartitionInfo.getPartitionColumn());
    String function = tablePartitionInfo.getPartitionFunctionName();
    int numSelectedServers = instanceIdToSegmentsMap.size();
    if (keyIndex == -1) {
      LOGGER.warn("Unable to find partition column {} in table scan fields {}", tablePartitionInfo.getPartitionColumn(),
          fieldNames);
      return null;
    } else if (numPartitions < numSelectedServers) {
      return null;
    } else if (numSelectedServers == 1) {
      // ==> scan will have a single stream, so partitioned distribution doesn't matter
      return null;
    }
    // Pre-compute segmentToServer map for quick lookup later.
    Map<String, String> segmentToServer = new HashMap<>();
    for (var entry : instanceIdToSegmentsMap.entrySet()) {
      String instanceId = entry.getKey();
      for (String segment : entry.getValue()) {
        segmentToServer.put(segment, instanceId);
      }
    }
    // For each partition, we expect at most 1 server which will be stored in this array.
    String[] partitionToServerMap = new String[tablePartitionInfo.getNumPartitions()];
    TablePartitionInfo.PartitionInfo[] partitionInfos = tablePartitionInfo.getPartitionInfoMap();
    Map<Integer, List<String>> segmentsByPartition = new HashMap<>();
    // Ensure each partition is assigned to exactly 1 server.
    for (int partitionNum = 0; partitionNum < numPartitions; partitionNum++) {
      TablePartitionInfo.PartitionInfo info = partitionInfos[partitionNum];
      List<String> selectedSegments = new ArrayList<>();
      if (info != null) {
        String chosenServer;
        for (String segment : info._segments) {
          chosenServer = segmentToServer.get(segment);
          // segmentToServer may return null if TPI has a segment not present in instanceIdToSegmentsMap.
          // This can happen when the segment was not selected for the query (due to pruning for instance).
          if (chosenServer != null) {
            selectedSegments.add(segment);
            if (partitionToServerMap[partitionNum] == null) {
              partitionToServerMap[partitionNum] = chosenServer;
            } else if (!partitionToServerMap[partitionNum].equals(chosenServer)) {
              return null;
            }
          }
        }
      }
      segmentsByPartition.put(partitionNum, selectedSegments);
    }
    // Initialize workers list. Initially each element is empty. We have 1 worker for each selected server.
    List<String> workers = new ArrayList<>();
    for (int i = 0; i < numSelectedServers; i++) {
      workers.add("");
    }
    // Try to assign workers in such a way that partition P goes to worker = P % num-workers.
    for (int partitionNum = 0; partitionNum < numPartitions; partitionNum++) {
      if (partitionToServerMap[partitionNum] != null) {
        int workerId = partitionNum % workers.size();
        if (workers.get(workerId).isEmpty()) {
          workers.set(workerId, partitionToServerMap[partitionNum]);
        } else if (!workers.get(workerId).equals(partitionToServerMap[partitionNum])) {
          return null;
        }
      }
    }
    // Build the workerId to segments map.
    Map<Integer, Map<String, List<String>>> workerIdToSegmentsMap = new HashMap<>();
    for (int workerId = 0; workerId < workers.size(); workerId++) {
      List<String> segmentsForWorker = new ArrayList<>();
      for (int partitionNum = workerId; partitionNum < numPartitions; partitionNum += workers.size()) {
        segmentsForWorker.addAll(segmentsByPartition.get(partitionNum));
      }
      workers.set(workerId, String.format("%s@%s", workerId, workers.get(workerId)));
      workerIdToSegmentsMap.put(workerId, ImmutableMap.of(tableType, segmentsForWorker));
    }
    HashDistributionDesc desc = new HashDistributionDesc(ImmutableList.of(keyIndex), function, numPartitions);
    PinotDataDistribution dataDistribution = new PinotDataDistribution(RelDistribution.Type.HASH_DISTRIBUTED,
        workers, workers.hashCode(), ImmutableSet.of(desc), null);
    return new TableScanWorkerAssignmentResult(dataDistribution, workerIdToSegmentsMap);
  }

  private Map<String, TablePartitionInfo> calculateTablePartitionInfo(String tableName, Set<String> tableTypes) {
    Map<String, TablePartitionInfo> result = new HashMap<>();
    if (tableTypes.contains("OFFLINE")) {
      String offlineTableType = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
      TablePartitionInfo tablePartitionInfo = _routingManager.getTablePartitionInfo(offlineTableType);
      if (tablePartitionInfo != null) {
        result.put("OFFLINE", tablePartitionInfo);
      }
    }
    if (tableTypes.contains("REALTIME")) {
      String realtimeTableType = TableNameBuilder.REALTIME.tableNameWithType(tableName);
      TablePartitionInfo tablePartitionInfo = _routingManager.getTablePartitionInfo(realtimeTableType);
      if (tablePartitionInfo != null) {
        result.put("REALTIME", tablePartitionInfo);
      }
    }
    return result;
  }

  /**
   * Acquire routing table for items listed in TableScanNode.
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
        CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM \"" + tableNameWithType + "\""), requestId);
  }

  private Map<String, String> getTableOptions(List<RelHint> hints) {
    Map<String, String> tmp = PlanNode.NodeHint.fromRelHints(hints).getHintOptions().get(
        PinotHintOptions.TABLE_HINT_OPTIONS);
    return tmp == null ? Map.of() : tmp;
  }

  private String getActualTableName(TableScan tableScan) {
    RelOptTable table = tableScan.getTable();
    List<String> qualifiedName = table.getQualifiedName();
    String tmp = qualifiedName.size() == 1 ? qualifiedName.get(0)
        : DatabaseUtils.constructFullyQualifiedTableName(qualifiedName.get(0), qualifiedName.get(1));
    return _tableCache.getActualTableName(tmp);
  }

  static class TableScanWorkerAssignmentResult {
    final PinotDataDistribution _pinotDataDistribution;
    final Map<Integer, Map<String, List<String>>> _workerIdToSegmentsMap;

    TableScanWorkerAssignmentResult(PinotDataDistribution pinotDataDistribution,
        Map<Integer, Map<String, List<String>>> workerIdToSegmentsMap) {
      _pinotDataDistribution = pinotDataDistribution;
      _workerIdToSegmentsMap = workerIdToSegmentsMap;
    }
  }

  static class InstanceIdToSegments {
    @Nullable
    Map<String, List<String>> _offlineTableSegmentsMap;
    @Nullable
    Map<String, List<String>> _realtimeTableSegmentsMap;

    @Nullable
    Map<String, List<String>> getSegmentsMap(TableType tableType) {
      return tableType == TableType.OFFLINE ? _offlineTableSegmentsMap : _realtimeTableSegmentsMap;
    }

    Set<String> getActiveTableTypes() {
      Set<String> tableTypes = new HashSet<>();
      if (MapUtils.isNotEmpty(_offlineTableSegmentsMap)) {
        tableTypes.add(TableType.OFFLINE.name());
      }
      if (MapUtils.isNotEmpty(_realtimeTableSegmentsMap)) {
        tableTypes.add(TableType.REALTIME.name());
      }
      return tableTypes;
    }
  }
}
