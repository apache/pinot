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
package org.apache.pinot.query.planner.physical.v2.rules;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.calcite.rel.HashDistributionDesc;
import org.apache.pinot.calcite.rel.PinotDataDistribution;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.hint.PinotHintStrategyTable;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.ServerRouteInfo;
import org.apache.pinot.core.routing.TablePartitionInfo;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.apache.pinot.query.planner.physical.v2.MappingGen;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.PRelOptRule;
import org.apache.pinot.query.planner.physical.v2.PRelOptRuleCall;
import org.apache.pinot.query.planner.physical.v2.TableScanMetadata;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;


public class LeafStageWorkerAssignmentRule extends PRelOptRule {
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
    if (call._currentNode.isLeafStage()) {
      return true;
    }
    if (call._currentNode.getRelNode() instanceof Aggregate && call._currentNode.getInput(0).isLeafStage()
        && isProjectFilterOrScan(call._currentNode.getInput(0))) {
      // If aggregate is partitioned or forced-partitioned, then promote it to leaf stage if it exists on the boundary.
      Aggregate aggRel = (Aggregate) call._currentNode.getRelNode();
      boolean hasGroupBy = !aggRel.getGroupSet().isEmpty();
      if (!hasGroupBy) {
        return false;
      }
      Map<String, String> hintOptions =
          PinotHintStrategyTable.getHintOptions(aggRel.getHints(), PinotHintOptions.AGGREGATE_HINT_OPTIONS);
      hintOptions = hintOptions == null ? Map.of() : hintOptions;
      if (Boolean.parseBoolean(hintOptions.get(PinotHintOptions.AggregateOptions.IS_PARTITIONED_BY_GROUP_BY_KEYS))) {
        return true;
      }
      PinotDataDistribution inputDistribution = call._currentNode.getInput(0).getPinotDataDistributionOrThrow();
      if (inputDistribution.getType() != RelDistribution.Type.HASH_DISTRIBUTED) {
        return false;
      }
      HashDistributionDesc partitionDesc = inputDistribution.getHashDistributionDesc().iterator()
          .next();
      return aggRel.getGroupSet().asSet().stream().anyMatch(x -> partitionDesc.getKeyIndexes().contains(x));
    }
    return false;
  }

  @Override
  public PRelNode onMatch(PRelOptRuleCall call) {
    if (call._currentNode.getRelNode() instanceof TableScan) {
      return assignTableScan(call._currentNode);
    }
    PRelNode currentNode = call._currentNode;
    int currentNodeId = currentNode.getNodeId();
    Preconditions.checkState(currentNode.isLeafStage() || currentNode.getRelNode() instanceof Aggregate);
    currentNode = currentNode.isLeafStage() ? currentNode : currentNode.asLeafStage(() -> currentNodeId);
    Map<Integer, List<Integer>> mapping = MappingGen.compute(currentNode.getInput(0).getRelNode(),
        currentNode.getRelNode(), null);
    PinotDataDistribution derivedDistribution = currentNode.getInput(0).getPinotDataDistributionOrThrow()
        .apply(mapping);
    return currentNode.withPinotDataDistribution(derivedDistribution);
  }

  private PRelNode assignTableScan(PRelNode pRelNode) {
    TableScan tableScan = (TableScan) pRelNode.getRelNode();
    String tableName = getActualTableName(tableScan);
    // TODO: Support server pruning based on filter. Filter can be extracted from parent stack.
    String filter = "";
    Map<String, RoutingTable> routingTableMap = getRoutingTable(tableName, _physicalPlannerContext.getRequestId());
    Preconditions.checkState(!routingTableMap.isEmpty(), "Unable to find routing entries for table: %s", tableName);
    TimeBoundaryInfo timeBoundaryInfo = null;

    // acquire time boundary info if it is a hybrid table.
    if (routingTableMap.size() > 1) {
      timeBoundaryInfo = _routingManager.getTimeBoundaryInfo(
          TableNameBuilder.forType(TableType.OFFLINE)
              .tableNameWithType(TableNameBuilder.extractRawTableName(tableName)));
      if (timeBoundaryInfo == null) {
        // remove offline table routing if no time boundary info is acquired.
        routingTableMap.remove(TableType.OFFLINE.name());
      }
    }

    // extract all the instances associated to each table type
    Map<String, Set<String>> segmentUnavailableMap = new HashMap<>();
    Map<ServerInstance, Map<String, List<String>>> serverInstanceToSegmentsMap = new HashMap<>();
    for (Map.Entry<String, RoutingTable> routingEntry : routingTableMap.entrySet()) {
      String tableType = routingEntry.getKey();
      RoutingTable routingTable = routingEntry.getValue();
      // for each server instance, attach all table types and their associated segment list.
      Map<ServerInstance, ServerRouteInfo> segmentsMap = routingTable.getServerInstanceToSegmentsMap();
      for (Map.Entry<ServerInstance, ServerRouteInfo> serverEntry : segmentsMap.entrySet()) {
        Map<String, List<String>> tableTypeToSegmentListMap =
            serverInstanceToSegmentsMap.computeIfAbsent(serverEntry.getKey(), k -> new HashMap<>());
        // TODO: support optional segments for multi-stage engine.
        Preconditions.checkState(tableTypeToSegmentListMap.put(
            tableType, serverEntry.getValue().getSegments()) == null,
            "Entry for server {} and table type: {} already exist!", serverEntry.getKey(), tableType);
      }
      if (!routingTable.getUnavailableSegments().isEmpty()) {
        // Set unavailable segments in context, keyed by PRelNode ID.
        segmentUnavailableMap.put(tableName, new HashSet<>(routingTable.getUnavailableSegments()));
      }
    }
    Set<String> scannedTables = new HashSet<>();
    scannedTables.add(tableName);
    List<RelHint> hints = ((TableScan) pRelNode.getRelNode()).getHints();
    Map<String, String> tableOptions = getTableOptions(hints);
    for (Map.Entry<ServerInstance, Map<String, List<String>>> entry : serverInstanceToSegmentsMap.entrySet()) {
      String instanceId = entry.getKey().getInstanceId();
      _physicalPlannerContext.getInstanceIdToQueryServerInstance().putIfAbsent(
          instanceId, new QueryServerInstance(entry.getKey()));
    }
    TablePartitionInfo tablePartitionInfo = _routingManager.getTablePartitionInfo(
        TableNameBuilder.OFFLINE.tableNameWithType(tableName));
    if (tablePartitionInfo != null) {
      var distAndSegmentsMap = computePartitionedDistribution(pRelNode, tablePartitionInfo,
          tableScan.getRowType().getFieldNames(), serverInstanceToSegmentsMap, false);
      if (distAndSegmentsMap != null) {
        PinotDataDistribution pinotDataDistribution = distAndSegmentsMap.getLeft();
        Map<Integer, Map<String, List<String>>> workerIdToSegmentsMap = distAndSegmentsMap.getRight();
        return pRelNode.with(pinotDataDistribution, new TableScanMetadata(scannedTables,
            workerIdToSegmentsMap, tableOptions,
            segmentUnavailableMap, timeBoundaryInfo));
      }
    }
    int workerId = 0;
    Map<Integer, Map<String, List<String>>> workerIdToSegmentsMap = new HashMap<>();
    List<String> workers = new ArrayList<>();
    for (Map.Entry<ServerInstance, Map<String, List<String>>> entry : serverInstanceToSegmentsMap.entrySet()) {
      String instanceId = entry.getKey().getInstanceId();
      workerIdToSegmentsMap.put(workerId, entry.getValue());
      workers.add(String.format("%s@%s", workerId, instanceId));
      workerId++;
    }
    PinotDataDistribution pinotDataDistribution;
    String partitionColumn = PinotHintStrategyTable.getHintOption(tableScan.getHints(),
        PinotHintOptions.TABLE_HINT_OPTIONS, PinotHintOptions.TableHintOptions.PARTITION_KEY);
    if (partitionColumn == null) {
      pinotDataDistribution = new PinotDataDistribution(RelDistribution.Type.RANDOM_DISTRIBUTED, workers,
          workers.hashCode(), null, null);
    } else {
      String hashFunction = PinotHintStrategyTable.getHintOption(tableScan.getHints(),
          PinotHintOptions.TABLE_HINT_OPTIONS, PinotHintOptions.TableHintOptions.PARTITION_FUNCTION);
      hashFunction = hashFunction == null ? "murmur" : hashFunction;
      String partitionSize = PinotHintStrategyTable.getHintOption(tableScan.getHints(),
          PinotHintOptions.TABLE_HINT_OPTIONS, PinotHintOptions.TableHintOptions.PARTITION_SIZE);
      Preconditions.checkNotNull(partitionSize, "partition size should be set");
      HashDistributionDesc desc = new HashDistributionDesc(
          ImmutableList.of(tableScan.getRowType().getFieldNames().indexOf(partitionColumn)),
          hashFunction, Integer.parseInt(partitionSize));
      pinotDataDistribution = new PinotDataDistribution(RelDistribution.Type.HASH_DISTRIBUTED,
          workers, workers.hashCode(), ImmutableSet.of(desc), null);
    }
    return pRelNode.with(pinotDataDistribution, new TableScanMetadata(scannedTables, workerIdToSegmentsMap,
        tableOptions, segmentUnavailableMap, timeBoundaryInfo));
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

  @Nullable
  private Pair<PinotDataDistribution, Map<Integer, Map<String, List<String>>>> computePartitionedDistribution(
      PRelNode pRelNode, TablePartitionInfo tablePartitionInfo, List<String> fieldNames,
      Map<ServerInstance, Map<String, List<String>>> serverInstanceToSegmentsMap, boolean isPartitionParallelism) {
    if (!tablePartitionInfo.getSegmentsWithInvalidPartition().isEmpty()) {
      return null;
    }
    String tableType = TableNameBuilder.isOfflineTableResource(tablePartitionInfo.getTableNameWithType()) ? "OFFLINE"
        : "REALTIME";
    int numPartitions = tablePartitionInfo.getNumPartitions();
    int keyIndex = fieldNames.indexOf(tablePartitionInfo.getPartitionColumn());
    String function = tablePartitionInfo.getPartitionFunctionName();
    int numSelectedServers = serverInstanceToSegmentsMap.size();
    if (numPartitions < numSelectedServers) {
      return null;
    }
    Map<String, String> segmentToServer = new HashMap<>();
    for (var entry : serverInstanceToSegmentsMap.entrySet()) {
      String instanceId = entry.getKey().getInstanceId();
      for (String segment : entry.getValue().get(tableType)) {
        segmentToServer.put(segment, instanceId);
      }
    }
    String[] partitionServerMap = new String[tablePartitionInfo.getNumPartitions()];
    TablePartitionInfo.PartitionInfo[] partitionInfos = tablePartitionInfo.getPartitionInfoMap();
    // Ensure each partition is assigned to exactly 1 server.
    Map<Integer, List<String>> segmentsByPartition = new HashMap<>();
    for (int partitionNum = 0; partitionNum < numPartitions; partitionNum++) {
      TablePartitionInfo.PartitionInfo info = partitionInfos[partitionNum];
      List<String> segments = new ArrayList<>();
      if (info != null) {
        String chosenServer;
        for (String segment : info._segments) {
          chosenServer = segmentToServer.get(segment);
          if (chosenServer != null) {
            segments.add(segment);
            if (partitionServerMap[partitionNum] == null) {
              partitionServerMap[partitionNum] = chosenServer;
            } else if (!partitionServerMap[partitionNum].equals(chosenServer)) {
              return null;
            }
          }
        }
      }
      segmentsByPartition.put(partitionNum, segments);
    }
    if (isPartitionParallelism) {
      // For partition parallelism, can assign workers based on the array above.
      throw new UnsupportedOperationException("Not supported yet");
    }
    // Get ordered list of unique servers. num-workers = num-servers-selected.
    List<String> workers = new ArrayList<>();
    for (int i = 0; i < numSelectedServers; i++) {
      workers.add("");
    }
    for (int partitionNum = 0; partitionNum < numPartitions; partitionNum++) {
      if (partitionServerMap[partitionNum] != null) {
        int workerId = partitionNum % workers.size();
        if (workers.get(workerId).isEmpty()) {
          workers.set(workerId, partitionServerMap[partitionNum]);
        } else if (!workers.get(workerId).equals(partitionServerMap[partitionNum])) {
          return null;
        }
      }
    }
    Map<Integer, Map<String, List<String>>> workerIdToSegmentsMap = new HashMap<>();
    for (int workerId = 0; workerId < workers.size(); workerId++) {
      List<String> segmentsForWorker = new ArrayList<>();
      for (int partitionNum = workerId; partitionNum < numPartitions; partitionNum += workers.size()) {
        segmentsForWorker.addAll(segmentsByPartition.get(partitionNum));
      }
      workers.set(workerId, String.format("%s@%s", workerId, workers.get(workerId)));
      workerIdToSegmentsMap.put(workerId, ImmutableMap.of(tableType, segmentsForWorker));
    }
    HashDistributionDesc desc = new HashDistributionDesc(
        ImmutableList.of(keyIndex), function, numPartitions);
    PinotDataDistribution dataDistribution = new PinotDataDistribution(RelDistribution.Type.HASH_DISTRIBUTED,
        workers, workers.hashCode(), ImmutableSet.of(desc), null);
    return Pair.of(dataDistribution, workerIdToSegmentsMap);
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

  private static boolean isProjectFilterOrScan(PRelNode pRelNode) {
    return pRelNode.getRelNode() instanceof TableScan || pRelNode.getRelNode() instanceof Project
        || pRelNode.getRelNode() instanceof Filter;
  }
}
