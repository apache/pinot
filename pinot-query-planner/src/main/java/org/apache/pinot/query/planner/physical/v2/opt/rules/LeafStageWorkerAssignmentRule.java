package org.apache.pinot.query.planner.physical.v2.opt.rules;

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
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.hint.PinotHintStrategyTable;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.request.Expression;
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
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalFilter;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalTableScan;
import org.apache.pinot.query.planner.physical.v2.opt.PRelOptRule;
import org.apache.pinot.query.planner.physical.v2.opt.PRelOptRuleCall;
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
    return call._currentNode.isLeafStage();
  }

  @Override
  public PRelNode onMatch(PRelOptRuleCall call) {
    if (call._currentNode.unwrap() instanceof TableScan) {
      return assignTableScan((PhysicalTableScan) call._currentNode);
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

  /**
   *
   */
  private PRelNode assignTableScan(PhysicalTableScan tableScan, @Nullable PhysicalFilter filter, long requestId) {
    // Step-1: Init tableName, table options, routing table and time boundary info.
    String tableName = getActualTableName(tableScan);
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
    Map<String, Map<String, List<String>>> instanceIdToSegmentsMap = new HashMap<>();
    for (Map.Entry<String, RoutingTable> routingEntry : routingTableMap.entrySet()) {
      String tableType = routingEntry.getKey();
      RoutingTable routingTable = routingEntry.getValue();
      Map<ServerInstance, ServerRouteInfo> segmentsMap = routingTable.getServerInstanceToSegmentsMap();
      for (Map.Entry<ServerInstance, ServerRouteInfo> serverEntry : segmentsMap.entrySet()) {
        String instanceId = serverEntry.getKey().getInstanceId();
        Map<String, List<String>> tableTypeToSegmentListMap =
            instanceIdToSegmentsMap.computeIfAbsent(instanceId, k -> new HashMap<>());
        Preconditions.checkState(tableTypeToSegmentListMap.put(
                tableType, serverEntry.getValue().getSegments()) == null,
            "Entry for server {} and table type: {} already exist!", serverEntry.getKey(), tableType);
        QueryServerInstance queryServerInstance = new QueryServerInstance(serverEntry.getKey());
        _physicalPlannerContext.getInstanceIdToQueryServerInstance().putIfAbsent(queryServerInstance.getInstanceId(),
            queryServerInstance);
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
    Map<Integer, Map<String, List<String>>> workedIdToSegmentsMap = assignTableScan(tableName, fieldNames,
        tableOptions, instanceIdToSegmentsMap, _physicalPlannerContext.getInstanceIdToQueryServerInstance(),
        tablePartitionInfoMap);
    TableScanMetadata metadata = new TableScanMetadata(Set.of(tableName), workedIdToSegmentsMap, tableOptions,
        segmentUnavailableMap, timeBoundaryInfo);
    PinotDataDistribution scanDataDistribution = null;
    return tableScan.with(scanDataDistribution, metadata);
  }

  /**
   * worker id to segment mapping. Second mapping is by table-type.
   */
  private Map<Integer, Map<String, List<String>>> assignTableScan(String tableName, List<String> fieldNames,
      Map<String, String> tableOptions, Map<String, Map<String, List<String>>> instanceIdToSegmentsMap,
      Map<String, QueryServerInstance> instanceIdToQueryServerInstance, Map<String, TablePartitionInfo> tpiMap) {
    // i0: [s0..]
    return null;
  }

  /**
   *
   */
  private PRelNode assignTableScan(PhysicalTableScan tableScan) {
    String tableName = getActualTableName(tableScan);
    // TODO(mse-physical): Support server pruning based on filter. Filter can be extracted from parent stack.
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
    List<RelHint> hints = tableScan.getHints();
    Map<String, String> tableOptions = getTableOptions(hints);
    for (Map.Entry<ServerInstance, Map<String, List<String>>> entry : serverInstanceToSegmentsMap.entrySet()) {
      String instanceId = entry.getKey().getInstanceId();
      _physicalPlannerContext.getInstanceIdToQueryServerInstance().putIfAbsent(
          instanceId, new QueryServerInstance(entry.getKey()));
    }
    TablePartitionInfo tablePartitionInfo = _routingManager.getTablePartitionInfo(
        TableNameBuilder.OFFLINE.tableNameWithType(tableName));
    if (tablePartitionInfo != null) {
      var distAndSegmentsMap = computePartitionedDistribution(tableScan, tablePartitionInfo,
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

  private Map<String, TablePartitionInfo> calculateTablePartitionInfo(String tableName, Set<String> tableTypes) {
    Map<String, TablePartitionInfo> result = new HashMap<>();
    if (tableTypes.contains("OFFLINE")) {
      String offlineTableType = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
      TablePartitionInfo tablePartitionInfo = _routingManager.getTablePartitionInfo(offlineTableType);
      if (tablePartitionInfo != null) {
        result.put(offlineTableType, tablePartitionInfo);
      }
    }
    if (tableTypes.contains("REALTIME")) {
      String realtimeTableType = TableNameBuilder.REALTIME.tableNameWithType(tableName);
      TablePartitionInfo tablePartitionInfo = _routingManager.getTablePartitionInfo(realtimeTableType);
      if (tablePartitionInfo != null) {
        result.put(realtimeTableType, _routingManager.getTablePartitionInfo(tableName));
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

  private RoutingTable getRoutingTable(String tableName, TableType tableType, long requestId) {
    String tableNameWithType =
        TableNameBuilder.forType(tableType).tableNameWithType(TableNameBuilder.extractRawTableName(tableName));
    return _routingManager.getRoutingTable(
        CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM \"" + tableNameWithType + "\""), requestId);
  }
}
