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
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.misc.OrderedHashSet;
import org.apache.calcite.util.Util;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.planner.PlannerUtils;
import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;


/**
 * The {@code WorkerManager} manages stage to worker assignment.
 *
 * <p>It contains the logic to assign worker to a particular stages. If it is a leaf stage the logic fallback to
 * how Pinot server assigned server and server-segment mapping.
 *
 * TODO: Currently it is implemented by wrapping routing manager from Pinot Broker. however we can abstract out
 * the worker manager later when we split out the query-spi layer.
 * TODO: clean up singleton instance assignment using Pinot-specific RelDistributionTrait
 */
public class WorkerManager {
  private static final Random RANDOM = new Random();

  private final String _hostName;
  private final int _port;
  private final RoutingManager _routingManager;

  public WorkerManager(String hostName, int port, RoutingManager routingManager) {
    _hostName = hostName;
    _port = port;
    _routingManager = routingManager;
  }

  public void assignWorkerToStage(int stageId, Map<Integer, StageMetadata> stageMetadataMap, long requestId,
      Map<String, String> options) {
    StageMetadata currentStageMetadata = stageMetadataMap.get(stageId);
    List<String> scannedTables = currentStageMetadata.getScannedTables();
    if (scannedTables.size() == 1) {
      // --- LEAF STAGE ---
      assignWorkerToLeafStage(requestId, currentStageMetadata, scannedTables);
    } else if (PlannerUtils.isRootStage(stageId)) {
      // --- ROOT STAGE / BROKER REDUCE STAGE ---
      // ROOT stage doesn't have a QueryServer as it is strictly only reducing results.
      // here we simply assign the worker instance with identical server/mailbox port number.
      currentStageMetadata.setServerInstances(Lists.newArrayList(
          new VirtualServer(new WorkerInstance(_hostName, _port, _port, _port, _port), Collections.singletonList(0))));
    } else {
      // --- INTERMEDIATE STAGES ---
      // TODO: actually make assignment strategy decisions for intermediate stages
      assignWorkerToIntermediateStage(requestId, currentStageMetadata, stageMetadataMap, options);
    }
  }

  private void assignWorkerToIntermediateStage(long requestId, StageMetadata currentStageMetadata,
      Map<Integer, StageMetadata> stageMetadataMap, Map<String, String> options) {
    // step 1: compute all server instances used by previous stages.
    //   - we also only retain the available server instances from this routing manager.
    OrderedHashSet<ServerInstance> servers = new OrderedHashSet<>();
    for (int inboundStageId : currentStageMetadata.getInboundStageMap()) {
      for (VirtualServer virtualServer : stageMetadataMap.get(inboundStageId).getServerInstances()) {
        servers.add(virtualServer.getServer());
      }
    }
    servers.retainAll(_routingManager.getEnabledServerInstanceMap().values());
    // step 2: determine the stage parallelism based on query options or previous stage partition count.
    // TODO: right now it is hint-based only, but we should compute based on previous stages as well.
    int stageParallelism = Integer.parseInt(
        options.getOrDefault(CommonConstants.Broker.Request.QueryOptionKey.STAGE_PARALLELISM, "1"));

    // step 3: assign instances and partitions to this stage.
    List<VirtualServer> serverInstances = new ArrayList<>();
    if (currentStageMetadata.isRequiresSingletonInstance()) {
      // Current stage require singleton instance
      ServerInstance randomInstance = servers.get(RANDOM.nextInt(servers.size()));
      serverInstances.add(new VirtualServer(randomInstance, Collections.singletonList(0)));
    } else {
      // Current stage is partitioned-based.
      int partitionCount = 0;
      for (ServerInstance server : servers) {
        String hostname = server.getHostname();
        if (server.getQueryServicePort() > 0 && server.getQueryMailboxPort() > 0
            && !hostname.startsWith(CommonConstants.Helix.PREFIX_OF_BROKER_INSTANCE)
            && !hostname.startsWith(CommonConstants.Helix.PREFIX_OF_CONTROLLER_INSTANCE)
            && !hostname.startsWith(CommonConstants.Helix.PREFIX_OF_MINION_INSTANCE)) {
          serverInstances.add(new VirtualServer(server, Util.range(partitionCount, partitionCount + stageParallelism)));
          partitionCount += stageParallelism;
        }
      }
    }
    currentStageMetadata.setServerInstances(serverInstances);

    // step 4: update previous stage to current stage mapping.

  }

  private void assignWorkerToLeafStage(long requestId, StageMetadata currentStageMetadata, List<String> scannedTables) {
    // table scan stage, need to attach server as well as segment info for each physical table type.
    String logicalTableName = scannedTables.get(0);
    Map<String, RoutingTable> routingTableMap = getRoutingTable(logicalTableName, requestId);
    if (routingTableMap.size() == 0) {
      throw new IllegalArgumentException("Unable to find routing entries for table: " + logicalTableName);
    }
    // acquire time boundary info if it is a hybrid table.
    if (routingTableMap.size() > 1) {
      TimeBoundaryInfo timeBoundaryInfo = _routingManager.getTimeBoundaryInfo(TableNameBuilder
          .forType(TableType.OFFLINE).tableNameWithType(TableNameBuilder.extractRawTableName(logicalTableName)));
      if (timeBoundaryInfo != null) {
        currentStageMetadata.setTimeBoundaryInfo(timeBoundaryInfo);
      } else {
        // remove offline table routing if no time boundary info is acquired.
        routingTableMap.remove(TableType.OFFLINE.name());
      }
    }

    // extract all the instances associated to each table type
    Map<ServerInstance, Map<String, Map<Integer, List<String>>>> serverAndPartitionToSegmentMap = new HashMap<>();
    for (Map.Entry<String, RoutingTable> routingEntry : routingTableMap.entrySet()) {
      String tableType = routingEntry.getKey();
      RoutingTable routingTable = routingEntry.getValue();
      // for each server instance, attach all table types and their associated segment list.
      for (Map.Entry<ServerInstance, List<String>> serverEntry
          : routingTable.getServerInstanceToSegmentsMap().entrySet()) {
        serverAndPartitionToSegmentMap.putIfAbsent(serverEntry.getKey(), new HashMap<>());
        Map<String, Map<Integer, List<String>>> tableTypeToPartitionedSegmentListMap =
            serverAndPartitionToSegmentMap.get(serverEntry.getKey());
        // TODO: support partitioned segment list routing.
        Map<Integer, List<String>> existingRouting =
            tableTypeToPartitionedSegmentListMap.put(tableType, Collections.singletonMap(0, serverEntry.getValue()));
        Preconditions.checkState(existingRouting == null, "Entry for server {} and table type: {} already exist!",
            serverEntry.getKey(), tableType);
      }
    }
    // TODO: see the leaf stage doesn't allow control of partition yet. see: {@link ServerRequestPlanVisitor#build}
    currentStageMetadata.setServerInstances(new ArrayList<>(
        serverAndPartitionToSegmentMap.keySet()
            .stream()
            .map(server -> new VirtualServer(server, Collections.singletonList(0)))
            .collect(Collectors.toList())));
    currentStageMetadata.setServerAndPartitionToSegmentMap(serverAndPartitionToSegmentMap);
  }

  /**
   * Acquire routing table for items listed in {@link org.apache.pinot.query.planner.stage.TableScanNode}.
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
    String tableNameWithType = TableNameBuilder.forType(tableType).tableNameWithType(
        TableNameBuilder.extractRawTableName(tableName));
    return _routingManager.getRoutingTable(
        CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM " + tableNameWithType), requestId);
  }
}
