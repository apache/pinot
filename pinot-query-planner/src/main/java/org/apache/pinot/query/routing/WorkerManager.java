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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
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

  public void assignWorkerToStage(int stageId, StageMetadata stageMetadata, long requestId,
      Map<String, String> options, Set<String> tableNames) {
    if (isLeafStage(stageMetadata)) {
      // --- LEAF STAGE ---
      // table scan stage, need to attach server as well as segment info for each physical table type.
      List<String> scannedTables = stageMetadata.getScannedTables();
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
          stageMetadata.setTimeBoundaryInfo(timeBoundaryInfo);
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
        for (Map.Entry<ServerInstance, List<String>> serverEntry
            : routingTable.getServerInstanceToSegmentsMap().entrySet()) {
          serverInstanceToSegmentsMap.putIfAbsent(serverEntry.getKey(), new HashMap<>());
          Map<String, List<String>> tableTypeToSegmentListMap = serverInstanceToSegmentsMap.get(serverEntry.getKey());
          Preconditions.checkState(tableTypeToSegmentListMap.put(tableType, serverEntry.getValue()) == null,
              "Entry for server {} and table type: {} already exist!", serverEntry.getKey(), tableType);
        }
      }
      int globalIdx = 0;
      List<VirtualServer> serverInstanceList = new ArrayList<>();
      for (ServerInstance serverInstance : serverInstanceToSegmentsMap.keySet()) {
        serverInstanceList.add(new VirtualServer(serverInstance, globalIdx++));
      }
      stageMetadata.setServerInstances(serverInstanceList);
      stageMetadata.setServerInstanceToSegmentsMap(serverInstanceToSegmentsMap);
    } else if (PlannerUtils.isRootStage(stageId)) {
      // --- ROOT STAGE / BROKER REDUCE STAGE ---
      // ROOT stage doesn't have a QueryServer as it is strictly only reducing results.
      // here we simply assign the worker instance with identical server/mailbox port number.
      stageMetadata.setServerInstances(Lists.newArrayList(
          new VirtualServer(new WorkerInstance(_hostName, _port, _port, _port, _port), 0)));
    } else {
      // --- INTERMEDIATE STAGES ---
      // If the query has more than one table, it is possible that the tables could be hosted on different tenants.
      // The intermediate stage will be processed on servers randomly picked from the tenants belonging to either or
      // all of the tables in the query.
      // TODO: actually make assignment strategy decisions for intermediate stages
      Set<ServerInstance> serverInstances = new HashSet<>();
      if (tableNames.size() == 0) {
        // This could be the case from queries that don't actually fetch values from the tables. In such cases the
        // routing need not be tenant aware.
        // Eg: SELECT 1 AS one FROM select_having_expression_test_test_having HAVING 1 > 2;
        serverInstances = _routingManager.getEnabledServerInstanceMap().values().stream().collect(Collectors.toSet());
      } else {
        serverInstances = fetchServersForIntermediateStage(tableNames);
      }

      stageMetadata.setServerInstances(
          assignServers(serverInstances, stageMetadata.isRequiresSingletonInstance(), options));
    }
  }

  private static List<VirtualServer> assignServers(Set<ServerInstance> servers,
      boolean requiresSingletonInstance, Map<String, String> options) {
    int stageParallelism = Integer.parseInt(
        options.getOrDefault(CommonConstants.Broker.Request.QueryOptionKey.STAGE_PARALLELISM, "1"));
    List<ServerInstance> serverInstances = new ArrayList<>(servers);
    List<VirtualServer> virtualServerList = new ArrayList<>();
    if (requiresSingletonInstance) {
      // require singleton should return a single global worker ID with 0;
      ServerInstance serverInstance = serverInstances.get(RANDOM.nextInt(serverInstances.size()));
      virtualServerList.add(new VirtualServer(serverInstance, 0));
    } else {
      int globalIdx = 0;
      for (ServerInstance server : servers) {
        String hostname = server.getHostname();
        if (server.getQueryServicePort() > 0 && server.getQueryMailboxPort() > 0
            && !hostname.startsWith(CommonConstants.Helix.PREFIX_OF_BROKER_INSTANCE)
            && !hostname.startsWith(CommonConstants.Helix.PREFIX_OF_CONTROLLER_INSTANCE)
            && !hostname.startsWith(CommonConstants.Helix.PREFIX_OF_MINION_INSTANCE)) {
          for (int virtualId = 0; virtualId < stageParallelism; virtualId++) {
              virtualServerList.add(new VirtualServer(server, globalIdx++));
            }
          }
        }
    }
    return virtualServerList;
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

  // TODO: Find a better way to determine whether a stage is leaf stage or intermediary. We could have query plans that
  //       process table data even in intermediary stages.
  private boolean isLeafStage(StageMetadata stageMetadata) {
    return stageMetadata.getScannedTables().size() == 1;
  }

  private Set<ServerInstance> fetchServersForIntermediateStage(Set<String> tableNames) {
    Set<ServerInstance> serverInstances = new HashSet<>();

    for (String table : tableNames) {
      String rawTableName = TableNameBuilder.extractRawTableName(table);
      TableType tableType = TableNameBuilder.getTableTypeFromTableName(table);
      if (tableType == null) {
        String offlineTable = TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(rawTableName);
        String realtimeTable = TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(rawTableName);

        // Servers in the offline table's tenant.
        Map<String, ServerInstance> servers = _routingManager.getEnabledServersForTableTenant(offlineTable);
        serverInstances.addAll(servers.values());

        // Servers in the online table's tenant.
        servers = _routingManager.getEnabledServersForTableTenant(realtimeTable);
        serverInstances.addAll(servers.values());
      } else {
        Map<String, ServerInstance> servers = _routingManager.getEnabledServersForTableTenant(table);
        serverInstances.addAll(servers.values());
      }
    }

    return serverInstances;
  }
}
