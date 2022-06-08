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

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.RoutingTable;
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

  private final String _hostName;
  private final int _port;
  private final RoutingManager _routingManager;

  public WorkerManager(String hostName, int port, RoutingManager routingManager) {
    _hostName = hostName;
    _port = port;
    _routingManager = routingManager;
  }

  public void assignWorkerToStage(int stageId, StageMetadata stageMetadata) {
    List<String> scannedTables = stageMetadata.getScannedTables();
    if (scannedTables.size() == 1) { // table scan stage, need to attach server as well as segment info.
      RoutingTable routingTable = getRoutingTable(scannedTables.get(0));
      Map<ServerInstance, List<String>> serverInstanceToSegmentsMap = routingTable.getServerInstanceToSegmentsMap();
      stageMetadata.setServerInstances(new ArrayList<>(serverInstanceToSegmentsMap.keySet()));
      stageMetadata.setServerInstanceToSegmentsMap(new HashMap<>(serverInstanceToSegmentsMap));
    } else if (PlannerUtils.isRootStage(stageId)) {
      // ROOT stage doesn't have a QueryServer as it is strictly only reducing results.
      // here we simply assign the worker instance with identical server/mailbox port number.
      stageMetadata.setServerInstances(Lists.newArrayList(new WorkerInstance(_hostName, _port, _port, _port, _port)));
    } else {
      stageMetadata.setServerInstances(filterServers(_routingManager.getEnabledServerInstanceMap().values()));
    }
  }

  private static List<ServerInstance> filterServers(Collection<ServerInstance> servers) {
    List<ServerInstance> serverInstances = new ArrayList<>();
    for (ServerInstance server : servers) {
      String hostname = server.getHostname();
      if (server.getQueryServicePort() > 0 && server.getQueryMailboxPort() > 0
          && !hostname.startsWith(CommonConstants.Helix.PREFIX_OF_BROKER_INSTANCE)
          && !hostname.startsWith(CommonConstants.Helix.PREFIX_OF_CONTROLLER_INSTANCE)
          && !hostname.startsWith(CommonConstants.Helix.PREFIX_OF_MINION_INSTANCE)) {
        serverInstances.add(server);
      }
    }
    return serverInstances;
  }

  private RoutingTable getRoutingTable(String tableName) {
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    // TODO: support both offline and realtime, now we hard code offline table.
    String tableNameWithType = TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(rawTableName);
    return _routingManager.getRoutingTable(CalciteSqlCompiler.compileToBrokerRequest(
        "SELECT * FROM " + tableNameWithType));
  }
}
