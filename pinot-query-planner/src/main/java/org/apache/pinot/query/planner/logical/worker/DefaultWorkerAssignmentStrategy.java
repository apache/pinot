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
package org.apache.pinot.query.planner.logical.worker;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.planner.PlannerUtils;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/**
 * The default worker assignment strategy. This assigns workers using the following:
 * 1. For the root-stage, we assign the broker as the only worker.
 * 2. For intermediate stages, we assign all available servers as workers. We haven't implemented tenant isolation yet
 *    so all available servers across all tenants are picked for the intermediate stage, even if the tables in the query
 *    belong to a single tenant.
 * 3. Workers for the leaf-stage are determined by the segment/server selection made by RoutingManager.
 */
public class DefaultWorkerAssignmentStrategy implements WorkerAssignmentStrategy {
  private final WorkerManager _workerManager;

  DefaultWorkerAssignmentStrategy(WorkerManager workerManager) {
    _workerManager = workerManager;
  }

  public void assignWorkers(QueryPlan queryPlan, long requestId) {
    // assign workers to each stage.
    for (Map.Entry<Integer, StageMetadata> e : queryPlan.getStageMetadataMap().entrySet()) {
      assignWorkerToStage(e.getKey(), e.getValue(), requestId);
    }
  }

  private void assignWorkerToStage(int stageId, StageMetadata stageMetadata, long requestId) {
    List<String> scannedTables = stageMetadata.getScannedTables();
    if (scannedTables.size() == 1) {
      // table scan stage, need to attach server as well as segment info for each physical table type.
      String logicalTableName = scannedTables.get(0);
      Map<String, RoutingTable> routingTableMap = _workerManager.getRoutingTable(logicalTableName, requestId);
      if (routingTableMap.size() == 0) {
        throw new IllegalArgumentException("Unable to find routing entries for table: " + logicalTableName);
      }
      // acquire time boundary info if it is a hybrid table.
      if (routingTableMap.size() > 1) {
        TimeBoundaryInfo timeBoundaryInfo = _workerManager.getRoutingManager().getTimeBoundaryInfo(TableNameBuilder
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
      stageMetadata.setServerInstances(new ArrayList<>(serverInstanceToSegmentsMap.keySet()));
      stageMetadata.setServerInstanceToSegmentsMap(serverInstanceToSegmentsMap);
    } else if (PlannerUtils.isRootStage(stageId)) {
      // ROOT stage doesn't have a QueryServer as it is strictly only reducing results.
      // here we simply assign the worker instance with identical server/mailbox port number.
      stageMetadata.setServerInstances(Lists.newArrayList(_workerManager.getCurrentWorker()));
    } else {
      stageMetadata.setServerInstances(_workerManager.getMultiStageWorkers());
    }
  }
}
