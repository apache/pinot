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
package org.apache.pinot.query.planner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.routing.WorkerMetadata;


public class DispatchablePlanFragment {

  public static final String TABLE_NAME_KEY = "tableName";
  public static final String TIME_BOUNDARY_COLUMN_KEY = "timeBoundaryInfo.timeColumn";
  public static final String TIME_BOUNDARY_VALUE_KEY = "timeBoundaryInfo.timeValue";
  private final PlanFragment _planFragment;
  private final List<WorkerMetadata> _workerMetadataList;

  // This is used at broker stage - we don't need to ship it to the server.
  private final Map<QueryServerInstance, List<Integer>> _serverInstanceToWorkerIdMap;

  // used for table scan stage - we use ServerInstance instead of VirtualServer
  // here because all virtual servers that share a server instance will have the
  // same segments on them
  private final Map<Integer, Map<String, List<String>>> _workerIdToSegmentsMap;

  // used for passing custom properties to build StageMetadata on the server.
  private final Map<String, String> _customProperties;

  public DispatchablePlanFragment(PlanFragment planFragment) {
    this(planFragment, new ArrayList<>(), new HashMap<>(), new HashMap<>());
  }

  public DispatchablePlanFragment(PlanFragment planFragment, List<WorkerMetadata> workerMetadataList,
      Map<QueryServerInstance, List<Integer>> serverInstanceToWorkerIdMap, Map<String, String> customPropertyMap) {
    _planFragment = planFragment;
    _workerMetadataList = workerMetadataList;
    _serverInstanceToWorkerIdMap = serverInstanceToWorkerIdMap;
    _workerIdToSegmentsMap = new HashMap<>();
    _customProperties = customPropertyMap;
  }

  public PlanFragment getPlanFragment() {
    return _planFragment;
  }

  public List<WorkerMetadata> getWorkerMetadataList() {
    return _workerMetadataList;
  }

  public Map<QueryServerInstance, List<Integer>> getServerInstanceToWorkerIdMap() {
    return _serverInstanceToWorkerIdMap;
  }

  public Map<String, String> getCustomProperties() {
    return _customProperties;
  }

  public String getTableName() {
    return _customProperties.get(TABLE_NAME_KEY);
  }

  public String setTableName(String tableName) {
    return _customProperties.put(TABLE_NAME_KEY, tableName);
  }

  public TimeBoundaryInfo getTimeBoundary() {
    return new TimeBoundaryInfo(_customProperties.get(TIME_BOUNDARY_COLUMN_KEY),
        _customProperties.get(TIME_BOUNDARY_VALUE_KEY));
  }

  public void setTimeBoundaryInfo(TimeBoundaryInfo timeBoundaryInfo) {
    _customProperties.put(TIME_BOUNDARY_COLUMN_KEY, timeBoundaryInfo.getTimeColumn());
    _customProperties.put(TIME_BOUNDARY_VALUE_KEY, timeBoundaryInfo.getTimeValue());
  }

  public Map<Integer, Map<String, List<String>>> getWorkerIdToSegmentsMap() {
    return _workerIdToSegmentsMap;
  }

  public void setWorkerIdToSegmentsMap(Map<Integer, Map<String, List<String>>> workerIdToSegmentsMap) {
    _workerIdToSegmentsMap.clear();
    _workerIdToSegmentsMap.putAll(workerIdToSegmentsMap);
  }

  public void setWorkerMetadataList(List<WorkerMetadata> workerMetadataList) {
    _workerMetadataList.clear();
    _workerMetadataList.addAll(workerMetadataList);
  }

  public void setServerInstanceToWorkerIdMap(Map<QueryServerInstance, List<Integer>> serverInstanceToWorkerIdMap) {
    _serverInstanceToWorkerIdMap.clear();
    _serverInstanceToWorkerIdMap.putAll(serverInstanceToWorkerIdMap);
  }
}
