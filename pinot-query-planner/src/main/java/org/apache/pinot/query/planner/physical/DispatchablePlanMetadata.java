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
package org.apache.pinot.query.planner.physical;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.query.routing.MailboxMetadata;
import org.apache.pinot.query.routing.QueryServerInstance;


/**
 * The {@code DispatchablePlanMetadata} info contains the information for dispatching a particular plan fragment.
 *
 * <p>It contains information aboute:
 * <ul>
 *   <li>the tables it is suppose to scan for</li>
 *   <li>the underlying segments a stage requires to execute upon.</li>
 *   <li>the server instances to which this stage should be execute on</li>
 * </ul>
 */
public class DispatchablePlanMetadata implements Serializable {
  private List<String> _scannedTables;

  // used for assigning server/worker nodes.
  private Map<QueryServerInstance, List<Integer>> _serverInstanceToWorkerIdMap;

  // used for table scan stage - we use ServerInstance instead of VirtualServer
  // here because all virtual servers that share a server instance will have the
  // same segments on them
  private Map<Integer, Map<String, List<String>>> _workerIdToSegmentsMap;

  // used for build mailboxes between workers.
  // workerId -> {planFragmentId -> mailbox list}
  private Map<Integer, Map<Integer, MailboxMetadata>> _workerIdToMailboxesMap;

  // time boundary info
  private TimeBoundaryInfo _timeBoundaryInfo;

  // whether a stage requires singleton instance to execute, e.g. stage contains global reduce (sort/agg) operator.
  private boolean _requiresSingletonInstance;

  // Total worker count of this stage.
  private int _totalWorkerCount;

  public DispatchablePlanMetadata() {
    _scannedTables = new ArrayList<>();
    _serverInstanceToWorkerIdMap = new HashMap<>();
    _workerIdToSegmentsMap = new HashMap<>();
    _workerIdToMailboxesMap = new HashMap<>();
    _timeBoundaryInfo = null;
    _requiresSingletonInstance = false;
  }

  public List<String> getScannedTables() {
    return _scannedTables;
  }

  public void addScannedTable(String tableName) {
    _scannedTables.add(tableName);
  }

  // -----------------------------------------------
  // attached physical plan context.
  // -----------------------------------------------

  public Map<Integer, Map<String, List<String>>> getWorkerIdToSegmentsMap() {
    return _workerIdToSegmentsMap;
  }

  public void setWorkerIdToSegmentsMap(
      Map<Integer, Map<String, List<String>>> workerIdToSegmentsMap) {
    _workerIdToSegmentsMap = workerIdToSegmentsMap;
  }

  public Map<Integer, Map<Integer, MailboxMetadata>> getWorkerIdToMailBoxIdsMap() {
    return _workerIdToMailboxesMap;
  }

  public void setWorkerIdToMailBoxIdsMap(Map<Integer, Map<Integer, MailboxMetadata>> workerIdToMailboxesMap) {
    _workerIdToMailboxesMap.putAll(workerIdToMailboxesMap);
  }

  public void addWorkerIdToMailBoxIdsMap(int planFragmentId,
      Map<Integer, MailboxMetadata> planFragmentIdToMailboxesMap) {
    _workerIdToMailboxesMap.put(planFragmentId, planFragmentIdToMailboxesMap);
  }

  public Map<QueryServerInstance, List<Integer>> getServerInstanceToWorkerIdMap() {
    return _serverInstanceToWorkerIdMap;
  }

  public void setServerInstanceToWorkerIdMap(Map<QueryServerInstance, List<Integer>> serverInstances) {
    _serverInstanceToWorkerIdMap = serverInstances;
  }

  public TimeBoundaryInfo getTimeBoundaryInfo() {
    return _timeBoundaryInfo;
  }

  public void setTimeBoundaryInfo(TimeBoundaryInfo timeBoundaryInfo) {
    _timeBoundaryInfo = timeBoundaryInfo;
  }

  public boolean isRequiresSingletonInstance() {
    return _requiresSingletonInstance;
  }

  public void setRequireSingleton(boolean newRequireInstance) {
    _requiresSingletonInstance = _requiresSingletonInstance || newRequireInstance;
  }

  public int getTotalWorkerCount() {
    return _totalWorkerCount;
  }

  public void setTotalWorkerCount(int totalWorkerCount) {
    _totalWorkerCount = totalWorkerCount;
  }

  @Override
  public String toString() {
    return "DispatchablePlanMetadata{" + "_scannedTables=" + _scannedTables + ", _serverInstanceToWorkerIdMap="
        + _serverInstanceToWorkerIdMap + ", _workerIdToSegmentsMap=" + _workerIdToSegmentsMap
        + ", _workerIdToMailboxesMap=" + _workerIdToMailboxesMap
        + ", _timeBoundaryInfo=" + _timeBoundaryInfo + '}';
  }
}
