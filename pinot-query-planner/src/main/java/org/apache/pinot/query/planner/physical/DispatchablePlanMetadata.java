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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
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
  // These 2 fields are extracted from TableScanNode
  private final List<String> _scannedTables;
  private Map<String, String> _tableOptions;

  // used for assigning server/worker nodes.
  private Map<Integer, QueryServerInstance> _workerIdToServerInstanceMap;

  // used for table scan stage - we use ServerInstance instead of VirtualServer
  // here because all virtual servers that share a server instance will have the
  // same segments on them
  private Map<Integer, Map<String, List<String>>> _workerIdToSegmentsMap;

  // used for build mailboxes between workers.
  // workerId -> {planFragmentId -> mailbox list}
  private final Map<Integer, Map<Integer, MailboxMetadata>> _workerIdToMailboxesMap;

  // used for tracking unavailable segments from routing table, then assemble missing segments exception.
  private final Map<String, Set<String>> _tableToUnavailableSegmentsMap;

  // time boundary info
  private TimeBoundaryInfo _timeBoundaryInfo;

  // whether a stage requires singleton instance to execute, e.g. stage contains global reduce (sort/agg) operator.
  private boolean _requiresSingletonInstance;

  // whether a stage is partitioned by the same way the sending exchange is desired
  private boolean _isPartitioned;
  private int _partitionParallelism;

  public DispatchablePlanMetadata() {
    _scannedTables = new ArrayList<>();
    _workerIdToMailboxesMap = new HashMap<>();
    _tableToUnavailableSegmentsMap = new HashMap<>();
  }

  public List<String> getScannedTables() {
    return _scannedTables;
  }

  public void addScannedTable(String tableName) {
    _scannedTables.add(tableName);
  }

  @Nullable
  public Map<String, String> getTableOptions() {
    return _tableOptions;
  }

  public void setTableOptions(Map<String, String> tableOptions) {
    _tableOptions = tableOptions;
  }

  // -----------------------------------------------
  // attached physical plan context.
  // -----------------------------------------------

  public Map<Integer, QueryServerInstance> getWorkerIdToServerInstanceMap() {
    return _workerIdToServerInstanceMap;
  }

  public void setWorkerIdToServerInstanceMap(Map<Integer, QueryServerInstance> workerIdToServerInstanceMap) {
    _workerIdToServerInstanceMap = workerIdToServerInstanceMap;
  }

  @Nullable
  public Map<Integer, Map<String, List<String>>> getWorkerIdToSegmentsMap() {
    return _workerIdToSegmentsMap;
  }

  public void setWorkerIdToSegmentsMap(Map<Integer, Map<String, List<String>>> workerIdToSegmentsMap) {
    _workerIdToSegmentsMap = workerIdToSegmentsMap;
  }

  public Map<Integer, Map<Integer, MailboxMetadata>> getWorkerIdToMailboxesMap() {
    return _workerIdToMailboxesMap;
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

  public boolean isPartitioned() {
    return _isPartitioned;
  }

  public void setPartitioned(boolean isPartitioned) {
    _isPartitioned = isPartitioned;
  }

  public int getPartitionParallelism() {
    return _partitionParallelism;
  }

  public void setPartitionParallelism(int partitionParallelism) {
    _partitionParallelism = partitionParallelism;
  }

  public Map<String, Set<String>> getTableToUnavailableSegmentsMap() {
    return _tableToUnavailableSegmentsMap;
  }

  public void addUnavailableSegments(String tableName, Collection<String> unavailableSegments) {
    _tableToUnavailableSegmentsMap.computeIfAbsent(tableName, k -> new HashSet<>()).addAll(unavailableSegments);
  }
}
