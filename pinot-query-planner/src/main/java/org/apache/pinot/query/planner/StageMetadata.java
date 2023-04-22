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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.routing.VirtualServer;
import org.apache.pinot.query.routing.WorkerMetadata;


/**
 * The {@code StageMetadata} info contains the information for dispatching a particular stage.
 *
 * <p>It contains information aboute:
 * <ul>
 *   <li>the tables it is suppose to scan for</li>
 *   <li>the underlying segments a stage requires to execute upon.</li>
 *   <li>the server instances to which this stage should be execute on</li>
 * </ul>
 */
public class StageMetadata {
  private List<String> _scannedTables;

  // used for assigning server/worker nodes.
  private Map<ServerInstance, List<Integer>> _serverToWorkerIdsMap;

  private List<WorkerMetadata> _workerMetadataList;

  // time boundary info
  private TimeBoundaryInfo _timeBoundaryInfo;

  // whether a stage requires singleton instance to execute, e.g. stage contains global reduce (sort/agg) operator.
  private boolean _requiresSingletonInstance;

  public StageMetadata() {
    _scannedTables = new ArrayList<>();
    _serverToWorkerIdsMap = new HashMap<>();
    _workerMetadataList = new ArrayList<>();
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

  public Map<ServerInstance, List<Integer>> getServerInstanceToSegmentsMap() {
    return _serverToWorkerIdsMap;
  }

  public TimeBoundaryInfo getTimeBoundaryInfo() {
    return _timeBoundaryInfo;
  }

  public boolean isRequiresSingletonInstance() {
    return _requiresSingletonInstance;
  }

  @Override
  public String toString() {
    return "StageMetadata{" + "_scannedTables=" + _scannedTables + ", _serverInstances=" + _serverToWorkerIdsMap
        + ", _workerMetadata=" + _workerMetadataList + ", _timeBoundaryInfo=" + _timeBoundaryInfo + '}';
  }
}
