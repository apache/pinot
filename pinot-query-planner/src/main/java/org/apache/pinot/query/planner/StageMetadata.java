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
import org.apache.pinot.query.planner.stage.StageNode;
import org.apache.pinot.query.planner.stage.TableScanNode;


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
public class StageMetadata implements Serializable {
  private List<String> _scannedTables;

  // used for assigning server/worker nodes.
  private List<ServerInstance> _serverInstances;

  // used for table scan stage.
  private Map<ServerInstance, Map<String, List<String>>> _serverInstanceToSegmentsMap;

  // time boundary info
  private TimeBoundaryInfo _timeBoundaryInfo;


  public StageMetadata() {
    _scannedTables = new ArrayList<>();
    _serverInstances = new ArrayList<>();
    _serverInstanceToSegmentsMap = new HashMap<>();
    _timeBoundaryInfo = null;
  }

  public void attach(StageNode stageNode) {
    if (stageNode instanceof TableScanNode) {
      _scannedTables.add(((TableScanNode) stageNode).getTableName());
    }
  }

  public List<String> getScannedTables() {
    return _scannedTables;
  }

  // -----------------------------------------------
  // attached physical plan context.
  // -----------------------------------------------

  public Map<ServerInstance, Map<String, List<String>>> getServerInstanceToSegmentsMap() {
    return _serverInstanceToSegmentsMap;
  }

  public void setServerInstanceToSegmentsMap(
      Map<ServerInstance, Map<String, List<String>>> serverInstanceToSegmentsMap) {
    _serverInstanceToSegmentsMap = serverInstanceToSegmentsMap;
  }

  public List<ServerInstance> getServerInstances() {
    return _serverInstances;
  }

  public void setServerInstances(List<ServerInstance> serverInstances) {
    _serverInstances = serverInstances;
  }

  public TimeBoundaryInfo getTimeBoundaryInfo() {
    return _timeBoundaryInfo;
  }

  public void setTimeBoundaryInfo(TimeBoundaryInfo timeBoundaryInfo) {
    _timeBoundaryInfo = timeBoundaryInfo;
  }
}
