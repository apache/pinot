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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.routing.VirtualServer;


/**
 * The {@code StageMetadata} info contains the information for dispatching a particular stage.
 *
 * <p>It contains information about:</p>
 * <ul>
 *   <li>the tables it is suppose to scan (including real-time and offline portions separately)</li>
 *   <li>the underlying segments a stage requires to execute upon.</li>
 *   <li>the server instances to which this stage should be execute on</li>
 *   <li>the set of query partition IDs each server instance is responsible for</li>
 * </ul>
 *
 * <p>The stage information also contains the receiving stage metadata in order to construct proper bi-directional
 * metadata to convert {@link org.apache.pinot.query.planner.stage.MailboxSendNode} and
 * {@link org.apache.pinot.query.planner.stage.MailboxReceiveNode} into proper operators. Here is how:</p>
 *
 * <p>The {@code List<VirtualServer>} represents the server instances for the current stage, as well as the associated
 *   set of partitionIDs for each server instances</p>
 * <p>The {@code Map<Integer, Map<Integer, List<ServerInstance>>>} represents the receiving stage info</p>
 * <ul>
 *   <li>Top level map is keyed by the sending side partitionID.</li>
 *   <li>Top level map value is another map from receiving side partitionID to receiving side server instance.</li>
 * </ul>
 * <p>When mailbox operators are constructed on each server instances. it will
 * <ul>
 *   <li>create 1 operator chain per partitionID responsible by each server instances</li>
 *   <li>for each operator chain that is associated with one particular partitionID, it will used the receiving stage
 *     info map to lookup the destination partitionID, as well as the list of server instances.</li>
 *   <li>for each pair of receiving stage partitionID-serverInstance pair, it will create a direct mailbox</li>
 * </ul>
 * <p>To summarize the mailboxes created are all uniquely identifiable by:
 *   [sending_server:sending_partition:receiving_server:receiving_partition]</p>
 */
public class StageMetadata implements Serializable {
  private List<String> _scannedTables;

  /**
   * This is used for server/worker assignment. {@link VirtualServer} contains information of:
   *   - {@link ServerInstance} and
   *   - {@link List<Integer>} as partition IDs associated with this particular serverInstances. partitionIDs are
   *       globally-indexed.
   */
  private List<VirtualServer> _serverInstances;

  /**
   * Similar to {@link List<VirtualServer>}, this used for server/worker assignment specific to table scan stage.
   * - For each partition, we will issue a ServerExecutorRequest and generate a result for
   *     that specific partitionID.
   */
  private Map<ServerInstance, Map<String, Map<Integer, List<String>>>> _serverAndPartitionToSegmentMap;

  /**
   * This is ued for indicating what are the expected receiving servers for the outbound mailbox.
   *   - top-level map is added to support Spool:
   *     - key: receiving stage ID, normally there’s only 1 receiving stage.
   *     - value: inner map indicate partition-ID-to-server mapping.
   *       - key: receiving-side partitionID (this is different from the sending side partitionID.
   *       - value: list of receiving server instances
   * Note for inner map:
   *   - for partitioned exchanges, the inner map contains multiple entries with a singleton list.
   *   - for non-partitioned exchanges, the inner map contains 1 entry with a list of all receiving server instances.
   */
  private Map<Integer, List<VirtualServer>> _receivingServerInstanceMap;

  // time boundary info
  private TimeBoundaryInfo _timeBoundaryInfo;

  // --------------------------------------------------------------------------
  // Transient objects that are only used during DispatchablePlanVisitor
  // --------------------------------------------------------------------------

  // whether a stage requires singleton instance to execute, e.g. stage contains global reduce (sort/agg) operator.
  private transient boolean _requiresSingletonInstance;

  // what are the inbound stage towards the current stage.
  private transient Map<Integer, RelDistribution.Type> _inboundStageMap;

  public StageMetadata() {
    _scannedTables = new ArrayList<>();
    _serverInstances = new ArrayList<>();
    _serverAndPartitionToSegmentMap = new HashMap<>();
    _timeBoundaryInfo = null;
    _requiresSingletonInstance = false;
    _inboundStageMap = new HashMap<>();
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

  public Map<ServerInstance, Map<String, Map<Integer, List<String>>>> getServerAndPartitionToSegmentMap() {
    return _serverAndPartitionToSegmentMap;
  }

  public void setServerAndPartitionToSegmentMap(
      Map<ServerInstance, Map<String, Map<Integer, List<String>>>> serverAndPartitionToSegmentMap) {
    _serverAndPartitionToSegmentMap = serverAndPartitionToSegmentMap;
  }

  public Map<Integer, Map<Integer, List<ServerInstance>>> getReceivingServerInstanceMap() {
    return _receivingServerInstanceMap;
  }

  public void setReceivingServerInstanceMap(
      Map<Integer, Map<Integer, List<ServerInstance>>> receivingServerInstanceMap) {
    _receivingServerInstanceMap = receivingServerInstanceMap;
  }

  public List<VirtualServer> getServerInstances() {
    return _serverInstances;
  }

  public void setServerInstances(List<VirtualServer> serverInstances) {
    _serverInstances = serverInstances;
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

  public Set<Integer> getInboundStageMap() {
    return _inboundStageMap;
  }

  public void addInboundStage(int inboundStageId) {
    _inboundStageMap.add(inboundStageId);
  }

  @Override
  public String toString() {
    return "StageMetadata{" + "_scannedTables=" + _scannedTables + ", _serverInstances=" + _serverInstances
        + ", _serverInstanceToSegmentsMap=" + _serverAndPartitionToSegmentMap + ", _receivingServerInstanceMap="
        + _receivingServerInstanceMap + ", _timeBoundaryInfo=" + _timeBoundaryInfo + '}';
  }
}
