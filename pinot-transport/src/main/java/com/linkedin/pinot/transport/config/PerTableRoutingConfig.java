/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.transport.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.configuration.Configuration;


/**
 * Maintains static routing config of servers to partitions
 *
 * Relevant config for illustration:
 *
 * pinot.broker.routing.midas.numPartitions=2
 * pinot.broker.routing.midas.serversForPartitions.default=localhost:9099
 * pinot.broker.routing.midas.serversForPartitions.0=localhost:9099
 * pinot.broker.routing.midas.serversForPartitions.1=localhost:9099
 *
 *
 */
public class PerTableRoutingConfig {
  // Keys to load config
  private static final String NUM_NODES_PER_REPLICA = "numNodesPerReplica";
  private static final String SERVERS_FOR_NODE = "serversForNode";
  private static final String DEFAULT_SERVERS_FOR_NODE = "default";

  private final Configuration _tableCfg;
  private int _numNodes;
  private List<String> _defaultServers;
  private final Map<Integer, List<String>> _nodeToInstancesMap;

  public PerTableRoutingConfig(Configuration cfg) {
    _tableCfg = cfg;
    _nodeToInstancesMap = new HashMap<>();
    _defaultServers = new ArrayList<>();
    loadConfig();
  }

  @SuppressWarnings("unchecked")
  private void loadConfig() {
    if (null == _tableCfg) {
      return;
    }

    _nodeToInstancesMap.clear();

    _numNodes = _tableCfg.getInt(NUM_NODES_PER_REPLICA);
    for (int i = 0; i < _numNodes; i++) {
      _nodeToInstancesMap.put(i, _tableCfg.getList(getKey(SERVERS_FOR_NODE, Integer.toString(i))));
    }

    _defaultServers = _tableCfg.getList(getKey(SERVERS_FOR_NODE, DEFAULT_SERVERS_FOR_NODE));
  }

  private String getKey(String prefix, String suffix) {
    String s = prefix + "." + suffix;
    return s;
  }

  public int getNumNodes() {
    return _numNodes;
  }

  public List<String> getDefaultServers() {
    return _defaultServers;
  }

  //
  //  /**
  //   * Builds a map needed for routing the partitions in the partition-group passed.
  //   * There could be different set of servers for each partition in the passed partition-group.
  //   *
  //   * @param pg segmentSet for which the routing map needs to be built.
  //   * @return
  //   */
  //  public Map<SegmentIdSet, List<ServerInstance>> buildRequestRoutingMap() {
  //    Map<SegmentIdSet, List<ServerInstance>> resultMap = new HashMap<SegmentIdSet, List<ServerInstance>>();
  //
  //    /**
  //     * NOTE: After we removed the concept of partition, this needed rewriting.
  //     * For now, The File-based routing config maps nodeIds to Instances instead of segments to instances.
  //     * This is because, it becomes difficult for configuring all segments in routing config. Instead,
  //     * we configure the number of nodes that constitute a replica-set. For each node, different instances
  //     * (as comma-seperated list) is provided. we pick one instance from each node.
  //     *
  //     */
  //    for (Entry<Integer, List<ServerInstance>> e : _nodeToInstancesMap.entrySet()) {
  //      SegmentId id = new SegmentId("" + e.getKey());
  //      SegmentIdSet idSet = new SegmentIdSet();
  //      idSet.addSegment(id);
  //      resultMap.put(idSet, e.getValue());
  //    }
  //
  //    // Add default
  //    SegmentId id = new SegmentId("default");
  //    SegmentIdSet idSet = new SegmentIdSet();
  //    idSet.addSegment(id);
  //    resultMap.put(idSet, _defaultServers);
  //    return resultMap;
  //  }

  /**
   * Builds a map needed for routing the partitions in the partition-group passed.
   * There could be different set of servers for each partition in the passed partition-group.
   *
   * @return
   */
  public Map<String, List<String>> buildRequestRoutingMap() {
    Map<String, List<String>> resultMap = new HashMap<>();
    for (String serverName : _defaultServers) {
      resultMap.put(serverName, Collections.singletonList("default"));
    }
    return resultMap;
  }

  public Map<Integer, List<String>> getNodeToInstancesMap() {
    return _nodeToInstancesMap;
  }

  @Override
  public String toString() {
    return "PerTableRoutingConfig [_tableCfg=" + _tableCfg + ", _numNodes=" + _numNodes + ", _defaultServers="
        + _defaultServers + ", _nodeToInstancesMap=" + _nodeToInstancesMap + "]";
  }
}
