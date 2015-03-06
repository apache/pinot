/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.Configuration;

import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.common.SegmentId;
import com.linkedin.pinot.transport.common.SegmentIdSet;


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
 * @author bvaradar
 *
 */
public class ResourceRoutingConfig {
  // Keys to load config
  private static final String NUM_NODES_PER_REPLICA = "numNodesPerReplica";
  private static final String SERVERS_FOR_NODE = "serversForNode";
  private static final String DEFAULT_SERVERS_FOR_NODE = "default";

  private final Configuration _resourceCfg;
  private int _numNodes;
  private List<ServerInstance> _defaultServers;
  private final Map<Integer, List<ServerInstance>> _nodeToInstancesMap;

  public ResourceRoutingConfig(Configuration cfg) {
    _resourceCfg = cfg;
    _nodeToInstancesMap = new HashMap<Integer, List<ServerInstance>>();
    _defaultServers = new ArrayList<ServerInstance>();
    loadConfig();
  }

  @SuppressWarnings("unchecked")
  private void loadConfig() {

    if (null == _resourceCfg) {
      return;
    }

    _nodeToInstancesMap.clear();

    _numNodes = _resourceCfg.getInt(NUM_NODES_PER_REPLICA);
    for (int i = 0; i < _numNodes; i++) {
      List<String> servers = _resourceCfg.getList(getKey(SERVERS_FOR_NODE, i + ""));

      if ((null != servers) && (!servers.isEmpty())) {
        List<ServerInstance> servers2 = getServerInstances(servers);
        _nodeToInstancesMap.put(i, servers2);
      }
    }

    List<String> servers = _resourceCfg.getList(getKey(SERVERS_FOR_NODE, DEFAULT_SERVERS_FOR_NODE));
    if ((null != servers) && (!servers.isEmpty())) {
      _defaultServers = getServerInstances(servers);
    }
  }

  private String getKey(String prefix, String suffix) {
    String s = prefix + "." + suffix;
    return s;
  }

  public int getNumNodes() {
    return _numNodes;
  }

  public List<ServerInstance> getDefaultServers() {
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
   * @param pg segmentSet for which the routing map needs to be built.
   * @return
   */
  public Map<ServerInstance, SegmentIdSet> buildRequestRoutingMap() {
    Map<ServerInstance, SegmentIdSet> resultMap = new HashMap<ServerInstance, SegmentIdSet>();
    for (ServerInstance serverInstance : _defaultServers) {
      SegmentId id = new SegmentId("default");
      SegmentIdSet idSet = new SegmentIdSet();
      idSet.addSegment(id);
      resultMap.put(serverInstance, idSet);
    }
    return resultMap;
  }

  /**
   * Generate server instances from their string representations
   * @param servers
   * @return
   */
  private static List<ServerInstance> getServerInstances(List<String> servers) {
    List<ServerInstance> servers2 = new ArrayList<ServerInstance>();
    for (String s : servers) {
      servers2.add(new ServerInstance(s));
    }
    return servers2;
  }

  public Map<Integer, List<ServerInstance>> getNodeToInstancesMap() {
    return _nodeToInstancesMap;
  }

  @Override
  public String toString() {
    return "ResourceRoutingConfig [_resourceCfg=" + _resourceCfg + ", _numNodes=" + _numNodes + ", _defaultServers="
        + _defaultServers + ", _nodeToInstancesMap=" + _nodeToInstancesMap + "]";
  }
}
