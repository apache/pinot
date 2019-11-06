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
package org.apache.pinot.broker.routing.builder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.response.ServerInstance;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.HLCSegmentName;
import org.apache.pinot.common.utils.SegmentName;


public class HighLevelConsumerBasedRoutingTableBuilder extends BaseRoutingTableBuilder {

  @Override
  protected Map<String, List<ServerInstance>> computeSegmentToServersMapFromExternalView(ExternalView externalView,
      List<InstanceConfig> instanceConfigs) {
    Map<String, List<ServerInstance>> segmentToServersMap = new HashMap<>();
    RoutingTableInstancePruner instancePruner = new RoutingTableInstancePruner(instanceConfigs);
    for (String segmentName : externalView.getPartitionSet()) {
      List<ServerInstance> servers = new ArrayList<>();
      for (Map.Entry<String, String> entry : externalView.getStateMap(segmentName).entrySet()) {
        String serverName = entry.getKey();
        if (entry.getValue().equals(CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE)
            && !instancePruner.isInactive(serverName) && SegmentName.isHighLevelConsumerSegmentName(segmentName)) {
          servers.add(ServerInstance.forInstanceName(serverName));
        }
      }
      if (servers.size() != 0) {
        segmentToServersMap.put(segmentName, servers);
      } else {
        handleNoServingHost(segmentName);
      }
    }
    return segmentToServersMap;
  }

  @Override
  protected List<Map<ServerInstance, List<String>>> computeRoutingTablesFromSegmentToServersMap(
      Map<String, List<ServerInstance>> segmentsToServerMap) {
    List<Map<ServerInstance, List<String>>> routingTables = new ArrayList<>();
    Map<String, Map<ServerInstance, List<String>>> groupIdToRouting = new HashMap<>();
    for (Map.Entry<String, List<ServerInstance>> entry : segmentsToServerMap.entrySet()) {
      String segmentName = entry.getKey();
      HLCSegmentName hlcSegmentName = new HLCSegmentName(segmentName);
      String groupId = hlcSegmentName.getGroupId();
      Map<ServerInstance, List<String>> routingTableForGroupId =
          groupIdToRouting.computeIfAbsent(groupId, k -> new HashMap<>());

      List<ServerInstance> servers = entry.getValue();
      for (ServerInstance serverInstance : servers) {
        List<String> segmentsForServer = routingTableForGroupId.computeIfAbsent(serverInstance, k -> new ArrayList<>());
        segmentsForServer.add(segmentName);
      }
    }
    routingTables.addAll(groupIdToRouting.values());
    return routingTables;
  }

  @Override
  public Map<ServerInstance, List<String>> computeDynamicRoutingTable(Map<String, List<ServerInstance>> segmentToServersMap,
      Set<String> segmentsToQuery) {
    throw new UnsupportedOperationException(
        "Dynamic routing table computation for high level consumer base routing is not supported");
  }
}
