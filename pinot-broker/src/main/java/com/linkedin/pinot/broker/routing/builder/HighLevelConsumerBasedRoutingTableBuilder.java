/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.broker.routing.builder;

import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.HLCSegmentName;
import com.linkedin.pinot.common.utils.SegmentName;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;


public class HighLevelConsumerBasedRoutingTableBuilder extends BaseRoutingTableBuilder {

  @Override
  protected Map<String, List<String>> computeSegmentToServersMapFromExternalView(ExternalView externalView,
      List<InstanceConfig> instanceConfigs) {
    Map<String, List<String>> segmentToServersMap = new HashMap<>();
    RoutingTableInstancePruner instancePruner = new RoutingTableInstancePruner(instanceConfigs);
    for (String segmentName : externalView.getPartitionSet()) {
      List<String> servers = new ArrayList<>();
      for (Map.Entry<String, String> entry : externalView.getStateMap(segmentName).entrySet()) {
        String serverName = entry.getKey();
        if (entry.getValue().equals(CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE)
            && !instancePruner.isInactive(serverName) && SegmentName.isHighLevelConsumerSegmentName(segmentName)) {
          servers.add(serverName);
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
  protected List<Map<String, List<String>>> computeRoutingTablesFromSegmentToServersMap(
      Map<String, List<String>> segmentsToServerMap) {
    List<Map<String, List<String>>> routingTables = new ArrayList<>();
    Map<String, Map<String, List<String>>> groupIdToRouting = new HashMap<>();
    for (Map.Entry<String, List<String>> entry : segmentsToServerMap.entrySet()) {
      String segmentName = entry.getKey();
      HLCSegmentName hlcSegmentName = new HLCSegmentName(segmentName);
      String groupId = hlcSegmentName.getGroupId();
      Map<String, List<String>> routingTableForGroupId =
          groupIdToRouting.computeIfAbsent(groupId, k -> new HashMap<>());

      List<String> servers = entry.getValue();
      for (String serverName : servers) {
        List<String> segmentsForServer = routingTableForGroupId.computeIfAbsent(serverName, k -> new ArrayList<>());
        segmentsForServer.add(segmentName);
      }
    }
    routingTables.addAll(groupIdToRouting.values());
    return routingTables;
  }

  @Override
  public Map<String, List<String>> computeDynamicRoutingTable(Map<String, List<String>> segmentToServersMap,
      Set<String> segmentsToQuery) {
    throw new UnsupportedOperationException(
        "Dynamic routing table computation for high level consumer base routing is not supported");
  }
}
