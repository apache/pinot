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
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.HLCSegmentName;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.common.utils.SegmentName;
import org.apache.pinot.core.transport.ServerInstance;


public class HighLevelConsumerBasedRoutingTableBuilder extends BaseRoutingTableBuilder {

  @Override
  protected Map<String, List<ServerInstance>> computeSegmentToServersMapFromExternalView(ExternalView externalView,
      List<InstanceConfig> instanceConfigs) {
    Map<String, Map<String, String>> segmentAssignment = externalView.getRecord().getMapFields();
    Map<String, List<ServerInstance>> segmentToServersMap =
        new HashMap<>(HashUtil.getHashMapCapacity(segmentAssignment.size()));
    InstanceConfigManager instanceConfigManager = new InstanceConfigManager(instanceConfigs);
    for (Map.Entry<String, Map<String, String>> entry : segmentAssignment.entrySet()) {
      String segmentName = entry.getKey();
      if (!SegmentName.isHighLevelConsumerSegmentName(segmentName)) {
        continue;
      }
      Map<String, String> instanceStateMap = entry.getValue();
      List<ServerInstance> servers = new ArrayList<>(instanceStateMap.size());
      for (Map.Entry<String, String> instanceStateEntry : instanceStateMap.entrySet()) {
        if (instanceStateEntry.getValue()
            .equals(CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE)) {
          InstanceConfig instanceConfig = instanceConfigManager.getActiveInstanceConfig(instanceStateEntry.getKey());
          if (instanceConfig != null) {
            servers.add(new ServerInstance(instanceConfig));
          }
        }
      }
      if (!servers.isEmpty()) {
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
    Map<String, Map<ServerInstance, List<String>>> groupIdToRouting = new HashMap<>();
    for (Map.Entry<String, List<ServerInstance>> entry : segmentsToServerMap.entrySet()) {
      String segmentName = entry.getKey();
      HLCSegmentName hlcSegmentName = new HLCSegmentName(segmentName);
      String groupId = hlcSegmentName.getGroupId();
      Map<ServerInstance, List<String>> routingTableForGroupId =
          groupIdToRouting.computeIfAbsent(groupId, k -> new HashMap<>());

      List<ServerInstance> serverInstances = entry.getValue();
      for (ServerInstance serverInstance : serverInstances) {
        List<String> segmentsForServer = routingTableForGroupId.computeIfAbsent(serverInstance, k -> new ArrayList<>());
        segmentsForServer.add(segmentName);
      }
    }
    return new ArrayList<>(groupIdToRouting.values());
  }

  @Override
  public Map<ServerInstance, List<String>> computeDynamicRoutingTable(
      Map<String, List<ServerInstance>> segmentToServersMap, Set<String> segmentsToQuery) {
    throw new UnsupportedOperationException(
        "Dynamic routing table computation for high level consumer base routing is not supported");
  }
}
