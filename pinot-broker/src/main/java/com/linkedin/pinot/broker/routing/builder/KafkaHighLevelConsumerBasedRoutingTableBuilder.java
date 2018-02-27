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
package com.linkedin.pinot.broker.routing.builder;

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.HLCSegmentName;
import com.linkedin.pinot.common.utils.SegmentName;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;


public class KafkaHighLevelConsumerBasedRoutingTableBuilder extends BaseRoutingTableBuilder {

  @Override
  public void init(Configuration configuration, TableConfig tableConfig, ZkHelixPropertyStore<ZNRecord> propertyStore) {
  }

  @Override
  public void computeRoutingTableFromExternalView(String tableName, ExternalView externalView,
      List<InstanceConfig> instanceConfigs) {
    List<Map<String, List<String>>> routingTables = new ArrayList<>();
    Map<String, Map<String, List<String>>> groupIdToRouting = new HashMap<>();

    RoutingTableInstancePruner instancePruner = new RoutingTableInstancePruner(instanceConfigs);
    for (String segmentName : externalView.getPartitionSet()) {
      for (Map.Entry<String, String> entry : externalView.getStateMap(segmentName).entrySet()) {
        String serverName = entry.getKey();
        if (entry.getValue().equals(CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE)
            && !instancePruner.isInactive(serverName) && SegmentName.isHighLevelConsumerSegmentName(segmentName)) {
          HLCSegmentName hlcSegmentName = new HLCSegmentName(segmentName);
          String groupId = hlcSegmentName.getGroupId();

          Map<String, List<String>> routingTableForGroupId = groupIdToRouting.get(groupId);
          if (routingTableForGroupId == null) {
            routingTableForGroupId = new HashMap<>();
            groupIdToRouting.put(groupId, routingTableForGroupId);
          }
          List<String> segmentsForServer = routingTableForGroupId.get(serverName);
          if (segmentsForServer == null) {
            segmentsForServer = new ArrayList<>();
            routingTableForGroupId.put(serverName, segmentsForServer);
          }
          segmentsForServer.add(segmentName);
        }
      }
    }
    routingTables.addAll(groupIdToRouting.values());

    setRoutingTables(routingTables);
  }
}
