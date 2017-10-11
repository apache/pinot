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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;


/**
 * Assign balanced number of segments to each server.
 */
public class BalancedRandomRoutingTableBuilder extends BaseRoutingTableBuilder {
  private static final int DEFAULT_NUM_ROUTING_TABLES = 10;
  private static final String NUM_ROUTING_TABLES_KEY = "numOfRoutingTables";

  private int _numRoutingTables = DEFAULT_NUM_ROUTING_TABLES;

  @Override
  public void init(Configuration configuration, TableConfig tableConfig, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _numRoutingTables = configuration.getInt(NUM_ROUTING_TABLES_KEY, DEFAULT_NUM_ROUTING_TABLES);
  }

  @Override
  public void computeRoutingTableFromExternalView(String tableName, ExternalView externalView,
      List<InstanceConfig> instanceConfigs) {
    List<Map<String, List<String>>> routingTables = new ArrayList<>(_numRoutingTables);
    for (int i = 0; i < _numRoutingTables; i++) {
      routingTables.add(new HashMap<String, List<String>>());
    }

    RoutingTableInstancePruner instancePruner = new RoutingTableInstancePruner(instanceConfigs);
    for (String segmentName : externalView.getPartitionSet()) {
      // List of servers that are active and are serving the segment
      List<String> servers = new ArrayList<>();
      for (Map.Entry<String, String> entry : externalView.getStateMap(segmentName).entrySet()) {
        String serverName = entry.getKey();
        if (entry.getValue().equals(CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE)
            && !instancePruner.isInactive(serverName)) {
          servers.add(serverName);
        }
      }
      int numServers = servers.size();
      if (numServers != 0) {
        for (Map<String, List<String>> routingTable : routingTables) {
          // Assign the segment to the server with least segments assigned
          routingTable.get(getServerWithLeastSegmentsAssigned(servers, routingTable)).add(segmentName);
        }
      }
    }

    setRoutingTables(routingTables);
  }
}
