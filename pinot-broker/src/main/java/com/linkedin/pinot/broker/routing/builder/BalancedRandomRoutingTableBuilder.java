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

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;


/**
 * Assign balanced number of segments to each server.
 */
public class BalancedRandomRoutingTableBuilder extends BaseRoutingTableBuilder {
  private static final int DEFAULT_NUM_ROUTING_TABLES = 10;
  private static final String NUM_ROUTING_TABLES_KEY = "numOfRoutingTables";

  private int _numRoutingTables = DEFAULT_NUM_ROUTING_TABLES;

  @Override
  public void init(Configuration configuration, TableConfig tableConfig, ZkHelixPropertyStore<ZNRecord> propertyStore,
      BrokerMetrics brokerMetrics) {
    super.init(configuration, tableConfig, propertyStore, brokerMetrics);
    _numRoutingTables = configuration.getInt(NUM_ROUTING_TABLES_KEY, DEFAULT_NUM_ROUTING_TABLES);
  }

  protected List<Map<String, List<String>>> computeRoutingTablesFromSegmentToServersMap(
      Map<String, List<String>> segmentToServersMap) {
    List<Map<String, List<String>>> routingTables = new ArrayList<>(_numRoutingTables);
    Set<String> segmentsToQuery = segmentToServersMap.keySet();

    for (int i = 0; i < _numRoutingTables; i++) {
      Map<String, List<String>> routingTable = new HashMap<>();
      for (String segmentName : segmentsToQuery) {
        List<String> servers = segmentToServersMap.get(segmentName);
        routingTable.get(getServerWithLeastSegmentsAssigned(servers, routingTable)).add(segmentName);
      }
      routingTables.add(routingTable);
    }
    return routingTables;
  }
}
