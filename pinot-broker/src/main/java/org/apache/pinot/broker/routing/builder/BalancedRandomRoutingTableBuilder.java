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
import org.apache.commons.configuration.Configuration;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.core.transport.ServerInstance;


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
    int numReplicas = tableConfig.getValidationConfig().getReplicationNumber();
    _numRoutingTables = numReplicas == 1 ? 1 : configuration.getInt(NUM_ROUTING_TABLES_KEY, DEFAULT_NUM_ROUTING_TABLES);
  }

  @Override
  protected List<Map<ServerInstance, List<String>>> computeRoutingTablesFromSegmentToServersMap(
      Map<String, List<ServerInstance>> segmentToServersMap) {
    List<Map<ServerInstance, List<String>>> routingTables = new ArrayList<>(_numRoutingTables);
    for (int i = 0; i < _numRoutingTables; i++) {
      Map<ServerInstance, List<String>> routingTable = new HashMap<>();
      for (Map.Entry<String, List<ServerInstance>> entry : segmentToServersMap.entrySet()) {
        assignSegmentToLeastAssignedServer(entry.getKey(), entry.getValue(), routingTable);
      }
      routingTables.add(routingTable);
    }
    return routingTables;
  }
}
