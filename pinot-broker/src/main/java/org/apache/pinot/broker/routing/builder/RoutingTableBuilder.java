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

import java.util.List;
import java.util.Map;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.broker.routing.RoutingTableLookupRequest;
import org.apache.pinot.broker.routing.selector.SegmentSelector;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.metrics.BrokerMetrics;


/**
 * Build routing tables based on ExternalView from Helix.
 */
public interface RoutingTableBuilder {

  /**
   * Initiate the routing table builder.
   */
  void init(Configuration configuration, TableConfig tableConfig, ZkHelixPropertyStore<ZNRecord> propertyStore,
      BrokerMetrics brokerMetrics);

  /**
   * Compute routing tables (map from server to list of segments) that are used for query routing from ExternalView.
   * <p>Should be called whenever there is an ExternalView change.
   */
  void computeOnExternalViewChange(String tableName, ExternalView externalView, List<InstanceConfig> instanceConfigs);

  /**
   * Get the routing table based on the given lookup request and segment selector
   *
   * TODO: we need to consider relocating segment selector into the routing table builder instead of passing it
   * from outside.
   */
  Map<String, List<String>> getRoutingTable(RoutingTableLookupRequest request, SegmentSelector segmentSelector);

  /**
   * Get all pre-computed routing tables.
   */
  List<Map<String, List<String>>> getRoutingTables();
}
