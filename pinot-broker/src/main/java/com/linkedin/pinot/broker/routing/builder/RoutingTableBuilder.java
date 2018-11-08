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

import com.linkedin.pinot.broker.routing.RoutingTableLookupRequest;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import java.util.List;
import java.util.Map;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;


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
   * Get the routing table based on the given lookup request.
   */
  Map<String, List<String>> getRoutingTable(RoutingTableLookupRequest request);

  /**
   * Get all pre-computed routing tables.
   */
  List<Map<String, List<String>>> getRoutingTables();
}
