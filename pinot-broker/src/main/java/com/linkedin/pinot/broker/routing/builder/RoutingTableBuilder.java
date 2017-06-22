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

import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;

import com.linkedin.pinot.broker.routing.RoutingTable;
import com.linkedin.pinot.broker.routing.RoutingTableLookupRequest;
import com.linkedin.pinot.broker.routing.ServerToSegmentSetMap;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.common.SegmentIdSet;


/**
 * Interface for creating a list of ServerToSegmentSetMap based on ExternalView from Helix.
 */
public interface RoutingTableBuilder {

  /**
   * Inits the routing table builder.
   *
   * @param configuration The configuration to use
   */
  void init(Configuration configuration, TableConfig tableConfig, ZkHelixPropertyStore<ZNRecord> propertyStore);

  /**
   * Builds one or more routing tables (maps of servers to segment lists) that are used for query routing. The union of
   * the segment lists that are in each routing table should contain all data in Pinot.
   *
   * @param tableName The table name for which to build the routing table
   * @param externalView The external view for the table
   * @param instanceConfigList The instance configurations for the instances serving this particular table (used for
   *                           pruning)
   */
  void computeRoutingTableFromExternalView(String tableName, ExternalView externalView,
      List<InstanceConfig> instanceConfigList);

  /**
   * Return the candidate set of servers that hosts each segment-set.
   * The List of services are expected to be ordered so that replica-selection strategy can be
   * applied to them to select one Service among the list for each segment.
   *
   * @return SegmentSet to Servers map.
   */
  Map<ServerInstance, SegmentIdSet> findServers(RoutingTableLookupRequest request);

  /**
   * @return List of routing tables used to route queries
   */
  List<ServerToSegmentSetMap> getRoutingTables();
  
  boolean isPartitionAware();


}
