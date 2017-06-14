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
package com.linkedin.pinot.broker.routing;

import java.util.Map;

import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.common.SegmentIdSet;


public interface RoutingTable {
  /**
   * Return the candidate set of servers that hosts each segment-set.
   * The List of services are expected to be ordered so that replica-selection strategy can be
   * applied to them to select one Service among the list for each segment.
   *
   * @return SegmentSet to Servers map.
   */
  Map<ServerInstance, SegmentIdSet> findServers(RoutingTableLookupRequest request);

  /**
   * Returns whether or not a routing table exists and is not empty for a given table.
   *
   * @param tableName The table name for which to check if a routing table exists
   * @return true if the routing table exists and isn't empty
   */
  boolean routingTableExists(String tableName);

  /**
   * Initialize and start the Routing table population
   */
  void start();

  /**
   * Shutdown Routing table cleanly
   */
  void shutdown();

  /**
   * Dumps a snapshot of the routing table.
   *
   * @param tableName The table name or empty string for all tables.
   * @throws Exception
   */
  String dumpSnapshot(String tableName) throws Exception;
}
