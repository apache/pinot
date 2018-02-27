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

import java.util.List;
import java.util.Map;


/**
 * Instance level routing table manager.
 */
public interface RoutingTable {

  /**
   * Get the routing table (map from server to list of segments) based on the lookup request.
   *
   * @param request Routing table lookup request
   * @return Map from server to list of segments
   */
  Map<String, List<String>> getRoutingTable(RoutingTableLookupRequest request);

  /**
   * Return whether the routing table for the given table exists.
   *
   * @param tableName Table name
   * @return Whether the routing table exists
   */
  boolean routingTableExists(String tableName);

  /**
   * Dump a snapshot of all the routing tables for the given table.
   *
   * @param tableName Table name or null for all tables.
   * @throws Exception
   */
  String dumpSnapshot(String tableName) throws Exception;
}
