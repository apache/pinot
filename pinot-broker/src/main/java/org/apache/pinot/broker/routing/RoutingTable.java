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
package org.apache.pinot.broker.routing;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.core.transport.ServerInstance;


/**
 * Instance level routing table manager.
 */
public interface RoutingTable {

  /**
   * Returns the routing table (map from server instance to list of segments hosted by the server) based on the lookup
   * request.
   *
   * @param request Routing table lookup request
   * @return Map from server instance to list of segments hosted by the server
   */
  Map<ServerInstance, List<String>> getRoutingTable(RoutingTableLookupRequest request);

  /**
   * Returns whether the routing table for the given table exists.
   *
   * @param tableNameWithType Table name with type suffix
   * @return Whether the routing table exists
   */
  boolean routingTableExists(String tableNameWithType);

  /**
   * Dumps a snapshot of all the routing tables for the given table.
   *
   * @param tableName Table name (with or without type suffix) or null for all tables
   */
  String dumpSnapshot(@Nullable String tableName)
      throws Exception;
}
