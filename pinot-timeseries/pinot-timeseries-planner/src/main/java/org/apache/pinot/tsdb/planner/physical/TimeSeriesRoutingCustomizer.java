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
package org.apache.pinot.tsdb.planner.physical;

import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.tsdb.spi.plan.LeafTimeSeriesPlanNode;


/// Broker hook that adjusts how each leaf of a time-series query is routed, after the routing manager has picked
/// its segments. A broker can narrow a leaf to a subset of segments and add query options the servers need to read
/// them. For example, a table format with snapshots can pin every leaf of a query to one snapshot and tell the servers
/// which one, the way the single-stage engine lets a broker rewrite its routing and server query options.
///
/// [TableScanVisitor] calls it once per leaf, in plan order, on the broker thread that plans the query: after the
/// query's tables are authorized and before anything is dispatched to servers. The [TimeSeriesRoutingContext] carries
/// the request's query options (read-only) and the routing query options: entries added there are sent to the servers
/// with every leaf of the query, replacing a leaf's own option with the same key. A later leaf sees what an earlier one
/// added, so an implementation can keep all leaves of a query consistent. One customizer serves every query of a
/// broker, so it must be thread-safe.
public interface TimeSeriesRoutingCustomizer {
  /// Routes every leaf exactly as the routing manager chose and adds no query options.
  TimeSeriesRoutingCustomizer NO_OP = (leaf, routingTable, context) -> routingTable;

  /// A leaf narrowed to no segments on any server yields no data, as a leaf the routing manager found no segments for
  /// does, unless the returned routing reports unavailable segments, which fails the query.
  ///
  /// @param leaf the leaf being routed; its table name carries the table type
  /// @param routingTable the routing the routing manager chose for the leaf
  /// @param context the query being planned; add options for the servers to its routing query options
  /// @return the routing to use for the leaf, never null
  RoutingTable customize(LeafTimeSeriesPlanNode leaf, RoutingTable routingTable, TimeSeriesRoutingContext context);
}
