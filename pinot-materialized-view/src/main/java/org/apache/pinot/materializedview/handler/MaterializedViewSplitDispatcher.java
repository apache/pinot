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
package org.apache.pinot.materializedview.handler;

import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.core.routing.TableRouteInfo;
import org.apache.pinot.spi.data.Schema;


/// Callback supplied by the broker so the MV handler can perform the dual route-build +
/// scatter-gather + reduce without depending on broker-internal types (request id, server stats,
/// request context, table-cache helpers — all captured in the closure of the lambda the broker
/// registers per-query).
///
/// The MV handler's job is to compute time boundaries and attach the boundary filters to the
/// per-branch queries; the broker owns the rest of the route-and-dispatch dance because it
/// touches generic broker internals (hybrid offline/realtime split, optimizer invocation,
/// scatter-gather via `QueryRouter`, dual reduce via `BrokerReduceService`).
@FunctionalInterface
public interface MaterializedViewSplitDispatcher {

  /// Builds offline/realtime broker requests for the base table (handling hybrid time-boundary +
  /// always-false-filter pruning), builds the MV-side broker request and route, then issues two
  /// scatter-gather requests and reduces them with `originalBrokerRequest` as the merge key.
  ///
  /// @param originalBrokerRequest the user's original query (used as the reduce-time merge key
  ///     so the user's LIMIT/OFFSET semantics are preserved).
  /// @param baseQueryWithTimeFilter base-side server query with the `ts >= boundary` filter
  ///     already attached.
  /// @param baseRouteInfo routing info pre-computed for the base table.
  /// @param baseSchema base-table schema (for query optimization).
  /// @param viewQueryWithTimeFilter MV-side server query with the
  ///     `materializedViewTime < boundary` filter already attached.
  /// @param viewTableNameWithType fully-qualified MV table name.
  /// @param viewSchema MV-table schema (for query optimization).
  /// @param timeoutMs remaining timeout in milliseconds.
  BrokerResponseNative dispatch(BrokerRequest originalBrokerRequest, PinotQuery baseQueryWithTimeFilter,
      TableRouteInfo baseRouteInfo, Schema baseSchema, PinotQuery viewQueryWithTimeFilter,
      String viewTableNameWithType, Schema viewSchema, long timeoutMs) throws Exception;
}
