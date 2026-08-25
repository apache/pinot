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
package org.apache.pinot.core.routing;

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.routing.timeboundary.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.annotations.InterfaceAudience;
import org.apache.pinot.spi.annotations.InterfaceStability;


/// The `RouteManager` provides the routing information for a query that requests access to a Pinot table.
///
/// The implementation of this interface should ensure the routing and server information are up-to-date at the
/// time when the routing request was made.
///
/// set by the user. This needs to be added to support features like segment pruning.
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface RoutingManager {

  /// Get all enabled server instances in the cluster.
  ///
  /// @return all currently enabled server instances.
  Map<String, ServerInstance> getEnabledServerInstanceMap();

  /// Get all routable server instances in the cluster -- enabled servers that have not been excluded from routing by
  /// the broker (e.g. by the FailureDetector). Callers that pick workers without going through per-table instance
  /// selection (such as MSE intermediate stage worker selection) should prefer this over
  /// [#getEnabledServerInstanceMap()] so that FailureDetector exclusions are honored.
  ///
  /// The default implementation delegates to [#getEnabledServerInstanceMap()] for backward compatibility with
  /// implementations that do not track exclusions separately.
  default Map<String, ServerInstance> getRoutableServerInstanceMap() {
    return getEnabledServerInstanceMap();
  }

  /// Returns whether the given table is enabled
  /// @param tableNameWithType Table name with type
  /// @return Whether the given table is enabled
  default boolean isTableDisabled(String tableNameWithType) {
    return false;
  }

  /// Get the [RoutingTable] for a specific broker request.
  ///
  /// @param brokerRequest the broker request constructed from a query.
  /// @return the route table.
  @Nullable
  RoutingTable getRoutingTable(BrokerRequest brokerRequest, long requestId);

  /// Get the [RoutingTable] for a specific broker request.
  /// @param brokerRequest the broker request constructed from a query.
  /// @param tableNameWithType the name of the table.
  /// @param requestId the request id.
  /// @return the route table.
  @Nullable
  RoutingTable getRoutingTable(BrokerRequest brokerRequest, String tableNameWithType, long requestId);

  /// Returns the segments that are relevant for the given broker request. Returns `null` if the table does not
  /// exist.
  @Nullable
  List<String> getSegments(BrokerRequest brokerRequest);

  /// Returns the segments that are relevant for the given broker request and optional sampler name.
  /// Returns `null` if the table does not exist.
  @Nullable
  default List<String> getSegments(BrokerRequest brokerRequest, @Nullable String samplerName) {
    return getSegments(brokerRequest);
  }

  /// Returns the segments that the segment pruners *provably eliminated* for the given broker request, i.e. the
  /// selected segments that cannot hold a row matching the request's filter. Absence from the returned set means
  /// nothing: a segment may be missing because it matches, because it was never selected, or because the table does
  /// not exist. Only presence is a proof.
  ///
  /// This is the complement of [#getSegments(BrokerRequest)], and the distinction is what makes it usable as a
  /// planning-time emptiness proof. Deciding "this segment does not match" from *absence* of a survivor conflates
  /// pruning with the several innocent reasons a segment can be missing from a routing result -- it was classified as
  /// optional by instance selection, its server left the enabled server map, or it entered the partition metadata
  /// before it became selectable -- and each of those would silently drop matching data. Deciding it from presence in
  /// this set cannot.
  ///
  /// Instance selection deliberately takes no part, so the result depends only on the request and the pruners, never
  /// on a request id or on which replica a query happens to pick.
  ///
  /// Returns `null` if the table does not exist, as [#getSegments(BrokerRequest)] does, which is not the same as an
  /// empty set: a broker that does not have the table eliminated nothing because it would have routed nothing, while
  /// an empty set is a broker that ran the pruners and proved nothing. A caller combining several brokers' verdicts
  /// has to tell those apart, and gets both from this one lookup rather than from a separate existence check that
  /// could disagree with it.
  ///
  /// The returned set is for reading only: an implementation may hand back an immutable or a shared set, so a caller
  /// that needs to modify it must copy it first.
  ///
  /// The default implementation returns an empty set, i.e. proves nothing about any segment. Note that this is a
  /// statement about segments only: a caller may still act on emptiness it can see for itself, such as a partition
  /// that lists no segment at all.
  @Nullable
  default Set<String> getPrunedSegments(BrokerRequest brokerRequest) {
    return Set.of();
  }

  /// Validate routing exist for a table
  ///
  /// @param tableNameWithType the name of the table.
  /// @return true if the route table exists.
  boolean routingExists(String tableNameWithType);

  /// Acquire the time boundary info. Useful for hybrid logical table queries that needs to split between
  /// realtime and offline.
  /// @param offlineTableName offline table name
  /// @return time boundary info.
  @Nullable
  TimeBoundaryInfo getTimeBoundaryInfo(String offlineTableName);

  /// Returns the [TablePartitionInfo] for a given table.
  @Nullable
  TablePartitionInfo getTablePartitionInfo(String tableNameWithType);

  /// Returns the [TablePartitionReplicatedServersInfo] for a given table.
  @Nullable
  TablePartitionReplicatedServersInfo getTablePartitionReplicatedServersInfo(String tableNameWithType);

  /// Returns the enabled server instances currently serving the given table.
  @Nullable
  Set<String> getServingInstances(String tableNameWithType);
}
