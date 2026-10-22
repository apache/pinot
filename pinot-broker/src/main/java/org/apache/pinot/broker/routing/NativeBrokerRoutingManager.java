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
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.SegmentsToQuery;
import org.apache.pinot.core.routing.TablePartitionInfo;
import org.apache.pinot.core.routing.TablePartitionReplicatedServersInfo;
import org.apache.pinot.core.routing.timeboundary.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;

import static com.google.common.base.Preconditions.checkArgument;


/// Normal Pinot routing interface over one captured, completely covered OFFLINE table. It neither changes with
/// later metadata updates nor performs I/O, sampling or partition pruning; execution still requires native fences.
final class NativeBrokerRoutingManager implements RoutingManager {
  private final String _tableName;
  private final Map<String, ServerInstance> _servers;
  private final RoutingTable _route;
  private final List<String> _segments;

  NativeBrokerRoutingManager(String tableName, Map<ServerInstance, SegmentsToQuery> routes) {
    _tableName = tableName;
    _servers = Map.copyOf(routes.keySet().stream()
        .collect(Collectors.toMap(ServerInstance::getInstanceId, Function.identity())));
    _route = new RoutingTable(Map.copyOf(routes), List.of(), 0);
    _segments = routes.values().stream().flatMap(value -> value.getSegments().stream()).sorted().toList();
  }

  @Override
  public Map<String, ServerInstance> getEnabledServerInstanceMap() {
    return _servers;
  }

  @Nullable
  @Override
  public RoutingTable getRoutingTable(BrokerRequest request, long requestId) {
    return getRoutingTable(request, request.getQuerySource().getTableName(), requestId);
  }

  @Nullable
  @Override
  public RoutingTable getRoutingTable(BrokerRequest request, String tableName, long requestId) {
    rejectSampling(request, null);
    if (!_tableName.equals(tableName)) {
      return null;
    }
    checkArgument(tableName.equals(request.getQuerySource().getTableName()),
        "Native routing cannot redirect another table's request");
    return _route;
  }

  @Nullable
  @Override
  public List<String> getSegments(BrokerRequest request) {
    return getSegments(request, null);
  }

  @Nullable
  @Override
  public List<String> getSegments(BrokerRequest request, @Nullable String samplerName) {
    rejectSampling(request, samplerName);
    return _tableName.equals(request.getQuerySource().getTableName()) ? _segments : null;
  }

  @Nullable
  @Override
  public Set<String> getPrunedSegments(BrokerRequest request) {
    rejectSampling(request, null);
    return _tableName.equals(request.getQuerySource().getTableName()) ? Set.of() : null;
  }

  @Override
  public boolean routingExists(String tableName) {
    return _tableName.equals(tableName);
  }

  @Override
  public Set<String> getRoutableTables() {
    return Set.of(_tableName);
  }

  @Nullable
  @Override
  public TimeBoundaryInfo getTimeBoundaryInfo(String tableName) {
    return null;
  }

  @Nullable
  @Override
  public TablePartitionInfo getTablePartitionInfo(String tableName) {
    return null;
  }

  @Nullable
  @Override
  public TablePartitionReplicatedServersInfo getTablePartitionReplicatedServersInfo(String tableName) {
    return null;
  }

  @Nullable
  @Override
  public Set<String> getServingInstances(String tableName) {
    return _tableName.equals(tableName) ? _servers.keySet() : null;
  }

  private static void rejectSampling(BrokerRequest request, @Nullable String samplerName) {
    checkArgument(samplerName == null && (!request.isSetPinotQuery()
        || QueryOptionsUtils.getTableSampler(request.getPinotQuery().getQueryOptions()) == null),
        "Native captured routing does not support table sampling");
  }
}
