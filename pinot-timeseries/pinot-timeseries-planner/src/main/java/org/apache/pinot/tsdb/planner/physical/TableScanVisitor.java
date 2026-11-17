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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.QuerySource;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.TableRouteInfo;
import org.apache.pinot.core.routing.TableRouteProvider;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.plan.LeafTimeSeriesPlanNode;


public class TableScanVisitor {
  public static final TableScanVisitor INSTANCE = new TableScanVisitor();
  private RoutingManager _routingManager;
  private TableRouteProvider _tableRouteProvider;
  private TableCache _tableCache;

  private TableScanVisitor() {
  }

  public void init(RoutingManager routingManager, TableRouteProvider tableRouteProvider, TableCache tableCache) {
    _routingManager = routingManager;
    _tableRouteProvider = tableRouteProvider;
    _tableCache = tableCache;
  }

  public void assignSegmentsToPlan(BaseTimeSeriesPlanNode planNode, TimeBuckets timeBuckets, Context context) {
    if (planNode instanceof LeafTimeSeriesPlanNode) {
      LeafTimeSeriesPlanNode sfpNode = (LeafTimeSeriesPlanNode) planNode;
      Expression filterExpression = CalciteSqlParser.compileToExpression(sfpNode.getEffectiveFilter(timeBuckets));
      context.addTableName(sfpNode.getTableName());
      RoutingTable routingTable = _routingManager.getRoutingTable(
          compileBrokerRequest(sfpNode.getTableName(), filterExpression),
          context._requestId);
      Preconditions.checkNotNull(routingTable, "Failed to get routing table for table: " + sfpNode.getTableName());
      routingTable = context._routingCustomizer.customize(sfpNode, routingTable, context._routingContext);
      Preconditions.checkNotNull(routingTable, "Routing customizer returned no routing for table: "
          + sfpNode.getTableName());
      boolean routed = false;
      for (var entry : routingTable.getServerInstanceToSegmentsMap().entrySet()) {
        ServerInstance serverInstance = entry.getKey();
        List<String> segments = entry.getValue().getSegments() == null ? List.of() : entry.getValue().getSegments();
        List<String> optionalSegments = entry.getValue().getOptionalSegments() == null ? List.of()
            : entry.getValue().getOptionalSegments();
        // A server reads the leaf only if it has segments for it: it then sends exactly one block for the leaf, which
        // is what the leaf's exchange counts on.
        if (segments.isEmpty() && optionalSegments.isEmpty()) {
          continue;
        }
        routed = true;
        context.getLeafIdToSegmentsByServer().computeIfAbsent(serverInstance, (x) -> new HashMap<>())
            .put(sfpNode.getId(), segments);
        if (!optionalSegments.isEmpty()) {
          context._leafIdToOptionalSegmentsByServer.computeIfAbsent(serverInstance, (x) -> new HashMap<>())
              .put(sfpNode.getId(), optionalSegments);
        }
      }
      List<String> unavailableSegments = routingTable.getUnavailableSegments();
      if (!routed && unavailableSegments != null && !unavailableSegments.isEmpty()) {
        // No data in range is an empty result; segments no server can serve are an outage, not an empty result.
        throw new QueryException(QueryErrorCode.SERVER_SEGMENT_MISSING, String.format(
            "No server can serve %d unavailable segment(s) of table: %s", unavailableSegments.size(),
            sfpNode.getTableName()));
      }
    }
    for (BaseTimeSeriesPlanNode childNode : planNode.getInputs()) {
      assignSegmentsToPlan(childNode, timeBuckets, context);
    }
  }

  /// Adds the query options the [TimeSeriesRoutingCustomizer] collected during [#assignSegmentsToPlan] to every leaf,
  /// so the servers receive them. Returns the plan unchanged when there are none. A collected option replaces a leaf's
  /// own option with the same key, so the servers read exactly what the leaf was routed for.
  ///
  /// @param planNode The [BaseTimeSeriesPlanNode] to process.
  /// @param queryOptions The options collected for the query, from [Context#getRoutingQueryOptions()].
  /// @return The plan with the options added to every leaf.
  public BaseTimeSeriesPlanNode addQueryOptionsToPlan(BaseTimeSeriesPlanNode planNode,
      Map<String, String> queryOptions) {
    if (queryOptions.isEmpty()) {
      return planNode;
    }
    if (planNode instanceof LeafTimeSeriesPlanNode) {
      LeafTimeSeriesPlanNode leafNode = (LeafTimeSeriesPlanNode) planNode;
      Map<String, String> leafQueryOptions = new HashMap<>();
      if (leafNode.getQueryOptions() != null) {
        leafQueryOptions.putAll(leafNode.getQueryOptions());
      }
      leafQueryOptions.putAll(queryOptions);
      return leafNode.withQueryOptions(leafQueryOptions);
    }
    List<BaseTimeSeriesPlanNode> newInputs = new ArrayList<>();
    for (BaseTimeSeriesPlanNode childNode : planNode.getInputs()) {
      newInputs.add(addQueryOptionsToPlan(childNode, queryOptions));
    }
    return planNode.withInputs(newInputs);
  }

  /// Adds table type information (offline/realtime) to the plan node.
  /// If the plan node is a leaf node, it retrieves the table route info and updates the table name with type.
  /// If the plan node has child nodes, it recursively processes each child node.
  ///
  /// @param planNode The [BaseTimeSeriesPlanNode] to process.
  /// @return The updated [BaseTimeSeriesPlanNode] with table type information.
  public BaseTimeSeriesPlanNode addTableTypeInfoToPlan(BaseTimeSeriesPlanNode planNode, RequestContext requestContext) {
    if (planNode instanceof LeafTimeSeriesPlanNode) {
      LeafTimeSeriesPlanNode sfpNode = (LeafTimeSeriesPlanNode) planNode;
      TableRouteInfo routeInfo = _tableRouteProvider.getTableRouteInfo(sfpNode.getTableName(), _tableCache,
          _routingManager);
      String tableNameWithType = getTableNameWithType(routeInfo);
      Preconditions.checkNotNull(tableNameWithType, "Table not found for table name: " + sfpNode.getTableName());
      requestContext.setTableName(tableNameWithType);
      return sfpNode.withTableName(tableNameWithType);
    }

    List<BaseTimeSeriesPlanNode> newInputs = new ArrayList<>();
    for (BaseTimeSeriesPlanNode childNode : planNode.getInputs()) {
      newInputs.add(addTableTypeInfoToPlan(childNode, requestContext));
    }
    return planNode.withInputs(newInputs);
  }


  /// Returns the table name with type (offline/realtime) if the table exists, otherwise returns null.
  ///
  /// @param routeInfo The [TableRouteInfo] for the table.
  /// @return The table name with type, or null if the table does not exist.
  @Nullable
  private String getTableNameWithType(TableRouteInfo routeInfo) {
    Preconditions.checkState(!routeInfo.isHybrid(),
        "Hybrid tables are not supported yet for timeseries queries");
    if (routeInfo.isOffline()) {
      return routeInfo.getOfflineTableName();
    }
    if (routeInfo.isRealtime()) {
      return routeInfo.getRealtimeTableName();
    }
    return null;
  }

  public static Context createContext(Long requestId, @Nullable Map<String, String> requestQueryOptions,
      TimeSeriesRoutingCustomizer routingCustomizer) {
    return new Context(requestId, requestQueryOptions, routingCustomizer);
  }

  public static class Context {
    private final Map<ServerInstance, Map<String, List<String>>> _leafIdToSegmentsByServer = new HashMap<>();
    private final Map<ServerInstance, Map<String, List<String>>> _leafIdToOptionalSegmentsByServer = new HashMap<>();
    private final Long _requestId;
    private final List<String> _tableNames = new ArrayList<>();
    private final Map<String, String> _requestQueryOptions;
    private final Map<String, String> _routingQueryOptions = new HashMap<>();
    private final TimeSeriesRoutingCustomizer _routingCustomizer;
    // The customizer's view of this query, so it cannot reach the planner's own state through the context.
    private final TimeSeriesRoutingContext _routingContext = new TimeSeriesRoutingContext() {
      @Override
      public Map<String, String> getRequestQueryOptions() {
        return _requestQueryOptions;
      }

      @Override
      public Map<String, String> getRoutingQueryOptions() {
        return Collections.unmodifiableMap(_routingQueryOptions);
      }

      @Override
      public void putRoutingQueryOption(String key, String value) {
        _routingQueryOptions.put(key, value);
      }
    };

    public Context(Long requestId, @Nullable Map<String, String> requestQueryOptions,
        TimeSeriesRoutingCustomizer routingCustomizer) {
      _requestId = requestId;
      _requestQueryOptions =
          requestQueryOptions == null ? Map.of() : Collections.unmodifiableMap(new HashMap<>(requestQueryOptions));
      _routingCustomizer = routingCustomizer;
    }

    public List<TimeSeriesQueryServerInstance> getQueryServers() {
      return _leafIdToSegmentsByServer.keySet().stream().map(TimeSeriesQueryServerInstance::new).collect(
          Collectors.toList());
    }

    public Map<String, Map<String, List<String>>> getLeafIdToSegmentsByInstanceId() {
      return byInstanceId(_leafIdToSegmentsByServer);
    }

    /// Per server, the non-empty optional segments of each leaf: segments the server queries if it has them.
    public Map<String, Map<String, List<String>>> getLeafIdToOptionalSegmentsByInstanceId() {
      return byInstanceId(_leafIdToOptionalSegmentsByServer);
    }

    private static Map<String, Map<String, List<String>>> byInstanceId(
        Map<ServerInstance, Map<String, List<String>>> byServer) {
      Map<String, Map<String, List<String>>> result = new HashMap<>();
      for (var entry : byServer.entrySet()) {
        result.put(entry.getKey().getInstanceId(), entry.getValue());
      }
      return result;
    }

    Map<ServerInstance, Map<String, List<String>>> getLeafIdToSegmentsByServer() {
      return _leafIdToSegmentsByServer;
    }

    public List<String> getTableNames() {
      return _tableNames;
    }

    public void addTableName(String tableName) {
      _tableNames.add(tableName);
    }

    /// The query options of the request, read-only.
    @VisibleForTesting
    Map<String, String> getRequestQueryOptions() {
      return _requestQueryOptions;
    }

    /// Query options the [TimeSeriesRoutingCustomizer] added while routing the leaves of this query, read-only.
    public Map<String, String> getRoutingQueryOptions() {
      return Collections.unmodifiableMap(_routingQueryOptions);
    }

    /// What the [TimeSeriesRoutingCustomizer] sees of this query.
    @VisibleForTesting
    TimeSeriesRoutingContext getRoutingContext() {
      return _routingContext;
    }
  }

  private BrokerRequest compileBrokerRequest(String tableName, Expression filterExpression) {
    DataSource dataSource = new DataSource();
    dataSource.setTableName(tableName);
    PinotQuery pinotQuery = new PinotQuery();
    pinotQuery.setDataSource(dataSource);
    pinotQuery.setFilterExpression(filterExpression);
    // Time-series servers query optional segments, so a server that holds only optional segments of a leaf still
    // reads it, for example a segment it has loaded while the external view lags.
    Map<String, String> queryOptions = new HashMap<>();
    queryOptions.put(RoutingManager.ROUTE_SERVERS_WITH_ONLY_OPTIONAL_SEGMENTS, "true");
    pinotQuery.setQueryOptions(queryOptions);
    QuerySource querySource = new QuerySource();
    querySource.setTableName(tableName);
    BrokerRequest dummyRequest = new BrokerRequest();
    dummyRequest.setPinotQuery(pinotQuery);
    dummyRequest.setQuerySource(querySource);
    return dummyRequest;
  }
}
