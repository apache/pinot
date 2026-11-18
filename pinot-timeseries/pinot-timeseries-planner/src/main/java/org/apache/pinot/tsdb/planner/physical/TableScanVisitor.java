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
import org.apache.pinot.core.routing.SegmentsToQuery;
import org.apache.pinot.core.routing.TableRouteInfo;
import org.apache.pinot.core.routing.TableRouteProvider;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.config.table.TableConfig;
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
      RoutingTable routingTable = _routingManager.getRoutingTableWithMissingServerSegments(
          compileBrokerRequest(sfpNode.getTableName(), filterExpression), context._requestId);
      Preconditions.checkNotNull(routingTable, "Failed to get routing table for table: " + sfpNode.getTableName());
      routingTable = customizeRouting(sfpNode, routingTable, context);
      // A later callback can still hold references to this leaf's routing lists.
      boolean copyAssignments = context._routingCustomizer != TimeSeriesRoutingCustomizer.NO_OP;
      boolean hasRequiredSegments = false;
      for (var entry : routingTable.getServerInstanceToSegmentsMap().entrySet()) {
        ServerInstance serverInstance = entry.getKey();
        List<String> segments = entry.getValue().getSegments();
        // The exchange waits for each dispatched server. Optional segments alone cannot require an RPC.
        if (segments == null || segments.isEmpty()) {
          continue;
        }
        hasRequiredSegments = true;
        List<String> optionalSegments = entry.getValue().getOptionalSegments();
        context.getLeafIdToSegmentsByServer().computeIfAbsent(serverInstance, (x) -> new HashMap<>())
            .put(sfpNode.getId(), copyAssignments ? List.copyOf(segments) : segments);
        if (optionalSegments != null && !optionalSegments.isEmpty()) {
          context._leafIdToOptionalSegmentsByServer.computeIfAbsent(serverInstance, (x) -> new HashMap<>())
              .put(sfpNode.getId(), copyAssignments ? List.copyOf(optionalSegments) : optionalSegments);
        }
      }
      List<String> unavailableSegments = routingTable.getUnavailableSegments();
      if (!hasRequiredSegments && unavailableSegments != null && !unavailableSegments.isEmpty()) {
        // Optional segments may still be loading. Their assignments do not cover unavailable required segments.
        throw new QueryException(QueryErrorCode.SERVER_SEGMENT_MISSING, String.format(
            "No server can serve %d unavailable segment(s) of table: %s", unavailableSegments.size(),
            sfpNode.getTableName()));
      }
    }
    for (BaseTimeSeriesPlanNode childNode : planNode.getInputs()) {
      assignSegmentsToPlan(childNode, timeBuckets, context);
    }
  }

  private RoutingTable customizeRouting(LeafTimeSeriesPlanNode leaf, RoutingTable routingTable, Context context) {
    TimeSeriesRoutingCustomizer customizer = context._routingCustomizer;
    if (customizer == TimeSeriesRoutingCustomizer.NO_OP) {
      return routingTable;
    }
    String tableName = leaf.getTableName();
    TableConfig tableConfig = _tableCache.getTableConfig(tableName);
    if (tableConfig == null) {
      throw QueryErrorCode.BROKER_RESOURCE_MISSING.asException("Table config not found for " + tableName);
    }
    // Upsert execution adds recently loaded segments on the server. Snapshot the assignments before the hook
    // so that it cannot accidentally narrow an upsert scan, including by modifying the routing table in place.
    RoutingAssignments original = tableConfig.isUpsertEnabled() ? RoutingAssignments.snapshot(routingTable) : null;
    RoutingTable customized = Preconditions.checkNotNull(customizer.customize(leaf, routingTable,
        context._routingContext), "Routing customizer returned no routing for table: " + tableName);
    if (original != null && !original.equals(RoutingAssignments.snapshot(customized))) {
      throw QueryErrorCode.QUERY_VALIDATION.asException(
          "Time-series routing cannot change segment assignments for upsert table " + tableName);
    }
    return customized;
  }

  private record SegmentAssignment(List<String> required, List<String> optional) {
  }

  private record RoutingAssignments(Map<ServerInstance, SegmentAssignment> byServer, List<String> unavailable) {
    private static RoutingAssignments snapshot(RoutingTable routingTable) {
      Map<ServerInstance, SegmentAssignment> byServer = new HashMap<>();
      for (var entry : routingTable.getServerInstanceToSegmentsMap().entrySet()) {
        SegmentsToQuery segments = entry.getValue();
        byServer.put(entry.getKey(),
            new SegmentAssignment(copySegments(segments.getSegments()), copySegments(segments.getOptionalSegments())));
      }
      return new RoutingAssignments(byServer, copySegments(routingTable.getUnavailableSegments()));
    }

    private static List<String> copySegments(@Nullable List<String> segments) {
      return segments == null ? List.of() : List.copyOf(segments);
    }
  }

  /// Adds the query options the [TimeSeriesRoutingCustomizer] collected during [#assignSegmentsToPlan] to every leaf,
  /// so the servers receive them. Returns the plan unchanged when there are none. A collected option replaces a leaf's
  /// own option with the same key.
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
    QuerySource querySource = new QuerySource();
    querySource.setTableName(tableName);
    BrokerRequest dummyRequest = new BrokerRequest();
    dummyRequest.setPinotQuery(pinotQuery);
    dummyRequest.setQuerySource(querySource);
    return dummyRequest;
  }
}
