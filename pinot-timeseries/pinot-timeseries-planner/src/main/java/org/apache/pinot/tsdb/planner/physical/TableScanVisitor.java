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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
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
      for (var entry : routingTable.getServerInstanceToSegmentsMap().entrySet()) {
        ServerInstance serverInstance = entry.getKey();
        List<String> segments = entry.getValue().getSegments();
        context.getLeafIdToSegmentsByServer().computeIfAbsent(serverInstance, (x) -> new HashMap<>())
            .put(sfpNode.getId(), segments);
      }
    }
    for (BaseTimeSeriesPlanNode childNode : planNode.getInputs()) {
      assignSegmentsToPlan(childNode, timeBuckets, context);
    }
  }

  /**
   * Adds table type information (offline/realtime) to the plan node.
   * If the plan node is a leaf node, it retrieves the table route info and updates the table name with type.
   * If the plan node has child nodes, it recursively processes each child node.
   *
   * @param planNode The {@link BaseTimeSeriesPlanNode} to process.
   * @return The updated {@link BaseTimeSeriesPlanNode} with table type information.
   */
  public BaseTimeSeriesPlanNode addTableTypeInfoToPlan(BaseTimeSeriesPlanNode planNode) {
    if (planNode instanceof LeafTimeSeriesPlanNode) {
      LeafTimeSeriesPlanNode sfpNode = (LeafTimeSeriesPlanNode) planNode;
      TableRouteInfo routeInfo = _tableRouteProvider.getTableRouteInfo(sfpNode.getTableName(), _tableCache,
        _routingManager);
      String tableNameWithType = getTableNameWithType(routeInfo);
      Preconditions.checkNotNull(tableNameWithType, "Table not found for table name: " + sfpNode.getTableName());
      return sfpNode.withTableName(tableNameWithType);
    }

    List<BaseTimeSeriesPlanNode> newInputs = new ArrayList<>();
    for (BaseTimeSeriesPlanNode childNode : planNode.getInputs()) {
      newInputs.add(addTableTypeInfoToPlan(childNode));
    }
    return planNode.withInputs(newInputs);
  }


  /**
   * Returns the table name with type (offline/realtime) if the table exists, otherwise returns null.
   *
   * @param routeInfo The {@link TableRouteInfo} for the table.
   * @return The table name with type, or null if the table does not exist.
   */
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

  public static Context createContext(Long requestId) {
    return new Context(requestId);
  }

  public static class Context {
    private final Map<ServerInstance, Map<String, List<String>>> _leafIdToSegmentsByServer = new HashMap<>();
    private final Long _requestId;
    private final List<String> _tableNames = new ArrayList<>();

    public Context(Long requestId) {
      _requestId = requestId;
    }

    public List<TimeSeriesQueryServerInstance> getQueryServers() {
      return _leafIdToSegmentsByServer.keySet().stream().map(TimeSeriesQueryServerInstance::new).collect(
          Collectors.toList());
    }

    public Map<String, Map<String, List<String>>> getLeafIdToSegmentsByInstanceId() {
      Map<String, Map<String, List<String>>> result = new HashMap<>();
      for (var entry : _leafIdToSegmentsByServer.entrySet()) {
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
