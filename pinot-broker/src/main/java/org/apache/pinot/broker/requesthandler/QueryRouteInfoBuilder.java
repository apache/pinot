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
package org.apache.pinot.broker.requesthandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.broker.routing.BrokerRoutingManager;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.BrokerQueryPhase;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.QuerySource;
import org.apache.pinot.core.query.optimizer.QueryOptimizer;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.ServerRouteInfo;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.core.transport.QueryRouteInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.broker.requesthandler.BaseSingleStageBrokerRequestHandler.*;
//CHECKSTYLE:ON

public class QueryRouteInfoBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryRouteInfoBuilder.class);

  public static class Exception extends java.lang.Exception {
    private final int _errorCode;

    Exception(int errorCode, String errorMessage) {
      super(errorMessage);
      _errorCode = errorCode;
    }

    public int getErrorCode() {
      return _errorCode;
    }
  }

  private String _brokerId;
  private String _tableName;
  private TableCache _tableCache;
  private BrokerMetrics _brokerMetrics;
  private long _requestId;
  private String _query;
  private RequestContext _requestContext;
  private PinotQuery _serverPinotQuery;
  private BrokerRequest _serverBrokerRequest;
  private QueryOptimizer _queryOptimizer;
  private BrokerRoutingManager _routingManager;
  private boolean _disableGroovy;
  private boolean _useApproximateFunction;
  private int _queryResponseLimit;
  private PinotConfiguration _configuration;
  private int _maxUnavailableSegmentsToPrintInQueryException;

  QueryRouteInfoBuilder() {
  }

  public QueryRouteInfoBuilder setBrokerId(String brokerId) {
    _brokerId = brokerId;
    return this;
  }

  public QueryRouteInfoBuilder setTableCache(TableCache tableCache) {
    _tableCache = tableCache;
    return this;
  }

  public QueryRouteInfoBuilder setBrokerMetrics(BrokerMetrics brokerMetrics) {
    _brokerMetrics = brokerMetrics;
    return this;
  }

  public QueryRouteInfoBuilder setRequestId(long requestId) {
    _requestId = requestId;
    return this;
  }

  public QueryRouteInfoBuilder setQuery(String query) {
    _query = query;
    return this;
  }

  public QueryRouteInfoBuilder setRequestContext(RequestContext requestContext) {
    _requestContext = requestContext;
    return this;
  }

  public QueryRouteInfoBuilder setTableName(String tableName) {
    _tableName = tableName;
    return this;
  }

  public QueryRouteInfoBuilder setServerPinotQuery(PinotQuery serverPinotQuery) {
    _serverPinotQuery = serverPinotQuery;
    return this;
  }

  public QueryRouteInfoBuilder setDisableGroovy(boolean disableGroovy) {
    _disableGroovy = disableGroovy;
    return this;
  }

  public QueryRouteInfoBuilder setUseApproximateFunction(boolean useApproximateFunction) {
    _useApproximateFunction = useApproximateFunction;
    return this;
  }

  public QueryRouteInfoBuilder setQueryOptimizer(QueryOptimizer queryOptimizer) {
    _queryOptimizer = queryOptimizer;
    return this;
  }

  public QueryRouteInfoBuilder setRoutingManager(BrokerRoutingManager routingManager) {
    _routingManager = routingManager;
    return this;
  }

  public QueryRouteInfoBuilder setConfiguration(PinotConfiguration configuration) {
    _configuration = configuration;
    return this;
  }

  public QueryRouteInfoBuilder setServerBrokerRequest(BrokerRequest serverBrokerRequest) {
    _serverBrokerRequest = serverBrokerRequest;
    return this;
  }

  public QueryRouteInfoBuilder setQueryResponseLimit(int queryResponseLimit) {
    _queryResponseLimit = queryResponseLimit;
    return this;
  }

  public QueryRouteInfoBuilder setMaxUnavailableSegmentsToPrintInQueryException(
      int maxUnavailableSegmentsToPrintInQueryException) {
    _maxUnavailableSegmentsToPrintInQueryException = maxUnavailableSegmentsToPrintInQueryException;
    return this;
  }

  public QueryRouteInfo build()
      throws Exception {
    String rawTableName = TableNameBuilder.extractRawTableName(_tableName);
    Schema schema = _tableCache.getSchema(rawTableName);

    PinotQuery serverPinotQuery = _serverPinotQuery.deepCopy();
    serverPinotQuery.getDataSource().setTableName(rawTableName);

    // Get the tables hit by the request
    String offlineTableName = null;
    String realtimeTableName = null;
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(_tableName);
    if (tableType == TableType.OFFLINE) {
      // Offline table
      if (_routingManager.routingExists(_tableName)) {
        offlineTableName = _tableName;
      }
    } else if (tableType == TableType.REALTIME) {
      // Realtime table
      if (_routingManager.routingExists(_tableName)) {
        realtimeTableName = _tableName;
      }
    } else {
      // Hybrid table (check both OFFLINE and REALTIME)
      String offlineTableNameToCheck = TableNameBuilder.OFFLINE.tableNameWithType(_tableName);
      if (_routingManager.routingExists(offlineTableNameToCheck)) {
        offlineTableName = offlineTableNameToCheck;
      }
      String realtimeTableNameToCheck = TableNameBuilder.REALTIME.tableNameWithType(_tableName);
      if (_routingManager.routingExists(realtimeTableNameToCheck)) {
        realtimeTableName = realtimeTableNameToCheck;
      }
    }

    TableConfig offlineTableConfig =
        _tableCache.getTableConfig(TableNameBuilder.OFFLINE.tableNameWithType(rawTableName));
    TableConfig realtimeTableConfig =
        _tableCache.getTableConfig(TableNameBuilder.REALTIME.tableNameWithType(rawTableName));

    if (offlineTableName == null && realtimeTableName == null) {
      // No table matches the request
      if (realtimeTableConfig == null && offlineTableConfig == null) {
        throw new Exception(QueryException.TABLE_DOES_NOT_EXIST_ERROR_CODE,
            "Table not found for request " + _requestId + ": " + _query);
      }
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.RESOURCE_MISSING_EXCEPTIONS, 1);

      throw new Exception(QueryException.BROKER_RESOURCE_MISSING_ERROR_CODE,
          "No table matches for request " + _requestId + ": " + _query);
    }

    // Handle query rewrite that can be overridden by the table configs
    if (offlineTableName == null) {
      offlineTableConfig = null;
    }
    if (realtimeTableName == null) {
      realtimeTableConfig = null;
    }

    BaseSingleStageBrokerRequestHandler.HandlerContext handlerContext =
        BaseSingleStageBrokerRequestHandler.getHandlerContext(_disableGroovy, _useApproximateFunction,
            offlineTableConfig, realtimeTableConfig);
    if (handlerContext._disableGroovy) {
      BaseSingleStageBrokerRequestHandler.rejectGroovyQuery(serverPinotQuery);
    }
    if (handlerContext._useApproximateFunction) {
      handleApproximateFunctionOverride(serverPinotQuery);
    }

    // Validate the request
    try {
      validateRequest(serverPinotQuery, _queryResponseLimit);
    } catch (IllegalStateException e) {
      throw new Exception(QueryException.QUERY_VALIDATION_ERROR_CODE,
          "Caught exception while validating request " + _requestId + ": " + e.getMessage());
    }

    // Prepare OFFLINE and REALTIME requests
    BrokerRequest offlineBrokerRequest = null;
    BrokerRequest realtimeBrokerRequest = null;
    TimeBoundaryInfo timeBoundaryInfo = null;
    if (offlineTableName != null && realtimeTableName != null) {
      // Time boundary info might be null when there is no segment in the offline table, query real-time side only
      timeBoundaryInfo = _routingManager.getTimeBoundaryInfo(offlineTableName);
      if (timeBoundaryInfo == null) {
        LOGGER.debug("No time boundary info found for hybrid table: {}", rawTableName);
        offlineTableName = null;
      }
    }
    if (offlineTableName != null && realtimeTableName != null) {
      // Hybrid
      PinotQuery offlinePinotQuery = serverPinotQuery.deepCopy();
      offlinePinotQuery.getDataSource().setTableName(offlineTableName);
      attachTimeBoundary(offlinePinotQuery, timeBoundaryInfo, true);
      handleExpressionOverride(offlinePinotQuery, _tableCache.getExpressionOverrideMap(offlineTableName));
      handleTimestampIndexOverride(offlinePinotQuery, offlineTableConfig, _tableCache);
      _queryOptimizer.optimize(offlinePinotQuery, offlineTableConfig, schema);
      offlineBrokerRequest = CalciteSqlCompiler.convertToBrokerRequest(offlinePinotQuery);

      PinotQuery realtimePinotQuery = serverPinotQuery.deepCopy();
      realtimePinotQuery.getDataSource().setTableName(realtimeTableName);
      attachTimeBoundary(realtimePinotQuery, timeBoundaryInfo, false);
      handleExpressionOverride(realtimePinotQuery, _tableCache.getExpressionOverrideMap(realtimeTableName));
      handleTimestampIndexOverride(realtimePinotQuery, realtimeTableConfig, _tableCache);
      _queryOptimizer.optimize(realtimePinotQuery, realtimeTableConfig, schema);
      realtimeBrokerRequest = CalciteSqlCompiler.convertToBrokerRequest(realtimePinotQuery);
    } else if (offlineTableName != null) {
      // OFFLINE only
      handleExpressionOverride(serverPinotQuery, _tableCache.getExpressionOverrideMap(offlineTableName));
      handleTimestampIndexOverride(serverPinotQuery, offlineTableConfig, _tableCache);
      _queryOptimizer.optimize(serverPinotQuery, offlineTableConfig, schema);
      offlineBrokerRequest = new BrokerRequest();
      PinotQuery offlinePinotQuery = serverPinotQuery.deepCopy();
      offlinePinotQuery.getDataSource().setTableName(offlineTableName);
      offlineBrokerRequest.setPinotQuery(offlinePinotQuery);
      QuerySource querySource = new QuerySource();
      querySource.setTableName(offlineTableName);
      offlineBrokerRequest.setQuerySource(querySource);
    } else {
      // REALTIME only
      handleExpressionOverride(serverPinotQuery, _tableCache.getExpressionOverrideMap(realtimeTableName));
      handleTimestampIndexOverride(serverPinotQuery, realtimeTableConfig, _tableCache);
      _queryOptimizer.optimize(serverPinotQuery, realtimeTableConfig, schema);
      realtimeBrokerRequest = _serverBrokerRequest;
    }

    // Check if response can be sent without server query evaluation.
    if (offlineBrokerRequest != null && isFilterAlwaysFalse(offlineBrokerRequest.getPinotQuery())) {
      // We don't need to evaluate offline request
      offlineBrokerRequest = null;
    }
    if (realtimeBrokerRequest != null && isFilterAlwaysFalse(realtimeBrokerRequest.getPinotQuery())) {
      // We don't need to evaluate realtime request
      realtimeBrokerRequest = null;
    }

    if (offlineBrokerRequest == null && realtimeBrokerRequest == null) {
      return QueryRouteInfo.EMPTY;
    }

    if (offlineBrokerRequest != null && isFilterAlwaysTrue(offlineBrokerRequest.getPinotQuery())) {
      // Drop offline request filter since it is always true
      offlineBrokerRequest.getPinotQuery().setFilterExpression(null);
    }
    if (realtimeBrokerRequest != null && isFilterAlwaysTrue(realtimeBrokerRequest.getPinotQuery())) {
      // Drop realtime request filter since it is always true
      realtimeBrokerRequest.getPinotQuery().setFilterExpression(null);
    }

    // Calculate routing table for the query
    long routingStartTimeNs = System.nanoTime();
    Map<ServerInstance, ServerRouteInfo> offlineRoutingTable = null;
    Map<ServerInstance, ServerRouteInfo> realtimeRoutingTable = null;
    List<String> unavailableSegments = new ArrayList<>();
    int numPrunedSegmentsTotal = 0;
    boolean offlineTableDisabled = false;
    boolean realtimeTableDisabled = false;
    if (offlineBrokerRequest != null) {
      offlineTableDisabled = _routingManager.isTableDisabled(offlineTableName);
      // NOTE: Routing table might be null if table is just removed
      RoutingTable routingTable = null;
      if (!offlineTableDisabled) {
        routingTable = _routingManager.getRoutingTable(offlineBrokerRequest, offlineTableName, _requestId);
      }
      if (routingTable != null) {
        unavailableSegments.addAll(routingTable.getUnavailableSegments());
        Map<ServerInstance, ServerRouteInfo> serverInstanceToSegmentsMap =
            routingTable.getServerInstanceToSegmentsMap();
        if (!serverInstanceToSegmentsMap.isEmpty()) {
          offlineRoutingTable = serverInstanceToSegmentsMap;
        } else {
          offlineBrokerRequest = null;
        }
        numPrunedSegmentsTotal += routingTable.getNumPrunedSegments();
      } else {
        offlineBrokerRequest = null;
      }
    }
    if (realtimeBrokerRequest != null) {
      realtimeTableDisabled = _routingManager.isTableDisabled(realtimeTableName);
      // NOTE: Routing table might be null if table is just removed
      RoutingTable routingTable = null;
      if (!realtimeTableDisabled) {
        routingTable = _routingManager.getRoutingTable(realtimeBrokerRequest, offlineTableName, _requestId);
      }
      if (routingTable != null) {
        unavailableSegments.addAll(routingTable.getUnavailableSegments());
        Map<ServerInstance, ServerRouteInfo> serverInstanceToSegmentsMap =
            routingTable.getServerInstanceToSegmentsMap();
        if (!serverInstanceToSegmentsMap.isEmpty()) {
          realtimeRoutingTable = serverInstanceToSegmentsMap;
        } else {
          realtimeBrokerRequest = null;
        }
        numPrunedSegmentsTotal += routingTable.getNumPrunedSegments();
      } else {
        realtimeBrokerRequest = null;
      }
    }

    if (offlineTableDisabled || realtimeTableDisabled) {
      String errorMessage = null;
      if (((realtimeTableConfig != null && offlineTableConfig != null) && (offlineTableDisabled
          && realtimeTableDisabled)) || (offlineTableConfig == null && realtimeTableDisabled) || (
          realtimeTableConfig == null && offlineTableDisabled)) {
        throw new Exception(QueryException.TABLE_IS_DISABLED_ERROR_CODE, "Table " + _tableName + " is disabled");
      } else if ((realtimeTableConfig != null && offlineTableConfig != null) && realtimeTableDisabled) {
        errorMessage = "Realtime table is disabled in hybrid table";
      } else if ((realtimeTableConfig != null && offlineTableConfig != null) && offlineTableDisabled) {
        errorMessage = "Offline table is disabled in hybrid table";
      }
      throw new Exception(QueryException.TABLE_IS_DISABLED_ERROR_CODE, errorMessage);
    }

    int numUnavailableSegments = unavailableSegments.size();
    _requestContext.setNumUnavailableSegments(numUnavailableSegments);

    if (numUnavailableSegments > 0) {
      String errorMessage;
      if (numUnavailableSegments > _maxUnavailableSegmentsToPrintInQueryException) {
        errorMessage = String.format("%d segments unavailable, sampling %d: %s", numUnavailableSegments,
            _maxUnavailableSegmentsToPrintInQueryException,
            unavailableSegments.subList(0, _maxUnavailableSegmentsToPrintInQueryException));
      } else {
        errorMessage = String.format("%d segments unavailable: %s", numUnavailableSegments, unavailableSegments);
      }
      String realtimeRoutingPolicy = realtimeBrokerRequest != null ? getRoutingPolicy(realtimeTableConfig) : null;
      String offlineRoutingPolicy = offlineBrokerRequest != null ? getRoutingPolicy(offlineTableConfig) : null;
      errorMessage = addRoutingPolicyInErrMsg(errorMessage, realtimeRoutingPolicy, offlineRoutingPolicy);
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.BROKER_RESPONSES_WITH_UNAVAILABLE_SEGMENTS, 1);
      throw new Exception(QueryException.BROKER_SEGMENT_UNAVAILABLE_ERROR_CODE, errorMessage);
    }

    if (offlineBrokerRequest == null && realtimeBrokerRequest == null) {
      // When all segments have been pruned, we can just return an empty response.
      return QueryRouteInfo.EMPTY;
    }
    long routingEndTimeNs = System.nanoTime();
    _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.QUERY_ROUTING, routingEndTimeNs - routingStartTimeNs);

    // Set the maximum serialized response size per server, and ask server to directly return final response when only
    // one server is queried
    int numServers = 0;
    if (offlineRoutingTable != null) {
      numServers += offlineRoutingTable.size();
    }
    if (realtimeRoutingTable != null) {
      numServers += realtimeRoutingTable.size();
    }
    if (offlineBrokerRequest != null) {
      Map<String, String> queryOptions = offlineBrokerRequest.getPinotQuery().getQueryOptions();
      setMaxServerResponseSizeBytes(_configuration, numServers, queryOptions, offlineTableConfig);
      // Set the query option to directly return final result for single server query unless it is explicitly disabled
      if (numServers == 1) {
        // Set the same flag in the original server request to be used in the reduce phase for hybrid table
        if (queryOptions.putIfAbsent(CommonConstants.Broker.Request.QueryOptionKey.SERVER_RETURN_FINAL_RESULT, "true")
            == null && offlineBrokerRequest != _serverBrokerRequest) {
          _serverBrokerRequest.getPinotQuery().getQueryOptions()
              .put(CommonConstants.Broker.Request.QueryOptionKey.SERVER_RETURN_FINAL_RESULT, "true");
        }
      }
    }
    if (realtimeBrokerRequest != null) {
      Map<String, String> queryOptions = realtimeBrokerRequest.getPinotQuery().getQueryOptions();
      setMaxServerResponseSizeBytes(_configuration, numServers, queryOptions, realtimeTableConfig);
      // Set the query option to directly return final result for single server query unless it is explicitly disabled
      if (numServers == 1) {
        // Set the same flag in the original server request to be used in the reduce phase for hybrid table
        if (queryOptions.putIfAbsent(CommonConstants.Broker.Request.QueryOptionKey.SERVER_RETURN_FINAL_RESULT, "true")
            == null && realtimeBrokerRequest != _serverBrokerRequest) {
          _serverBrokerRequest.getPinotQuery().getQueryOptions()
              .put(CommonConstants.Broker.Request.QueryOptionKey.SERVER_RETURN_FINAL_RESULT, "true");
        }
      }
    }

    return new QueryRouteInfo(_brokerId, _requestId, rawTableName, null, _serverBrokerRequest, offlineTableName,
        offlineBrokerRequest, offlineRoutingTable, realtimeTableName, realtimeBrokerRequest, realtimeRoutingTable,
        numPrunedSegmentsTotal, 0, _requestContext);
  }
}
