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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.broker.api.RequesterIdentity;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.querylog.QueryLogger;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.routing.BrokerRoutingManager;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.BrokerQueryPhase;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.auth.AuthorizationResult;
import org.apache.pinot.spi.auth.TableAuthorizationResult;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.eventlistener.query.BrokerQueryEventListener;
import org.apache.pinot.spi.eventlistener.query.BrokerQueryEventListenerFactory;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@ThreadSafe
public abstract class BaseBrokerRequestHandler implements BrokerRequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseBrokerRequestHandler.class);
  protected final PinotConfiguration _config;
  protected final String _brokerId;
  protected final BrokerRoutingManager _routingManager;
  protected final AccessControlFactory _accessControlFactory;
  protected final QueryQuotaManager _queryQuotaManager;
  protected final TableCache _tableCache;
  protected final BrokerMetrics _brokerMetrics;
  protected final BrokerQueryEventListener _brokerQueryEventListener;
  protected final Set<String> _trackedHeaders;
  protected final BrokerRequestIdGenerator _requestIdGenerator;
  protected final long _brokerTimeoutMs;
  protected final QueryLogger _queryLogger;
  @Nullable
  protected final String _enableNullHandling;
  /**
   * Maps broker-generated query id to the query string.
   */
  protected final Map<Long, String> _queriesById;
  /**
   * Maps broker-generated query id to client-provided query id.
   */
  protected final Map<Long, String> _clientQueryIds;

  public BaseBrokerRequestHandler(PinotConfiguration config, String brokerId, BrokerRoutingManager routingManager,
      AccessControlFactory accessControlFactory, QueryQuotaManager queryQuotaManager, TableCache tableCache) {
    _config = config;
    _brokerId = brokerId;
    _routingManager = routingManager;
    _accessControlFactory = accessControlFactory;
    _queryQuotaManager = queryQuotaManager;
    _tableCache = tableCache;
    _brokerMetrics = BrokerMetrics.get();
    _brokerQueryEventListener = BrokerQueryEventListenerFactory.getBrokerQueryEventListener();
    _trackedHeaders = BrokerQueryEventListenerFactory.getTrackedHeaders();
    _requestIdGenerator = new BrokerRequestIdGenerator(brokerId);
    _brokerTimeoutMs = config.getProperty(Broker.CONFIG_OF_BROKER_TIMEOUT_MS, Broker.DEFAULT_BROKER_TIMEOUT_MS);
    _queryLogger = new QueryLogger(config);
    _enableNullHandling = config.getProperty(Broker.CONFIG_OF_BROKER_QUERY_ENABLE_NULL_HANDLING);

    boolean enableQueryCancellation =
        Boolean.parseBoolean(config.getProperty(CommonConstants.Broker.CONFIG_OF_BROKER_ENABLE_QUERY_CANCELLATION));
    if (enableQueryCancellation) {
      _queriesById = new ConcurrentHashMap<>();
      _clientQueryIds = new ConcurrentHashMap<>();
    } else {
      _queriesById = null;
      _clientQueryIds = null;
    }
  }

  @Override
  public BrokerResponse handleRequest(JsonNode request, @Nullable SqlNodeAndOptions sqlNodeAndOptions,
      @Nullable RequesterIdentity requesterIdentity, RequestContext requestContext, @Nullable HttpHeaders httpHeaders)
      throws Exception {
    try (QueryThreadContext.CloseableContext closeMe = QueryThreadContext.open()) {
      QueryThreadContext.setStartTimeMs(requestContext.getRequestArrivalTimeMillis());
      requestContext.setBrokerId(_brokerId);
      QueryThreadContext.setBrokerId(_brokerId);
      long requestId = _requestIdGenerator.get();
      requestContext.setRequestId(requestId);

      if (httpHeaders != null && !_trackedHeaders.isEmpty()) {
        MultivaluedMap<String, String> requestHeaders = httpHeaders.getRequestHeaders();
        Map<String, List<String>> trackedHeadersMap = Maps.newHashMapWithExpectedSize(_trackedHeaders.size());
        for (Map.Entry<String, List<String>> entry : requestHeaders.entrySet()) {
          String key = entry.getKey().toLowerCase();
          if (_trackedHeaders.contains(key)) {
            trackedHeadersMap.put(key, entry.getValue());
          }
        }
        requestContext.setRequestHttpHeaders(trackedHeadersMap);
      }

      // First-stage access control to prevent unauthenticated requests from using up resources. Secondary table-level
      // check comes later.
      AccessControl accessControl = _accessControlFactory.create();
      AuthorizationResult authorizationResult = accessControl.authorize(requesterIdentity);
      if (!authorizationResult.hasAccess()) {
        _brokerMetrics.addMeteredGlobalValue(BrokerMeter.REQUEST_DROPPED_DUE_TO_ACCESS_ERROR, 1);
        requestContext.setErrorCode(QueryErrorCode.ACCESS_DENIED);
        _brokerQueryEventListener.onQueryCompletion(requestContext);
        String failureMessage = authorizationResult.getFailureMessage();
        if (StringUtils.isNotBlank(failureMessage)) {
          failureMessage = "Reason: " + failureMessage;
        }
        throw new WebApplicationException("Permission denied." + failureMessage, Response.Status.FORBIDDEN);
      }

      JsonNode sql = request.get(Broker.Request.SQL);
      if (sql == null || !sql.isTextual()) {
        requestContext.setErrorCode(QueryErrorCode.SQL_PARSING);
        _brokerQueryEventListener.onQueryCompletion(requestContext);
        throw new BadQueryRequestException("Failed to find 'sql' in the request: " + request);
      }

      String query = sql.textValue();
      requestContext.setQuery(query);
      QueryThreadContext.setSql(query);

      // Parse the query if needed
      if (sqlNodeAndOptions == null) {
        try {
          sqlNodeAndOptions = RequestUtils.parseQuery(query, request);
        } catch (Exception e) {
          // Do not log or emit metric here because it is pure user error
          requestContext.setErrorCode(QueryErrorCode.SQL_PARSING);
          return new BrokerResponseNative(QueryErrorCode.SQL_PARSING, e.getMessage());
        }
      }
      String cid = extractClientRequestId(sqlNodeAndOptions);
      QueryThreadContext.setIds(requestId, cid != null ? cid : Long.toString(requestId));

      // check app qps before doing anything
      String application = sqlNodeAndOptions.getOptions().get(Broker.Request.QueryOptionKey.APPLICATION_NAME);
      if (application != null && !_queryQuotaManager.acquireApplication(application)) {
        String errorMessage =
            "Request " + requestId + ": " + query + " exceeds query quota for application: " + application;
        LOGGER.info(errorMessage);
        requestContext.setErrorCode(QueryErrorCode.TOO_MANY_REQUESTS);
        return new BrokerResponseNative(QueryErrorCode.TOO_MANY_REQUESTS, errorMessage);
      }

      // Add null handling option from broker config only if there is no override in the query
      if (_enableNullHandling != null) {
        sqlNodeAndOptions.getOptions()
            .putIfAbsent(Broker.Request.QueryOptionKey.ENABLE_NULL_HANDLING, _enableNullHandling);
      }

      BrokerResponse brokerResponse =
          handleRequest(requestId, query, sqlNodeAndOptions, request, requesterIdentity, requestContext, httpHeaders,
              accessControl);
      brokerResponse.setBrokerId(_brokerId);
      brokerResponse.setRequestId(Long.toString(requestId));
      _brokerQueryEventListener.onQueryCompletion(requestContext);

      return brokerResponse;
    }
  }

  protected abstract BrokerResponse handleRequest(long requestId, String query, SqlNodeAndOptions sqlNodeAndOptions,
      JsonNode request, @Nullable RequesterIdentity requesterIdentity, RequestContext requestContext,
      @Nullable HttpHeaders httpHeaders, AccessControl accessControl)
      throws Exception;

  /**
   * Attemps to cancel an ongoing query identified by its broker-generated id.
   * @return true if the query was successfully cancelled, false otherwise.
   */
  protected abstract boolean handleCancel(long queryId, int timeoutMs, Executor executor,
      HttpClientConnectionManager connMgr, Map<String, Integer> serverResponses) throws Exception;

  protected static void augmentStatistics(RequestContext statistics, BrokerResponse response) {
    statistics.setNumRowsResultSet(response.getNumRowsResultSet());
    // TODO: Add partial result flag to RequestContext
    List<QueryProcessingException> exceptions = response.getExceptions();
    int numExceptions = exceptions.size();
    List<String> processingExceptions = new ArrayList<>(numExceptions);
    for (QueryProcessingException exception : exceptions) {
      processingExceptions.add(exception.toString());
    }
    statistics.setProcessingExceptions(processingExceptions);
    statistics.setNumExceptions(numExceptions);
    statistics.setNumGroupsLimitReached(response.isNumGroupsLimitReached());
    statistics.setProcessingTimeMillis(response.getTimeUsedMs());
    statistics.setNumDocsScanned(response.getNumDocsScanned());
    statistics.setTotalDocs(response.getTotalDocs());
    statistics.setNumEntriesScannedInFilter(response.getNumEntriesScannedInFilter());
    statistics.setNumEntriesScannedPostFilter(response.getNumEntriesScannedPostFilter());
    statistics.setNumServersQueried(response.getNumServersQueried());
    statistics.setNumServersResponded(response.getNumServersResponded());
    statistics.setNumSegmentsQueried(response.getNumSegmentsQueried());
    statistics.setNumSegmentsProcessed(response.getNumSegmentsProcessed());
    statistics.setNumSegmentsMatched(response.getNumSegmentsMatched());
    statistics.setNumConsumingSegmentsQueried(response.getNumConsumingSegmentsQueried());
    statistics.setNumConsumingSegmentsProcessed(response.getNumConsumingSegmentsProcessed());
    statistics.setNumConsumingSegmentsMatched(response.getNumConsumingSegmentsMatched());
    statistics.setMinConsumingFreshnessTimeMs(response.getMinConsumingFreshnessTimeMs());
    statistics.setNumSegmentsPrunedByBroker(response.getNumSegmentsPrunedByBroker());
    statistics.setNumSegmentsPrunedByServer(response.getNumSegmentsPrunedByServer());
    statistics.setNumSegmentsPrunedInvalid(response.getNumSegmentsPrunedInvalid());
    statistics.setNumSegmentsPrunedByLimit(response.getNumSegmentsPrunedByLimit());
    statistics.setNumSegmentsPrunedByValue(response.getNumSegmentsPrunedByValue());
    statistics.setReduceTimeMillis(response.getBrokerReduceTimeMs());
    statistics.setOfflineThreadCpuTimeNs(response.getOfflineThreadCpuTimeNs());
    statistics.setRealtimeThreadCpuTimeNs(response.getRealtimeThreadCpuTimeNs());
    statistics.setOfflineSystemActivitiesCpuTimeNs(response.getOfflineSystemActivitiesCpuTimeNs());
    statistics.setRealtimeSystemActivitiesCpuTimeNs(response.getRealtimeSystemActivitiesCpuTimeNs());
    statistics.setOfflineResponseSerializationCpuTimeNs(response.getOfflineResponseSerializationCpuTimeNs());
    statistics.setRealtimeResponseSerializationCpuTimeNs(response.getRealtimeResponseSerializationCpuTimeNs());
    statistics.setOfflineTotalCpuTimeNs(response.getOfflineTotalCpuTimeNs());
    statistics.setRealtimeTotalCpuTimeNs(response.getRealtimeTotalCpuTimeNs());
    statistics.setExplainPlanNumEmptyFilterSegments(response.getExplainPlanNumEmptyFilterSegments());
    statistics.setExplainPlanNumMatchAllFilterSegments(response.getExplainPlanNumMatchAllFilterSegments());
    statistics.setTraceInfo(response.getTraceInfo());
  }

  @Override
  public Map<Long, String> getRunningQueries() {
    Preconditions.checkState(isQueryCancellationEnabled(), "Query cancellation is not enabled on broker");
    return Collections.unmodifiableMap(_queriesById);
  }

  @Override
  public boolean cancelQuery(long queryId, int timeoutMs, Executor executor, HttpClientConnectionManager connMgr,
      Map<String, Integer> serverResponses)
      throws Exception {
    Preconditions.checkState(isQueryCancellationEnabled(), "Query cancellation is not enabled on broker");
    try {
      return handleCancel(queryId, timeoutMs, executor, connMgr, serverResponses);
    } finally {
      onQueryFinish(queryId);
    }
  }

  @Override
  public boolean cancelQueryByClientId(String clientQueryId, int timeoutMs, Executor executor,
      HttpClientConnectionManager connMgr, Map<String, Integer> serverResponses)
      throws Exception {
    Preconditions.checkState(isQueryCancellationEnabled(), "Query cancellation is not enabled on broker");
    OptionalLong requestId = getRequestIdByClientId(clientQueryId);
    if (requestId.isPresent()) {
      return cancelQuery(requestId.getAsLong(), timeoutMs, executor, connMgr, serverResponses);
    } else {
      LOGGER.warn("Query cancellation cannot be performed due to unknown client query id: {}", clientQueryId);
      return false;
    }
  }

  @Override
  public OptionalLong getRequestIdByClientId(String clientQueryId) {
    return _clientQueryIds.entrySet().stream()
        .filter(e -> clientQueryId.equals(e.getValue()))
        .mapToLong(Map.Entry::getKey)
        .findFirst();
  }

  @Nullable
  protected String extractClientRequestId(SqlNodeAndOptions sqlNodeAndOptions) {
    return sqlNodeAndOptions.getOptions() != null
        ? sqlNodeAndOptions.getOptions().get(Broker.Request.QueryOptionKey.CLIENT_QUERY_ID) : null;
  }

  protected void onQueryStart(long requestId, String clientRequestId, String query, Object... extras) {
    if (isQueryCancellationEnabled()) {
      _queriesById.put(requestId, query);
      if (StringUtils.isNotBlank(clientRequestId)) {
        _clientQueryIds.put(requestId, clientRequestId);
        LOGGER.debug("Keep track of running query: {} (with client id {})", requestId, clientRequestId);
      } else {
        LOGGER.debug("Keep track of running query: {}", requestId);
      }
    }
  }

  protected void onQueryFinish(long requestId) {
    if (isQueryCancellationEnabled()) {
      _queriesById.remove(requestId);
      _clientQueryIds.remove(requestId);
      LOGGER.debug("Remove track of running query: {}", requestId);
    }
  }

  protected boolean isQueryCancellationEnabled() {
    return _queriesById != null;
  }

  protected void updatePhaseTimingForTables(Set<String> tableNames, BrokerQueryPhase phase, long time) {
    for (String tableName : tableNames) {
      String rawTableName = TableNameBuilder.extractRawTableName(tableName);
      _brokerMetrics.addPhaseTiming(rawTableName, phase, time);
    }
  }

  /**
   * Validates whether the requester has access to all the tables.
   */
  protected TableAuthorizationResult hasTableAccess(RequesterIdentity requesterIdentity, Set<String> tableNames,
      RequestContext requestContext, HttpHeaders httpHeaders) {
    final long startTimeNs = System.nanoTime();
    AccessControl accessControl = _accessControlFactory.create();

    TableAuthorizationResult tableAuthorizationResult = accessControl.authorize(requesterIdentity, tableNames);

    Set<String> failedTables = tableNames.stream()
        .filter(table -> !accessControl.hasAccess(httpHeaders, TargetType.TABLE, table, Actions.Table.QUERY))
        .collect(Collectors.toSet());

    failedTables.addAll(tableAuthorizationResult.getFailedTables());

    if (!failedTables.isEmpty()) {
      tableAuthorizationResult = new TableAuthorizationResult(failedTables);
    } else {
      tableAuthorizationResult = TableAuthorizationResult.success();
    }

    if (!tableAuthorizationResult.hasAccess()) {
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.REQUEST_DROPPED_DUE_TO_ACCESS_ERROR, 1);
      LOGGER.warn("Access denied for requestId {}", requestContext.getRequestId());
      requestContext.setErrorCode(QueryErrorCode.ACCESS_DENIED);
    }

    updatePhaseTimingForTables(tableNames, BrokerQueryPhase.AUTHORIZATION, System.nanoTime() - startTimeNs);

    return tableAuthorizationResult;
  }

  /**
   * Returns true if the QPS quota of query tables, database or application has been exceeded.
   */
  protected boolean hasExceededQPSQuota(@Nullable String database, Set<String> tableNames,
      RequestContext requestContext) {
    if (database != null && !_queryQuotaManager.acquireDatabase(database)) {
      LOGGER.warn("Request {}: query exceeds quota for database: {}", requestContext.getRequestId(), database);
      requestContext.setErrorCode(QueryErrorCode.TOO_MANY_REQUESTS);
      return true;
    }
    for (String tableName : tableNames) {
      if (!_queryQuotaManager.acquire(tableName)) {
        LOGGER.warn("Request {}: query exceeds quota for table: {}", requestContext.getRequestId(), tableName);
        requestContext.setErrorCode(QueryErrorCode.TOO_MANY_REQUESTS);
        String rawTableName = TableNameBuilder.extractRawTableName(tableName);
        _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.QUERY_QUOTA_EXCEEDED, 1);
        return true;
      }
    }
    return false;
  }
}
