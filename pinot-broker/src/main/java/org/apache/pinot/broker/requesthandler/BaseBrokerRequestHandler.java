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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
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
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.auth.AuthorizationResult;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.eventlistener.query.BrokerQueryEventListener;
import org.apache.pinot.spi.eventlistener.query.BrokerQueryEventListenerFactory;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
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
    requestContext.setBrokerId(_brokerId);
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
      requestContext.setErrorCode(QueryException.ACCESS_DENIED_ERROR_CODE);
      _brokerQueryEventListener.onQueryCompletion(requestContext);
      String failureMessage = authorizationResult.getFailureMessage();
      if (StringUtils.isNotBlank(failureMessage)) {
        failureMessage = "Reason: " + failureMessage;
      }
      throw new WebApplicationException("Permission denied." + failureMessage, Response.Status.FORBIDDEN);
    }

    JsonNode sql = request.get(Broker.Request.SQL);
    if (sql == null || !sql.isTextual()) {
      requestContext.setErrorCode(QueryException.SQL_PARSING_ERROR_CODE);
      _brokerQueryEventListener.onQueryCompletion(requestContext);
      throw new BadQueryRequestException("Failed to find 'sql' in the request: " + request);
    }

    String query = sql.textValue();
    requestContext.setQuery(query);

    // Parse the query if needed
    if (sqlNodeAndOptions == null) {
      try {
        sqlNodeAndOptions = RequestUtils.parseQuery(query, request);
      } catch (Exception e) {
        // Do not log or emit metric here because it is pure user error
        requestContext.setErrorCode(QueryException.SQL_PARSING_ERROR_CODE);
        return new BrokerResponseNative(QueryException.getException(QueryException.SQL_PARSING_ERROR, e));
      }
    }

    // check app qps before doing anything
    String application = sqlNodeAndOptions.getOptions().get(Broker.Request.QueryOptionKey.APPLICATION_NAME);
    if (application != null && !_queryQuotaManager.acquireApplication(application)) {
      String errorMessage =
          "Request " + requestId + ": " + query + " exceeds query quota for application: " + application;
      LOGGER.info(errorMessage);
      requestContext.setErrorCode(QueryException.TOO_MANY_REQUESTS_ERROR_CODE);
      return new BrokerResponseNative(QueryException.getException(QueryException.QUOTA_EXCEEDED_ERROR, errorMessage));
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
    Optional<Long> requestId = _clientQueryIds.entrySet().stream()
        .filter(e -> clientQueryId.equals(e.getValue())).map(Map.Entry::getKey).findFirst();
    if (requestId.isPresent()) {
      return cancelQuery(requestId.get(), timeoutMs, executor, connMgr, serverResponses);
    } else {
      LOGGER.warn("Query cancellation cannot be performed due to unknown client query id: {}", clientQueryId);
      return false;
    }
  }

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
}
