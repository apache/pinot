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
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
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
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.eventlistener.query.BrokerQueryEventListener;
import org.apache.pinot.spi.eventlistener.query.BrokerQueryEventListenerFactory;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;


@ThreadSafe
public abstract class BaseBrokerRequestHandler implements BrokerRequestHandler {
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
    if (!accessControl.hasAccess(requesterIdentity)) {
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.REQUEST_DROPPED_DUE_TO_ACCESS_ERROR, 1);
      requestContext.setErrorCode(QueryException.ACCESS_DENIED_ERROR_CODE);
      _brokerQueryEventListener.onQueryCompletion(requestContext);
      throw new WebApplicationException("Permission denied", Response.Status.FORBIDDEN);
    }

    JsonNode sql = request.get(Broker.Request.SQL);
    if (sql == null || !sql.isTextual()) {
      requestContext.setErrorCode(QueryException.SQL_PARSING_ERROR_CODE);
      _brokerQueryEventListener.onQueryCompletion(requestContext);
      throw new BadQueryRequestException("Failed to find 'sql' in the request: " + request);
    }

    String query = sql.textValue();
    requestContext.setQuery(query);
    BrokerResponse brokerResponse =
        handleRequest(requestId, query, sqlNodeAndOptions, request, requesterIdentity, requestContext, httpHeaders,
            accessControl);
    brokerResponse.setBrokerId(_brokerId);
    brokerResponse.setRequestId(Long.toString(requestId));
    _brokerQueryEventListener.onQueryCompletion(requestContext);

    return brokerResponse;
  }

  protected abstract BrokerResponse handleRequest(long requestId, String query,
      @Nullable SqlNodeAndOptions sqlNodeAndOptions, JsonNode request, @Nullable RequesterIdentity requesterIdentity,
      RequestContext requestContext, @Nullable HttpHeaders httpHeaders, AccessControl accessControl)
      throws Exception;

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
}
