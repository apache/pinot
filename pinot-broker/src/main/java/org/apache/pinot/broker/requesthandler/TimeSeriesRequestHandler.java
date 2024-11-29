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
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.ws.rs.core.HttpHeaders;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.broker.api.RequesterIdentity;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.routing.BrokerRoutingManager;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerTimer;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.PinotBrokerTimeSeriesResponse;
import org.apache.pinot.common.utils.HumanReadableDuration;
import org.apache.pinot.query.service.dispatch.QueryDispatcher;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.apache.pinot.tsdb.planner.TimeSeriesQueryEnvironment;
import org.apache.pinot.tsdb.planner.physical.TimeSeriesDispatchablePlan;
import org.apache.pinot.tsdb.spi.RangeTimeSeriesRequest;
import org.apache.pinot.tsdb.spi.TimeSeriesLogicalPlanResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TimeSeriesRequestHandler extends BaseBrokerRequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimeSeriesRequestHandler.class);
  private static final long DEFAULT_STEP_SECONDS = 60L;
  private final TimeSeriesQueryEnvironment _queryEnvironment;
  private final QueryDispatcher _queryDispatcher;

  public TimeSeriesRequestHandler(PinotConfiguration config, String brokerId, BrokerRoutingManager routingManager,
      AccessControlFactory accessControlFactory, QueryQuotaManager queryQuotaManager, TableCache tableCache,
      QueryDispatcher queryDispatcher) {
    super(config, brokerId, routingManager, accessControlFactory, queryQuotaManager, tableCache);
    _queryEnvironment = new TimeSeriesQueryEnvironment(config, routingManager, tableCache);
    _queryEnvironment.init(config);
    _queryDispatcher = queryDispatcher;
  }

  @Override
  protected BrokerResponse handleRequest(long requestId, String query, @Nullable SqlNodeAndOptions sqlNodeAndOptions,
      JsonNode request, @Nullable RequesterIdentity requesterIdentity, RequestContext requestContext,
      @Nullable HttpHeaders httpHeaders, AccessControl accessControl)
      throws Exception {
    throw new IllegalArgumentException("Not supported yet");
  }

  @Override
  public void start() {
    LOGGER.info("Starting time-series request handler");
  }

  @Override
  public void shutDown() {
    LOGGER.info("Shutting down time-series request handler");
  }

  @Override
  public PinotBrokerTimeSeriesResponse handleTimeSeriesRequest(String lang, String rawQueryParamString,
      RequestContext requestContext) {
    PinotBrokerTimeSeriesResponse timeSeriesResponse = null;
    long queryStartTime = System.currentTimeMillis();
    try {
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.TIME_SERIES_GLOBAL_QUERIES, 1);
      requestContext.setBrokerId(_brokerId);
      requestContext.setRequestId(_requestIdGenerator.get());
      RangeTimeSeriesRequest timeSeriesRequest = null;
      try {
        timeSeriesRequest = buildRangeTimeSeriesRequest(lang, rawQueryParamString);
      } catch (URISyntaxException e) {
        return PinotBrokerTimeSeriesResponse.newErrorResponse("BAD_REQUEST", "Error building RangeTimeSeriesRequest");
      }
      TimeSeriesLogicalPlanResult logicalPlanResult = _queryEnvironment.buildLogicalPlan(timeSeriesRequest);
      TimeSeriesDispatchablePlan dispatchablePlan =
          _queryEnvironment.buildPhysicalPlan(timeSeriesRequest, requestContext, logicalPlanResult);
      timeSeriesResponse = _queryDispatcher.submitAndGet(requestContext, dispatchablePlan,
          timeSeriesRequest.getTimeout().toMillis(), new HashMap<>());
      return timeSeriesResponse;
    } finally {
      _brokerMetrics.addTimedValue(BrokerTimer.QUERY_TOTAL_TIME_MS, System.currentTimeMillis() - queryStartTime,
          TimeUnit.MILLISECONDS);
      if (timeSeriesResponse == null
          || timeSeriesResponse.getStatus().equals(PinotBrokerTimeSeriesResponse.ERROR_STATUS)) {
        _brokerMetrics.addMeteredGlobalValue(BrokerMeter.TIME_SERIES_GLOBAL_QUERIES_FAILED, 1);
      }
    }
  }

  @Override
  public Map<Long, String> getRunningQueries() {
    // TODO: Implement this.
    return Map.of();
  }

  @Override
  public boolean cancelQuery(long queryId, int timeoutMs, Executor executor, HttpClientConnectionManager connMgr,
      Map<String, Integer> serverResponses)
      throws Exception {
    // TODO: Implement this.
    return false;
  }

  private RangeTimeSeriesRequest buildRangeTimeSeriesRequest(String language, String queryParamString)
      throws URISyntaxException {
    List<NameValuePair> pairs = URLEncodedUtils.parse(
        new URI("http://localhost?" + queryParamString), "UTF-8");
    String query = null;
    Long startTs = null;
    Long endTs = null;
    String step = null;
    String timeoutStr = null;
    for (NameValuePair nameValuePair : pairs) {
      switch (nameValuePair.getName()) {
        case "query":
          query = nameValuePair.getValue();
          break;
        case "start":
          startTs = Long.parseLong(nameValuePair.getValue());
          break;
        case "end":
          endTs = Long.parseLong(nameValuePair.getValue());
          break;
        case "step":
          step = nameValuePair.getValue();
          break;
        case "timeout":
          timeoutStr = nameValuePair.getValue();
          break;
        default:
          /* Okay to ignore unknown parameters since the language implementor may be using them. */
          break;
      }
    }
    Long stepSeconds = getStepSeconds(step);
    Preconditions.checkNotNull(query, "Query cannot be null");
    Preconditions.checkNotNull(startTs, "Start time cannot be null");
    Preconditions.checkNotNull(endTs, "End time cannot be null");
    Preconditions.checkState(stepSeconds != null && stepSeconds > 0, "Step must be a positive integer");
    Duration timeout = Duration.ofMillis(_brokerTimeoutMs);
    if (StringUtils.isNotBlank(timeoutStr)) {
      timeout = HumanReadableDuration.from(timeoutStr);
    }
    // TODO: Pass full raw query param string to the request
    return new RangeTimeSeriesRequest(language, query, startTs, endTs, stepSeconds, timeout, queryParamString);
  }

  public static Long getStepSeconds(@Nullable String step) {
    if (step == null) {
      return DEFAULT_STEP_SECONDS;
    }
    try {
      return Long.parseLong(step);
    } catch (NumberFormatException ignored) {
    }
    return HumanReadableDuration.from(step).getSeconds();
  }
}
