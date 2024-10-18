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
import java.util.Map;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;
import javax.ws.rs.core.HttpHeaders;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.pinot.broker.api.RequesterIdentity;
import org.apache.pinot.common.cursors.AbstractResponseStore;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.CursorResponse;
import org.apache.pinot.common.response.PinotBrokerTimeSeriesResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.CursorResponseNative;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;


/**
 * {@code BrokerRequestHandlerDelegate} delegates the inbound broker request to one of the enabled
 * {@link BrokerRequestHandler} based on the requested handle type.
 *
 * {@see: @CommonConstant
 */
public class BrokerRequestHandlerDelegate implements BrokerRequestHandler {
  private final BaseSingleStageBrokerRequestHandler _singleStageBrokerRequestHandler;
  private final MultiStageBrokerRequestHandler _multiStageBrokerRequestHandler;
  private final TimeSeriesRequestHandler _timeSeriesRequestHandler;
  private final AbstractResponseStore _resultStore;
  private final String _brokerHost;
  private final int _brokerPort;
  private final long _expirationIntervalInMs;


  public BrokerRequestHandlerDelegate(BaseSingleStageBrokerRequestHandler singleStageBrokerRequestHandler,
      @Nullable MultiStageBrokerRequestHandler multiStageBrokerRequestHandler,
      @Nullable TimeSeriesRequestHandler timeSeriesRequestHandler,
      String brokerHost, int brokerPort, AbstractResponseStore resultStore, String expirationTime) {
    _singleStageBrokerRequestHandler = singleStageBrokerRequestHandler;
    _multiStageBrokerRequestHandler = multiStageBrokerRequestHandler;
    _timeSeriesRequestHandler = timeSeriesRequestHandler;
    _brokerHost = brokerHost;
    _brokerPort = brokerPort;
    _resultStore = resultStore;
    _expirationIntervalInMs = TimeUtils.convertPeriodToMillis(expirationTime);
  }

  @Override
  public void start() {
    _singleStageBrokerRequestHandler.start();
    if (_multiStageBrokerRequestHandler != null) {
      _multiStageBrokerRequestHandler.start();
    }
    if (_timeSeriesRequestHandler != null) {
      _timeSeriesRequestHandler.start();
    }
  }

  @Override
  public void shutDown() {
    _singleStageBrokerRequestHandler.shutDown();
    if (_multiStageBrokerRequestHandler != null) {
      _multiStageBrokerRequestHandler.shutDown();
    }
    if (_timeSeriesRequestHandler != null) {
      _timeSeriesRequestHandler.shutDown();
    }
  }

  @Override
  public BrokerResponse handleRequest(JsonNode request, @Nullable SqlNodeAndOptions sqlNodeAndOptions,
      @Nullable RequesterIdentity requesterIdentity, RequestContext requestContext, @Nullable HttpHeaders httpHeaders)
      throws Exception {
    // Pinot installations may either use PinotClientRequest or this class in order to process a query that
    // arrives via their custom container. The custom code may add its own overhead in either pre-processing
    // or post-processing stages, and should be measured independently.
    // In order to accommodate for both code paths, we set the request arrival time only if it is not already set.
    if (requestContext.getRequestArrivalTimeMillis() <= 0) {
      requestContext.setRequestArrivalTimeMillis(System.currentTimeMillis());
    }
    // Parse the query if needed
    if (sqlNodeAndOptions == null) {
      try {
        sqlNodeAndOptions = RequestUtils.parseQuery(request.get(Request.SQL).asText(), request);
      } catch (Exception e) {
        // Do not log or emit metric here because it is pure user error
        requestContext.setErrorCode(QueryException.SQL_PARSING_ERROR_CODE);
        return new BrokerResponseNative(QueryException.getException(QueryException.SQL_PARSING_ERROR, e));
      }
    }

    BaseBrokerRequestHandler requestHandler = _singleStageBrokerRequestHandler;
    if (QueryOptionsUtils.isUseMultistageEngine(sqlNodeAndOptions.getOptions())) {
      if (_multiStageBrokerRequestHandler != null) {
        requestHandler = _multiStageBrokerRequestHandler;
      } else {
        return new BrokerResponseNative(QueryException.getException(QueryException.INTERNAL_ERROR,
            "V2 Multi-Stage query engine not enabled."));
      }
    }

    BrokerResponse response = requestHandler.handleRequest(request, sqlNodeAndOptions, requesterIdentity,
        requestContext, httpHeaders);

    if (response.getExceptionsSize() == 0 && QueryOptionsUtils.isGetCursor(sqlNodeAndOptions.getOptions())) {
      response = getCursorResponse(QueryOptionsUtils.getCursorNumRows(sqlNodeAndOptions.getOptions()), response);
    }
    return response;
  }

  @Override
  public PinotBrokerTimeSeriesResponse handleTimeSeriesRequest(String lang, String rawQueryParamString,
      RequestContext requestContext) {
    if (_timeSeriesRequestHandler != null) {
      return _timeSeriesRequestHandler.handleTimeSeriesRequest(lang, rawQueryParamString, requestContext);
    }
    return new PinotBrokerTimeSeriesResponse("error", null, "error", "Time series query engine not enabled.");
  }

  @Override
  public Map<Long, String> getRunningQueries() {
    // TODO: add support for multiStaged engine: track running queries for multiStaged engine and combine its
    //       running queries with those from singleStaged engine. Both engines share the same request Id generator, so
    //       the query will have unique ids across the two engines.
    return _singleStageBrokerRequestHandler.getRunningQueries();
  }

  @Override
  public boolean cancelQuery(long queryId, int timeoutMs, Executor executor, HttpClientConnectionManager connMgr,
      Map<String, Integer> serverResponses)
      throws Exception {
    // TODO: add support for multiStaged engine, basically try to cancel the query on multiStaged engine firstly; if
    //       not found, try on the singleStaged engine.
    return _singleStageBrokerRequestHandler.cancelQuery(queryId, timeoutMs, executor, connMgr, serverResponses);
  }

  private CursorResponse getCursorResponse(int numRows, BrokerResponse response)
      throws Exception {
    CursorResponse cursorResponse = createCursorResponse(response);

    long submissionTimeMs = System.currentTimeMillis();
    // Initialize all CursorResponse specific metadata
    cursorResponse.setBrokerHost(_brokerHost);
    cursorResponse.setBrokerPort(_brokerPort);
    cursorResponse.setSubmissionTimeMs(submissionTimeMs);
    cursorResponse.setExpirationTimeMs(submissionTimeMs + _expirationIntervalInMs);
    cursorResponse.setOffset(0);
    cursorResponse.setNumRows(response.getNumRowsResultSet());

    long cursorStoreStartTimeMs = System.currentTimeMillis();
    _resultStore.storeResponse(cursorResponse);
    long cursorStoreTimeMs = System.currentTimeMillis() - cursorStoreStartTimeMs;
    CursorResponse cursorResponse1 = _resultStore.handleCursorRequest(response.getRequestId(), 0, numRows);
    cursorResponse1.setCursorResultWriteTimeMs(cursorStoreTimeMs);
    return cursorResponse1;
  }

  public static CursorResponseNative createCursorResponse(BrokerResponse response) {
    CursorResponseNative responseNative = new CursorResponseNative();

    // Copy all the member variables of BrokerResponse to CursorResponse.
    responseNative.setResultTable(response.getResultTable());
    responseNative.setNumRowsResultSet(response.getNumRowsResultSet());
    responseNative.setExceptions(response.getExceptions());
    responseNative.setNumGroupsLimitReached(response.isNumGroupsLimitReached());
    responseNative.setTimeUsedMs(response.getTimeUsedMs());
    responseNative.setRequestId(response.getRequestId());
    responseNative.setBrokerId(response.getBrokerId());
    responseNative.setNumDocsScanned(response.getNumDocsScanned());
    responseNative.setTotalDocs(response.getTotalDocs());
    responseNative.setNumEntriesScannedInFilter(response.getNumEntriesScannedInFilter());
    responseNative.setNumEntriesScannedPostFilter(response.getNumEntriesScannedPostFilter());
    responseNative.setNumServersQueried(response.getNumServersQueried());
    responseNative.setNumServersResponded(response.getNumServersResponded());
    responseNative.setNumSegmentsQueried(response.getNumSegmentsQueried());
    responseNative.setNumSegmentsProcessed(response.getNumSegmentsProcessed());
    responseNative.setNumSegmentsMatched(response.getNumSegmentsMatched());
    responseNative.setNumConsumingSegmentsQueried(response.getNumConsumingSegmentsQueried());
    responseNative.setNumConsumingSegmentsProcessed(response.getNumConsumingSegmentsProcessed());
    responseNative.setNumConsumingSegmentsMatched(response.getNumConsumingSegmentsMatched());
    responseNative.setMinConsumingFreshnessTimeMs(response.getMinConsumingFreshnessTimeMs());
    responseNative.setNumSegmentsPrunedByBroker(response.getNumSegmentsPrunedByBroker());
    responseNative.setNumSegmentsPrunedByServer(response.getNumSegmentsPrunedByServer());
    responseNative.setNumSegmentsPrunedInvalid(response.getNumSegmentsPrunedInvalid());
    responseNative.setNumSegmentsPrunedByLimit(response.getNumSegmentsPrunedByLimit());
    responseNative.setNumSegmentsPrunedByValue(response.getNumSegmentsPrunedByValue());
    responseNative.setBrokerReduceTimeMs(response.getBrokerReduceTimeMs());
    responseNative.setOfflineThreadCpuTimeNs(response.getOfflineThreadCpuTimeNs());
    responseNative.setRealtimeThreadCpuTimeNs(response.getRealtimeThreadCpuTimeNs());
    responseNative.setOfflineSystemActivitiesCpuTimeNs(response.getOfflineSystemActivitiesCpuTimeNs());
    responseNative.setRealtimeSystemActivitiesCpuTimeNs(response.getRealtimeSystemActivitiesCpuTimeNs());
    responseNative.setOfflineResponseSerializationCpuTimeNs(response.getOfflineResponseSerializationCpuTimeNs());
    responseNative.setRealtimeResponseSerializationCpuTimeNs(response.getRealtimeResponseSerializationCpuTimeNs());
    responseNative.setExplainPlanNumEmptyFilterSegments(response.getExplainPlanNumEmptyFilterSegments());
    responseNative.setExplainPlanNumMatchAllFilterSegments(response.getExplainPlanNumMatchAllFilterSegments());
    responseNative.setTraceInfo(response.getTraceInfo());

    return responseNative;
  }
}
