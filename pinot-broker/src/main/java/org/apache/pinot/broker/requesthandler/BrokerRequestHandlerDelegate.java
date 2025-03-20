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
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;
import javax.ws.rs.core.HttpHeaders;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.pinot.broker.api.RequesterIdentity;
import org.apache.pinot.common.cursors.AbstractResponseStore;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.CursorResponse;
import org.apache.pinot.common.response.PinotBrokerTimeSeriesResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request;
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
  private final AbstractResponseStore _responseStore;

  public BrokerRequestHandlerDelegate(BaseSingleStageBrokerRequestHandler singleStageBrokerRequestHandler,
      @Nullable MultiStageBrokerRequestHandler multiStageBrokerRequestHandler,
      @Nullable TimeSeriesRequestHandler timeSeriesRequestHandler, AbstractResponseStore responseStore) {
    _singleStageBrokerRequestHandler = singleStageBrokerRequestHandler;
    _multiStageBrokerRequestHandler = multiStageBrokerRequestHandler;
    _timeSeriesRequestHandler = timeSeriesRequestHandler;
    _responseStore = responseStore;
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
        requestContext.setErrorCode(QueryErrorCode.SQL_PARSING);
        return new BrokerResponseNative(QueryErrorCode.SQL_PARSING, e.getMessage());
      }
    }

    BaseBrokerRequestHandler requestHandler = _singleStageBrokerRequestHandler;
    if (QueryOptionsUtils.isUseMultistageEngine(sqlNodeAndOptions.getOptions())) {
      if (_multiStageBrokerRequestHandler != null) {
        requestHandler = _multiStageBrokerRequestHandler;
      } else {
        return new BrokerResponseNative(QueryErrorCode.INTERNAL, "V2 Multi-Stage query engine not enabled.");
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
    // Both engines share the same request ID generator, so the query will have unique IDs across the two engines.
    Map<Long, String> queries = new HashMap<>(_singleStageBrokerRequestHandler.getRunningQueries());
    if (_multiStageBrokerRequestHandler != null) {
      queries.putAll(_multiStageBrokerRequestHandler.getRunningQueries());
    }
    return queries;
  }

  @Override
  public boolean cancelQuery(long queryId, int timeoutMs, Executor executor, HttpClientConnectionManager connMgr,
      Map<String, Integer> serverResponses)
      throws Exception {
    if (_multiStageBrokerRequestHandler != null && _multiStageBrokerRequestHandler.cancelQuery(
        queryId, timeoutMs, executor, connMgr, serverResponses)) {
        return true;
    }
    return _singleStageBrokerRequestHandler.cancelQuery(queryId, timeoutMs, executor, connMgr, serverResponses);
  }

  @Override
  public boolean cancelQueryByClientId(String clientQueryId, int timeoutMs, Executor executor,
      HttpClientConnectionManager connMgr, Map<String, Integer> serverResponses)
      throws Exception {
    if (_multiStageBrokerRequestHandler != null && _multiStageBrokerRequestHandler.cancelQueryByClientId(
        clientQueryId, timeoutMs, executor, connMgr, serverResponses)) {
      return true;
    }
    return _singleStageBrokerRequestHandler.cancelQueryByClientId(
        clientQueryId, timeoutMs, executor, connMgr, serverResponses);
  }

  @Override
  public OptionalLong getRequestIdByClientId(String clientQueryId) {
    if (_multiStageBrokerRequestHandler != null) {
      OptionalLong mseReqId = _multiStageBrokerRequestHandler.getRequestIdByClientId(clientQueryId);
      if (mseReqId.isPresent()) {
        return mseReqId;
      }
    }
    return _singleStageBrokerRequestHandler.getRequestIdByClientId(clientQueryId);
  }

  private CursorResponse getCursorResponse(Integer numRows, BrokerResponse response)
      throws Exception {
    if (numRows == null) {
      throw new RuntimeException(
          "numRows not specified when requesting a cursor for request id: " + response.getRequestId());
    }
    long cursorStoreStartTimeMs = System.currentTimeMillis();
    _responseStore.storeResponse(response);
    long cursorStoreTimeMs = System.currentTimeMillis() - cursorStoreStartTimeMs;
    CursorResponse cursorResponse = _responseStore.handleCursorRequest(response.getRequestId(), 0, numRows);
    cursorResponse.setCursorResultWriteTimeMs(cursorStoreTimeMs);
    return cursorResponse;
  }
}
