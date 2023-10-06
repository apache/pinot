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
import lombok.AllArgsConstructor;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.pinot.broker.api.RequesterIdentity;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@code BrokerRequestHandlerDelegate} delegates the inbound broker request to one of the enabled
 * {@link BrokerRequestHandler} based on the requested handle type.
 *
 * {@see: @CommonConstant
 */
@AllArgsConstructor
public class BrokerRequestHandlerDelegate implements BrokerRequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerRequestHandlerDelegate.class);

  private final String _brokerId;
  private final BrokerRequestHandler _singleStageBrokerRequestHandler;
  @Nullable
  private final BrokerRequestHandler _multiStageBrokerRequestHandler;
  private final BrokerMetrics _brokerMetrics;

  @Override
  public void start() {
    if (_singleStageBrokerRequestHandler != null) {
      _singleStageBrokerRequestHandler.start();
    }
    if (_multiStageBrokerRequestHandler != null) {
      _multiStageBrokerRequestHandler.start();
    }
  }

  @Override
  public void shutDown() {
    if (_singleStageBrokerRequestHandler != null) {
      _singleStageBrokerRequestHandler.shutDown();
    }
    if (_multiStageBrokerRequestHandler != null) {
      _multiStageBrokerRequestHandler.shutDown();
    }
  }

  @Override
  public BrokerResponse handleRequest(JsonNode request, @Nullable SqlNodeAndOptions sqlNodeAndOptions,
      @Nullable RequesterIdentity requesterIdentity, RequestContext requestContext, HttpHeaders httpHeaders)
      throws Exception {
    requestContext.setBrokerId(_brokerId);
    if (sqlNodeAndOptions == null) {
      try {
        sqlNodeAndOptions = RequestUtils.parseQuery(request.get(CommonConstants.Broker.Request.SQL).asText(), request);
      } catch (Exception e) {
        LOGGER.info("Caught exception while compiling SQL: {}, {}", request, e.getMessage());
        _brokerMetrics.addMeteredGlobalValue(BrokerMeter.REQUEST_COMPILATION_EXCEPTIONS, 1);
        requestContext.setErrorCode(QueryException.SQL_PARSING_ERROR_CODE);
        return new BrokerResponseNative(QueryException.getException(QueryException.SQL_PARSING_ERROR, e));
      }
    }
    if (request.has(CommonConstants.Broker.Request.QUERY_OPTIONS)) {
      sqlNodeAndOptions.setExtraOptions(RequestUtils.getOptionsFromJson(request,
          CommonConstants.Broker.Request.QUERY_OPTIONS));
    }

    if (_multiStageBrokerRequestHandler != null && Boolean.parseBoolean(sqlNodeAndOptions.getOptions().get(
          CommonConstants.Broker.Request.QueryOptionKey.USE_MULTISTAGE_ENGINE))) {
        return _multiStageBrokerRequestHandler.handleRequest(request, requesterIdentity, requestContext, httpHeaders);
    } else {
      return _singleStageBrokerRequestHandler.handleRequest(request, sqlNodeAndOptions, requesterIdentity,
          requestContext, httpHeaders);
    }
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
}
