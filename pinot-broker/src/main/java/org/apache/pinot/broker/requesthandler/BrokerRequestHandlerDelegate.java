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
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.pinot.broker.api.RequesterIdentity;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@code BrokerRequestHandlerDelegate} delegates the inbound broker request to one of the enabled
 * {@link BrokerRequestHandler} based on the requested handle type.
 *
 * {@see: @CommonConstant
 */
public class BrokerRequestHandlerDelegate implements BrokerRequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerRequestHandlerDelegate.class);

  private final BrokerRequestHandler _singleStageBrokerRequestHandler;
  private final BrokerRequestHandler _multiStageWorkerRequestHandler;
  private final BrokerMetrics _brokerMetrics;

  public BrokerRequestHandlerDelegate(BrokerRequestHandler singleStageBrokerRequestHandler,
      @Nullable BrokerRequestHandler multiStageWorkerRequestHandler, BrokerMetrics brokerMetrics) {
    _singleStageBrokerRequestHandler = singleStageBrokerRequestHandler;
    _multiStageWorkerRequestHandler = multiStageWorkerRequestHandler;
    _brokerMetrics = brokerMetrics;
  }

  @Override
  public void start() {
    if (_singleStageBrokerRequestHandler != null) {
      _singleStageBrokerRequestHandler.start();
    }
    if (_multiStageWorkerRequestHandler != null) {
      _multiStageWorkerRequestHandler.start();
    }
  }

  @Override
  public void shutDown() {
    if (_singleStageBrokerRequestHandler != null) {
      _singleStageBrokerRequestHandler.shutDown();
    }
    if (_multiStageWorkerRequestHandler != null) {
      _multiStageWorkerRequestHandler.shutDown();
    }
  }

  @Override
  public BrokerResponse handleRequest(JsonNode request, @Nullable SqlNodeAndOptions sqlNodeAndOptions,
      @Nullable RequesterIdentity requesterIdentity, RequestContext requestContext)
      throws Exception {
    if (sqlNodeAndOptions == null) {
      JsonNode sql = request.get(Request.SQL);
      if (sql == null) {
        throw new BadQueryRequestException("Failed to find 'sql' in the request: " + request);
      }
      try {
        sqlNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(sql.asText());
      } catch (Exception e) {
        LOGGER.info("Caught exception while compiling SQL: {}, {}", sql.asText(), e.getMessage());
        _brokerMetrics.addMeteredGlobalValue(BrokerMeter.REQUEST_COMPILATION_EXCEPTIONS, 1);
        requestContext.setErrorCode(QueryException.SQL_PARSING_ERROR_CODE);
        return new BrokerResponseNative(QueryException.getException(QueryException.SQL_PARSING_ERROR, e));
      }
    }

    if (_multiStageWorkerRequestHandler != null && useMultiStageEngine(request, sqlNodeAndOptions)) {
      return _multiStageWorkerRequestHandler.handleRequest(request, sqlNodeAndOptions, requesterIdentity,
          requestContext);
    } else {
      return _singleStageBrokerRequestHandler.handleRequest(request, sqlNodeAndOptions, requesterIdentity,
          requestContext);
    }
  }

  private boolean useMultiStageEngine(JsonNode request, SqlNodeAndOptions sqlNodeAndOptions) {
    Map<String, String> optionsFromSql = sqlNodeAndOptions.getOptions();
    if (Boolean.parseBoolean(optionsFromSql.get(QueryOptionKey.USE_MULTISTAGE_ENGINE))) {
      return true;
    }
    if (request.has(Request.QUERY_OPTIONS)) {
      Map<String, String> optionsFromRequest =
          BaseBrokerRequestHandler.getOptionsFromJson(request, Request.QUERY_OPTIONS);
      return Boolean.parseBoolean(optionsFromRequest.get(QueryOptionKey.USE_MULTISTAGE_ENGINE));
    }
    return false;
  }

  @Override
  public Map<Long, String> getRunningQueries() {
    // TODO: add support for multiStaged engine: track running queries for multiStaged engine and combine its
    //       running queries with those from singleStaged engine. Both engines share the same request Id generator, so
    //       the query will have unique ids across the two engines.
    return _singleStageBrokerRequestHandler.getRunningQueries();
  }

  @Override
  public boolean cancelQuery(long queryId, int timeoutMs, Executor executor, HttpConnectionManager connMgr,
      Map<String, Integer> serverResponses)
      throws Exception {
    // TODO: add support for multiStaged engine, basically try to cancel the query on multiStaged engine firstly; if
    //       not found, try on the singleStaged engine.
    return _singleStageBrokerRequestHandler.cancelQuery(queryId, timeoutMs, executor, connMgr, serverResponses);
  }
}
