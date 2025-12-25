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
import java.util.HashSet;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;
import javax.ws.rs.core.HttpHeaders;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.pinot.common.cursors.AbstractResponseStore;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.CursorResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.auth.broker.RequesterIdentity;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@code BrokerRequestHandlerDelegate} delegates the inbound broker request to one of the enabled
 * {@link BrokerRequestHandler} implementations based on the request type.
 */
public class BrokerRequestHandlerDelegate implements BrokerRequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerRequestHandlerDelegate.class);

  private final BaseSingleStageBrokerRequestHandler _singleStageBrokerRequestHandler;
  private final SystemTableBrokerRequestHandler _systemTableBrokerRequestHandler;
  private final MultiStageBrokerRequestHandler _multiStageBrokerRequestHandler;
  private final TimeSeriesRequestHandler _timeSeriesRequestHandler;
  private final AbstractResponseStore _responseStore;

  public BrokerRequestHandlerDelegate(BaseSingleStageBrokerRequestHandler singleStageBrokerRequestHandler,
      SystemTableBrokerRequestHandler systemTableBrokerRequestHandler,
      @Nullable MultiStageBrokerRequestHandler multiStageBrokerRequestHandler,
      @Nullable TimeSeriesRequestHandler timeSeriesRequestHandler, AbstractResponseStore responseStore) {
    _singleStageBrokerRequestHandler = singleStageBrokerRequestHandler;
    _systemTableBrokerRequestHandler = systemTableBrokerRequestHandler;
    _multiStageBrokerRequestHandler = multiStageBrokerRequestHandler;
    _timeSeriesRequestHandler = timeSeriesRequestHandler;
    _responseStore = responseStore;
  }

  @Override
  public void start() {
    _systemTableBrokerRequestHandler.start();
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
    _systemTableBrokerRequestHandler.shutDown();
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
    } else if (_systemTableBrokerRequestHandler != null) {
      try {
        Set<String> tableNames = extractTableNames(sqlNodeAndOptions.getSqlNode());
        if (tableNames != null && tableNames.size() == 1
            && _systemTableBrokerRequestHandler.canHandle(tableNames.iterator().next())) {
          requestHandler = _systemTableBrokerRequestHandler;
        }
      } catch (Exception e) {
        // Ignore exceptions here; the selected request handler will surface them appropriately.
        LOGGER.debug("Failed to extract table names while determining request handler", e);
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
  public TimeSeriesBlock handleTimeSeriesRequest(String lang, String rawQueryParamString,
      Map<String, String> queryParams, RequestContext requestContext, RequesterIdentity requesterIdentity,
      HttpHeaders httpHeaders) throws QueryException {
    if (_timeSeriesRequestHandler != null) {
      return _timeSeriesRequestHandler.handleTimeSeriesRequest(lang, rawQueryParamString, queryParams, requestContext,
          requesterIdentity, httpHeaders);
    }
    throw new QueryException(QueryErrorCode.INTERNAL, "Time series query engine not enabled.");
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

  @Nullable
  private static Set<String> extractTableNames(SqlNode sqlNode) {
    if (sqlNode == null) {
      return null;
    }
    SqlNodeTableNameExtractor extractor = new SqlNodeTableNameExtractor();
    extractor.extractTableNames(sqlNode);
    Set<String> tableNames = extractor.getTableNames();
    return tableNames.isEmpty() ? null : tableNames;
  }

  private static final class SqlNodeTableNameExtractor {
    private final Set<String> _tableNames = new HashSet<>();
    private final Set<String> _cteNames = new HashSet<>();
    private boolean _inFromClause;

    Set<String> getTableNames() {
      return _tableNames;
    }

    void extractTableNames(SqlNode node) {
      if (node == null) {
        return;
      }
      if (node instanceof SqlExplain) {
        extractTableNames(((SqlExplain) node).getExplicandum());
      } else if (node instanceof SqlWith) {
        visitWith((SqlWith) node);
      } else if (node instanceof SqlOrderBy) {
        visitOrderBy((SqlOrderBy) node);
      } else if (node instanceof SqlWithItem) {
        visitWithItem((SqlWithItem) node);
      } else if (node instanceof SqlSelect) {
        visitSelect((SqlSelect) node);
      } else if (node instanceof SqlJoin) {
        visitJoin((SqlJoin) node);
      } else if (node instanceof SqlBasicCall) {
        visitBasicCall((SqlBasicCall) node);
      } else if (node instanceof SqlIdentifier) {
        visitIdentifier((SqlIdentifier) node);
      } else if (node instanceof SqlNodeList) {
        visitNodeList((SqlNodeList) node);
      }
    }

    private void visitWith(SqlWith with) {
      if (with.withList != null) {
        visitNodeList(with.withList);
      }
      if (with.body != null) {
        extractTableNames(with.body);
      }
    }

    private void visitOrderBy(SqlOrderBy orderBy) {
      if (orderBy.query != null) {
        extractTableNames(orderBy.query);
      }
      if (orderBy.orderList != null) {
        visitNodeList(orderBy.orderList);
      }
      if (orderBy.offset != null) {
        extractTableNames(orderBy.offset);
      }
      if (orderBy.fetch != null) {
        extractTableNames(orderBy.fetch);
      }
    }

    private void visitWithItem(SqlWithItem withItem) {
      if (withItem.name != null) {
        _cteNames.add(withItem.name.getSimple());
      }
      if (withItem.query != null) {
        extractTableNames(withItem.query);
      }
    }

    private void visitSelect(SqlSelect select) {
      boolean wasInFromClause = _inFromClause;
      try {
        if (select.getFrom() != null) {
          _inFromClause = true;
          extractTableNames(select.getFrom());
        }
        _inFromClause = false;
        if (select.getWhere() != null) {
          extractTableNames(select.getWhere());
        }
        if (select.getGroup() != null) {
          visitNodeList(select.getGroup());
        }
        if (select.getHaving() != null) {
          extractTableNames(select.getHaving());
        }
        if (select.getOrderList() != null) {
          visitNodeList(select.getOrderList());
        }
        if (select.getSelectList() != null) {
          visitNodeList(select.getSelectList());
        }
      } finally {
        _inFromClause = wasInFromClause;
      }
    }

    private void visitJoin(SqlJoin join) {
      boolean wasInFromClause = _inFromClause;
      try {
        if (join.getLeft() != null) {
          _inFromClause = true;
          extractTableNames(join.getLeft());
        }
        if (join.getRight() != null) {
          _inFromClause = true;
          extractTableNames(join.getRight());
        }
        if (join.getCondition() != null) {
          _inFromClause = false;
          extractTableNames(join.getCondition());
        }
      } finally {
        _inFromClause = wasInFromClause;
      }
    }

    private void visitBasicCall(SqlBasicCall call) {
      if (call.getKind() == SqlKind.AS) {
        if (!call.getOperandList().isEmpty() && call.getOperandList().get(0) != null) {
          extractTableNames(call.getOperandList().get(0));
        }
        return;
      }
      if (call.getKind() == SqlKind.VALUES) {
        return;
      }
      for (SqlNode operand : call.getOperandList()) {
        if (operand != null) {
          extractTableNames(operand);
        }
      }
    }

    private void visitIdentifier(SqlIdentifier identifier) {
      if (!_inFromClause) {
        return;
      }
      if (identifier.isSimple() && _cteNames.contains(identifier.getSimple())) {
        return;
      }
      String tableName = identifier.toString();
      if (!tableName.isEmpty() && !tableName.startsWith("$")) {
        _tableNames.add(tableName);
      }
    }

    private void visitNodeList(SqlNodeList nodeList) {
      for (SqlNode node : nodeList) {
        extractTableNames(node);
      }
    }
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
