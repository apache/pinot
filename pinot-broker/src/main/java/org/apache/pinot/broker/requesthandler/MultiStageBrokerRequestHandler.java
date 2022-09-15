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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.calcite.jdbc.CalciteSchemaBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.broker.api.RequesterIdentity;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.routing.BrokerRoutingManager;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.proto.Mailbox;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.catalog.PinotCatalog;
import org.apache.pinot.query.mailbox.GrpcMailboxService;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.QueryPlan;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.query.service.QueryConfig;
import org.apache.pinot.query.service.QueryDispatcher;
import org.apache.pinot.query.type.TypeFactory;
import org.apache.pinot.query.type.TypeSystem;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MultiStageBrokerRequestHandler extends BaseBrokerRequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(MultiStageBrokerRequestHandler.class);
  private static final long DEFAULT_TIMEOUT_NANO = 10_000_000_000L;
  private final String _reducerHostname;
  private final int _reducerPort;

  private final MailboxService<Mailbox.MailboxContent> _mailboxService;
  private final QueryEnvironment _queryEnvironment;
  private final QueryDispatcher _queryDispatcher;

  public MultiStageBrokerRequestHandler(PinotConfiguration config, BrokerRoutingManager routingManager,
      AccessControlFactory accessControlFactory, QueryQuotaManager queryQuotaManager, TableCache tableCache,
      BrokerMetrics brokerMetrics) {
    super(config, routingManager, accessControlFactory, queryQuotaManager, tableCache, brokerMetrics);
    LOGGER.info("Using Multi-stage BrokerRequestHandler.");
    String reducerHostname = config.getProperty(QueryConfig.KEY_OF_QUERY_RUNNER_HOSTNAME);
    if (reducerHostname == null) {
      // use broker ID as host name, but remove the
      String brokerId = config.getProperty(CommonConstants.Broker.CONFIG_OF_BROKER_ID);
      brokerId = brokerId.startsWith(CommonConstants.Helix.PREFIX_OF_BROKER_INSTANCE) ? brokerId.substring(
          CommonConstants.Helix.SERVER_INSTANCE_PREFIX_LENGTH) : brokerId;
      brokerId = StringUtils.split(brokerId, "_").length > 1 ? StringUtils.split(brokerId, "_")[0] : brokerId;
      reducerHostname = brokerId;
    }
    _reducerHostname = reducerHostname;
    _reducerPort = config.getProperty(QueryConfig.KEY_OF_QUERY_RUNNER_PORT, QueryConfig.DEFAULT_QUERY_RUNNER_PORT);
    _queryEnvironment = new QueryEnvironment(new TypeFactory(new TypeSystem()),
        CalciteSchemaBuilder.asRootSchema(new PinotCatalog(tableCache)),
        new WorkerManager(_reducerHostname, _reducerPort, routingManager));
    _queryDispatcher = new QueryDispatcher();
    _mailboxService = new GrpcMailboxService(_reducerHostname, _reducerPort, config);

    // TODO: move this to a startUp() function.
    _mailboxService.start();
  }

  @Override
  public BrokerResponse handleRequest(JsonNode request, @Nullable SqlNodeAndOptions sqlNodeAndOptions,
      @Nullable RequesterIdentity requesterIdentity, RequestContext requestContext)
      throws Exception {
    long requestId = _requestIdGenerator.incrementAndGet();
    requestContext.setBrokerId(_brokerId);
    requestContext.setRequestId(requestId);
    requestContext.setRequestArrivalTimeMillis(System.currentTimeMillis());

    // First-stage access control to prevent unauthenticated requests from using up resources. Secondary table-level
    // check comes later.
    boolean hasAccess = _accessControlFactory.create().hasAccess(requesterIdentity);
    if (!hasAccess) {
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.REQUEST_DROPPED_DUE_TO_ACCESS_ERROR, 1);
      LOGGER.info("Access denied for requestId {}", requestId);
      requestContext.setErrorCode(QueryException.ACCESS_DENIED_ERROR_CODE);
      return new BrokerResponseNative(QueryException.ACCESS_DENIED_ERROR);
    }

    JsonNode sql = request.get(CommonConstants.Broker.Request.SQL);
    if (sql == null) {
      throw new BadQueryRequestException("Failed to find 'sql' in the request: " + request);
    }
    String query = sql.asText();
    requestContext.setQuery(query);
    return handleRequest(requestId, query, sqlNodeAndOptions, request, requesterIdentity, requestContext);
  }

  private BrokerResponseNative handleRequest(long requestId, String query,
      @Nullable SqlNodeAndOptions sqlNodeAndOptions, JsonNode request, @Nullable RequesterIdentity requesterIdentity,
      RequestContext requestContext)
      throws Exception {
    LOGGER.debug("SQL query for request {}: {}", requestId, query);

    long compilationStartTimeNs;
    QueryPlan queryPlan;
    try {
      // Parse the request
      sqlNodeAndOptions = sqlNodeAndOptions != null ? sqlNodeAndOptions : RequestUtils.parseQuery(query, request);
      // Compile the request
      compilationStartTimeNs = System.nanoTime();
      switch (sqlNodeAndOptions.getSqlNode().getKind()) {
        case EXPLAIN:
          String plan = _queryEnvironment.explainQuery(query, sqlNodeAndOptions);
          return constructMultistageExplainPlan(query, plan);
        case SELECT:
        default:
          queryPlan = _queryEnvironment.planQuery(query, sqlNodeAndOptions);
          break;
      }
    } catch (Exception e) {
      LOGGER.info("Caught exception while compiling SQL request {}: {}, {}", requestId, query, e.getMessage());
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.REQUEST_COMPILATION_EXCEPTIONS, 1);
      requestContext.setErrorCode(QueryException.SQL_PARSING_ERROR_CODE);
      return new BrokerResponseNative(QueryException.getException(QueryException.SQL_PARSING_ERROR, e));
    }

    List<DataTable> queryResults = null;
    try {
      queryResults = _queryDispatcher.submitAndReduce(requestId, queryPlan, _mailboxService, DEFAULT_TIMEOUT_NANO);
    } catch (Exception e) {
      LOGGER.info("query execution failed", e);
      return new BrokerResponseNative(QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, e));
    }

    BrokerResponseNative brokerResponse = new BrokerResponseNative();
    long executionEndTimeNs = System.nanoTime();

    // Set total query processing time
    long totalTimeMs = TimeUnit.NANOSECONDS.toMillis(sqlNodeAndOptions.getParseTimeNs()
        + (executionEndTimeNs - compilationStartTimeNs));
    brokerResponse.setTimeUsedMs(totalTimeMs);
    brokerResponse.setResultTable(toResultTable(queryResults));
    requestContext.setQueryProcessingTime(totalTimeMs);
    augmentStatistics(requestContext, brokerResponse);
    return brokerResponse;
  }

  private BrokerResponseNative constructMultistageExplainPlan(String sql, String plan) {
    BrokerResponseNative brokerResponse = BrokerResponseNative.empty();
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{sql, plan});
    DataSchema multistageExplainResultSchema = new DataSchema(new String[]{"SQL", "PLAN"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING});
    brokerResponse.setResultTable(new ResultTable(multistageExplainResultSchema, rows));
    return brokerResponse;
  }

  @Override
  protected BrokerResponseNative processBrokerRequest(long requestId, BrokerRequest originalBrokerRequest,
      BrokerRequest serverBrokerRequest, @Nullable BrokerRequest offlineBrokerRequest,
      @Nullable Map<ServerInstance, List<String>> offlineRoutingTable, @Nullable BrokerRequest realtimeBrokerRequest,
      @Nullable Map<ServerInstance, List<String>> realtimeRoutingTable, long timeoutMs, ServerStats serverStats,
      RequestContext requestContext)
      throws Exception {
    throw new UnsupportedOperationException();
  }

  private ResultTable toResultTable(List<DataTable> queryResult) {
    DataSchema resultDataSchema = null;
    List<Object[]> resultRows = new ArrayList<>();
    for (DataTable dataTable : queryResult) {
      resultDataSchema = resultDataSchema == null ? dataTable.getDataSchema() : resultDataSchema;
      int numColumns = resultDataSchema.getColumnNames().length;
      DataSchema.ColumnDataType[] resultColumnDataTypes = resultDataSchema.getColumnDataTypes();
      List<Object[]> rows = new ArrayList<>(dataTable.getNumberOfRows());
      for (int rowId = 0; rowId < dataTable.getNumberOfRows(); rowId++) {
        Object[] row = new Object[numColumns];
        Object[] rawRow = SelectionOperatorUtils.extractRowFromDataTable(dataTable, rowId);
        for (int i = 0; i < numColumns; i++) {
          row[i] = resultColumnDataTypes[i].convertAndFormat(rawRow[i]);
        }
        rows.add(row);
      }
      resultRows.addAll(rows);
    }
    return new ResultTable(resultDataSchema, resultRows);
  }

  @Override
  public void start() {
    // no-op
  }

  @Override
  public void shutDown() {
    _queryDispatcher.shutdown();
    _mailboxService.shutdown();
  }
}
