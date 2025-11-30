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
import com.google.common.annotations.VisibleForTesting;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.querylog.QueryLogger;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.client.ConnectionTimeouts;
import org.apache.pinot.client.PinotClientException;
import org.apache.pinot.client.SystemTableDataTableClient;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTableImplV4;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.QuerySource;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.systemtable.SystemTableProvider;
import org.apache.pinot.common.systemtable.SystemTableRegistry;
import org.apache.pinot.common.utils.NamedThreadFactory;
import org.apache.pinot.common.utils.config.InstanceUtils;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.instance.context.BrokerContext;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.plan.Plan;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.plan.maker.PlanMaker;
import org.apache.pinot.core.query.reduce.BrokerReduceService;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.routing.MultiClusterRoutingContext;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.spi.accounting.ThreadAccountant;
import org.apache.pinot.spi.auth.AuthorizationResult;
import org.apache.pinot.spi.auth.broker.RequesterIdentity;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Broker request handler for system tables (handled entirely on the broker).
 */
public class SystemTableBrokerRequestHandler extends BaseBrokerRequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(SystemTableBrokerRequestHandler.class);
  private static final String SYSTEM_TABLE_PSEUDO_HOST = "localhost";
  private static final int SYSTEM_TABLE_PSEUDO_PORT = 0;
  private static final String SYSTEM_TABLE_DATATABLE_API_PATH = "/query/systemTable/datatable";
  // Hop-by-hop headers per RFC 7230 plus content-length/host which are request-specific.
  private static final Set<String> HOP_BY_HOP_HEADERS_TO_SKIP = Set.of(
      "connection",
      "keep-alive",
      "proxy-authenticate",
      "proxy-authorization",
      "te",
      "trailer",
      "transfer-encoding",
      "upgrade",
      "host",
      "content-length");

  private final BrokerReduceService _brokerReduceService;
  private final PlanMaker _planMaker;
  private final ExecutorService _executorService;
  private final ExecutorService _scatterGatherExecutorService;
  private final SystemTableDataTableClient _systemTableDataTableClient;
  @Nullable
  private final HelixManager _helixManager;

  public SystemTableBrokerRequestHandler(PinotConfiguration config, String brokerId,
      BrokerRequestIdGenerator requestIdGenerator, RoutingManager routingManager,
      AccessControlFactory accessControlFactory, QueryQuotaManager queryQuotaManager, TableCache tableCache,
      ThreadAccountant threadAccountant, @Nullable MultiClusterRoutingContext multiClusterRoutingContext,
      @Nullable HelixManager helixManager) {
    super(config, brokerId, requestIdGenerator, routingManager, accessControlFactory, queryQuotaManager, tableCache,
        threadAccountant, multiClusterRoutingContext);
    _brokerReduceService = new BrokerReduceService(_config);
    _planMaker = new InstancePlanMakerImplV2();
    _planMaker.init(_config);
    _helixManager = helixManager;
    int executorPoolSize = config.getProperty(CommonConstants.Broker.CONFIG_OF_SYSTEM_TABLE_EXECUTOR_POOL_SIZE,
        CommonConstants.Broker.DEFAULT_SYSTEM_TABLE_EXECUTOR_POOL_SIZE);
    executorPoolSize = Math.max(1, executorPoolSize);
    _executorService = QueryThreadContext.contextAwareExecutorService(Executors.newFixedThreadPool(executorPoolSize,
        new NamedThreadFactory("system-table-query-executor")));
    _scatterGatherExecutorService =
        QueryThreadContext.contextAwareExecutorService(Executors.newFixedThreadPool(executorPoolSize,
            new NamedThreadFactory("system-table-scatter-gather-executor")));
    SSLContext sslContext = BrokerContext.getInstance().getClientHttpsContext();
    int timeoutMs = (int) Math.min(Integer.MAX_VALUE, _brokerTimeoutMs);
    if (timeoutMs < 1) {
      LOGGER.warn("Configured broker timeout {}ms is non-positive; clamping to 1ms for system table client connections",
          _brokerTimeoutMs);
      timeoutMs = 1;
    }
    ConnectionTimeouts connectionTimeouts = ConnectionTimeouts.create(timeoutMs, timeoutMs, timeoutMs);
    _systemTableDataTableClient =
        new SystemTableDataTableClient(connectionTimeouts, sslContext);
  }

  @Override
  public void start() {
  }

  @Override
  public void shutDown() {
    _executorService.shutdownNow();
    _scatterGatherExecutorService.shutdownNow();
    try {
      _systemTableDataTableClient.close();
    } catch (Exception e) {
      LOGGER.debug("Failed to close system table data table client: {}", e.toString());
    }
    _brokerReduceService.shutDown();
  }

  public boolean canHandle(String tableName) {
    return isSystemTable(tableName) && SystemTableRegistry.isRegistered(tableName);
  }

  @Override
  protected BrokerResponse handleRequest(long requestId, String query, SqlNodeAndOptions sqlNodeAndOptions,
      JsonNode request, @Nullable RequesterIdentity requesterIdentity, RequestContext requestContext,
      @Nullable HttpHeaders httpHeaders, AccessControl accessControl)
      throws Exception {
    long startTimeMs = requestContext.getRequestArrivalTimeMillis();
    long deadlineMs = startTimeMs + _brokerTimeoutMs;
    QueryExecutionContext executionContext =
        new QueryExecutionContext(QueryExecutionContext.QueryType.STE, requestId, Long.toString(requestId),
            QueryOptionsUtils.getWorkloadName(sqlNodeAndOptions.getOptions()), startTimeMs, deadlineMs, deadlineMs,
            _brokerId, _brokerId, org.apache.pinot.spi.utils.CommonConstants.Broker.DEFAULT_QUERY_HASH);
    try (QueryThreadContext ignore = QueryThreadContext.open(executionContext, _threadAccountant)) {
      PinotQuery pinotQuery;
      try {
        pinotQuery = CalciteSqlParser.compileToPinotQuery(sqlNodeAndOptions);
      } catch (Exception e) {
        requestContext.setErrorCode(QueryErrorCode.SQL_PARSING);
        return new BrokerResponseNative(QueryErrorCode.SQL_PARSING, e.getMessage());
      }

      Set<String> tableNames = RequestUtils.getTableNames(pinotQuery);
      if (tableNames == null || tableNames.isEmpty()) {
        requestContext.setErrorCode(QueryErrorCode.QUERY_VALIDATION);
        return new BrokerResponseNative(QueryErrorCode.QUERY_VALIDATION, "Failed to extract table name");
      }
      if (tableNames.size() != 1) {
        requestContext.setErrorCode(QueryErrorCode.QUERY_VALIDATION);
        return new BrokerResponseNative(QueryErrorCode.QUERY_VALIDATION, "System tables do not support joins");
      }
      String tableName = tableNames.iterator().next();
      if (!isSystemTable(tableName)) {
        requestContext.setErrorCode(QueryErrorCode.QUERY_VALIDATION);
        return new BrokerResponseNative(QueryErrorCode.QUERY_VALIDATION, "Not a system table query");
      }
      AuthorizationResult authorizationResult =
          hasTableAccess(requesterIdentity, Set.of(tableName), requestContext, httpHeaders);
      if (!authorizationResult.hasAccess()) {
        requestContext.setErrorCode(QueryErrorCode.ACCESS_DENIED);
        return new BrokerResponseNative(QueryErrorCode.ACCESS_DENIED, authorizationResult.getFailureMessage());
      }

      boolean queryWasLogged = _queryLogger.logQueryReceived(requestId, query);
      return handleSystemTableQuery(request, pinotQuery, tableName, requestContext, requesterIdentity, query,
          httpHeaders, queryWasLogged);
    }
  }

  @Override
  protected boolean handleCancel(long queryId, int timeoutMs, Executor executor,
      HttpClientConnectionManager connMgr, Map<String, Integer> serverResponses) {
    return false;
  }

  @Override
  public boolean cancelQueryByClientId(String clientQueryId, int timeoutMs, Executor executor,
      HttpClientConnectionManager connMgr, Map<String, Integer> serverResponses)
      throws Exception {
    return false;
  }

  @Override
  public Map<Long, String> getRunningQueries() {
    return Collections.emptyMap();
  }

  @Override
  public OptionalLong getRequestIdByClientId(String clientQueryId) {
    return OptionalLong.empty();
  }

  private boolean isSystemTable(String tableName) {
    return tableName != null && tableName.toLowerCase(Locale.ROOT).startsWith("system.");
  }

  /**
   * Executes a system table query against the local broker and returns the raw {@link DataTable} results.
   * <p>
   * This method is used by the internal broker-to-broker scatter-gather endpoint and must never perform fanout.
   */
  public DataTable handleSystemTableDataTableRequest(JsonNode request, @Nullable RequesterIdentity requesterIdentity,
      RequestContext requestContext, @Nullable HttpHeaders httpHeaders) {
    long startTimeMs = requestContext.getRequestArrivalTimeMillis();
    if (startTimeMs <= 0) {
      startTimeMs = System.currentTimeMillis();
      requestContext.setRequestArrivalTimeMillis(startTimeMs);
    }
    long requestId = _requestIdGenerator.get();
    long deadlineMs = startTimeMs + _brokerTimeoutMs;

    JsonNode sql = request.get(CommonConstants.Broker.Request.SQL);
    if (sql == null || !sql.isTextual()) {
      return exceptionDataTable(QueryErrorCode.JSON_PARSING, "Failed to find 'sql' in the request: " + request);
    }
    String query = sql.textValue();
    requestContext.setQuery(query);

    SqlNodeAndOptions sqlNodeAndOptions;
    try {
      sqlNodeAndOptions = RequestUtils.parseQuery(query, request);
    } catch (Exception e) {
      requestContext.setErrorCode(QueryErrorCode.SQL_PARSING);
      return exceptionDataTable(QueryErrorCode.SQL_PARSING, e.getMessage());
    }

    QueryExecutionContext executionContext =
        new QueryExecutionContext(QueryExecutionContext.QueryType.STE, requestId, Long.toString(requestId),
            QueryOptionsUtils.getWorkloadName(sqlNodeAndOptions.getOptions()), startTimeMs, deadlineMs, deadlineMs,
            _brokerId, _brokerId, org.apache.pinot.spi.utils.CommonConstants.Broker.DEFAULT_QUERY_HASH);
    try (QueryThreadContext ignore = QueryThreadContext.open(executionContext, _threadAccountant)) {
      AccessControl accessControl = _accessControlFactory.create();
      AuthorizationResult authorizationResult = accessControl.authorize(requesterIdentity);
      if (!authorizationResult.hasAccess()) {
        requestContext.setErrorCode(QueryErrorCode.ACCESS_DENIED);
        return exceptionDataTable(QueryErrorCode.ACCESS_DENIED, authorizationResult.getFailureMessage());
      }

      PinotQuery pinotQuery;
      try {
        pinotQuery = CalciteSqlParser.compileToPinotQuery(sqlNodeAndOptions);
      } catch (Exception e) {
        requestContext.setErrorCode(QueryErrorCode.SQL_PARSING);
        return exceptionDataTable(QueryErrorCode.SQL_PARSING, e.getMessage());
      }

      Set<String> tableNames = RequestUtils.getTableNames(pinotQuery);
      if (tableNames == null || tableNames.isEmpty()) {
        requestContext.setErrorCode(QueryErrorCode.QUERY_VALIDATION);
        return exceptionDataTable(QueryErrorCode.QUERY_VALIDATION, "Failed to extract table name");
      }
      if (tableNames.size() != 1) {
        requestContext.setErrorCode(QueryErrorCode.QUERY_VALIDATION);
        return exceptionDataTable(QueryErrorCode.QUERY_VALIDATION, "System tables do not support joins");
      }
      String tableName = tableNames.iterator().next();
      if (!isSystemTable(tableName)) {
        requestContext.setErrorCode(QueryErrorCode.QUERY_VALIDATION);
        return exceptionDataTable(QueryErrorCode.QUERY_VALIDATION, "Not a system table query");
      }

      AuthorizationResult tableAuthorizationResult =
          hasTableAccess(requesterIdentity, Set.of(tableName), requestContext, httpHeaders);
      if (!tableAuthorizationResult.hasAccess()) {
        requestContext.setErrorCode(QueryErrorCode.ACCESS_DENIED);
        return exceptionDataTable(QueryErrorCode.ACCESS_DENIED, tableAuthorizationResult.getFailureMessage());
      }

      SystemTableProvider provider = SystemTableRegistry.get(tableName);
      if (provider == null) {
        requestContext.setErrorCode(QueryErrorCode.TABLE_DOES_NOT_EXIST);
        return exceptionDataTable(QueryErrorCode.TABLE_DOES_NOT_EXIST, "System table does not exist: " + tableName);
      }

      try {
        return executeLocalSystemTableQuery(pinotQuery, provider);
      } catch (BadQueryRequestException e) {
        requestContext.setErrorCode(QueryErrorCode.QUERY_VALIDATION);
        _brokerMetrics.addMeteredGlobalValue(BrokerMeter.QUERY_VALIDATION_EXCEPTIONS, 1);
        return exceptionDataTable(QueryErrorCode.QUERY_VALIDATION, e.getMessage());
      } catch (Exception e) {
        LOGGER.warn("Caught exception while handling system table datatable query {}: {}", tableName, e.getMessage(),
            e);
        requestContext.setErrorCode(QueryErrorCode.QUERY_EXECUTION);
        return exceptionDataTable(QueryErrorCode.QUERY_EXECUTION, e.getMessage());
      }
    }
  }

  private BrokerResponse handleSystemTableQuery(JsonNode request, PinotQuery pinotQuery, String tableName,
      RequestContext requestContext, @Nullable RequesterIdentity requesterIdentity, String query,
      @Nullable HttpHeaders httpHeaders, boolean queryWasLogged) {
    if (pinotQuery.isExplain()) {
      return BrokerResponseNative.BROKER_ONLY_EXPLAIN_PLAN_OUTPUT;
    }
    SystemTableProvider provider = SystemTableRegistry.get(tableName);
    if (provider == null) {
      requestContext.setErrorCode(QueryErrorCode.TABLE_DOES_NOT_EXIST);
      return BrokerResponseNative.TABLE_DOES_NOT_EXIST;
    }
    try {
      Map<ServerRoutingInstance, DataTable> dataTableMap;
      if (provider.getExecutionMode() == SystemTableProvider.ExecutionMode.BROKER_SCATTER_GATHER) {
        dataTableMap = scatterGatherSystemTableDataTables(provider, pinotQuery, tableName, request, httpHeaders);
      } else {
        dataTableMap = new HashMap<>(1);
        // Use a synthetic routing instance for broker-local execution of system table queries.
        dataTableMap.put(new ServerRoutingInstance(SYSTEM_TABLE_PSEUDO_HOST, SYSTEM_TABLE_PSEUDO_PORT,
            TableType.OFFLINE), executeLocalSystemTableQuery(pinotQuery, provider));
      }

      BrokerResponseNative brokerResponse;
      BrokerRequest brokerRequest = new BrokerRequest();
      QuerySource querySource = new QuerySource();
      querySource.setTableName(tableName);
      brokerRequest.setQuerySource(querySource);
      brokerRequest.setPinotQuery(pinotQuery);
      brokerResponse = _brokerReduceService.reduceOnDataTable(brokerRequest, brokerRequest, dataTableMap,
          _brokerTimeoutMs, _brokerMetrics);
      brokerResponse.setTablesQueried(Set.of(TableNameBuilder.extractRawTableName(tableName)));
      brokerResponse.setTimeUsedMs(System.currentTimeMillis() - requestContext.getRequestArrivalTimeMillis());
      _queryLogger.logQueryCompleted(
          new QueryLogger.QueryLogParams(requestContext, tableName, brokerResponse,
              QueryLogger.QueryLogParams.QueryEngine.SINGLE_STAGE, requesterIdentity, null),
          queryWasLogged);
      return brokerResponse;
    } catch (BadQueryRequestException e) {
      requestContext.setErrorCode(QueryErrorCode.QUERY_VALIDATION);
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.QUERY_VALIDATION_EXCEPTIONS, 1);
      return new BrokerResponseNative(QueryErrorCode.QUERY_VALIDATION, e.getMessage());
    } catch (Exception e) {
      LOGGER.warn("Caught exception while handling system table query {}: {}", tableName, e.getMessage(), e);
      requestContext.setErrorCode(QueryErrorCode.QUERY_EXECUTION);
      return new BrokerResponseNative(QueryErrorCode.QUERY_EXECUTION, e.getMessage());
    }
  }

  private DataTable executeLocalSystemTableQuery(PinotQuery pinotQuery, SystemTableProvider provider)
      throws Exception {
    IndexSegment dataSource = provider.getDataSource();
    try {
      QueryContext queryContext = QueryContextConverterUtils.getQueryContext(pinotQuery);
      queryContext.setSchema(provider.getSchema());
      queryContext.setEndTimeMs(System.currentTimeMillis() + _brokerTimeoutMs);

      // Pass null for serverMetrics because system table queries run broker-local against an in-memory IndexSegment.
      Plan plan = _planMaker.makeInstancePlan(List.of(new SegmentContext(dataSource)), queryContext, _executorService,
          null);
      InstanceResponseBlock instanceResponse = plan.execute();
      return instanceResponse.toDataTable();
    } finally {
      dataSource.destroy();
    }
  }

  private static final class BrokerTarget {
    final ServerRoutingInstance _routingInstance;
    final String _dataTableUrl;

    BrokerTarget(ServerRoutingInstance routingInstance, String dataTableUrl) {
      _routingInstance = routingInstance;
      _dataTableUrl = dataTableUrl;
    }
  }

  @VisibleForTesting
  protected Map<ServerRoutingInstance, DataTable> scatterGatherSystemTableDataTables(SystemTableProvider provider,
      PinotQuery pinotQuery, String tableName, JsonNode request, @Nullable HttpHeaders httpHeaders) {
    if (_helixManager == null) {
      throw new IllegalStateException(
          "HelixManager is required for scatter-gather execution of system table: " + tableName);
    }

    HelixDataAccessor dataAccessor = _helixManager.getHelixDataAccessor();
    List<String> liveInstances = dataAccessor.getChildNames(dataAccessor.keyBuilder().liveInstances());
    if (liveInstances == null || liveInstances.isEmpty()) {
      throw new IllegalStateException("No live instances found for scatter-gather execution of system table: "
          + tableName);
    }

    String localInstanceId = _brokerId;
    List<BrokerTarget> remoteTargets = new ArrayList<>();
    @Nullable ServerRoutingInstance localRoutingInstance = null;
    for (String instanceId : liveInstances) {
      if (!InstanceTypeUtils.isBroker(instanceId)) {
        continue;
      }
      InstanceConfig instanceConfig = dataAccessor.getProperty(dataAccessor.keyBuilder().instanceConfig(instanceId));
      if (instanceConfig == null) {
        continue;
      }
      URI baseUri = URI.create(InstanceUtils.getInstanceBaseUri(instanceConfig));
      ServerRoutingInstance routingInstance = new ServerRoutingInstance(baseUri.getHost(), baseUri.getPort(),
          TableType.OFFLINE);
      if (instanceId.equals(localInstanceId)) {
        localRoutingInstance = routingInstance;
      } else {
        remoteTargets.add(new BrokerTarget(routingInstance, baseUri.toString() + SYSTEM_TABLE_DATATABLE_API_PATH));
      }
    }

    Map<ServerRoutingInstance, DataTable> dataTableMap = new HashMap<>(remoteTargets.size() + 1);
    ServerRoutingInstance routingInstance;
    if (localRoutingInstance != null) {
      routingInstance = localRoutingInstance;
    } else {
      routingInstance = new ServerRoutingInstance(SYSTEM_TABLE_PSEUDO_HOST, SYSTEM_TABLE_PSEUDO_PORT,
          TableType.OFFLINE);
    }
    try {
      dataTableMap.put(routingInstance, executeLocalSystemTableQuery(pinotQuery, provider));
    } catch (Exception e) {
      LOGGER.error("Failed to execute system table query locally for query: {}", pinotQuery, e);
      dataTableMap.put(routingInstance,
          exceptionDataTable(QueryErrorCode.QUERY_EXECUTION, "Failed to execute system table query locally: "
              + e.getMessage()));
    }

    if (remoteTargets.isEmpty()) {
      return dataTableMap;
    }

    String requestBody = request.toString();
    @Nullable Map<String, String> requestHeaders = toSingleValueRequestHeaders(httpHeaders);
    if (requestHeaders == null) {
      requestHeaders = new HashMap<>();
    }
    requestHeaders.putIfAbsent("Content-Type", MediaType.APPLICATION_JSON);
    Map<String, String> requestHeadersFinal = requestHeaders;
    List<Pair<BrokerTarget, Future<DataTable>>> futures = new ArrayList<>(remoteTargets.size());
    for (BrokerTarget target : remoteTargets) {
      Future<DataTable> future =
          _scatterGatherExecutorService.submit(() -> fetchDataTableFromBroker(target._dataTableUrl, requestBody,
              requestHeadersFinal));
      futures.add(Pair.of(target, future));
    }

    for (Pair<BrokerTarget, Future<DataTable>> pair : futures) {
      BrokerTarget target = pair.getLeft();
      try {
        dataTableMap.put(target._routingInstance, pair.getRight().get());
      } catch (Exception e) {
        // Unexpected errors should be surfaced in the merged response.
        dataTableMap.put(target._routingInstance, exceptionDataTable(QueryErrorCode.BROKER_REQUEST_SEND,
            "Failed to gather system table response from " + target._dataTableUrl + ": " + e.getMessage()));
      }
    }
    return dataTableMap;
  }

  private DataTable fetchDataTableFromBroker(String url, String requestBody, Map<String, String> requestHeaders) {
    try {
      return _systemTableDataTableClient.fetchDataTable(url, requestBody, requestHeaders);
    } catch (PinotClientException e) {
      return exceptionDataTable(QueryErrorCode.BROKER_REQUEST_SEND,
          String.format("Scatter-gather system table request failed for %s: %s", url, e.getMessage()));
    }
  }

  private static @Nullable Map<String, String> toSingleValueRequestHeaders(@Nullable HttpHeaders httpHeaders) {
    if (httpHeaders == null) {
      return null;
    }
    Map<String, String> requestHeaders = new HashMap<>();
    for (Map.Entry<String, List<String>> entry : httpHeaders.getRequestHeaders().entrySet()) {
      String headerName = entry.getKey();
      // Do not forward hop-by-hop headers or content-specific headers that may be invalid for the new request body.
      // See https://www.rfc-editor.org/rfc/rfc7230#section-6.1
      String headerNameLower = headerName.toLowerCase(Locale.ROOT);
      if (HOP_BY_HOP_HEADERS_TO_SKIP.contains(headerNameLower)) {
        continue;
      }
      if (entry.getValue() != null && !entry.getValue().isEmpty()) {
        requestHeaders.put(headerName, entry.getValue().get(0));
      }
    }
    return requestHeaders;
  }

  private static DataTable exceptionDataTable(QueryErrorCode errorCode, String message) {
    DataTable dataTable = new DataTableImplV4();
    dataTable.addException(errorCode, message);
    return dataTable;
  }
}
