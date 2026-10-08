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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.broker.AllowAllAccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.common.failuredetector.FailureDetector;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNativeV2;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.query.utils.idset.IdSet;
import org.apache.pinot.core.query.utils.idset.IdSets;
import org.apache.pinot.core.routing.MockRoutingManagerFactory;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.auth.TableAuthorizationResult;
import org.apache.pinot.spi.auth.broker.RequesterIdentity;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.eventlistener.query.BrokerQueryEventListenerFactory;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.trace.DefaultRequestContext;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests how [MultiStageBrokerRequestHandler] runs the IdSet subqueries of a query, see [IdSetSubqueryRewriter].
///
/// Queries plan against the tables of [QueryEnvironmentTestBase]. The handler answers the subqueries it is given
/// responses for, and no server runs, so a query that gets past planning stops at the mocked throttler.
public class MultiStageIdSetSubqueryTest {
  private static final long REQUEST_ID = 1L;
  private static final String SUBQUERY = "SELECT IDSET(col1) FROM b";
  // The text of a scalar subquery includes its parentheses
  private static final String SCALAR_SUBQUERY = "(" + SUBQUERY + ")";
  private static final String ID_SET;

  static {
    IdSet idSet = IdSets.create(DataType.STRING);
    idSet.add("foo");
    try {
      ID_SET = idSet.toBase64String();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private final RequesterIdentity _requesterIdentity = mock(RequesterIdentity.class);
  private final HttpHeaders _httpHeaders = mock(HttpHeaders.class);
  // Responses to the subqueries, and errors they throw, by subquery
  private final Map<String, BrokerResponse> _subqueryResponses = new HashMap<>();
  private final Map<String, QueryException> _subqueryErrors = new HashMap<>();
  // All the requests the handler runs, including the queries of the tests
  private final List<Request> _requests = new ArrayList<>();
  private final List<RequestContext> _completedRequests = new ArrayList<>();

  @BeforeMethod
  public void setUp() {
    _subqueryResponses.clear();
    _subqueryErrors.clear();
    _requests.clear();
    _completedRequests.clear();
  }

  @Test
  public void testSubqueryInheritsTheOptionsAndTheTimeLeft()
      throws Exception {
    String subquery = "SET numGroupsLimit = 8; " + SUBQUERY;
    _subqueryResponses.put(subquery, idSetResponse(ID_SET));
    String sql = "SET timeoutMs = 60000; SET clientQueryId = 'myQuery'; SET dropResults = true; "
        + "SET numGroupsLimit = 7; SET maxRowsInJoin = 9; "
        + "SELECT COUNT(*) FROM a WHERE IN_SUBQUERY(col1, '" + subquery + "') = 1";
    SqlNodeAndOptions sqlNodeAndOptions = RequestUtils.parseQuery(sql);
    long startTimeMs = System.currentTimeMillis();
    handleRequest(newHandler(new AllowAllAccessControlFactory()), sql, sqlNodeAndOptions);

    Request request = getSubqueryRequest();
    assertNotEquals(request._requestId, REQUEST_ID);
    assertEquals(request._query, subquery);
    assertEquals(request._requestContext.getRequestId(), request._requestId);
    assertEquals(request._requestContext.getQuery(), subquery);
    assertTrue(request._requestContext.getRequestArrivalTimeMillis() >= startTimeMs);
    assertSame(request._requesterIdentity, _requesterIdentity);
    assertSame(request._httpHeaders, _httpHeaders);
    Map<String, String> options = request._options;
    // The options set in the subquery override those of the query
    assertEquals(options.get(QueryOptionKey.NUM_GROUPS_LIMIT), "8");
    assertEquals(options.get(QueryOptionKey.MAX_ROWS_IN_JOIN), "9");
    // The client query id lets the client cancel the subquery, which fails the query
    assertEquals(options.get(QueryOptionKey.CLIENT_QUERY_ID), "myQuery");
    assertFalse(options.containsKey(QueryOptionKey.DROP_RESULTS));
    long timeoutMs = Long.parseLong(options.get(QueryOptionKey.TIMEOUT_MS));
    assertTrue(timeoutMs > 0 && timeoutMs <= 60_000, "Timeout: " + timeoutMs);

    // The query uses the IdSet, and its options do not change
    String rewrittenQuery = sqlNodeAndOptions.getSqlNode().toString();
    assertTrue(rewrittenQuery.contains(ID_SET), rewrittenQuery);
    assertFalse(rewrittenQuery.toUpperCase().contains("IN_SUBQUERY"), rewrittenQuery);
    assertEquals(sqlNodeAndOptions.getOptions().get(QueryOptionKey.NUM_GROUPS_LIMIT), "7");
    assertEquals(sqlNodeAndOptions.getOptions().get(QueryOptionKey.DROP_RESULTS), "true");

    // The subquery is reported to the query event listener like any other query
    assertEquals(_completedRequests.size(), 1);
    assertSame(_completedRequests.get(0), request._requestContext);
  }

  @Test
  public void testSubqueryGetsTheTimeLeftOfTheQuery()
      throws Exception {
    _subqueryResponses.put(SUBQUERY, idSetResponse(ID_SET));
    String sql = "SET timeoutMs = 10000; SELECT COUNT(*) FROM a WHERE IN_SUBQUERY(col1, '" + SUBQUERY + "') = 1";
    MultiStageBrokerRequestHandler handler = newHandler(new AllowAllAccessControlFactory());

    // The query arrived 4 seconds ago, so the subquery gets at most the 6 seconds left
    RequestContext requestContext = new DefaultRequestContext();
    requestContext.setRequestArrivalTimeMillis(System.currentTimeMillis() - 4_000);
    handler.handleRequestThrowing(REQUEST_ID, sql, RequestUtils.parseQuery(sql), null, requestContext, null);
    long timeoutMs = Long.parseLong(getSubqueryRequest()._options.get(QueryOptionKey.TIMEOUT_MS));
    assertTrue(timeoutMs > 0 && timeoutMs <= 6_000, "Timeout: " + timeoutMs);

    // A shorter timeout of the subquery wins
    _requests.clear();
    String subquery = "SET timeoutMs = 100; " + SUBQUERY;
    _subqueryResponses.put(subquery, idSetResponse(ID_SET));
    String shorterTimeoutSql = "SELECT COUNT(*) FROM a WHERE IN_SUBQUERY(col1, '" + subquery + "') = 1";
    handleRequest(handler, shorterTimeoutSql, RequestUtils.parseQuery(shorterTimeoutSql));
    assertEquals(getSubqueryRequest()._options.get(QueryOptionKey.TIMEOUT_MS), "100");

    // No time left
    _requests.clear();
    requestContext.setRequestArrivalTimeMillis(System.currentTimeMillis() - 10_000);
    QueryException exception = expectThrows(QueryException.class,
        () -> handler.handleRequestThrowing(REQUEST_ID, sql, RequestUtils.parseQuery(sql), null, requestContext,
            null));
    assertEquals(exception.getErrorCode(), QueryErrorCode.BROKER_TIMEOUT);
    assertNoSubqueryRan();
  }

  @Test
  public void testUsesTheEmptyIdSetForNoRowsOrNull()
      throws Exception {
    MultiStageBrokerRequestHandler handler = newHandler(new AllowAllAccessControlFactory());
    for (BrokerResponse response : List.of(response(stringColumn(), List.of()),
        response(stringColumn(), List.<Object[]>of(new Object[]{null})))) {
      _subqueryResponses.put(SUBQUERY, response);
      String sql = "SELECT COUNT(*) FROM a WHERE IN_SUBQUERY(col1, '" + SUBQUERY + "') = 0";
      SqlNodeAndOptions sqlNodeAndOptions = RequestUtils.parseQuery(sql);
      handleRequest(handler, sql, sqlNodeAndOptions);
      String rewrittenQuery = sqlNodeAndOptions.getSqlNode().toString();
      assertTrue(rewrittenQuery.contains("'" + IdSetSubqueryRewriter.EMPTY_ID_SET + "'"), rewrittenQuery);
    }
  }

  @Test
  public void testFailsWhenTheSubqueryFails()
      throws Exception {
    MultiStageBrokerRequestHandler handler = newHandler(new AllowAllAccessControlFactory());
    String sql = "SELECT COUNT(*) FROM a WHERE IN_SUBQUERY(col1, '" + SUBQUERY + "') = 1";

    BrokerResponseNativeV2 failedResponse = new BrokerResponseNativeV2();
    failedResponse.addException(new QueryProcessingException(QueryErrorCode.EXECUTION_TIMEOUT, "Too slow"));
    _subqueryResponses.put(SUBQUERY, failedResponse);
    assertFails(handler, sql, QueryErrorCode.EXECUTION_TIMEOUT, "Subquery failed: " + SUBQUERY + ": Too slow");

    _subqueryResponses.clear();
    _subqueryErrors.put(SUBQUERY, QueryErrorCode.QUERY_VALIDATION.asException("Unknown column"));
    String message = assertFails(handler, sql, QueryErrorCode.QUERY_VALIDATION, "Subquery failed: " + SUBQUERY);
    assertTrue(message.contains("Unknown column"), message);
    _subqueryErrors.clear();

    // An IdSet built from part of the rows would make the query miss rows
    BrokerResponseNativeV2 partialResponse = (BrokerResponseNativeV2) idSetResponse(ID_SET);
    partialResponse.mergeNumGroupsLimitReached(true);
    _subqueryResponses.put(SUBQUERY, partialResponse);
    assertFails(handler, sql, QueryErrorCode.QUERY_EXECUTION,
        "Subquery returned a partial result [numGroupsLimitReached]: " + SUBQUERY);
    partialResponse = (BrokerResponseNativeV2) idSetResponse(ID_SET);
    partialResponse.mergeMaxRowsInWindowReached(true);
    _subqueryResponses.put(SUBQUERY, partialResponse);
    assertFails(handler, sql, QueryErrorCode.QUERY_EXECUTION,
        "Subquery returned a partial result [maxRowsInWindowReached]: " + SUBQUERY);

    _subqueryResponses.put(SUBQUERY, response(new DataSchema(new String[]{"a", "b"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.STRING}), List.of()));
    assertFails(handler, sql, QueryErrorCode.QUERY_VALIDATION, "Subquery must return one STRING column");

    _subqueryResponses.put(SUBQUERY,
        response(new DataSchema(new String[]{"a"}, new ColumnDataType[]{ColumnDataType.LONG}), List.of()));
    assertFails(handler, sql, QueryErrorCode.QUERY_VALIDATION, "Subquery must return one STRING column");

    _subqueryResponses.put(SUBQUERY,
        response(stringColumn(), List.of(new Object[]{ID_SET}, new Object[]{ID_SET})));
    assertFails(handler, sql, QueryErrorCode.QUERY_EXECUTION, "Subquery must return at most one row, got: 2");
  }

  @Test
  public void testFailsOnSubqueriesThatAreNotQueriesOrSetRejectedOptions()
      throws Exception {
    MultiStageBrokerRequestHandler handler = newHandler(new AllowAllAccessControlFactory());
    assertFails(handler, "SELECT COUNT(*) FROM a WHERE IN_SUBQUERY(col1, 'EXPLAIN PLAN FOR " + SUBQUERY + "') = 1",
        QueryErrorCode.QUERY_VALIDATION, "Subquery is not a query");
    assertFails(handler, "SELECT COUNT(*) FROM a WHERE IN_SUBQUERY(col1, 'SELECT FROM') = 1",
        QueryErrorCode.SQL_PARSING, "Failed to parse subquery");

    // The request does not allow options in the SQL, so the subquery cannot set any either
    String sql = "SELECT COUNT(*) FROM a WHERE IN_SUBQUERY(col1, 'SET numGroupsLimit = 8; " + SUBQUERY + "') = 1";
    SqlNodeAndOptions sqlNodeAndOptions = RequestUtils.parseQuery(sql);
    sqlNodeAndOptions.getOptions().put(QueryOptionKey.SQL_OPTIONS_MODE, "REJECT");
    QueryException exception = expectThrows(QueryException.class,
        () -> handleRequest(handler, sql, sqlNodeAndOptions));
    assertEquals(exception.getErrorCode(), QueryErrorCode.QUERY_VALIDATION);
    assertTrue(exception.getMessage().contains("Query options are not allowed in the SQL"), exception.getMessage());
    assertNoSubqueryRan();
  }

  @Test
  public void testChecksAccessToTheTablesOfTheSubquery()
      throws Exception {
    // Denies table b, which only the subquery reads. The test does not answer the subquery, so it compiles and gets
    // checked like any other query.
    AccessControl denyB = new AccessControl() {
      @Override
      public TableAuthorizationResult authorize(RequesterIdentity requesterIdentity, Set<String> tables) {
        Set<String> failedTables = tables.stream().filter(table -> table.startsWith("b")).collect(Collectors.toSet());
        return failedTables.isEmpty() ? TableAuthorizationResult.success() : new TableAuthorizationResult(failedTables);
      }
    };
    MultiStageBrokerRequestHandler handler = newHandler(new AccessControlFactory() {
      @Override
      public AccessControl create() {
        return denyB;
      }
    });
    String sql = "SELECT COUNT(*) FROM a WHERE IN_SUBQUERY(col1, '" + SUBQUERY + "') = 1";
    WebApplicationException exception = expectThrows(WebApplicationException.class,
        () -> handleRequest(handler, sql, RequestUtils.parseQuery(sql)));
    assertEquals(exception.getResponse().getStatus(), 403);
  }

  @Test
  public void testExplainRunsNoSubqueries()
      throws Exception {
    MultiStageBrokerRequestHandler handler = newHandler(new AllowAllAccessControlFactory());
    _subqueryResponses.put(SUBQUERY, idSetResponse(ID_SET));
    _subqueryResponses.put(SCALAR_SUBQUERY, idSetResponse(ID_SET));
    String sql = "EXPLAIN PLAN FOR SELECT COUNT(*) FROM a WHERE IN_SUBQUERY(col1, '" + SUBQUERY + "') = 1 "
        + "AND IN_ID_SET(col2, " + SCALAR_SUBQUERY + ") = 1";
    BrokerResponse response = handleRequest(handler, sql, RequestUtils.parseQuery(sql));
    assertNoSubqueryRan();
    assertTrue(response.getExceptions().isEmpty(), response.getExceptions().toString());
    ResultTable resultTable = response.getResultTable();
    List<String> columnNames = List.of(resultTable.getDataSchema().getColumnNames());
    assertEquals(columnNames.get(columnNames.size() - 1), "ID_SET_SUBQUERIES_NOT_RUN");
    Object[] row = resultTable.getRows().get(0);
    String plan = (String) row[1];
    assertTrue(plan.contains("'" + IdSetSubqueryRewriter.EMPTY_ID_SET + "'"), plan);
    assertFalse(plan.contains("Join"), plan);
    assertEquals(row[columnNames.size() - 1], "[\"" + SUBQUERY + "\",\"" + SCALAR_SUBQUERY + "\"]");

    // Without IdSet subqueries, the response has no extra column
    String plainSql = "EXPLAIN PLAN FOR SELECT COUNT(*) FROM a";
    response = handleRequest(handler, plainSql, RequestUtils.parseQuery(plainSql));
    assertFalse(List.of(response.getResultTable().getDataSchema().getColumnNames())
        .contains("ID_SET_SUBQUERIES_NOT_RUN"));
  }

  @Test
  public void testRunsUncorrelatedScalarSubqueriesFirst()
      throws Exception {
    MultiStageBrokerRequestHandler handler = newHandler(new AllowAllAccessControlFactory());
    _subqueryResponses.put(SCALAR_SUBQUERY, idSetResponse(ID_SET));
    String sql = "SELECT COUNT(*) FROM a WHERE IN_ID_SET(col1, " + SCALAR_SUBQUERY + ") = 1";
    SqlNodeAndOptions sqlNodeAndOptions = RequestUtils.parseQuery(sql);
    handleRequest(handler, sql, sqlNodeAndOptions);
    assertEquals(getSubqueryRequest()._query, SCALAR_SUBQUERY);
    assertTrue(sqlNodeAndOptions.getSqlNode().toString().contains(ID_SET));

    // A correlated subquery does not validate on its own, so it runs as part of the query
    _requests.clear();
    String correlatedSubquery = "(SELECT IDSET(b.col1) FROM b WHERE b.col2 = a.col2)";
    _subqueryResponses.put(correlatedSubquery, idSetResponse(ID_SET));
    String correlatedSql = "SELECT COUNT(*) FROM a WHERE IN_ID_SET(a.col1, " + correlatedSubquery + ") = 1";
    handleRequest(handler, correlatedSql, RequestUtils.parseQuery(correlatedSql));
    assertNoSubqueryRan();

    // On its own, the subquery would read table b instead of the WITH item b
    _requests.clear();
    String withSql = "WITH b AS (SELECT col1 FROM c WHERE col2 = 'foo') "
        + "SELECT COUNT(*) FROM a WHERE IN_ID_SET(col1, " + SCALAR_SUBQUERY + ") = 1";
    handleRequest(handler, withSql, RequestUtils.parseQuery(withSql));
    assertNoSubqueryRan();
  }

  /// Checks that the query fails with the error code and a message that contains the given one. Returns the message.
  private String assertFails(MultiStageBrokerRequestHandler handler, String sql, QueryErrorCode errorCode,
      String message) {
    QueryException exception =
        expectThrows(QueryException.class, () -> handleRequest(handler, sql, RequestUtils.parseQuery(sql)));
    assertEquals(exception.getErrorCode(), errorCode);
    assertTrue(exception.getMessage().contains(message), exception.getMessage());
    return exception.getMessage();
  }

  private BrokerResponse handleRequest(MultiStageBrokerRequestHandler handler, String sql,
      SqlNodeAndOptions sqlNodeAndOptions) {
    RequestContext requestContext = new DefaultRequestContext();
    requestContext.setRequestArrivalTimeMillis(System.currentTimeMillis());
    return handler.handleRequestThrowing(REQUEST_ID, sql, sqlNodeAndOptions, _requesterIdentity, requestContext,
        _httpHeaders);
  }

  /// Returns the only subquery request, i.e. the only request other than the query of the test.
  private Request getSubqueryRequest() {
    List<Request> subqueryRequests = getSubqueryRequests();
    assertEquals(subqueryRequests.size(), 1, subqueryRequests.toString());
    return subqueryRequests.get(0);
  }

  private void assertNoSubqueryRan() {
    assertEquals(getSubqueryRequests(), List.of());
  }

  private List<Request> getSubqueryRequests() {
    return _requests.stream().filter(request -> request._requestId != REQUEST_ID).collect(Collectors.toList());
  }

  private static BrokerResponse idSetResponse(String idSet) {
    return response(stringColumn(), List.<Object[]>of(new Object[]{idSet}));
  }

  private static DataSchema stringColumn() {
    return new DataSchema(new String[]{"idset(col1)"}, new ColumnDataType[]{ColumnDataType.STRING});
  }

  private static BrokerResponse response(DataSchema dataSchema, List<Object[]> rows) {
    BrokerResponseNativeV2 response = new BrokerResponseNativeV2();
    response.setResultTable(new ResultTable(dataSchema, rows));
    return response;
  }

  private MultiStageBrokerRequestHandler newHandler(AccessControlFactory accessControlFactory)
      throws IOException {
    int port1 = NetUtils.findOpenPort();
    int port2 = NetUtils.findOpenPort();
    MockRoutingManagerFactory factory = new MockRoutingManagerFactory(port1, port2);
    QueryEnvironmentTestBase.TABLE_SCHEMAS.forEach((tableName, schema) -> factory.registerTable(schema, tableName));
    QueryEnvironmentTestBase.SERVER1_SEGMENTS.forEach(
        (tableName, segments) -> segments.forEach(segment -> factory.registerSegment(port1, tableName, segment)));
    QueryEnvironmentTestBase.SERVER2_SEGMENTS.forEach(
        (tableName, segments) -> segments.forEach(segment -> factory.registerSegment(port2, tableName, segment)));
    RoutingManager routingManager = factory.buildRoutingManager(null);
    WorkerManager workerManager =
        new WorkerManager("Broker_localhost", "localhost", NetUtils.findOpenPort(), routingManager);

    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_HOSTNAME, "localhost");
    config.setProperty(MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT, Integer.toString(NetUtils.findOpenPort()));
    BrokerQueryEventListenerFactory.init(config);
    BrokerMetrics.register(mock(BrokerMetrics.class));
    QueryQuotaManager queryQuotaManager = mock(QueryQuotaManager.class);
    when(queryQuotaManager.acquire(anyString())).thenReturn(true);
    when(queryQuotaManager.acquireDatabase(anyString())).thenReturn(true);

    return new MultiStageBrokerRequestHandler(config, "testBrokerId", new BrokerRequestIdGenerator(), routingManager,
        accessControlFactory, queryQuotaManager, factory.buildTableCache(), mock(MultiStageQueryThrottler.class),
        mock(FailureDetector.class), ThreadAccountantUtils.getNoOpAccountant(), null, workerManager, workerManager) {
      @Override
      public void start() {
      }

      @Override
      public void shutDown() {
      }

      @Override
      protected BrokerResponse handleRequestThrowing(long requestId, String query,
          SqlNodeAndOptions sqlNodeAndOptions, @Nullable RequesterIdentity requesterIdentity,
          RequestContext requestContext, @Nullable HttpHeaders httpHeaders) {
        _requests.add(new Request(requestId, query, new HashMap<>(sqlNodeAndOptions.getOptions()), requestContext,
            requesterIdentity, httpHeaders));
        QueryException error = _subqueryErrors.get(query);
        if (error != null) {
          throw error;
        }
        BrokerResponse response = _subqueryResponses.get(query);
        return response != null
            ? response
            : super.handleRequestThrowing(requestId, query, sqlNodeAndOptions, requesterIdentity, requestContext,
                httpHeaders);
      }

      @Override
      protected void onQueryCompletion(RequestContext requestContext, BrokerResponse brokerResponse) {
        _completedRequests.add(requestContext);
      }
    };
  }

  private static class Request {
    final long _requestId;
    final String _query;
    final Map<String, String> _options;
    final RequestContext _requestContext;
    final RequesterIdentity _requesterIdentity;
    final HttpHeaders _httpHeaders;

    Request(long requestId, String query, Map<String, String> options, RequestContext requestContext,
        @Nullable RequesterIdentity requesterIdentity, @Nullable HttpHeaders httpHeaders) {
      _requestId = requestId;
      _query = query;
      _options = options;
      _requestContext = requestContext;
      _requesterIdentity = requesterIdentity;
      _httpHeaders = httpHeaders;
    }

    @Override
    public String toString() {
      return _requestId + ": " + _query;
    }
  }
}
