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
package org.apache.pinot.controller.api.resources;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.StreamingOutput;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.common.utils.config.QueryOptionsUtils.SqlOptionsMode;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.access.AccessControl;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.core.query.executor.sql.SqlQueryExecutor;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.apache.pinot.sql.parsers.dml.DataManipulationStatement;
import org.apache.pinot.sql.parsers.dml.DataManipulationStatementParser;
import org.apache.pinot.sql.parsers.dml.DeleteStatement;
import org.mockito.AdditionalAnswers;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class PinotQueryResourceTest {
  private static final Map<String, String> REQUEST_HEADERS = Map.of("Authorization", "Basic abc");

  @Mock
  PinotHelixResourceManager _resourceManager;
  @Mock
  TableCache _tableCache;
  @Mock
  Schema _schema;
  @Mock
  AccessControlFactory _accessControlFactory;
  @Mock
  ControllerConf _controllerConf;
  @Mock
  SqlQueryExecutor _sqlQueryExecutor;
  @InjectMocks
  PinotQueryResource _pinotQueryResource;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    when(_tableCache.getActualTableName(any())).then(AdditionalAnswers.returnsFirstArg());
    when(_resourceManager.getTableCache()).thenReturn(_tableCache);
    when(_tableCache.getSchema(any())).thenReturn(_schema);
  }

  @Test
  public void testV2QueryOnV1() {
    String response = streamingOutputToString(
        _pinotQueryResource.handleGetSql("WITH tmp AS (SELECT * FROM a) SELECT * FROM tmp", null, null, null)
    );
    Assert.assertTrue(response.contains(String.valueOf(QueryErrorCode.SQL_PARSING.getId())));
    Assert.assertTrue(response.contains("retry the query using the multi-stage query engine"));
  }

  @Test
  public void testInvalidQuery() {
    String response = streamingOutputToString(
        _pinotQueryResource.handleGetSql("INVALID QUERY", null, null, null)
    );
    Assert.assertTrue(response.contains(String.valueOf(QueryErrorCode.SQL_PARSING.getId())));
    Assert.assertFalse(response.contains("retry the query using the multi-stage query engine"));
  }

  @Test
  public void testDdlOnQueryEndpointReturnsValidationError() {
    String response = streamingOutputToString(
        _pinotQueryResource.handleGetSql("CREATE TABLE t (id INT) TABLE_TYPE = OFFLINE", null, null, null)
    );
    Assert.assertTrue(response.contains(String.valueOf(QueryErrorCode.QUERY_VALIDATION.getId())));
    Assert.assertTrue(response.contains("/sql/ddl"));
  }

  @Test
  public void testDeleteOnGetQueryEndpointReturnsParsingError() {
    String response = streamingOutputToString(
        _pinotQueryResource.handleGetSql("DELETE FROM t WHERE a = 1", null, null, null));
    // The error code of the broker for a statement that is not a query on its GET endpoint
    assertTrue(response.contains(String.valueOf(QueryErrorCode.SQL_PARSING.getId())), response);
    assertTrue(response.contains("GET /sql only supports DQL; use POST /sql instead"), response);
    verify(_sqlQueryExecutor, never()).executeDMLStatement(any(), any());
    verify(_sqlQueryExecutor, never()).executeStatement(any(), any());
    verify(_accessControlFactory, never()).create();
  }

  @Test
  public void testInsertOnGetQueryEndpointReturnsParsingError() {
    String response = streamingOutputToString(
        _pinotQueryResource.handleGetSql("INSERT INTO t FROM FILE 'file:///tmp/data'", null, null, null));
    assertTrue(response.contains(String.valueOf(QueryErrorCode.SQL_PARSING.getId())), response);
    assertTrue(response.contains("GET /sql only supports DQL; use POST /sql instead"), response);
    verify(_sqlQueryExecutor, never()).executeDMLStatement(any(), any());
    verify(_sqlQueryExecutor, never()).executeStatement(any(), any());
  }

  @Test
  public void testDeleteIsExecutedOnTheAuthorizedTable() {
    RecordingAccessControl accessControl = allowEveryCheck();
    // Table names are case-insensitive: the DELETE is authorized and executed on the table as it is defined
    when(_tableCache.isIgnoreCase()).thenReturn(true);
    when(_tableCache.getActualTableName("db1.T")).thenReturn("db1.t");

    String response = postSql("DELETE FROM T WHERE a = 1", "db1");

    assertFalse(response.contains("errorCode"), response);
    // The caller-level check before the table is looked up, then READ and QUERY on the table because the WHERE clause
    // reads it, then DELETE and DELETE_ROWS on it: stricter than the query path of the controller, which only checks
    // READ
    assertEquals(accessControl._checks, List.of("READ null", "READ db1.t", Actions.Table.QUERY + " TABLE db1.t",
        "DELETE db1.t", Actions.Table.DELETE_ROWS + " TABLE db1.t"));
    DeleteStatement statement = executedDelete();
    assertEquals(statement.getTableName(), "db1.t");
    assertEquals(statement.getPredicate(), "a = 1");
    verify(_sqlQueryExecutor, never()).executeDMLStatement(any(), any());
  }

  @Test
  public void testDeleteFromATableWithATypeIsAuthorizedOnItsRawName() {
    RecordingAccessControl accessControl = allowEveryCheck();

    postSql("DELETE FROM t_OFFLINE WHERE a = 1", null);

    // Like the table APIs of the controller, while the executor deletes from the table named by the statement
    assertEquals(accessControl._checks, List.of("READ null", "READ t", Actions.Table.QUERY + " TABLE t", "DELETE t",
        Actions.Table.DELETE_ROWS + " TABLE t"));
    assertEquals(executedDelete().getTableName(), "t_OFFLINE");
  }

  @DataProvider
  public Object[][] deleteChecks() {
    List<String> checks = List.of("READ null", "READ t", Actions.Table.QUERY + " TABLE t", "DELETE t",
        Actions.Table.DELETE_ROWS + " TABLE t");
    String deniedOnTable = "Permission denied to delete rows from table: t";
    return new Object[][]{
        {"READ null", checks.subList(0, 1), "Permission denied to delete rows"},
        {"READ t", checks.subList(0, 2), deniedOnTable},
        {Actions.Table.QUERY + " TABLE t", checks.subList(0, 3), deniedOnTable},
        {"DELETE t", checks.subList(0, 4), deniedOnTable},
        {Actions.Table.DELETE_ROWS + " TABLE t", checks, deniedOnTable}
    };
  }

  @Test(dataProvider = "deleteChecks")
  public void testDeleteIsDeniedByEachCheck(String deniedCheck, List<String> expectedChecks, String expectedMessage) {
    RecordingAccessControl accessControl = new RecordingAccessControl(deniedCheck);
    when(_accessControlFactory.create()).thenReturn(accessControl);

    String response = postSql("DELETE FROM t WHERE a = 1", null);

    assertTrue(response.contains(String.valueOf(QueryErrorCode.ACCESS_DENIED.getId())), response);
    assertTrue(response.contains(expectedMessage), response);
    assertEquals(accessControl._checks, expectedChecks);
    verify(_sqlQueryExecutor, never()).executeStatement(any(), any());
  }

  @Test
  public void testDeleteWithoutWhereClauseIsAParsingError() {
    allowEveryCheck();

    String response = postSql("DELETE FROM t", null);

    assertTrue(response.contains(String.valueOf(QueryErrorCode.SQL_PARSING.getId())), response);
    assertTrue(response.contains("requires a WHERE clause"), response);
    verify(_sqlQueryExecutor, never()).executeStatement(any(), any());
    verify(_sqlQueryExecutor, never()).executeDMLStatement(any(), any());
  }

  @Test
  public void testDeleteWithAConflictingDatabaseHeaderIsAValidationError() {
    allowEveryCheck();

    String response = postSql("DELETE FROM db1.t WHERE a = 1", "db2");

    assertTrue(response.contains(String.valueOf(QueryErrorCode.QUERY_VALIDATION.getId())), response);
    assertTrue(response.contains("does not match database name 'db2'"), response);
    verify(_sqlQueryExecutor, never()).executeStatement(any(), any());
  }

  @Test
  public void testDeleteFromALogicalTableIsAValidationErrorAfterAuthorization() {
    RecordingAccessControl accessControl = allowEveryCheck();
    when(_tableCache.getActualTableName("lt")).thenReturn(null);
    when(_tableCache.getActualLogicalTableName("lt")).thenReturn("lt");

    String response = postSql("DELETE FROM lt WHERE a = 1", null);

    assertTrue(response.contains(String.valueOf(QueryErrorCode.QUERY_VALIDATION.getId())), response);
    assertTrue(response.contains("DELETE does not support logical tables: lt"), response);
    // Rejected only once the caller is authorized on the name, like an unknown table, so that the names of the
    // logical tables are not leaked to a caller who is not authorized for them
    assertEquals(accessControl._checks, List.of("READ null", "READ lt", Actions.Table.QUERY + " TABLE lt",
        "DELETE lt", Actions.Table.DELETE_ROWS + " TABLE lt"));
    verify(_sqlQueryExecutor, never()).executeStatement(any(), any());
  }

  @Test
  public void testDeleteFromALogicalTableDoesNotLeakItToAnUnauthorizedCaller() {
    RecordingAccessControl accessControl = new RecordingAccessControl(Actions.Table.DELETE_ROWS + " TABLE lt");
    when(_accessControlFactory.create()).thenReturn(accessControl);
    when(_tableCache.getActualTableName("lt")).thenReturn(null);
    when(_tableCache.getActualLogicalTableName("lt")).thenReturn("lt");

    String response = postSql("DELETE FROM lt WHERE a = 1", null);

    assertTrue(response.contains(String.valueOf(QueryErrorCode.ACCESS_DENIED.getId())), response);
    assertFalse(response.contains("logical"), response);
    verify(_sqlQueryExecutor, never()).executeStatement(any(), any());
  }

  @Test
  public void testDeleteDenialNamesTheTableAsWritten() {
    // The cache resolves the name to the case the table is defined with: the checks run on that name, while the
    // denial names the table as written, so that it does not reveal that the table exists
    RecordingAccessControl accessControl = new RecordingAccessControl("DELETE SecretTable");
    when(_accessControlFactory.create()).thenReturn(accessControl);
    when(_tableCache.isIgnoreCase()).thenReturn(true);
    when(_tableCache.getActualTableName("secrettable")).thenReturn("SecretTable");

    String response = postSql("DELETE FROM secrettable WHERE a = 1", null);

    assertTrue(response.contains(String.valueOf(QueryErrorCode.ACCESS_DENIED.getId())), response);
    assertTrue(response.contains("Permission denied to delete rows from table: secrettable"), response);
    assertFalse(response.contains("SecretTable"), response);
    assertEquals(accessControl._checks, List.of("READ null", "READ SecretTable",
        Actions.Table.QUERY + " TABLE SecretTable", "DELETE SecretTable"));
    verify(_sqlQueryExecutor, never()).executeStatement(any(), any());
  }

  @Test
  public void testDeleteFromAnUnknownTableNamesItAsWritten() {
    allowEveryCheck();
    when(_tableCache.getActualTableName("db1.unknown")).thenReturn(null);

    String response = postSql("DELETE FROM unknown WHERE a = 1", "db1");

    // The name as written, not the one qualified with the database of the request
    assertTrue(response.contains(String.valueOf(QueryErrorCode.TABLE_DOES_NOT_EXIST.getId())), response);
    assertTrue(response.contains("Table does not exist: unknown"), response);
    assertFalse(response.contains("db1.unknown"), response);
    verify(_sqlQueryExecutor, never()).executeStatement(any(), any());
  }

  @Test(dataProvider = "legacyOptionModes")
  public void testDeleteWithLegacyOptionsFollowsTheClusterLegacySyntaxMode(SqlOptionsMode mode, String expectedError) {
    // The controller registers the legacy-syntax-mode-only query option config listener, so the cluster mode applies
    // to the DML it executes
    allowEveryCheck();
    SqlOptionsMode previousMode = QueryOptionsUtils.getLegacyOptionSyntaxMode();
    QueryOptionsUtils.setLegacyOptionSyntaxMode(mode);
    try {
      String response = postSql("DELETE FROM t WHERE a = 1 OPTION(dryRun=false)", null);
      assertTrue(response.contains(expectedError), response);
      verify(_sqlQueryExecutor, never()).executeStatement(any(), any());
    } finally {
      QueryOptionsUtils.setLegacyOptionSyntaxMode(previousMode);
    }
  }

  @DataProvider(name = "legacyOptionModes")
  public Object[][] legacyOptionModes() {
    return new Object[][]{
        {SqlOptionsMode.REJECT, "Legacy OPTION(...) query options are not allowed on this cluster"},
        {SqlOptionsMode.IGNORE, "Legacy OPTION(...) options are ignored on this cluster"}
    };
  }

  @Test
  public void testDeleteFromAnUnknownTableFailsAfterAuthorization() {
    RecordingAccessControl accessControl = allowEveryCheck();
    when(_tableCache.getActualTableName("unknown")).thenReturn(null);

    String response = postSql("DELETE FROM unknown WHERE a = 1", null);

    // Fails closed as the query path does, rather than handing a table the cluster does not know to the executor
    assertTrue(response.contains(String.valueOf(QueryErrorCode.TABLE_DOES_NOT_EXIST.getId())), response);
    assertTrue(response.contains("Table does not exist: unknown"), response);
    // Authorized first, with the name as written since the cache does not know its case
    assertEquals(accessControl._checks, List.of("READ null", "READ unknown", Actions.Table.QUERY + " TABLE unknown",
        "DELETE unknown", Actions.Table.DELETE_ROWS + " TABLE unknown"));
    verify(_sqlQueryExecutor, never()).executeStatement(any(), any());
  }

  @Test
  public void testDeleteFromAnUnknownTableDoesNotLeakItsExistenceToAnUnauthorizedCaller() {
    RecordingAccessControl accessControl = new RecordingAccessControl(Actions.Table.DELETE_ROWS + " TABLE unknown");
    when(_accessControlFactory.create()).thenReturn(accessControl);
    when(_tableCache.getActualTableName("unknown")).thenReturn(null);

    String response = postSql("DELETE FROM unknown WHERE a = 1", null);

    assertTrue(response.contains(String.valueOf(QueryErrorCode.ACCESS_DENIED.getId())), response);
    assertFalse(response.contains("does not exist"), response);
    verify(_sqlQueryExecutor, never()).executeStatement(any(), any());
  }

  @Test
  public void testDeleteWithTheMseOptionReachesTheExecutor() {
    allowEveryCheck();

    // The mocked controller conf reports the multi-stage engine as disabled, which only queries care about: a DELETE
    // passes its options through to the executor
    String response = postSql("SET useMultistageEngine = 'true'; DELETE FROM t WHERE a = 1", null);

    assertFalse(response.contains("errorCode"), response);
    DeleteStatement statement = executedDelete();
    assertEquals(statement.getTableName(), "t");
    assertEquals(statement.getOptions().get(CommonConstants.Broker.Request.QueryOptionKey.USE_MULTISTAGE_ENGINE),
        "true");
  }

  @Test
  public void testDeleteExecutorExceptionIsMappedToAnErrorResponse() {
    allowEveryCheck();
    when(_sqlQueryExecutor.executeStatement(any(), any())).thenThrow(new RuntimeException("executor failed"));

    // The executor runs before the response streams, so the exception is mapped like the other query errors rather
    // than escaping from the StreamingOutput as an HTTP 500
    String response = postSql("DELETE FROM t WHERE a = 1", null);

    assertTrue(response.contains(String.valueOf(QueryErrorCode.INTERNAL.getId())), response);
    assertTrue(response.contains("executor failed"), response);
  }

  @DataProvider
  public Object[][] clientHeaders() {
    return new Object[][]{
        // The addresses of X-Forwarded-For are joined with ';', the format the broker logs
        {"203.0.113.7, 10.0.0.1", null, "203.0.113.7; 10.0.0.1"},
        {"203.0.113.7, 10.0.0.1", "10.0.0.2", "203.0.113.7; 10.0.0.1"},
        {null, "10.0.0.2", "10.0.0.2"},
        {null, null, CommonConstants.UNKNOWN}
    };
  }

  @Test(dataProvider = "clientHeaders")
  public void testDeleteLogMessageLogsTheClientAsTheBroker(@Nullable String forwardedFor, @Nullable String realIp,
      String expectedClient) {
    DeleteStatement statement = resolvedDelete("DELETE FROM myTable WHERE a = 1", null);

    String message = PinotQueryResource.deleteLogMessage(statement, headersWithClient(forwardedFor, realIp));

    assertEquals(message,
        "Executing DELETE on table: myTable, predicate: a = 1, options: [], client: " + expectedClient);
  }

  @Test
  public void testDeleteLogMessageIsASingleLine() {
    // A line ending in a string literal of the predicate, in a key of the request options or in a proxy header is
    // escaped, as in a query logged by the broker, so that it cannot forge a log record
    DeleteStatement statement = resolvedDelete("DELETE FROM myTable WHERE col1 = 'a\nb'", "x\ny=1");

    String message = PinotQueryResource.deleteLogMessage(statement,
        headersWithClient("10.0.0.1,\r\n2026-10-10 INFO forged log record", null));

    assertEquals(message, "Executing DELETE on table: myTable, predicate: col1 = 'a\\nb', options: [x\\ny], "
        + "client: 10.0.0.1;\\r\\n2026-10-10 INFO forged log record");
    assertFalse(message.contains("\n") || message.contains("\r"), message);
  }

  @Test
  public void testInsertIntoFileIsNotAuthorizedAsADelete() {
    when(_sqlQueryExecutor.executeDMLStatement(any(), any())).thenReturn(new BrokerResponseNative());

    postSql("INSERT INTO t FROM FILE 'file:///tmp/data'", null);

    // Executed as before: the minion task API that runs it authorizes the caller with the request headers
    verify(_sqlQueryExecutor).executeDMLStatement(any(SqlNodeAndOptions.class), eq(REQUEST_HEADERS));
    verify(_sqlQueryExecutor, never()).executeStatement(any(), any());
    verify(_accessControlFactory, never()).create();
  }

  @Test
  public void testUnauthenticatedDeleteIsUnauthorized() {
    unauthenticatedAccessControl();

    // Not turned into an error response, so that the caller gets HTTP 401 as for the other APIs
    expectThrows(NotAuthorizedException.class, () -> postSql("DELETE FROM t WHERE a = 1", null));
    verify(_sqlQueryExecutor, never()).executeStatement(any(), any());
  }

  @Test
  public void testUnauthenticatedDeleteDoesNotLookUpTheTable() {
    unauthenticatedAccessControl();
    when(_tableCache.getActualTableName("lt")).thenReturn(null);
    when(_tableCache.getActualLogicalTableName("lt")).thenReturn("lt");

    // The caller is checked before the table cache is consulted, so that an unauthenticated caller does not even
    // reach the table lookup that tells logical tables and unknown tables apart
    expectThrows(NotAuthorizedException.class, () -> postSql("DELETE FROM lt WHERE a = 1", null));
    verify(_tableCache, never()).getActualTableName(any());
    verify(_tableCache, never()).getActualLogicalTableName(any());
    verify(_sqlQueryExecutor, never()).executeStatement(any(), any());
  }

  @Test
  public void testValidateMultiStageQueryReportsAQueryThatDoesNotParseWithoutFailingTheRequest() {
    SqlOptionsMode previousMode = QueryOptionsUtils.getLegacyOptionSyntaxMode();
    QueryOptionsUtils.setLegacyOptionSyntaxMode(SqlOptionsMode.REJECT);
    try {
      PinotQueryResource.MultiStageQueryValidationRequest request =
          new PinotQueryResource.MultiStageQueryValidationRequest(null, null, null, null,
              List.of("SELECT * FROM a OPTION(timeoutMs=1000)", "SELECT * FROM a"), false);

      List<PinotQueryResource.MultiStageQueryValidationResponse> responses =
          _pinotQueryResource.validateMultiStageQuery(request, mock(HttpHeaders.class));

      // The query the cluster rejects at parse time is one failed entry, and the next query is still validated
      assertEquals(responses.size(), 2);
      PinotQueryResource.MultiStageQueryValidationResponse rejected = responses.get(0);
      assertFalse(rejected.isCompiledSuccessfully());
      assertEquals(rejected.getErrorCode(), QueryErrorCode.SQL_PARSING);
      assertTrue(rejected.getErrorMessage().contains("Legacy OPTION(...) query options are not allowed"),
          rejected.getErrorMessage());
      assertEquals(rejected.getSql(), "SELECT * FROM a OPTION(timeoutMs=1000)");
      assertEquals(responses.get(1).getSql(), "SELECT * FROM a");
    } finally {
      QueryOptionsUtils.setLegacyOptionSyntaxMode(previousMode);
    }
  }

  /// Stubs an access control that allows every check, and an executor that answers every DELETE, as a deployment
  /// that implements row deletion does.
  private RecordingAccessControl allowEveryCheck() {
    RecordingAccessControl accessControl = new RecordingAccessControl(null);
    when(_accessControlFactory.create()).thenReturn(accessControl);
    when(_sqlQueryExecutor.executeStatement(any(), any())).thenReturn(new BrokerResponseNative());
    return accessControl;
  }

  /// Stubs an access control that rejects every caller as unauthenticated, as basic auth does, whichever `hasAccess`
  /// overload is called.
  private void unauthenticatedAccessControl() {
    AccessControl accessControl = mock(AccessControl.class);
    when(accessControl.hasAccess(any(AccessType.class), any(), any())).thenThrow(new NotAuthorizedException("Basic"));
    when(accessControl.hasAccess(any(), any(AccessType.class), any(), any()))
        .thenThrow(new NotAuthorizedException("Basic"));
    when(_accessControlFactory.create()).thenReturn(accessControl);
  }

  private String postSql(String sql, @Nullable String database) {
    HttpHeaders httpHeaders = mock(HttpHeaders.class);
    when(httpHeaders.getRequestHeaders()).thenReturn(new MultivaluedHashMap<>(REQUEST_HEADERS));
    when(httpHeaders.getHeaderString(CommonConstants.DATABASE)).thenReturn(database);
    return streamingOutputToString(
        _pinotQueryResource.handlePostSql(JsonUtils.newObjectNode().put("sql", sql).toString(), httpHeaders));
  }

  /// Parses the `DELETE` with the `queryOptions` of the request payload, if any, and resolves its table with the table
  /// cache, as the query endpoint does.
  private DeleteStatement resolvedDelete(String sql, @Nullable String queryOptions) {
    ObjectNode request = JsonUtils.newObjectNode();
    if (queryOptions != null) {
      request.put(CommonConstants.Broker.Request.QUERY_OPTIONS, queryOptions);
    }
    return ((DeleteStatement) DataManipulationStatementParser.parse(RequestUtils.parseQuery(sql, request)))
        .resolveTableName(null, _tableCache);
  }

  /// Mocks the headers of a request with the given `X-Forwarded-For` and `X-Real-IP` values, `null` leaving a header
  /// unset.
  private static HttpHeaders headersWithClient(@Nullable String forwardedFor, @Nullable String realIp) {
    HttpHeaders httpHeaders = mock(HttpHeaders.class);
    when(httpHeaders.getHeaderString("X-Forwarded-For")).thenReturn(forwardedFor);
    when(httpHeaders.getHeaderString("X-Real-IP")).thenReturn(realIp);
    return httpHeaders;
  }

  /// Returns the `DELETE` handed to the executor, checking that the executor also gets the request headers, which the
  /// APIs it calls use to authorize the caller again.
  private DeleteStatement executedDelete() {
    ArgumentCaptor<DataManipulationStatement> captor = ArgumentCaptor.forClass(DataManipulationStatement.class);
    verify(_sqlQueryExecutor).executeStatement(captor.capture(), eq(REQUEST_HEADERS));
    DeleteStatement statement = (DeleteStatement) captor.getValue();
    assertTrue(statement.isResolved());
    return statement;
  }

  /// Access control that records its checks as `<access type> <table name>` (the table name being `null` for the
  /// caller-level check) and `<action> <target type> <target id>`, and denies the check it is given.
  private static class RecordingAccessControl implements AccessControl {
    private final List<String> _checks = new ArrayList<>();
    @Nullable
    private final String _deniedCheck;

    RecordingAccessControl(@Nullable String deniedCheck) {
      _deniedCheck = deniedCheck;
    }

    @Override
    public boolean hasAccess(@Nullable String tableName, AccessType accessType, HttpHeaders httpHeaders,
        String endpointUrl) {
      return check(accessType + " " + tableName);
    }

    @Override
    public boolean hasAccess(HttpHeaders httpHeaders, TargetType targetType, String targetId, String action) {
      return check(action + " " + targetType + " " + targetId);
    }

    private boolean check(String check) {
      _checks.add(check);
      return !check.equals(_deniedCheck);
    }
  }

  @Test
  public void testDdlOnQueryEndpointWithMseOptionReturnsValidationError() {
    String response = streamingOutputToString(
        _pinotQueryResource.handleGetSql("CREATE TABLE t (id INT) TABLE_TYPE = OFFLINE", null,
            "useMultistageEngine=true", null)
    );
    Assert.assertTrue(response.contains(String.valueOf(QueryErrorCode.QUERY_VALIDATION.getId())));
    Assert.assertTrue(response.contains("/sql/ddl"));
  }

  @Test
  public void testDdlOnQueryEndpointWithSetMseOptionReturnsValidationError() {
    String response = streamingOutputToString(
        _pinotQueryResource.handleGetSql(
            "SET useMultistageEngine = 'true'; CREATE TABLE t (id INT) TABLE_TYPE = OFFLINE", null, null, null)
    );
    Assert.assertTrue(response.contains(String.valueOf(QueryErrorCode.QUERY_VALIDATION.getId())));
    Assert.assertTrue(response.contains("/sql/ddl"));
  }

  @Test
  public void testSqlOptionsDecideTheEngine() {
    // The mocked controller conf reports the multi-stage engine as disabled, so routing to it fails with INTERNAL
    String response = streamingOutputToString(
        _pinotQueryResource.handleGetSql("SET useMultistageEngine = 'true'; SELECT * FROM a", null, null, null));
    Assert.assertTrue(response.contains(String.valueOf(QueryErrorCode.INTERNAL.getId())), response);
    Assert.assertTrue(response.contains("Multi-Stage query engine not enabled"), response);
  }

  @Test
  public void testIgnoredSqlOptionsDoNotDecideTheEngine() {
    mockSingleStageBrokerSelection();
    String response = streamingOutputToString(
        _pinotQueryResource.handleGetSql("SET useMultistageEngine = 'true'; SELECT * FROM a", null,
            "sqlOptionsMode=ignore", null));
    Assert.assertTrue(response.contains(String.valueOf(QueryErrorCode.BROKER_RESOURCE_MISSING.getId())), response);
  }

  @Test
  public void testRejectedSqlOptionsFailBeforeRouting() {
    String response = streamingOutputToString(
        _pinotQueryResource.handleGetSql("SET useMultistageEngine = 'true'; SELECT * FROM a", null,
            "sqlOptionsMode=reject", null));
    Assert.assertTrue(response.contains(String.valueOf(QueryErrorCode.QUERY_VALIDATION.getId())), response);
    Assert.assertTrue(response.contains("useMultistageEngine"), response);
  }

  /// Lets a single-stage query get as far as broker selection, which fails with BROKER_RESOURCE_MISSING because no
  /// broker serves the table. Reaching that point proves the query was routed to the single-stage engine.
  private void mockSingleStageBrokerSelection() {
    when(_resourceManager.getActualTableName(any(), any())).then(AdditionalAnswers.returnsFirstArg());
    AccessControl accessControl = mock(AccessControl.class);
    when(accessControl.hasAccess(any(), eq(AccessType.READ), any(), any())).thenReturn(true);
    when(_accessControlFactory.create()).thenReturn(accessControl);
  }

  public static String streamingOutputToString(StreamingOutput streamingOutput) {
    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
      streamingOutput.write(byteArrayOutputStream);
      return byteArrayOutputStream.toString();
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while converting StreamingOutput to String", e);
    }
  }
}
