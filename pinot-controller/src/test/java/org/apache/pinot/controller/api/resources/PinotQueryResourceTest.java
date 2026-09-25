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

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.StreamingOutput;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
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


public class PinotQueryResourceTest {

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
  public void testDmlOnGetQueryEndpointReturnsValidationError() {
    for (String dml : new String[]{"DELETE FROM t WHERE a = 1", "INSERT INTO t FROM FILE 'file:///tmp/data'"}) {
      String response = streamingOutputToString(_pinotQueryResource.handleGetSql(dml, null, null, null));
      Assert.assertTrue(response.contains(String.valueOf(QueryErrorCode.QUERY_VALIDATION.getId())), response);
      Assert.assertTrue(response.contains("use POST /sql instead"), response);
    }
    verify(_sqlQueryExecutor, never()).executeDMLStatement(any(), any());
    verify(_sqlQueryExecutor, never()).executeStatement(any(), any());
  }

  @Test
  public void testDeleteIsExecutedOnTheAuthorizedTable() {
    RecordingAccessControl accessControl = new RecordingAccessControl(null);
    when(_accessControlFactory.create()).thenReturn(accessControl);
    // Table names are case-insensitive: the DELETE is authorized and executed on the table as it is defined
    when(_tableCache.isIgnoreCase()).thenReturn(true);
    when(_tableCache.getActualTableName("db1.T")).thenReturn("db1.t");
    when(_sqlQueryExecutor.executeStatement(any(), any())).thenReturn(new BrokerResponseNative());

    String response = postSql("DELETE FROM T WHERE a = 1", "db1");

    assertFalse(response.contains("errorCode"), response);
    // The checks of a query on the table, then those of the other deletions of the controller
    assertEquals(accessControl._checks, List.of("READ db1.t", Actions.Table.QUERY + " TABLE db1.t", "DELETE db1.t",
        Actions.Table.DELETE_ROWS + " TABLE db1.t"));
    DeleteStatement statement = executedDelete();
    assertEquals(statement.getTableName(), "db1.t");
    assertEquals(statement.getPredicate(), "a = 1");
    verify(_sqlQueryExecutor, never()).executeDMLStatement(any(), any());
  }

  @Test
  public void testDeleteFromATableWithATypeIsAuthorizedOnItsRawName() {
    RecordingAccessControl accessControl = new RecordingAccessControl(null);
    when(_accessControlFactory.create()).thenReturn(accessControl);
    when(_sqlQueryExecutor.executeStatement(any(), any())).thenReturn(new BrokerResponseNative());

    postSql("DELETE FROM t_OFFLINE WHERE a = 1", null);

    // Like the table APIs of the controller, while the executor deletes from the table named by the statement
    assertEquals(accessControl._checks, List.of("READ t", Actions.Table.QUERY + " TABLE t", "DELETE t",
        Actions.Table.DELETE_ROWS + " TABLE t"));
    assertEquals(executedDelete().getTableName(), "t_OFFLINE");
  }

  @DataProvider
  public Object[][] deleteChecks() {
    List<String> checks = List.of("READ t", Actions.Table.QUERY + " TABLE t", "DELETE t",
        Actions.Table.DELETE_ROWS + " TABLE t");
    return new Object[][]{
        {"READ", checks.subList(0, 1)},
        {Actions.Table.QUERY, checks.subList(0, 2)},
        {"DELETE", checks.subList(0, 3)},
        {Actions.Table.DELETE_ROWS, checks}
    };
  }

  @Test(dataProvider = "deleteChecks")
  public void testDeleteIsDeniedByEachCheck(String deniedCheck, List<String> expectedChecks) {
    RecordingAccessControl accessControl = new RecordingAccessControl(deniedCheck);
    when(_accessControlFactory.create()).thenReturn(accessControl);

    String response = postSql("DELETE FROM t WHERE a = 1", null);

    assertTrue(response.contains(String.valueOf(QueryErrorCode.ACCESS_DENIED.getId())), response);
    assertTrue(response.contains("Permission denied to delete rows from table: t"), response);
    assertEquals(accessControl._checks, expectedChecks);
    verify(_sqlQueryExecutor, never()).executeStatement(any(), any());
  }

  @Test
  public void testInsertIntoFileIsNotAuthorizedAsADelete() {
    when(_sqlQueryExecutor.executeDMLStatement(any(), any())).thenReturn(new BrokerResponseNative());

    postSql("INSERT INTO t FROM FILE 'file:///tmp/data'", null);

    // Executed as before: the minion task API that runs it authorizes the caller
    verify(_sqlQueryExecutor).executeDMLStatement(any(SqlNodeAndOptions.class), any());
    verify(_sqlQueryExecutor, never()).executeStatement(any(), any());
    verify(_accessControlFactory, never()).create();
  }

  private String postSql(String sql, @Nullable String database) {
    HttpHeaders httpHeaders = mock(HttpHeaders.class);
    when(httpHeaders.getRequestHeaders()).thenReturn(new MultivaluedHashMap<>());
    when(httpHeaders.getHeaderString(CommonConstants.DATABASE)).thenReturn(database);
    return streamingOutputToString(
        _pinotQueryResource.handlePostSql(JsonUtils.newObjectNode().put("sql", sql).toString(), httpHeaders));
  }

  private DeleteStatement executedDelete() {
    ArgumentCaptor<DataManipulationStatement> captor = ArgumentCaptor.forClass(DataManipulationStatement.class);
    verify(_sqlQueryExecutor).executeStatement(captor.capture(), any());
    DeleteStatement statement = (DeleteStatement) captor.getValue();
    assertTrue(statement.isResolved());
    return statement;
  }

  /// Access control that records its checks, and denies the access type or the action it is given.
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
      _checks.add(accessType + " " + tableName);
      return !accessType.name().equals(_deniedCheck);
    }

    @Override
    public boolean hasAccess(HttpHeaders httpHeaders, TargetType targetType, String targetId, String action) {
      _checks.add(action + " " + targetType + " " + targetId);
      return !action.equals(_deniedCheck);
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
