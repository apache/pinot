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
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.StreamingOutput;
import org.apache.calcite.sql.SqlDelete;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.access.AccessControl;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.core.query.executor.sql.SqlQueryExecutor;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.sql.parsers.PinotSqlType;
import org.mockito.AdditionalAnswers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


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
  }

  @Test
  public void testDmlOnPostQueryEndpointIsExecuted() {
    when(_sqlQueryExecutor.executeDMLStatement(any(), any())).thenReturn(new BrokerResponseNative());
    HttpHeaders httpHeaders = mock(HttpHeaders.class);
    when(httpHeaders.getRequestHeaders()).thenReturn(new MultivaluedHashMap<>());

    streamingOutputToString(
        _pinotQueryResource.handlePostSql("{\"sql\": \"DELETE FROM t WHERE a = 1\"}", httpHeaders));

    verify(_sqlQueryExecutor).executeDMLStatement(argThat(sqlNodeAndOptions ->
        sqlNodeAndOptions.getSqlType() == PinotSqlType.DML && sqlNodeAndOptions.getSqlNode() instanceof SqlDelete),
        any());
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
