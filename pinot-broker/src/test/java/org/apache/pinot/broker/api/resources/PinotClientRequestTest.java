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
package org.apache.pinot.broker.api.resources;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.pinot.broker.requesthandler.BrokerRequestHandler;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.executor.sql.SqlQueryExecutor;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.trace.QueryFingerprint;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.glassfish.grizzly.http.server.Request;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Controller.PINOT_QUERY_ERROR_CODE_HEADER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;


public class PinotClientRequestTest {

  @Mock
  private SqlQueryExecutor _sqlQueryExecutor;
  @Mock
  private BrokerRequestHandler _requestHandler;
  @Mock
  private BrokerMetrics _brokerMetrics;
  @Mock
  private Executor _executor;
  @Mock
  private HttpClientConnectionManager _httpConnMgr;
  @Mock
  private HttpHeaders _httpHeaders;
  @InjectMocks
  private PinotClientRequest _pinotClientRequest;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    doAnswer(invocation -> {
      Runnable runnable = invocation.getArgument(0);
      runnable.run();
      return null;
    }).when(_executor).execute(any(Runnable.class));
  }

  @Test
  public void testGetPinotQueryResponse()
      throws Exception {

    // for successful query result the 'X-Pinot-Error-Code' should be -1
    BrokerResponse emptyResultBrokerResponse = BrokerResponseNative.EMPTY_RESULT;
    Response successfulResponse = PinotClientRequest.getPinotQueryResponse(emptyResultBrokerResponse, _httpHeaders);
    assertEquals(successfulResponse.getStatus(), Response.Status.OK.getStatusCode());
    Assert.assertTrue(successfulResponse.getHeaders().containsKey(PINOT_QUERY_ERROR_CODE_HEADER));
    assertEquals(successfulResponse.getHeaders().get(PINOT_QUERY_ERROR_CODE_HEADER).size(), 1);
    assertEquals(successfulResponse.getHeaders().get(PINOT_QUERY_ERROR_CODE_HEADER).get(0), -1);

    // for failed query result the 'X-Pinot-Error-Code' should be Error code fo exception.
    BrokerResponse tableDoesNotExistBrokerResponse = BrokerResponseNative.TABLE_DOES_NOT_EXIST;
    Response tableDoesNotExistResponse =
        PinotClientRequest.getPinotQueryResponse(tableDoesNotExistBrokerResponse, _httpHeaders);
    assertEquals(tableDoesNotExistResponse.getStatus(), Response.Status.OK.getStatusCode());
    Assert.assertTrue(tableDoesNotExistResponse.getHeaders().containsKey(PINOT_QUERY_ERROR_CODE_HEADER));
    assertEquals(tableDoesNotExistResponse.getHeaders().get(PINOT_QUERY_ERROR_CODE_HEADER).size(), 1);
    assertEquals(tableDoesNotExistResponse.getHeaders().get(PINOT_QUERY_ERROR_CODE_HEADER).get(0),
        QueryErrorCode.TABLE_DOES_NOT_EXIST.getId());

    // for failed query result the response code should be corresponding http response code of the Error code if
    // USE_HTTP_STATUS_FOR_ERRORS_HEADER is set to true.
    when(_httpHeaders.getHeaderString(
        CommonConstants.Broker.USE_HTTP_STATUS_FOR_ERRORS_HEADER)).thenReturn("true");
    Response tableDoesNotExistResponseWithHttpResponseCode =
        PinotClientRequest.getPinotQueryResponse(tableDoesNotExistBrokerResponse, _httpHeaders);
    assertEquals(tableDoesNotExistResponseWithHttpResponseCode.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
    Assert.assertTrue(
        tableDoesNotExistResponseWithHttpResponseCode.getHeaders().containsKey(PINOT_QUERY_ERROR_CODE_HEADER));
    assertEquals(tableDoesNotExistResponseWithHttpResponseCode.getHeaders().get(PINOT_QUERY_ERROR_CODE_HEADER).size(),
        1);
    assertEquals(tableDoesNotExistResponseWithHttpResponseCode.getHeaders().get(PINOT_QUERY_ERROR_CODE_HEADER).get(0),
        QueryErrorCode.TABLE_DOES_NOT_EXIST.getId());
  }

  @Test
  public void testPinotQueryComparisonApiSameQuery() throws Exception {
    AsyncResponse asyncResponse = mock(AsyncResponse.class);
    Request request = mock(Request.class);
    when(request.getRequestURL()).thenReturn(new StringBuilder());
    when(_requestHandler.handleRequest(any(), any(), any(), any(), any()))
        .thenReturn(BrokerResponseNative.EMPTY_RESULT);
    _pinotClientRequest.processSqlQueryWithBothEnginesAndCompareResults("{\"sql\": \"SELECT * FROM mytable\"}",
        asyncResponse, request, null);

    ArgumentCaptor<JsonNode> requestCaptor = ArgumentCaptor.forClass(JsonNode.class);
    ArgumentCaptor<SqlNodeAndOptions> sqlNodeAndOptionsCaptor = ArgumentCaptor.forClass(SqlNodeAndOptions.class);
    verify(_requestHandler, times(2)).handleRequest(requestCaptor.capture(), sqlNodeAndOptionsCaptor.capture(),
        any(), any(), any());
    verify(asyncResponse, times(1)).resume(any(Response.class));

    assertEquals(requestCaptor.getAllValues().size(), 2);
    assertEquals(requestCaptor.getAllValues().get(0).get("sql").asText(), "SELECT * FROM mytable");
    assertEquals(requestCaptor.getAllValues().get(1).get("sql").asText(), "SELECT * FROM mytable");

    assertEquals(sqlNodeAndOptionsCaptor.getAllValues().size(), 2);
    assertFalse(sqlNodeAndOptionsCaptor.getAllValues().get(0).getOptions()
        .containsKey(CommonConstants.Broker.Request.QueryOptionKey.USE_MULTISTAGE_ENGINE));
    assertEquals(sqlNodeAndOptionsCaptor.getAllValues().get(1).getOptions()
        .get(CommonConstants.Broker.Request.QueryOptionKey.USE_MULTISTAGE_ENGINE), "true");
  }

  @Test
  public void testPinotQueryComparisonApiDifferentQuery() throws Exception {
    AsyncResponse asyncResponse = mock(AsyncResponse.class);
    Request request = mock(Request.class);
    when(request.getRequestURL()).thenReturn(new StringBuilder());
    when(_requestHandler.handleRequest(any(), any(), any(), any(), any()))
        .thenReturn(BrokerResponseNative.EMPTY_RESULT);
    _pinotClientRequest.processSqlQueryWithBothEnginesAndCompareResults("{\"sqlV1\": \"SELECT v1 FROM mytable\","
            + "\"sqlV2\": \"SELECT v2 FROM mytable\"}", asyncResponse, request, null);

    ArgumentCaptor<JsonNode> requestCaptor = ArgumentCaptor.forClass(JsonNode.class);
    verify(_requestHandler, times(2)).handleRequest(requestCaptor.capture(), any(), any(), any(), any());
    verify(asyncResponse, times(1)).resume(any(Response.class));

    assertEquals(requestCaptor.getAllValues().size(), 2);
    assertEquals(requestCaptor.getAllValues().get(0).get("sql").asText(), "SELECT v1 FROM mytable");
    assertEquals(requestCaptor.getAllValues().get(1).get("sql").asText(), "SELECT v2 FROM mytable");
  }

  @Test
  public void testPinotQueryComparisonApiMissingSql() throws Exception {
    AsyncResponse asyncResponse = mock(AsyncResponse.class);
    Request request = mock(Request.class);
    when(request.getRequestURL()).thenReturn(new StringBuilder());
    // Both v1sql and v2sql should be present
    _pinotClientRequest.processSqlQueryWithBothEnginesAndCompareResults("{\"v1sql\": \"SELECT v1 FROM mytable\"}",
        asyncResponse, request, null);

    verify(_requestHandler, never()).handleRequest(any(), any(), any(), any(), any());
    verify(asyncResponse, times(1)).resume(any(Throwable.class));
  }

  @Test
  public void testPinotQueryComparison() throws Exception {
    // Aggregation type difference

    BrokerResponse v1BrokerResponse = new BrokerResponseNative();
    DataSchema v1DataSchema = new DataSchema(new String[]{"sum(col)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE});
    v1BrokerResponse.setResultTable(new ResultTable(v1DataSchema, List.<Object[]>of(new Object[]{1234})));

    BrokerResponse v2BrokerResponse = new BrokerResponseNative();
    DataSchema v2DataSchema = new DataSchema(new String[]{"EXPR$0"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.LONG});
    v2BrokerResponse.setResultTable(new ResultTable(v2DataSchema, List.<Object[]>of(new Object[]{1234})));

    ObjectNode comparisonResponse = (ObjectNode) PinotClientRequest.getPinotQueryComparisonResponse(
        "SELECT SUM(col) FROM mytable", v1BrokerResponse, v2BrokerResponse).getEntity();

    List<String> comparisonAnalysis = new ObjectMapper().readerFor(new TypeReference<List<String>>() { })
        .readValue(comparisonResponse.get("comparisonAnalysis"));

    assertEquals(comparisonAnalysis.size(), 1);
    Assert.assertTrue(comparisonAnalysis.get(0).contains("v1 type: DOUBLE, v2 type: LONG"));

    // Default limit in v1

    v1DataSchema = new DataSchema(new String[]{"col1"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING});
    v2DataSchema = new DataSchema(new String[]{"col1"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING});
    List<Object[]> rows = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      rows.add(new Object[]{i});
    }
    v1BrokerResponse.setResultTable(new ResultTable(v1DataSchema, new ArrayList<>(rows)));
    for (int i = 10; i < 100; i++) {
      rows.add(new Object[]{i});
    }
    v2BrokerResponse.setResultTable(new ResultTable(v2DataSchema, new ArrayList<>(rows)));

    comparisonResponse = (ObjectNode) PinotClientRequest.getPinotQueryComparisonResponse(
        "SELECT col1 FROM mytable", v1BrokerResponse, v2BrokerResponse).getEntity();

    comparisonAnalysis = new ObjectMapper().readerFor(new TypeReference<List<String>>() { })
        .readValue(comparisonResponse.get("comparisonAnalysis"));

    assertEquals(comparisonAnalysis.size(), 1);
    Assert.assertTrue(comparisonAnalysis.get(0).contains("Mismatch in number of rows returned"));
  }

  @Test
  public void testGetQueryFingerprintSuccess() throws Exception {
    Request request = mock(Request.class);
    when(request.getRequestURL()).thenReturn(new StringBuilder());

    // single stage query
    String requestJson = "{\"sql\": \"SELECT * FROM myTable WHERE id IN (1, 2, 3)\"}";
    Response response = _pinotClientRequest.getQueryFingerprint(requestJson, request, null);

    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    QueryFingerprint fingerprint = (QueryFingerprint) response.getEntity();
    Assert.assertNotNull(fingerprint, "Valid Single-stage query should return a non-null QueryFingerprint object");
    Assert.assertNotNull(fingerprint.getQueryHash(),
        "Valid Single-stage query fingerprint should contain a non-null query hash");
    Assert.assertNotNull(fingerprint.getFingerprint(),
        "Valid Single-stage query fingerprint should contain a non-null SQL fingerprint");
    assertEquals(fingerprint.getFingerprint(), "SELECT * FROM `myTable` WHERE `id` IN (?)",
        "Valid Single-stage query fingerprint should normalize literals to placeholders");

    // multi stage query
    requestJson = "{\"sql\": \"SET useMultistageEngine=true; \\n"
      + "SELECT * FROM table1 t1 LEFT JOIN table2 t2 ON t1.id = t2.id WHERE t1.col1 > 100\"}";
    response = _pinotClientRequest.getQueryFingerprint(requestJson, request, null);

    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    fingerprint = (QueryFingerprint) response.getEntity();
    Assert.assertNotNull(fingerprint, "Valid Multi-stage query should return a non-null QueryFingerprint object");
    Assert.assertNotNull(fingerprint.getQueryHash(),
        "Valid Multi-stage query fingerprint should contain a non-null query hash");
    Assert.assertNotNull(fingerprint.getFingerprint(),
        "Valid Multi-stage query fingerprint should contain a non-null SQL fingerprint");
    assertEquals(fingerprint.getFingerprint(),
        "SELECT * FROM `table1` AS `t1` LEFT JOIN `table2` AS `t2` ON `t1`.`id` = `t2`.`id` WHERE `t1`.`col1` > ?",
        "Valid Multi-stage query fingerprint should normalize literals and preserve JOIN structure");
  }

  @Test
  public void testGetQueryFingerprintWithInvalidSql() throws Exception {
    Request request = mock(Request.class);
    when(request.getRequestURL()).thenReturn(new StringBuilder());

    String requestJson = "{\"sql\": \"INVALID SQL QUERY\"}";
    Response response = _pinotClientRequest.getQueryFingerprint(requestJson, request, null);

    assertEquals(response.getStatus(), Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
        "Invalid SQL query should return INTERNAL_SERVER_ERROR status");
  }
}
