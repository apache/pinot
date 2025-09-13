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
package org.apache.pinot.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Tests for Connection cursor functionality.
 */
public class CursorConnectionTest {
  private Connection _connection;
  private JsonAsyncHttpPinotClientTransport _mockTransport;
  private BrokerSelector _mockBrokerSelector;
  private CursorAwareBrokerResponse _mockCursorResponse;

  @BeforeMethod
  public void setUp() {
    _mockTransport = Mockito.mock(JsonAsyncHttpPinotClientTransport.class);
    _mockBrokerSelector = Mockito.mock(BrokerSelector.class);
    _connection = new Connection(_mockBrokerSelector, _mockTransport);

    // Create mock cursor response
    ObjectMapper mapper = new ObjectMapper();
    JsonNode mockResponseJson = mapper.createObjectNode()
        .put("offset", 0)
        .put("numRows", 10)
        .put("numRowsResultSet", 100)
        .put("cursorId", "test-cursor-123");
    _mockCursorResponse = CursorAwareBrokerResponse.fromJson(mockResponseJson);

    // Setup broker selector mock
    when(_mockBrokerSelector.selectBroker(any())).thenReturn("localhost:8000");
  }

  @Test
  public void testExecuteCursorQuery() throws Exception {
    // Setup mock transport response
    when(_mockTransport.executeQueryWithCursor(anyString(), anyString(), anyInt()))
        .thenReturn(_mockCursorResponse);

    // Execute cursor query
    CursorResultSetGroup result = _connection.executeCursorQuery("SELECT * FROM testTable", 10);

    // Verify result
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getOffset(), Long.valueOf(0));
    Assert.assertEquals(result.getPageSize(), 10);
    Assert.assertEquals(result.getNumRowsResultSet(), Long.valueOf(100));
  }

  @Test
  public void testExecuteCursorQueryAsync() throws Exception {
    // Setup mock transport response
    when(_mockTransport.executeQueryWithCursorAsync(anyString(), anyString(), anyInt()))
        .thenReturn(CompletableFuture.completedFuture(_mockCursorResponse));

    // Execute cursor query async
    CompletableFuture<CursorResultSetGroup> future =
        _connection.executeCursorQueryAsync("SELECT * FROM testTable", 10);
    CursorResultSetGroup result = future.get();

    // Verify result
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getOffset(), Long.valueOf(0));
    Assert.assertEquals(result.getPageSize(), 10);
    Assert.assertEquals(result.getNumRowsResultSet(), Long.valueOf(100));
  }

  @Test
  public void testExecuteCursorQueryWithNoBroker() {
    // Setup broker selector to return null
    when(_mockBrokerSelector.selectBroker(any())).thenReturn(null);

    // Execute cursor query should throw exception
    try {
      _connection.executeCursorQuery("SELECT * FROM testTable", 10);
      Assert.fail("Should throw PinotClientException");
    } catch (PinotClientException e) {
      Assert.assertEquals(e.getMessage(), "Could not find broker to execute cursor query");
    }
  }

  @Test
  public void testExecuteCursorQueryAsyncWithNoBroker() throws Exception {
    // Setup broker selector to return null
    when(_mockBrokerSelector.selectBroker(any())).thenReturn(null);

    // Execute cursor query async should return failed future
    CompletableFuture<CursorResultSetGroup> future =
        _connection.executeCursorQueryAsync("SELECT * FROM testTable", 10);

    try {
      future.get();
      Assert.fail("Should throw ExecutionException");
    } catch (ExecutionException e) {
      Assert.assertTrue(e.getCause() instanceof PinotClientException);
      Assert.assertEquals(e.getCause().getMessage(), "Could not find broker to execute cursor query");
    }
  }

  @Test
  public void testExecuteCursorQueryWithExceptions() throws Exception {
    // Create response with exceptions in the format expected by BrokerResponse
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode dataSchema = mapper.createObjectNode();
    dataSchema.set("columnNames", mapper.createArrayNode());
    dataSchema.set("columnDataTypes", mapper.createArrayNode());

    ObjectNode resultTable = mapper.createObjectNode();
    resultTable.set("dataSchema", dataSchema);
    resultTable.set("rows", mapper.createArrayNode());

    ObjectNode responseWithExceptions = mapper.createObjectNode();
    responseWithExceptions.put("offset", 0);
    responseWithExceptions.put("numRows", 0);
    responseWithExceptions.put("numRowsResultSet", 0);
    responseWithExceptions.set("resultTable", resultTable);
    responseWithExceptions.set("exceptions", mapper.createArrayNode()
        .add(mapper.createObjectNode().put("errorCode", 500).put("message", "Test exception")));

    CursorAwareBrokerResponse responseWithError = CursorAwareBrokerResponse.fromJson(responseWithExceptions);
    when(_mockTransport.executeQueryWithCursor(anyString(), anyString(), anyInt()))
        .thenReturn(responseWithError);

    // Execute cursor query should throw exception due to response exceptions
    try {
      _connection.executeCursorQuery("SELECT * FROM testTable", 10);
      Assert.fail("Should throw PinotClientException");
    } catch (PinotClientException e) {
      Assert.assertTrue(e.getMessage().contains("Query had processing exceptions")
          || e.getMessage().contains("Failed to execute cursor query"),
          "Expected exception message to contain exception info, but was: " + e.getMessage());
    }
  }

  @Test
  public void testFetchNextUnsupported() {
    // Create connection with non-JsonAsyncHttpPinotClientTransport
    PinotClientTransport<?> mockTransport = Mockito.mock(PinotClientTransport.class);
    Connection connection = new Connection(ImmutableList.of("localhost:8000"), mockTransport);

    // fetchNext should throw UnsupportedOperationException
    try {
      connection.fetchNext("test-cursor");
      Assert.fail("Should throw UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      Assert.assertEquals(e.getMessage(), "Cursor operations not supported by this connection type");
    }
  }

  @Test
  public void testFetchNextAsyncUnsupported() throws Exception {
    // Create connection with non-JsonAsyncHttpPinotClientTransport
    PinotClientTransport<?> mockTransport = Mockito.mock(PinotClientTransport.class);
    Connection connection = new Connection(ImmutableList.of("localhost:8000"), mockTransport);

    // fetchNextAsync should return failed future
    CompletableFuture<CursorResultSetGroup> future = connection.fetchNextAsync("test-cursor");

    try {
      future.get();
      Assert.fail("Should throw ExecutionException");
    } catch (ExecutionException e) {
      Assert.assertTrue(e.getCause() instanceof UnsupportedOperationException);
      Assert.assertEquals(e.getCause().getMessage(), "Cursor operations not supported by this connection type");
    }
  }

  @Test
  public void testFetchPreviousUnsupported() {
    // Create connection with non-JsonAsyncHttpPinotClientTransport
    PinotClientTransport<?> mockTransport = Mockito.mock(PinotClientTransport.class);
    Connection connection = new Connection(ImmutableList.of("localhost:8000"), mockTransport);

    // fetchPrevious should throw UnsupportedOperationException
    try {
      connection.fetchPrevious("test-cursor");
      Assert.fail("Should throw UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      Assert.assertEquals(e.getMessage(), "Cursor operations not supported by this connection type");
    }
  }

  @Test
  public void testFetchPreviousAsyncUnsupported() throws Exception {
    // Create connection with non-JsonAsyncHttpPinotClientTransport
    PinotClientTransport<?> mockTransport = Mockito.mock(PinotClientTransport.class);
    Connection connection = new Connection(ImmutableList.of("localhost:8000"), mockTransport);

    // fetchPreviousAsync should return failed future
    CompletableFuture<CursorResultSetGroup> future = connection.fetchPreviousAsync("test-cursor");

    try {
      future.get();
      Assert.fail("Should throw ExecutionException");
    } catch (ExecutionException e) {
      Assert.assertTrue(e.getCause() instanceof UnsupportedOperationException);
      Assert.assertEquals(e.getCause().getMessage(), "Cursor operations not supported by this connection type");
    }
  }

  @Test
  public void testSeekToPageUnsupported() {
    // Create connection with non-JsonAsyncHttpPinotClientTransport
    PinotClientTransport<?> mockTransport = Mockito.mock(PinotClientTransport.class);
    Connection connection = new Connection(ImmutableList.of("localhost:8000"), mockTransport);

    // seekToPage should throw UnsupportedOperationException
    try {
      connection.seekToPage("test-cursor", 5);
      Assert.fail("Should throw UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      Assert.assertEquals(e.getMessage(), "Cursor operations not supported by this connection type");
    }
  }

  @Test
  public void testSeekToPageAsyncUnsupported() throws Exception {
    // Create connection with non-JsonAsyncHttpPinotClientTransport
    PinotClientTransport<?> mockTransport = Mockito.mock(PinotClientTransport.class);
    Connection connection = new Connection(ImmutableList.of("localhost:8000"), mockTransport);

    // seekToPageAsync should return failed future
    CompletableFuture<CursorResultSetGroup> future = connection.seekToPageAsync("test-cursor", 5);

    try {
      future.get();
      Assert.fail("Should throw ExecutionException");
    } catch (ExecutionException e) {
      Assert.assertTrue(e.getCause() instanceof UnsupportedOperationException);
      Assert.assertEquals(e.getCause().getMessage(), "Cursor operations not supported by this connection type");
    }
  }
}
