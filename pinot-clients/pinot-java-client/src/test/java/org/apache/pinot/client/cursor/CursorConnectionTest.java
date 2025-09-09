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
package org.apache.pinot.client.cursor;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.CompletableFuture;
import org.apache.pinot.client.BrokerResponse;
import org.apache.pinot.client.Connection;
import org.apache.pinot.client.JsonAsyncHttpPinotClientTransport;
import org.apache.pinot.client.PinotClientTransport;
import org.apache.pinot.client.PreparedStatement;
import org.apache.pinot.client.cursor.exceptions.CursorException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Unit tests for CursorConnection and CursorConnectionImpl.
 */
public class CursorConnectionTest {

  @Mock
  private Connection _mockConnection;

  @Mock
  private JsonAsyncHttpPinotClientTransport _mockTransport;

  @Mock
  private BrokerResponse _mockBrokerResponse;

  private CursorConnectionImpl _cursorConnection;
  private ObjectMapper _objectMapper = new ObjectMapper();

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    // Setup mock connection to return the mock transport
    @SuppressWarnings({"unchecked", "rawtypes"})
    PinotClientTransport transport = _mockTransport;
    when(_mockConnection.getTransport()).thenReturn(transport);

    // Setup mock broker response with cursor metadata
    setupMockBrokerResponse();

    _cursorConnection = new CursorConnectionImpl(_mockConnection);
  }

  private void setupMockBrokerResponse() {
    try {
      // Mock the BrokerResponse to return the requestId (which is used as cursorId)
      when(_mockBrokerResponse.getRequestId()).thenReturn("test-cursor-123");
      when(_mockBrokerResponse.getBrokerId()).thenReturn("localhost:8099");
      when(_mockBrokerResponse.hasExceptions()).thenReturn(false);

      // Create mock result table with data schema
      com.fasterxml.jackson.databind.node.ObjectNode mockResultTable = _objectMapper.createObjectNode();

      // Add dataSchema
      com.fasterxml.jackson.databind.node.ObjectNode dataSchema = _objectMapper.createObjectNode();
      dataSchema.set("columnNames", _objectMapper.createArrayNode().add("col1").add("col2"));
      dataSchema.set("columnDataTypes", _objectMapper.createArrayNode().add("STRING").add("INT"));
      mockResultTable.set("dataSchema", dataSchema);

      // Add rows
      mockResultTable.set("rows", _objectMapper.createArrayNode()
          .add(_objectMapper.createArrayNode().add("value1").add(123))
          .add(_objectMapper.createArrayNode().add("value2").add(456)));

      when(_mockBrokerResponse.getResultTable()).thenReturn(mockResultTable);

      // Create mock original response with cursor metadata at root level
      com.fasterxml.jackson.databind.node.ObjectNode mockOriginalResponse = _objectMapper.createObjectNode();
      mockOriginalResponse.put("offset", 0);
      mockOriginalResponse.put("numRows", 100);
      mockOriginalResponse.put("numRowsResultSet", 1000);
      mockOriginalResponse.put("expirationTimeMs", System.currentTimeMillis() + 300000);
      mockOriginalResponse.set("resultTable", mockResultTable);

      when(_mockBrokerResponse.getOriginalResponse()).thenReturn(mockOriginalResponse);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testConstructorWithValidTransport() {
    // Should not throw exception with JsonAsyncHttpPinotClientTransport
    assertNotNull(_cursorConnection);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testConstructorWithInvalidTransport() {
    // Mock a different transport type
    Connection invalidConnection = mock(Connection.class);
    when(invalidConnection.getTransport()).thenReturn(mock(PinotClientTransport.class));

    new CursorConnectionImpl(invalidConnection);
  }

  @Test
  public void testExecuteWithCursorBasic() throws Exception {
    // Setup mocks
    when(_mockTransport.executeQueryWithCursorAsync("localhost:8099", "SELECT * FROM table", 100))
        .thenReturn(CompletableFuture.completedFuture(_mockBrokerResponse));

    // Execute
    CursorResultSetGroup result = _cursorConnection.executeWithCursor("SELECT * FROM table", 100);

    // Verify
    assertNotNull(result);
    assertNotNull(result.getCursorMetadata());
    assertEquals(result.getCursorMetadata().getCursorId(), "test-cursor-123");
    verify(_mockTransport).executeQueryWithCursorAsync("localhost:8099", "SELECT * FROM table", 100);
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testExecuteWithCursorPreparedStatement() throws Exception {
    PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
    // PreparedStatement doesn't have getQuery() method in this version
    // This should throw UnsupportedOperationException
    _cursorConnection.executeWithCursor(mockPreparedStatement, 75);
  }

  @Test
  public void testExecuteWithCursorAsync() throws Exception {
    CompletableFuture<BrokerResponse> future = CompletableFuture.completedFuture(_mockBrokerResponse);
    when(_mockTransport.executeQueryWithCursorAsync("localhost:8099", "SELECT * FROM table", 100))
        .thenReturn(future);

    CompletableFuture<CursorResultSetGroup> result = _cursorConnection.executeWithCursorAsync(
        "SELECT * FROM table", 100);
    assertNotNull(result);
    CursorResultSetGroup cursorResult = result.get();
    assertEquals(cursorResult.getCursorMetadata().getCursorId(), "test-cursor-123");
  }

  @Test
  public void testFetchNext() throws Exception {
    // Setup metadata response
    when(_mockTransport.getCursorMetadataAsync("localhost:8099", "test-cursor-123"))
        .thenReturn(CompletableFuture.completedFuture(_mockBrokerResponse));

    // Setup fetch response
    when(_mockTransport.fetchCursorResultsAsync("localhost:8099", "test-cursor-123", 100, 100))
        .thenReturn(CompletableFuture.completedFuture(_mockBrokerResponse));

    // Execute fetch
    CursorResultSetGroup result = _cursorConnection.fetchNext("test-cursor-123");

    assertNotNull(result);
    verify(_mockTransport).getCursorMetadataAsync("localhost:8099", "test-cursor-123");
    verify(_mockTransport).fetchCursorResultsAsync("localhost:8099", "test-cursor-123", 100, 100);
  }

  @Test
  public void testGetCursorMetadata() throws Exception {
    when(_mockTransport.getCursorMetadataAsync("localhost:8099", "test-cursor-123"))
        .thenReturn(CompletableFuture.completedFuture(_mockBrokerResponse));

    CursorMetadata metadata = _cursorConnection.getCursorMetadata("test-cursor-123");

    assertNotNull(metadata);
    assertEquals(metadata.getCursorId(), "test-cursor-123");
    assertEquals(metadata.getCurrentPage(), 0);
    assertEquals(metadata.getPageSize(), 100);
    assertEquals(metadata.getTotalRows(), 1000);
    assertEquals(metadata.getTotalPages(), 10);
    assertTrue(metadata.hasNext());
    assertFalse(metadata.hasPrevious());
    verify(_mockTransport).getCursorMetadataAsync("localhost:8099", "test-cursor-123");
  }

  @Test
  public void testCloseCursor() throws Exception {
    when(_mockTransport.deleteCursorAsync("localhost:8099", "test-cursor-123"))
        .thenReturn(CompletableFuture.completedFuture(null));

    _cursorConnection.closeCursor("test-cursor-123");

    verify(_mockTransport).deleteCursorAsync("localhost:8099", "test-cursor-123");
  }

  @Test
  public void testCloseCursorAsync() throws Exception {
    CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
    when(_mockTransport.deleteCursorAsync("localhost:8099", "test-cursor-123"))
        .thenReturn(future);

    CompletableFuture<Void> result = _cursorConnection.closeCursorAsync("test-cursor-123");

    assertNotNull(result);
    result.get(); // Should complete without exception
    verify(_mockTransport).deleteCursorAsync("localhost:8099", "test-cursor-123");
  }

  @Test
  public void testFetchNextWithNoBroker() throws Exception {
    // Since getBrokerList() is not accessible, we'll test a different scenario
    // Test with null transport response
    when(_mockTransport.getCursorMetadataAsync("localhost:8099", "test-cursor-123"))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("No broker available")));

    try {
      _cursorConnection.fetchNext("test-cursor-123");
      fail("Expected CursorException");
    } catch (CursorException e) {
      // Expected
    }
  }
}
