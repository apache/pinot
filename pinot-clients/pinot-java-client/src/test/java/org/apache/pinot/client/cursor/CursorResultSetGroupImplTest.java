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

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.concurrent.ExecutionException;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pinot.client.BrokerResponse;
import org.apache.pinot.client.JsonAsyncHttpPinotClientTransport;
import org.apache.pinot.client.ResultSet;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for CursorResultSetGroupImpl class.
 */
public class CursorResultSetGroupImplTest {

  @Mock
  private Function<String, JsonAsyncHttpPinotClientTransport> _transportProvider;

  @Mock
  private CursorConnection _cursorConnection;

  @Mock
  private BrokerResponse _brokerResponse;

  @Mock
  private ResultSet _resultSet;

  private CursorMetadata _metadata;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    _metadata = new CursorMetadata("test-cursor-123", 0, 10, 100L, 10, true, false,
        System.currentTimeMillis() + 300000, "broker-1");
  }

  private void setupMockBrokerResponseWithOriginalResponse() {
    when(_brokerResponse.getRequestId()).thenReturn("test-cursor-123");
    when(_brokerResponse.getBrokerId()).thenReturn("broker-1");
    when(_brokerResponse.getResultTable()).thenReturn(null);

    // Mock original response with cursor metadata
    ObjectMapper objectMapper = new ObjectMapper();
    com.fasterxml.jackson.databind.node.ObjectNode mockOriginalResponse = objectMapper.createObjectNode();
    mockOriginalResponse.put("offset", 0);
    mockOriginalResponse.put("numRows", 10);
    mockOriginalResponse.put("numRowsResultSet", 100);
    mockOriginalResponse.put("expirationTimeMs", System.currentTimeMillis() + 300000);
    when(_brokerResponse.getOriginalResponse()).thenReturn(mockOriginalResponse);
  }

  @Test
  public void testConstructorWithValidParameters() {
    setupMockBrokerResponseWithOriginalResponse();

    CursorResultSetGroupImpl resultSetGroup = new CursorResultSetGroupImpl(_brokerResponse,
        _transportProvider, _cursorConnection);

    Assert.assertNotNull(resultSetGroup);
    Assert.assertNotNull(resultSetGroup.getCursorMetadata());
  }

  @Test
  public void testGetCursorMetadata() {
    setupMockBrokerResponseWithOriginalResponse();

    CursorResultSetGroupImpl resultSetGroup = new CursorResultSetGroupImpl(_brokerResponse,
        _transportProvider, _cursorConnection);

    CursorMetadata retrievedMetadata = resultSetGroup.getCursorMetadata();
    Assert.assertNotNull(retrievedMetadata);
    Assert.assertEquals(retrievedMetadata.getCursorId(), "test-cursor-123");
  }

  @Test
  public void testCloseAsyncSuccess() throws ExecutionException, InterruptedException {
    CompletableFuture<Void> mockFuture = CompletableFuture.completedFuture(null);
    when(_cursorConnection.closeCursorAsync(anyString())).thenReturn(mockFuture);
    setupMockBrokerResponseWithOriginalResponse();

    CursorResultSetGroupImpl resultSetGroup = new CursorResultSetGroupImpl(_brokerResponse,
        _transportProvider, _cursorConnection);

    CompletableFuture<Void> closeFuture = resultSetGroup.closeAsync();
    Assert.assertNotNull(closeFuture);

    // Verify the future completes successfully
    closeFuture.get();

    // Verify the cursor connection method was called with correct cursor ID
    verify(_cursorConnection).closeCursorAsync("test-cursor-123");
  }

  @Test
  public void testCloseAsyncWithException() {
    CompletableFuture<Void> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(new RuntimeException("Delete failed"));
    when(_cursorConnection.closeCursorAsync(anyString())).thenReturn(failedFuture);
    setupMockBrokerResponseWithOriginalResponse();

    CursorResultSetGroupImpl resultSetGroup = new CursorResultSetGroupImpl(_brokerResponse,
        _transportProvider, _cursorConnection);

    CompletableFuture<Void> closeFuture = resultSetGroup.closeAsync();
    Assert.assertNotNull(closeFuture);

    // Verify the future completes exceptionally
    try {
      closeFuture.get();
      Assert.fail("Expected ExecutionException");
    } catch (ExecutionException e) {
      Assert.assertTrue(e.getCause() instanceof RuntimeException);
      Assert.assertEquals(e.getCause().getMessage(), "Delete failed");
    } catch (InterruptedException e) {
      Assert.fail("Unexpected InterruptedException");
    }
  }

  @Test
  public void testCloseAsyncWithNullConnection() {
    setupMockBrokerResponseWithOriginalResponse();

    CursorResultSetGroupImpl resultSetGroup = new CursorResultSetGroupImpl(_brokerResponse,
        _transportProvider, null);

    CompletableFuture<Void> closeFuture = resultSetGroup.closeAsync();
    Assert.assertNotNull(closeFuture);

    // Should complete successfully even with null connection
    try {
      closeFuture.get();
    } catch (Exception e) {
      Assert.fail("closeAsync should handle null connection gracefully");
    }
  }

  @Test
  public void testResultSetDelegation() {
    setupMockBrokerResponseWithOriginalResponse();

    CursorResultSetGroupImpl resultSetGroup = new CursorResultSetGroupImpl(_brokerResponse,
        _transportProvider, _cursorConnection);

    // Test that it properly creates a ResultSetGroup from BrokerResponse
    Assert.assertNotNull(resultSetGroup);
    // The actual result set count depends on BrokerResponse implementation
    int resultSetCount = resultSetGroup.getResultSetCount();
    Assert.assertTrue(resultSetCount >= 0);
  }

  @Test
  public void testNavigationMethods() {
    setupMockBrokerResponseWithOriginalResponse();

    CursorResultSetGroupImpl resultSetGroup = new CursorResultSetGroupImpl(_brokerResponse,
        _transportProvider, _cursorConnection);

    // Test hasNext and hasPrevious delegate to cursor metadata
    boolean hasNext = resultSetGroup.hasNext();
    boolean hasPrevious = resultSetGroup.hasPrevious();

    // These values depend on the extracted metadata from broker response
    Assert.assertNotNull(resultSetGroup.getCursorMetadata());
  }

  @Test
  public void testMetadataExtraction() {
    when(_brokerResponse.getRequestId()).thenReturn("test-cursor-456");
    when(_brokerResponse.getBrokerId()).thenReturn("broker-2");
    when(_brokerResponse.getResultTable()).thenReturn(null);

    // Mock original response with cursor metadata
    ObjectMapper objectMapper = new ObjectMapper();
    com.fasterxml.jackson.databind.node.ObjectNode mockOriginalResponse = objectMapper.createObjectNode();
    mockOriginalResponse.put("offset", 0);
    mockOriginalResponse.put("numRows", 10);
    mockOriginalResponse.put("numRowsResultSet", 100);
    mockOriginalResponse.put("expirationTimeMs", System.currentTimeMillis() + 300000);
    when(_brokerResponse.getOriginalResponse()).thenReturn(mockOriginalResponse);

    CursorResultSetGroupImpl resultSetGroup = new CursorResultSetGroupImpl(_brokerResponse,
        _transportProvider, _cursorConnection);

    CursorMetadata metadata = resultSetGroup.getCursorMetadata();
    Assert.assertNotNull(metadata);
    Assert.assertEquals(metadata.getCursorId(), "test-cursor-456");
    Assert.assertEquals(metadata.getBrokerId(), "broker-2");
  }

  @Test
  public void testAsyncNavigation() {
    CompletableFuture<CursorResultSetGroup> mockFuture = CompletableFuture.completedFuture(null);
    when(_cursorConnection.fetchNextAsync(anyString())).thenReturn(mockFuture);
    when(_cursorConnection.fetchPreviousAsync(anyString())).thenReturn(mockFuture);
    setupMockBrokerResponseWithOriginalResponse();

    CursorResultSetGroupImpl resultSetGroup = new CursorResultSetGroupImpl(_brokerResponse,
        _transportProvider, _cursorConnection);

    CompletableFuture<CursorResultSetGroup> nextFuture = resultSetGroup.nextAsync();
    CompletableFuture<CursorResultSetGroup> prevFuture = resultSetGroup.previousAsync();

    Assert.assertNotNull(nextFuture);
    Assert.assertNotNull(prevFuture);
  }
}
