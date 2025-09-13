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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ResultCursorTest {

  private static class MockCursorTransport extends JsonAsyncHttpPinotClientTransport {
    private int _currentPage = 1;
    private final int _totalPages = 3;
    private final int _pageSize = 10;

    public MockCursorTransport() {
      super();
    }

    @Override
    public CursorAwareBrokerResponse executeQueryWithCursor(String brokerHostPort, String query, int numRows) {
      _currentPage = 1;
      return createMockResponse(1, true, false);
    }

    @Override
    public CursorAwareBrokerResponse fetchNextPage(String brokerHostPort, String cursorId, int offset, int numRows) {
      _currentPage++;
      return createMockResponse(_currentPage, _currentPage < _totalPages, _currentPage > 1);
    }

    @Override
    public CompletableFuture<CursorAwareBrokerResponse> fetchNextPageAsync(String brokerHostPort, String cursorId,
        int offset, int numRows) {
      return CompletableFuture.completedFuture(fetchNextPage(brokerHostPort, cursorId, offset, numRows));
    }

    @Override
    public CursorAwareBrokerResponse fetchPreviousPage(String brokerHostPort, String cursorId, int offset,
        int numRows) {
      _currentPage--;
      return createMockResponse(_currentPage, _currentPage < _totalPages, _currentPage > 1);
    }

    @Override
    public CompletableFuture<CursorAwareBrokerResponse> fetchPreviousPageAsync(String brokerHostPort, String cursorId,
        int offset, int numRows) {
      return CompletableFuture.completedFuture(fetchPreviousPage(brokerHostPort, cursorId, offset, numRows));
    }

    @Override
    public CursorAwareBrokerResponse seekToPage(String brokerHostPort, String cursorId, int offset, int numRows) {
      _currentPage = (offset / _pageSize);
      return createMockResponse(_currentPage + 1, (_currentPage + 1) < _totalPages, _currentPage > 0);
    }

    @Override
    public CompletableFuture<CursorAwareBrokerResponse> seekToPageAsync(String brokerHostPort, String cursorId,
        int offset, int numRows) {
      _currentPage = (offset / _pageSize);
      return CompletableFuture.completedFuture(
          createMockResponse(_currentPage + 1, (_currentPage + 1) < _totalPages, _currentPage > 0));
    }

    private CursorAwareBrokerResponse createMockResponse(int pageNum, boolean hasNext, boolean hasPrevious) {
      try {
        ObjectMapper mapper = new ObjectMapper();
        // Calculate total rows across all pages
        int totalRows = _totalPages * _pageSize;
        String jsonResponse = String.format(
            "{"
            + "\"requestId\": \"cursor-test-%d\","
            + "\"numRowsResultSet\": %d,"
            + "\"numRows\": %d,"
            + "\"offset\": %d,"
            + "\"expirationTimeMs\": %d,"
            + "\"resultTable\": {"
            + "  \"dataSchema\": {"
            + "    \"columnNames\": [\"col1\", \"col2\"],"
            + "    \"columnDataTypes\": [\"STRING\", \"INT\"]"
            + "  },"
            + "  \"rows\": ["
            + "    [\"value%d_1\", %d],"
            + "    [\"value%d_2\", %d]"
            + "  ]"
            + "}"
            + "}",
            pageNum, totalRows, _pageSize, (pageNum - 1) * _pageSize,
            System.currentTimeMillis() + 300000, // 5 minutes from now
            pageNum, pageNum * 10,
            pageNum, pageNum * 10 + 1
        );

        JsonNode jsonNode = mapper.readTree(jsonResponse);
        return CursorAwareBrokerResponse.fromJson(jsonNode);
      } catch (Exception e) {
        throw new RuntimeException("Failed to create mock response", e);
      }
    }
  }

  @Test
  public void testCursorCreation() throws Exception {
    MockCursorTransport transport = new MockCursorTransport();
    CursorAwareBrokerResponse initialResponse = transport.executeQueryWithCursor("localhost:8000",
        "SELECT * FROM test", 10);

    try (ResultCursor cursor = new ResultCursorImpl(transport, "localhost:8000", initialResponse, false)) {
      Assert.assertNotNull(cursor.getCurrentPage());
      Assert.assertEquals(cursor.getCurrentPageNumber(), 1);
      Assert.assertEquals(cursor.getPageSize(), 10);
      Assert.assertTrue(cursor.hasNext());
      Assert.assertFalse(cursor.hasPrevious());
      Assert.assertNotNull(cursor.getCursorId());
      Assert.assertFalse(cursor.isExpired());
    }
  }

  @Test
  public void testCursorNavigation() throws Exception {
    MockCursorTransport transport = new MockCursorTransport();
    CursorAwareBrokerResponse initialResponse = transport.executeQueryWithCursor("localhost:8000",
        "SELECT * FROM test", 10);

    try (ResultCursor cursor = new ResultCursorImpl(transport, "localhost:8000", initialResponse,
        false)) {
      // Test next navigation
      Assert.assertTrue(cursor.hasNext());
      CursorResultSetGroup page1 = cursor.next();
      Assert.assertNotNull(page1);
      Assert.assertEquals(cursor.getCurrentPageNumber(), 2);
      Assert.assertTrue(cursor.hasNext());
      Assert.assertTrue(cursor.hasPrevious());

      // Test next again
      CursorResultSetGroup page2 = cursor.next();
      Assert.assertNotNull(page2);
      Assert.assertEquals(cursor.getCurrentPageNumber(), 3);
      Assert.assertFalse(cursor.hasNext());
      Assert.assertTrue(cursor.hasPrevious());

      // Test previous navigation
      CursorResultSetGroup page2Again = cursor.previous();
      Assert.assertNotNull(page2Again);
      Assert.assertEquals(cursor.getCurrentPageNumber(), 2);
      Assert.assertTrue(cursor.hasNext());
      Assert.assertTrue(cursor.hasPrevious());

      // Test previous to first page
      CursorResultSetGroup page1Back = cursor.previous();
      Assert.assertNotNull(page1Back);
      Assert.assertEquals(cursor.getCurrentPageNumber(), 1);
      Assert.assertTrue(cursor.hasNext());
      Assert.assertFalse(cursor.hasPrevious());
    }
  }

  @Test
  public void testSeekToPage() throws Exception {
    MockCursorTransport transport = new MockCursorTransport();
    CursorAwareBrokerResponse initialResponse = transport.executeQueryWithCursor("localhost:8000",
        "SELECT * FROM test", 10);

    try (ResultCursor cursor = new ResultCursorImpl(transport, "localhost:8000", initialResponse, false)) {
      // Seek to page 2
      CursorResultSetGroup page2 = cursor.seekToPage(2);
      Assert.assertNotNull(page2);
      Assert.assertEquals(cursor.getCurrentPageNumber(), 2);
      Assert.assertTrue(cursor.hasNext());
      Assert.assertTrue(cursor.hasPrevious());

      // Seek back to page 1
      CursorResultSetGroup page1 = cursor.seekToPage(1);
      Assert.assertNotNull(page1);
      Assert.assertEquals(cursor.getCurrentPageNumber(), 1);
      Assert.assertTrue(cursor.hasNext());
      Assert.assertFalse(cursor.hasPrevious());
    }
  }

  @Test
  public void testAsyncNavigation() throws Exception {
    MockCursorTransport transport = new MockCursorTransport();
    CursorAwareBrokerResponse initialResponse = transport.executeQueryWithCursor("localhost:8000",
        "SELECT * FROM test", 10);

    try (ResultCursor cursor = new ResultCursorImpl(transport, "localhost:8000", initialResponse, false)) {
      // Test async next
      CompletableFuture<CursorResultSetGroup> nextFuture = cursor.nextAsync();
      CursorResultSetGroup page2 = nextFuture.get();
      Assert.assertNotNull(page2);
      Assert.assertEquals(cursor.getCurrentPageNumber(), 2);

      // Test async previous
      CompletableFuture<CursorResultSetGroup> prevFuture = cursor.previousAsync();
      CursorResultSetGroup page1 = prevFuture.get();
      Assert.assertNotNull(page1);
      Assert.assertEquals(cursor.getCurrentPageNumber(), 1);

      // Test async seek
      CompletableFuture<CursorResultSetGroup> seekFuture = cursor.seekToPageAsync(2);
      CursorResultSetGroup page2Seek = seekFuture.get();
      Assert.assertNotNull(page2Seek);
      Assert.assertEquals(cursor.getCurrentPageNumber(), 2);
    }
  }

  @Test
  public void testNavigationBoundaryConditions() throws Exception {
    MockCursorTransport transport = new MockCursorTransport();
    CursorAwareBrokerResponse initialResponse = transport.executeQueryWithCursor("localhost:8000",
        "SELECT * FROM test", 10);

    try (ResultCursor cursor = new ResultCursorImpl(transport, "localhost:8000", initialResponse, false)) {
      // Try to go previous when at first page
      Assert.assertFalse(cursor.hasPrevious());
      try {
        cursor.previous();
        Assert.fail("Expected IllegalStateException");
      } catch (IllegalStateException e) {
        Assert.assertEquals(e.getMessage(), "No previous page available");
      }

      // Navigate to last page
      cursor.next();
      cursor.next();
      Assert.assertFalse(cursor.hasNext());

      // Try to go next when at last page
      try {
        cursor.next();
        Assert.fail("Expected IllegalStateException");
      } catch (IllegalStateException e) {
        Assert.assertEquals(e.getMessage(), "No next page available");
      }
    }
  }

  @Test
  public void testClosedCursorOperations() throws Exception {
    MockCursorTransport transport = new MockCursorTransport();
    CursorAwareBrokerResponse initialResponse = transport.executeQueryWithCursor("localhost:8000",
        "SELECT * FROM test", 10);

    ResultCursor cursor = new ResultCursorImpl(transport, "localhost:8000", initialResponse, false);
    cursor.close();
    // Test that all operations throw IllegalStateException after close
    try {
      cursor.getCurrentPage();
      Assert.fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      Assert.assertEquals(e.getMessage(), "Cursor is closed");
    }

    try {
      cursor.hasNext();
      Assert.fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      Assert.assertEquals(e.getMessage(), "Cursor is closed");
    }

    try {
      cursor.next();
      Assert.fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      Assert.assertEquals(e.getMessage(), "Cursor is closed");
    }
  }

  @Test
  public void testInvalidSeekPageNumber() throws Exception {
    MockCursorTransport transport = new MockCursorTransport();
    CursorAwareBrokerResponse initialResponse = transport.executeQueryWithCursor("localhost:8000",
        "SELECT * FROM test", 10);

    try (ResultCursor cursor = new ResultCursorImpl(transport, "localhost:8000", initialResponse, false)) {
      try {
        cursor.seekToPage(-1);
        Assert.fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) {
        Assert.assertEquals(e.getMessage(), "Page number must be positive (1-based)");
      }
    }
  }

  @Test
  public void testAsyncNavigationBoundaryConditions() throws Exception {
    MockCursorTransport transport = new MockCursorTransport();
    CursorAwareBrokerResponse initialResponse = transport.executeQueryWithCursor("localhost:8000",
        "SELECT * FROM test", 10);

    try (ResultCursor cursor = new ResultCursorImpl(transport, "localhost:8000", initialResponse, false)) {
      // Try async previous when at first page
      CompletableFuture<CursorResultSetGroup> prevFuture = cursor.previousAsync();
      try {
        prevFuture.get();
        Assert.fail("Expected ExecutionException");
      } catch (ExecutionException e) {
        Assert.assertTrue(e.getCause() instanceof IllegalStateException);
        Assert.assertEquals(e.getCause().getMessage(), "No previous page available");
      }

      // Navigate to last page
      cursor.next();
      cursor.next();

      // Try async next when at last page
      CompletableFuture<CursorResultSetGroup> nextFuture = cursor.nextAsync();
      try {
        nextFuture.get();
        Assert.fail("Expected ExecutionException");
      } catch (ExecutionException e) {
        Assert.assertTrue(e.getCause() instanceof IllegalStateException);
        Assert.assertEquals(e.getCause().getMessage(), "No next page available");
      }

      // Try async seek with invalid page number
      CompletableFuture<CursorResultSetGroup> seekFuture = cursor.seekToPageAsync(-1);
      try {
        seekFuture.get();
        Assert.fail("Expected ExecutionException");
      } catch (ExecutionException e) {
        Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
        Assert.assertEquals(e.getCause().getMessage(), "Page number must be positive (1-based)");
      }
    }
  }
}
