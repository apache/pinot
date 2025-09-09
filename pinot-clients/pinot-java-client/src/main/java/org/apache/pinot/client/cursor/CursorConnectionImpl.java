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

import com.fasterxml.jackson.databind.JsonNode;
import java.util.concurrent.CompletableFuture;
import org.apache.pinot.client.BrokerResponse;
import org.apache.pinot.client.Connection;
import org.apache.pinot.client.JsonAsyncHttpPinotClientTransport;
import org.apache.pinot.client.PreparedStatement;
import org.apache.pinot.client.cursor.exceptions.CursorException;

/**
 * Implementation of CursorConnection that provides cursor-based pagination support.
 * Wraps a standard Connection and adds cursor functionality through JsonAsyncHttpPinotClientTransport.
 */
public class CursorConnectionImpl implements CursorConnection {
  private final JsonAsyncHttpPinotClientTransport _transport;
  private final Connection _connection;

  /**
   * Creates a cursor connection wrapping the provided connection.
   *
   * @param connection the underlying connection
   * @throws IllegalArgumentException if the connection's transport is not JsonAsyncHttpPinotClientTransport
   */
  public CursorConnectionImpl(Connection connection) {
    _connection = connection;

    // Extract the transport and verify it's the correct type
    if (!(connection.getTransport() instanceof JsonAsyncHttpPinotClientTransport)) {
      throw new IllegalArgumentException(
          "CursorConnection requires JsonAsyncHttpPinotClientTransport, got: "
          + connection.getTransport().getClass().getSimpleName());
    }
    _transport = (JsonAsyncHttpPinotClientTransport) connection.getTransport();
  }

  private String selectBroker() {
    // Since getBrokerList() is not accessible, we'll use a simple approach
    // In a real implementation, this would need to be coordinated with the Connection class
    // For now, we'll assume the transport can handle broker selection
    return "localhost:8099"; // Default broker - this should be made configurable
  }

  @Override
  public CursorResultSetGroup executeWithCursor(String query, int pageSize) throws CursorException {
    try {
      return executeWithCursorAsync(query, pageSize).get();
    } catch (Exception e) {
      throw new CursorException("Failed to execute query with cursor", e);
    }
  }

  @Override
  public CompletableFuture<CursorResultSetGroup> executeWithCursorAsync(String query, int pageSize) {
    String brokerAddress = selectBroker();
    if (brokerAddress == null) {
      return CompletableFuture.failedFuture(
          new CursorException("Could not find broker to execute cursor query: " + query));
    }
    return _transport.executeQueryWithCursorAsync(brokerAddress, query, pageSize)
        .thenApply(brokerResponse -> createCursorResultSetGroup(brokerResponse, addr -> _transport));
  }

  @Override
  public CursorResultSetGroup executeWithCursor(PreparedStatement statement, int pageSize) throws CursorException {
    try {
      return executeWithCursorAsync(statement, pageSize).get();
    } catch (UnsupportedOperationException e) {
      throw e; // Re-throw UnsupportedOperationException as-is
    } catch (Exception e) {
      throw new CursorException("Failed to execute prepared statement with cursor", e);
    }
  }

  @Override
  public CompletableFuture<CursorResultSetGroup> executeWithCursorAsync(PreparedStatement statement, int pageSize) {
    // PreparedStatement doesn't expose getQuery(), so we need to execute it differently
    // We'll need to modify this approach or add a method to PreparedStatement
    throw new UnsupportedOperationException("Cursor operations with PreparedStatement not yet supported");
  }

  @Override
  public CursorResultSetGroup fetchNext(String cursorId) throws CursorException {
    try {
      return fetchNextAsync(cursorId).get();
    } catch (Exception e) {
      throw new CursorException("Failed to fetch next page", e);
    }
  }

  @Override
  public CompletableFuture<CursorResultSetGroup> fetchNextAsync(String cursorId) {
    return getCursorMetadataAsync(cursorId)
        .thenCompose(metadata -> {
          // Simple approach: use server metadata directly for navigation
          // The server knows the current offset and can determine next page
          int currentOffset = metadata.getCurrentPage() * metadata.getPageSize();
          int pageSize = metadata.getPageSize();
          int nextOffset = currentOffset + pageSize;

          if (nextOffset >= metadata.getTotalRows()) {
            return CompletableFuture.failedFuture(
                new CursorException("No next page available for cursor: " + cursorId));
          }

          return _transport.fetchCursorResultsAsync(metadata.getBrokerId(), cursorId, nextOffset, pageSize);
        })
        .thenApply(brokerResponse -> createCursorResultSetGroup(brokerResponse, brokerAddress -> _transport));
  }

  @Override
  public CursorResultSetGroup fetchPrevious(String cursorId) throws CursorException {
    try {
      return fetchPreviousAsync(cursorId).get();
    } catch (Exception e) {
      throw new CursorException("Failed to fetch previous page", e);
    }
  }

  @Override
  public CompletableFuture<CursorResultSetGroup> fetchPreviousAsync(String cursorId) {
    return getCursorMetadataAsync(cursorId)
        .thenCompose(metadata -> {
          int currentOffset = metadata.getCurrentPage() * metadata.getPageSize();
          int pageSize = metadata.getPageSize();
          int previousOffset = currentOffset - pageSize;

          if (previousOffset < 0) {
            return CompletableFuture.failedFuture(
                new CursorException("No previous page available for cursor: " + cursorId));
          }

          return _transport.fetchCursorResultsAsync(metadata.getBrokerId(), cursorId, previousOffset, pageSize);
        })
        .thenApply(brokerResponse -> createCursorResultSetGroup(brokerResponse, brokerAddress -> _transport));
  }

  @Override
  public CursorResultSetGroup seekToPage(String cursorId, int offset) throws CursorException {
    try {
      return seekToPageAsync(cursorId, offset).get();
    } catch (Exception e) {
      throw new CursorException("Failed to seek to page", e);
    }
  }

  @Override
  public CompletableFuture<CursorResultSetGroup> seekToPageAsync(String cursorId, int pageNumber) {
    return getCursorMetadataAsync(cursorId)
        .thenCompose(metadata -> {
          if (pageNumber < 0 || pageNumber >= metadata.getTotalPages()) {
            return CompletableFuture.failedFuture(
                new CursorException("Invalid page number: " + pageNumber
                    + ". Valid range: 0 to " + (metadata.getTotalPages() - 1)));
          }
          int pageSize = metadata.getPageSize();
          int offset = pageNumber * pageSize;
          return _transport.fetchCursorResultsAsync(metadata.getBrokerId(), cursorId, offset, pageSize);
        })
        .thenApply(brokerResponse -> createCursorResultSetGroup(brokerResponse, brokerAddress -> _transport));
  }

  @Override
  public CursorMetadata getCursorMetadata(String cursorId) throws CursorException {
    try {
      return getCursorMetadataAsync(cursorId).get();
    } catch (Exception e) {
      throw new CursorException("Failed to get cursor metadata", e);
    }
  }

  @Override
  public CompletableFuture<CursorMetadata> getCursorMetadataAsync(String cursorId) {
    String brokerAddress = selectBroker();
    if (brokerAddress == null) {
      return CompletableFuture.failedFuture(
          new CursorException("Could not find broker to get cursor metadata for: " + cursorId));
    }
    return _transport.getCursorMetadataAsync(brokerAddress, cursorId)
        .thenApply(brokerResponse -> extractCursorMetadata(cursorId, brokerResponse));
  }

  @Override
  public void closeCursor(String cursorId) throws CursorException {
    try {
      closeCursorAsync(cursorId).get();
    } catch (Exception e) {
      throw new CursorException("Failed to close cursor", e);
    }
  }

  @Override
  public CompletableFuture<Void> closeCursorAsync(String cursorId) {
    String brokerAddress = selectBroker();
    if (brokerAddress == null) {
      return CompletableFuture.failedFuture(
          new CursorException("Could not find broker to close cursor: " + cursorId));
    }
    return _transport.deleteCursorAsync(brokerAddress, cursorId);
  }

  private CursorResultSetGroup createCursorResultSetGroup(BrokerResponse brokerResponse,
      java.util.function.Function<String, JsonAsyncHttpPinotClientTransport> transportProvider) {
    return new CursorResultSetGroupImpl(brokerResponse, transportProvider, this);
  }

  private CursorMetadata extractCursorMetadata(String cursorId, BrokerResponse brokerResponse) throws CursorException {
    return extractCursorMetadata(cursorId, brokerResponse, -1);
  }

  private CursorMetadata extractCursorMetadata(String cursorId, BrokerResponse brokerResponse, int originalPageSize)
      throws CursorException {
    try {
      JsonNode originalResponse = brokerResponse.getOriginalResponse();
      if (originalResponse == null) {
        throw new CursorException("No original response available for cursor metadata extraction");
      }

      // Extract pagination information from root level
      int offset = originalResponse.get("offset").asInt();
      int pageSize = originalPageSize > 0 ? originalPageSize : originalResponse.get("numRows").asInt();
      long totalRows = originalResponse.get("numRowsResultSet").asLong();
      long expirationTime = originalResponse.get("expirationTimeMs").asLong();

      // Calculate pagination info
      int currentPage = pageSize > 0 ? offset / pageSize : 0;
      int totalPages = pageSize > 0 && totalRows > 0 ? (int) Math.ceil((double) totalRows / pageSize) : 1;
      boolean hasNext = (offset + pageSize) < totalRows;
      boolean hasPrevious = offset > 0;

      String brokerId = brokerResponse.getBrokerId();

      return new CursorMetadata(cursorId, currentPage, pageSize, totalRows, totalPages,
          hasNext, hasPrevious, expirationTime, brokerId);
    } catch (Exception e) {
      if (e instanceof CursorException) {
        throw e;
      }
      throw new CursorException("Failed to parse cursor metadata", e);
    }
  }


  // Delegation methods for backward compatibility with Connection interface
  public org.apache.pinot.client.ResultSetGroup execute(String query) {
    return _connection.execute(query);
  }

  public java.util.concurrent.CompletableFuture<org.apache.pinot.client.ResultSetGroup> executeAsync(String query) {
    return _connection.executeAsync(query);
  }

  // Note: PreparedStatement execution delegated to underlying connection

  public org.apache.pinot.client.PreparedStatement prepareStatement(String query) {
    return _connection.prepareStatement(query);
  }

  public void close() throws java.io.IOException {
    _connection.close();
  }

  public org.apache.pinot.client.PinotClientTransport<?> getTransport() {
    return _connection.getTransport();
  }
}
