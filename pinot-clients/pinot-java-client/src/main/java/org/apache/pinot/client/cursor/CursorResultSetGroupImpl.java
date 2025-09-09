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
import org.apache.pinot.client.JsonAsyncHttpPinotClientTransport;
import org.apache.pinot.client.ResultSet;
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.client.cursor.exceptions.CursorException;

/**
 * Implementation of CursorResultSetGroup that provides paginated result sets with cursor navigation.
 * Wraps a standard ResultSetGroup and adds cursor functionality.
 */
public class CursorResultSetGroupImpl implements CursorResultSetGroup {
  private final ResultSetGroup _resultSetGroup;
  private final CursorMetadata _cursorMetadata;
  private final CursorConnection _cursorConnection;

  /**
   * Creates a cursor result set group from a broker response.
   *
   * @param brokerResponse the broker response containing results and cursor metadata
   * @param transportProvider function to get transport for a broker address
   * @param cursorConnection the cursor connection for navigation operations
   */
  public CursorResultSetGroupImpl(BrokerResponse brokerResponse,
      java.util.function.Function<String, JsonAsyncHttpPinotClientTransport> transportProvider,
      CursorConnection cursorConnection) {
    _resultSetGroup = new ResultSetGroup(brokerResponse);
    _cursorMetadata = extractCursorMetadata(brokerResponse);
    _cursorConnection = cursorConnection;
  }

  @Override
  public int getResultSetCount() {
    return _resultSetGroup.getResultSetCount();
  }

  @Override
  public ResultSet getResultSet(int index) {
    return _resultSetGroup.getResultSet(index);
  }

  @Override
  public CursorMetadata getCursorMetadata() {
    return _cursorMetadata;
  }

  @Override
  public boolean hasNext() {
    return _cursorMetadata.hasNext();
  }

  @Override
  public boolean hasPrevious() {
    return _cursorMetadata.hasPrevious();
  }

  @Override
  public CursorResultSetGroup next() throws CursorException {
    try {
      return nextAsync().get();
    } catch (Exception e) {
      throw new CursorException("Failed to fetch next page", e);
    }
  }

  @Override
  public CompletableFuture<CursorResultSetGroup> nextAsync() {
    return _cursorConnection.fetchNextAsync(_cursorMetadata.getCursorId());
  }

  @Override
  public CursorResultSetGroup previous() throws CursorException {
    try {
      return previousAsync().get();
    } catch (Exception e) {
      throw new CursorException("Failed to fetch previous page", e);
    }
  }

  @Override
  public CompletableFuture<CursorResultSetGroup> previousAsync() {
    return _cursorConnection.fetchPreviousAsync(_cursorMetadata.getCursorId());
  }

  @Override
  public CursorResultSetGroup seek(int offset) throws CursorException {
    try {
      return seekAsync(offset).get();
    } catch (Exception e) {
      throw new RuntimeException("Failed to seek to page", e);
    }
  }

  @Override
  public CompletableFuture<CursorResultSetGroup> seekAsync(int offset) {
    int pageNumber = offset / _cursorMetadata.getPageSize();
    return _cursorConnection.seekToPageAsync(_cursorMetadata.getCursorId(), pageNumber)
        .thenApply(result -> result);
  }

  @Override
  public void close() throws CursorException {
    try {
      closeAsync().get();
    } catch (Exception e) {
      throw new CursorException("Failed to close cursor", e);
    }
  }

  @Override
  public CompletableFuture<Void> closeAsync() {
    if (_cursorConnection == null) {
      return CompletableFuture.completedFuture(null);
    }
    return _cursorConnection.closeCursorAsync(_cursorMetadata.getCursorId());
  }

  private CursorMetadata extractCursorMetadata(BrokerResponse brokerResponse) {
    try {
      String cursorId = brokerResponse.getRequestId();
      if (cursorId == null || cursorId.isEmpty()) {
        throw new CursorException("No cursor ID found in broker response");
      }

      // Extract cursor metadata from the root response (not from resultTable.metadata)
      JsonNode originalResponse = brokerResponse.getOriginalResponse();
      if (originalResponse == null) {
        throw new CursorException("No original response available for cursor metadata extraction");
      }

      // Extract pagination information from root level
      int offset = originalResponse.has("offset") ? originalResponse.get("offset").asInt(0) : 0;
      int pageSize = originalResponse.has("numRows") ? originalResponse.get("numRows").asInt(1000) : 1000;
      long totalRows = originalResponse.has("numRowsResultSet")
          ? originalResponse.get("numRowsResultSet").asLong(0) : 0;
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
}
