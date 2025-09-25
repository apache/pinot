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

import java.util.concurrent.CompletableFuture;

/**
 * Marker interface for transports that support cursor-based pagination operations.
 * Transports implementing this interface can be used with cursor pagination features.
 */
public interface CursorCapable {

  /**
   * Executes a query with cursor support.
   *
   * @param brokerAddress The broker address to send the query to
   * @param query The SQL query to execute
   * @param numRows The number of rows to return per page
   * @return CursorAwareBrokerResponse containing results and cursor metadata
   * @throws PinotClientException If query execution fails
   */
  default CursorAwareBrokerResponse executeQueryWithCursor(String brokerAddress, String query, int numRows)
      throws PinotClientException {
    throw new UnsupportedOperationException("Cursor operations not supported by this transport implementation");
  }

  /**
   * Executes a query with cursor support asynchronously.
   *
   * @param brokerAddress The broker address to send the query to
   * @param query The SQL query to execute
   * @param numRows The number of rows to return per page
   * @return CompletableFuture containing CursorAwareBrokerResponse with results and cursor metadata
   * @throws PinotClientException If query execution fails
   */
  default CompletableFuture<CursorAwareBrokerResponse> executeQueryWithCursorAsync(String brokerAddress,
      String query, int numRows) throws PinotClientException {
    throw new UnsupportedOperationException("Cursor operations not supported by this transport implementation");
  }

  /**
   * Fetches the next page for a cursor with specified offset and number of rows.
   *
   * @param brokerAddress The broker address to send the request to
   * @param cursorId The cursor identifier
   * @param offset The offset for pagination
   * @param numRows The number of rows to fetch
   * @return CursorAwareBrokerResponse containing the next page
   * @throws PinotClientException If the fetch operation fails
   */
  default CursorAwareBrokerResponse fetchNextPage(String brokerAddress, String cursorId, int offset, int numRows)
      throws PinotClientException {
    throw new UnsupportedOperationException("Cursor operations not supported by this transport implementation");
  }

  /**
   * Fetches the next page for a cursor asynchronously with specified offset and number of rows.
   *
   * @param brokerAddress The broker address to send the request to
   * @param cursorId The cursor identifier
   * @param offset The offset for pagination
   * @param numRows The number of rows to fetch
   * @return CompletableFuture containing the next page
   */
  default CompletableFuture<CursorAwareBrokerResponse> fetchNextPageAsync(String brokerAddress, String cursorId,
      int offset, int numRows) {
    throw new UnsupportedOperationException("Cursor operations not supported by this transport implementation");
  }

  /**
   * Fetches the previous page for a cursor with specified offset and number of rows.
   *
   * @param brokerAddress The broker address to send the request to
   * @param cursorId The cursor identifier
   * @param offset The offset for pagination
   * @param numRows The number of rows to fetch
   * @return CursorAwareBrokerResponse containing the previous page
   * @throws PinotClientException If the fetch operation fails
   */
  default CursorAwareBrokerResponse fetchPreviousPage(String brokerAddress, String cursorId, int offset, int numRows)
      throws PinotClientException {
    throw new UnsupportedOperationException("Cursor operations not supported by this transport implementation");
  }

  /**
   * Fetches the previous page for a cursor asynchronously with specified offset and number of rows.
   *
   * @param brokerAddress The broker address to send the request to
   * @param cursorId The cursor identifier
   * @param offset The offset for pagination
   * @param numRows The number of rows to fetch
   * @return CompletableFuture containing the previous page
   */
  default CompletableFuture<CursorAwareBrokerResponse> fetchPreviousPageAsync(String brokerAddress, String cursorId,
      int offset, int numRows) {
    throw new UnsupportedOperationException("Cursor operations not supported by this transport implementation");
  }

  /**
   * Seeks to a specific page for a cursor with specified number of rows.
   *
   * @param brokerAddress The broker address to send the request to
   * @param cursorId The cursor identifier
   * @param pageNumber The 1-based page number to seek to (will be converted to offset internally)
   * @param numRows The number of rows to fetch
   * @return CursorAwareBrokerResponse containing the requested page
   * @throws PinotClientException If the seek operation fails
   */
  default CursorAwareBrokerResponse seekToPage(String brokerAddress, String cursorId, int pageNumber, int numRows)
      throws PinotClientException {
    throw new UnsupportedOperationException("Cursor operations not supported by this transport implementation");
  }

  /**
   * Seeks to a specific page for a cursor asynchronously with specified number of rows.
   *
   * @param brokerAddress The broker address to send the request to
   * @param cursorId The cursor identifier
   * @param pageNumber The 1-based page number to seek to (will be converted to offset internally)
   * @param numRows The number of rows to fetch
   * @return CompletableFuture containing the requested page
   */
  default CompletableFuture<CursorAwareBrokerResponse> seekToPageAsync(String brokerAddress, String cursorId,
      int pageNumber, int numRows) {
    throw new UnsupportedOperationException("Cursor operations not supported by this transport implementation");
  }

  /**
   * Retrieves metadata for an existing cursor.
   *
   * @param brokerAddress The broker address in "host:port" format (must be same as cursor creator)
   * @param requestId The unique identifier of the cursor
   * @return BrokerResponse containing cursor metadata
   * @throws PinotClientException If metadata retrieval fails
   */
  default BrokerResponse getCursorMetadata(String brokerAddress, String requestId) throws PinotClientException {
    throw new UnsupportedOperationException("Cursor operations not supported by this transport implementation");
  }

  /**
   * Retrieves metadata for an existing cursor asynchronously.
   *
   * @param brokerAddress The broker address in "host:port" format (must be same as cursor creator)
   * @param requestId The unique identifier of the cursor
   * @return CompletableFuture containing BrokerResponse with cursor metadata
   */
  default CompletableFuture<BrokerResponse> getCursorMetadataAsync(String brokerAddress, String requestId) {
    throw new UnsupportedOperationException("Cursor operations not supported by this transport implementation");
  }

  /**
   * Deletes a cursor and cleans up its resources.
   *
   * @param brokerAddress The broker address in "host:port" format (must be same as cursor creator)
   * @param requestId The unique identifier of the cursor to delete
   * @throws PinotClientException If cursor deletion fails
   */
  default void deleteCursor(String brokerAddress, String requestId) throws PinotClientException {
    throw new UnsupportedOperationException("Cursor operations not supported by this transport implementation");
  }

  /**
   * Deletes a cursor and cleans up its resources asynchronously.
   *
   * @param brokerAddress The broker address in "host:port" format (must be same as cursor creator)
   * @param requestId The unique identifier of the cursor to delete
   * @return CompletableFuture that completes when the cursor is deleted
   */
  default CompletableFuture<Void> deleteCursorAsync(String brokerAddress, String requestId) {
    throw new UnsupportedOperationException("Cursor operations not supported by this transport implementation");
  }
}
