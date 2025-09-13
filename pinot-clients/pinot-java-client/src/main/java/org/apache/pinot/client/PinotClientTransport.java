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
 * Interface for plugging different client transports.
 */
public interface PinotClientTransport<METRICS> {

  BrokerResponse executeQuery(String brokerAddress, String query)
      throws PinotClientException;

  CompletableFuture<BrokerResponse> executeQueryAsync(String brokerAddress, String query)
      throws PinotClientException;

  /**
   * Executes a query with cursor support for pagination.
   * Default implementation throws UnsupportedOperationException for backward compatibility.
   *
   * @param brokerAddress The broker address to send the query to
   * @param query The SQL query to execute
   * @param numRows The number of rows to return per page
   * @return CursorAwareBrokerResponse containing results and cursor metadata
   * @throws PinotClientException If query execution fails
   * @throws UnsupportedOperationException If cursor support is not implemented
   */
  default CursorAwareBrokerResponse executeQueryWithCursor(String brokerAddress, String query, int numRows)
      throws PinotClientException {
    throw new UnsupportedOperationException("Cursor operations not supported by this transport implementation");
  }

  /**
   * Executes a query with cursor support asynchronously.
   * Default implementation throws UnsupportedOperationException for backward compatibility.
   *
   * @param brokerAddress The broker address to send the query to
   * @param query The SQL query to execute
   * @param numRows The number of rows to return per page
   * @return CompletableFuture containing CursorAwareBrokerResponse with results and cursor metadata
   * @throws PinotClientException If query execution fails
   * @throws UnsupportedOperationException If cursor support is not implemented
   */
  default CompletableFuture<CursorAwareBrokerResponse> executeQueryWithCursorAsync(String brokerAddress,
      String query, int numRows) throws PinotClientException {
    throw new UnsupportedOperationException("Cursor operations not supported by this transport implementation");
  }

  /**
   * Fetches the next page for a cursor.
   * Default implementation throws UnsupportedOperationException for backward compatibility.
   *
   * @param brokerAddress The broker address to send the request to
   * @param cursorId The cursor identifier
   * @return CursorAwareBrokerResponse containing the next page
   * @throws PinotClientException If fetch operation fails
   * @throws UnsupportedOperationException If cursor support is not implemented
   */
  default CursorAwareBrokerResponse fetchNextPage(String brokerAddress, String cursorId) throws PinotClientException {
    throw new UnsupportedOperationException("Cursor operations not supported by this transport implementation");
  }

  /**
   * Fetches the next page for a cursor asynchronously.
   * Default implementation throws UnsupportedOperationException for backward compatibility.
   *
   * @param brokerAddress The broker address to send the request to
   * @param cursorId The cursor identifier
   * @return CompletableFuture containing the next page
   * @throws UnsupportedOperationException If cursor support is not implemented
   */
  default CompletableFuture<CursorAwareBrokerResponse> fetchNextPageAsync(String brokerAddress, String cursorId) {
    return fetchNextPageAsync(brokerAddress, cursorId, 1, 1);
  }

  /**
   * Fetches the next page for a cursor asynchronously with specified offset and number of rows.
   * Default implementation throws UnsupportedOperationException for backward compatibility.
   *
   * @param brokerAddress The broker address to send the request to
   * @param cursorId The cursor identifier
   * @param offset The offset for pagination
   * @param numRows The number of rows to fetch
   * @return CompletableFuture containing the next page
   * @throws UnsupportedOperationException If cursor support is not implemented
   */
  default CompletableFuture<CursorAwareBrokerResponse> fetchNextPageAsync(String brokerAddress, String cursorId,
      int offset, int numRows) {
    throw new UnsupportedOperationException("Cursor operations not supported by this transport implementation");
  }

  /**
   * Fetches the previous page for a cursor.
   * Default implementation throws UnsupportedOperationException for backward compatibility.
   *
   * @param brokerAddress The broker address to send the request to
   * @param cursorId The cursor identifier
   * @return CursorAwareBrokerResponse containing the previous page
   * @throws PinotClientException If fetch operation fails
   * @throws UnsupportedOperationException If cursor support is not implemented
   */
  default CursorAwareBrokerResponse fetchPreviousPage(String brokerAddress, String cursorId)
      throws PinotClientException {
    throw new UnsupportedOperationException("Cursor operations not supported by this transport implementation");
  }

  /**
   * Fetches the previous page for a cursor asynchronously.
   * Default implementation throws UnsupportedOperationException for backward compatibility.
   *
   * @param brokerAddress The broker address to send the request to
   * @param cursorId The cursor identifier
   * @return CompletableFuture containing the previous page
   * @throws UnsupportedOperationException If cursor support is not implemented
   */
  default CompletableFuture<CursorAwareBrokerResponse> fetchPreviousPageAsync(String brokerAddress, String cursorId) {
    return fetchPreviousPageAsync(brokerAddress, cursorId, -1, 1);
  }

  /**
   * Fetches the previous page for a cursor asynchronously with specified offset and number of rows.
   * Default implementation throws UnsupportedOperationException for backward compatibility.
   *
   * @param brokerAddress The broker address to send the request to
   * @param cursorId The cursor identifier
   * @param offset The offset for pagination
   * @param numRows The number of rows to fetch
   * @return CompletableFuture containing the previous page
   * @throws UnsupportedOperationException If cursor support is not implemented
   */
  default CompletableFuture<CursorAwareBrokerResponse> fetchPreviousPageAsync(String brokerAddress, String cursorId,
      int offset, int numRows) {
    throw new UnsupportedOperationException("Cursor operations not supported by this transport implementation");
  }

  /**
   * Seeks to a specific page for a cursor.
   * Default implementation throws UnsupportedOperationException for backward compatibility.
   *
   * @param brokerAddress The broker address to send the request to
   * @param cursorId The cursor identifier
   * @param pageNumber The zero-based page number to seek to
   * @return CursorAwareBrokerResponse containing the requested page
   * @throws PinotClientException If seek operation fails
   * @throws UnsupportedOperationException If cursor support is not implemented
   */
  default CursorAwareBrokerResponse seekToPage(String brokerAddress, String cursorId, int pageNumber)
      throws PinotClientException {
    throw new UnsupportedOperationException("Cursor operations not supported by this transport implementation");
  }

  /**
   * Seeks to a specific page for a cursor asynchronously.
   * Default implementation throws UnsupportedOperationException for backward compatibility.
   *
   * @param brokerAddress The broker address to send the request to
   * @param cursorId The cursor identifier
   * @param pageNumber The zero-based page number to seek to
   * @return CompletableFuture containing the requested page
   * @throws UnsupportedOperationException If cursor support is not implemented
   */
  default CompletableFuture<CursorAwareBrokerResponse> seekToPageAsync(String brokerAddress,
      String cursorId, int pageNumber) {
    return seekToPageAsync(brokerAddress, cursorId, pageNumber, 1);
  }

  /**
   * Seeks to a specific page for a cursor asynchronously with specified number of rows.
   * Default implementation throws UnsupportedOperationException for backward compatibility.
   *
   * @param brokerAddress The broker address to send the request to
   * @param cursorId The cursor identifier
   * @param pageNumber The zero-based page number to seek to
   * @param numRows The number of rows to fetch
   * @return CompletableFuture containing the requested page
   * @throws UnsupportedOperationException If cursor support is not implemented
   */
  default CompletableFuture<CursorAwareBrokerResponse> seekToPageAsync(String brokerAddress, String cursorId,
      int pageNumber, int numRows) {
    throw new UnsupportedOperationException("Cursor operations not supported by this transport implementation");
  }

  void close()
      throws PinotClientException;

  /**
   * Access to the client metrics implementation if any.
   * This may be useful for observability into the client implementation.
   *
   * @return underlying client metrics if any
   */
  default METRICS getClientMetrics() {
    throw new UnsupportedOperationException("No useful client metrics available");
  }
}
