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
 * A cursor for navigating through paginated query results using the Cursor Handle Pattern.
 *
 * This interface provides a clean abstraction for cursor-based pagination,
 * where each page contains a complete CursorResultSetGroup with all result sets
 * and metadata. The cursor starts with the first page already loaded.
 *
 * Usage example:
 * <pre>
 * try (ResultCursor cursor = connection.openCursor("SELECT * FROM table", 1000)) {
 *     // First page is immediately available
 *     CursorResultSetGroup firstPage = cursor.getCurrentPage();
 *
 *     // Navigate through remaining pages
 *     while (cursor.hasNext()) {
 *         CursorResultSetGroup page = cursor.next();
 *         // Process all result sets in the page
 *         for (int i = 0; i < page.getResultSetCount(); i++) {
 *             ResultSet rs = page.getResultSet(i);
 *             // Process result set
 *         }
 *     }
 * }
 * </pre>
 */
public interface ResultCursor extends AutoCloseable {

  /**
   * Gets the current page of results without navigation.
   * The first page is available immediately when the cursor is created.
   *
   * @return the current page of results
   */
  CursorResultSetGroup getCurrentPage();

  /**
   * Checks if there are more pages available after the current page.
   *
   * @return true if more pages are available, false otherwise
   */
  boolean hasNext();

  /**
   * Checks if there are previous pages available before the current page.
   *
   * @return true if previous pages are available, false otherwise
   */
  boolean hasPrevious();

  /**
   * Fetches the next page of results and advances the cursor position.
   *
   * @return the next page of results
   * @throws PinotClientException if an error occurs while fetching
   * @throws IllegalStateException if no next page is available
   */
  CursorResultSetGroup next() throws PinotClientException;

  /**
   * Fetches the previous page of results and moves the cursor position backward.
   *
   * @return the previous page of results
   * @throws PinotClientException if an error occurs while fetching
   * @throws IllegalStateException if no previous page is available
   */
  CursorResultSetGroup previous() throws PinotClientException;

  /**
   * Seeks to a specific page number (1-based) and updates the cursor position.
   *
   * @param pageNumber the page number to seek to (1-based)
   * @return the requested page of results
   * @throws PinotClientException if an error occurs while seeking
   * @throws IllegalArgumentException if pageNumber is invalid
   */
  CursorResultSetGroup seekToPage(int pageNumber) throws PinotClientException;

  /**
   * Fetches the next page of results asynchronously.
   *
   * @return a CompletableFuture containing the next page of results
   */
  CompletableFuture<CursorResultSetGroup> nextAsync();

  /**
   * Fetches the previous page of results asynchronously.
   *
   * @return a CompletableFuture containing the previous page of results
   */
  CompletableFuture<CursorResultSetGroup> previousAsync();

  /**
   * Seeks to a specific page number asynchronously.
   *
   * @param pageNumber the page number to seek to (1-based)
   * @return a CompletableFuture containing the requested page of results
   */
  CompletableFuture<CursorResultSetGroup> seekToPageAsync(int pageNumber);

  /**
   * Gets the current cursor ID.
   *
   * @return the cursor ID
   */
  String getCursorId();

  /**
   * Gets the current page number (1-based).
   *
   * @return the current page number
   */
  int getCurrentPageNumber();

  /**
   * Gets the total number of rows across all pages, if known.
   *
   * @return the total number of rows, or -1 if unknown
   */
  long getTotalRows();

  /**
   * Gets the page size for this cursor.
   *
   * @return the page size
   */
  int getPageSize();

  /**
   * Checks if the cursor has expired on the server.
   *
   * @return true if the cursor has expired, false otherwise
   */
  boolean isExpired();

  /**
   * Closes the cursor and releases any associated resources.
   * This sends a cleanup request to the server to free cursor resources.
   *
   * @throws PinotClientException if an error occurs while closing
   */
  @Override
  void close() throws PinotClientException;
}
