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
import org.apache.pinot.client.PreparedStatement;
import org.apache.pinot.client.cursor.exceptions.CursorException;

/**
 * Interface for cursor-based pagination support in Pinot client.
 * Provides methods to execute queries with cursors and navigate through paginated results.
 */
public interface CursorConnection {

  /**
   * Executes a query with cursor support and returns the first page of results.
   *
   * @param query SQL query string
   * @param pageSize number of rows per page
   * @return CursorResultSetGroup containing the first page and cursor metadata
   * @throws CursorException if cursor creation fails
   */
  CursorResultSetGroup executeWithCursor(String query, int pageSize) throws CursorException;

  /**
   * Executes a query with cursor support asynchronously.
   *
   * @param query SQL query string
   * @param pageSize number of rows per page
   * @return CompletableFuture containing CursorResultSetGroup
   */
  CompletableFuture<CursorResultSetGroup> executeWithCursorAsync(String query, int pageSize);

  /**
   * Executes a prepared statement with cursor support.
   *
   * @param statement prepared statement
   * @param pageSize number of rows per page
   * @return CursorResultSetGroup containing the first page and cursor metadata
   * @throws CursorException if cursor creation fails
   */
  CursorResultSetGroup executeWithCursor(PreparedStatement statement, int pageSize) throws CursorException;

  /**
   * Executes a prepared statement with cursor support asynchronously.
   *
   * @param statement prepared statement
   * @param pageSize number of rows per page
   * @return CompletableFuture containing CursorResultSetGroup
   */
  CompletableFuture<CursorResultSetGroup> executeWithCursorAsync(PreparedStatement statement, int pageSize);

  /**
   * Fetches the next page of results for an existing cursor.
   *
   * @param cursorId cursor identifier
   * @return CursorResultSetGroup containing the next page
   * @throws CursorException if cursor is invalid or expired
   */
  CursorResultSetGroup fetchNext(String cursorId) throws CursorException;

  /**
   * Fetches the next page of results asynchronously.
   *
   * @param cursorId cursor identifier
   * @return CompletableFuture containing CursorResultSetGroup
   */
  CompletableFuture<CursorResultSetGroup> fetchNextAsync(String cursorId);

  /**
   * Fetches the previous page of results for an existing cursor.
   *
   * @param cursorId cursor identifier
   * @return CursorResultSetGroup containing the previous page
   * @throws CursorException if cursor is invalid or expired
   */
  CursorResultSetGroup fetchPrevious(String cursorId) throws CursorException;

  /**
   * Fetches the previous page of results asynchronously.
   *
   * @param cursorId cursor identifier
   * @return CompletableFuture containing CursorResultSetGroup
   */
  CompletableFuture<CursorResultSetGroup> fetchPreviousAsync(String cursorId);

  /**
   * Seeks to a specific page offset for an existing cursor.
   *
   * @param cursorId cursor identifier
   * @param offset page offset (0-based)
   * @return CursorResultSetGroup containing the requested page
   * @throws CursorException if cursor is invalid or expired
   */
  CursorResultSetGroup seekToPage(String cursorId, int offset) throws CursorException;

  /**
   * Seeks to a specific page offset asynchronously.
   *
   * @param cursorId cursor identifier
   * @param offset page offset (0-based)
   * @return CompletableFuture containing CursorResultSetGroup
   */
  CompletableFuture<CursorResultSetGroup> seekToPageAsync(String cursorId, int offset);

  /**
   * Gets metadata for an existing cursor.
   *
   * @param cursorId cursor identifier
   * @return CursorMetadata containing cursor state information
   * @throws CursorException if cursor is invalid or expired
   */
  CursorMetadata getCursorMetadata(String cursorId) throws CursorException;

  /**
   * Gets cursor metadata asynchronously.
   *
   * @param cursorId cursor identifier
   * @return CompletableFuture containing CursorMetadata
   */
  CompletableFuture<CursorMetadata> getCursorMetadataAsync(String cursorId);

  /**
   * Closes and cleans up a cursor, releasing server-side resources.
   *
   * @param cursorId cursor identifier
   * @throws CursorException if cursor cleanup fails
   */
  void closeCursor(String cursorId) throws CursorException;

  /**
   * Closes a cursor asynchronously.
   *
   * @param cursorId cursor identifier
   * @return CompletableFuture indicating completion
   */
  CompletableFuture<Void> closeCursorAsync(String cursorId);
}
