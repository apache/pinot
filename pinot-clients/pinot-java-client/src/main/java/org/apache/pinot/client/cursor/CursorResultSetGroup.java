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
import org.apache.pinot.client.ResultSet;
import org.apache.pinot.client.cursor.exceptions.CursorException;

/**
 * Interface for paginated result sets with cursor navigation support.
 * Extends the standard result set functionality with pagination capabilities.
 */
public interface CursorResultSetGroup {

  /**
   * Gets the number of result sets in this group.
   *
   * @return number of result sets
   */
  int getResultSetCount();

  /**
   * Gets a specific result set by index.
   *
   * @param index result set index
   * @return ResultSet at the specified index
   */
  ResultSet getResultSet(int index);

  /**
   * Gets cursor metadata for this result set group.
   *
   * @return CursorMetadata containing pagination information
   */
  CursorMetadata getCursorMetadata();

  /**
   * Checks if there is a next page available.
   *
   * @return true if next page exists
   */
  boolean hasNext();

  /**
   * Checks if there is a previous page available.
   *
   * @return true if previous page exists
   */
  boolean hasPrevious();

  /**
   * Fetches the next page of results.
   *
   * @return CursorResultSetGroup containing the next page
   * @throws CursorException if navigation fails
   */
  CursorResultSetGroup next() throws CursorException;

  /**
   * Fetches the next page of results asynchronously.
   *
   * @return CompletableFuture containing the next page
   */
  CompletableFuture<CursorResultSetGroup> nextAsync();

  /**
   * Fetches the previous page of results.
   *
   * @return CursorResultSetGroup containing the previous page
   * @throws CursorException if navigation fails
   */
  CursorResultSetGroup previous() throws CursorException;

  /**
   * Fetches the previous page of results asynchronously.
   *
   * @return CompletableFuture containing the previous page
   */
  CompletableFuture<CursorResultSetGroup> previousAsync();

  /**
   * Seeks to a specific page offset.
   *
   * @param offset page offset (0-based)
   * @return CursorResultSetGroup containing the requested page
   * @throws CursorException if seek fails
   */
  CursorResultSetGroup seek(int offset) throws CursorException;

  /**
   * Seeks to a specific page offset asynchronously.
   *
   * @param offset page offset (0-based)
   * @return CompletableFuture containing the requested page
   */
  CompletableFuture<CursorResultSetGroup> seekAsync(int offset);

  /**
   * Closes the cursor and releases server-side resources.
   *
   * @throws CursorException if cleanup fails
   */
  void close() throws CursorException;

  /**
   * Closes the cursor asynchronously.
   *
   * @return CompletableFuture indicating completion
   */
  CompletableFuture<Void> closeAsync();
}
