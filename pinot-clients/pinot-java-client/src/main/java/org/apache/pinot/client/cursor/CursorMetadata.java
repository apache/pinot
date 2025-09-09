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

/**
 * Metadata for cursor-based pagination, containing information about
 * cursor state, pagination details, and navigation capabilities.
 */
public class CursorMetadata {
  private final String _cursorId;
  private final int _currentPage;
  private final int _pageSize;
  private final long _totalRows;
  private final int _totalPages;
  private final boolean _hasNext;
  private final boolean _hasPrevious;
  private final long _expirationTimeMs;
  private final String _brokerId;

  /**
   * Creates cursor metadata.
   *
   * @param cursorId unique cursor identifier
   * @param currentPage current page number (0-based)
   * @param pageSize number of rows per page (original page size from cursor creation)
   * @param totalRows total number of rows in the result set
   * @param totalPages total number of pages
   * @param hasNext true if next page is available
   * @param hasPrevious true if previous page is available
   * @param expirationTimeMs cursor expiration timestamp
   * @param brokerId broker handling this cursor
   */
  public CursorMetadata(String cursorId, int currentPage, int pageSize, long totalRows, int totalPages,
      boolean hasNext, boolean hasPrevious, long expirationTimeMs, String brokerId) {
    _cursorId = cursorId;
    _currentPage = currentPage;
    _pageSize = pageSize;
    _totalRows = totalRows;
    _totalPages = totalPages;
    _hasNext = hasNext;
    _hasPrevious = hasPrevious;
    _expirationTimeMs = expirationTimeMs;
    _brokerId = brokerId;
  }

  /**
   * Gets the cursor identifier.
   *
   * @return cursor ID
   */
  public String getCursorId() {
    return _cursorId;
  }

  /**
   * Gets the current page number (0-based).
   *
   * @return current page
   */
  public int getCurrentPage() {
    return _currentPage;
  }

  /**
   * Gets the page size.
   *
   * @return number of rows per page
   */
  public int getPageSize() {
    return _pageSize;
  }

  /**
   * Gets the total number of rows.
   *
   * @return total rows
   */
  public long getTotalRows() {
    return _totalRows;
  }

  /**
   * Gets the total number of pages.
   *
   * @return total pages
   */
  public int getTotalPages() {
    return _totalPages;
  }

  /**
   * Checks if next page is available.
   *
   * @return true if next page exists
   */
  public boolean hasNext() {
    return _hasNext;
  }

  /**
   * Checks if previous page is available.
   *
   * @return true if previous page exists
   */
  public boolean hasPrevious() {
    return _hasPrevious;
  }

  /**
   * Gets cursor expiration timestamp.
   *
   * @return expiration time in milliseconds
   */
  public long getExpirationTimeMs() {
    return _expirationTimeMs;
  }

  /**
   * Gets the broker ID handling this cursor.
   *
   * @return broker identifier
   */
  public String getBrokerId() {
    return _brokerId;
  }

  /**
   * Checks if the cursor is expired.
   *
   * @return true if cursor has expired
   */
  public boolean isExpired() {
    return System.currentTimeMillis() > _expirationTimeMs;
  }

  @Override
  public String toString() {
    return "CursorMetadata{"
        + "cursorId='" + _cursorId + '\''
        + ", currentPage=" + _currentPage
        + ", pageSize=" + _pageSize
        + ", totalRows=" + _totalRows
        + ", totalPages=" + _totalPages
        + ", hasNext=" + _hasNext
        + ", hasPrevious=" + _hasPrevious
        + ", expirationTimeMs=" + _expirationTimeMs
        + ", brokerId='" + _brokerId + '\''
        + '}';
  }
}
