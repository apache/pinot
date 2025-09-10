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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of ResultCursor that manages cursor-based pagination.
 *
 * This class encapsulates the cursor state and navigation logic,
 * keeping the Connection and CursorResultSetGroup classes focused
 * on their primary responsibilities. The cursor starts with the first
 * page already loaded.
 */
public class ResultCursorImpl implements ResultCursor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ResultCursorImpl.class);

  private final PinotClientTransport<?> _transport;
  private final String _brokerHostPort;
  private final boolean _failOnExceptions;

  private CursorResultSetGroup _currentPage;
  private int _currentPageNumber;
  private final int _pageSize;
  private boolean _closed;

  /**
   * Creates a new cursor with the initial page of results already loaded.
   *
   * @param transport the transport to use for navigation
   * @param brokerHostPort the broker host and port
   * @param initialResponse the initial cursor-aware response (first page)
   * @param failOnExceptions whether to fail on query exceptions
   */
  public ResultCursorImpl(PinotClientTransport<?> transport, String brokerHostPort,
                         CursorAwareBrokerResponse initialResponse, boolean failOnExceptions) {
    _transport = transport;
    _brokerHostPort = brokerHostPort;
    _failOnExceptions = failOnExceptions;
    _currentPage = new CursorResultSetGroup(initialResponse);
    _currentPageNumber = 0;
    _pageSize = initialResponse.getNumRows() != null
        ? initialResponse.getNumRows().intValue() : -1;
    _closed = false;
  }

  @Override
  public CursorResultSetGroup getCurrentPage() {
    checkNotClosed();
    return _currentPage;
  }

  @Override
  public boolean hasNext() {
    checkNotClosed();
    CursorAwareBrokerResponse response = _currentPage.getCursorResponse();
    Long offset = response.getOffset();
    Integer numRows = response.getNumRows();
    Long totalRows = response.getNumRowsResultSet();

    if (offset == null || numRows == null || totalRows == null) {
      return false;
    }
    return (offset + numRows) < totalRows;
  }

  @Override
  public boolean hasPrevious() {
    checkNotClosed();
    CursorAwareBrokerResponse response = _currentPage.getCursorResponse();
    Long offset = response.getOffset();
    return offset != null && offset > 0;
  }

  @Override
  public CursorResultSetGroup next() throws PinotClientException {
    checkNotClosed();
    if (!hasNext()) {
      throw new IllegalStateException("No next page available");
    }

    if (!(_transport instanceof JsonAsyncHttpPinotClientTransport)) {
      throw new UnsupportedOperationException("Cursor operations not supported by this transport type");
    }

    JsonAsyncHttpPinotClientTransport cursorTransport = (JsonAsyncHttpPinotClientTransport) _transport;

    try {
      int nextOffset = _currentPageNumber * _pageSize;
      CursorAwareBrokerResponse response = cursorTransport.fetchNextPage(_brokerHostPort, getCursorId(),
          nextOffset, _pageSize);
      if (response.hasExceptions() && _failOnExceptions) {
        throw new PinotClientException("Query had processing exceptions: \n" + response.getExceptions());
      }

      _currentPage = new CursorResultSetGroup(response);
      _currentPageNumber++;
      return _currentPage;
    } catch (PinotClientException e) {
      throw new PinotClientException("Failed to fetch next page", e);
    }
  }

  @Override
  public CursorResultSetGroup previous() throws PinotClientException {
    checkNotClosed();
    if (!hasPrevious()) {
      throw new IllegalStateException("No previous page available");
    }

    if (!(_transport instanceof JsonAsyncHttpPinotClientTransport)) {
      throw new UnsupportedOperationException("Cursor operations not supported by this transport type");
    }

    JsonAsyncHttpPinotClientTransport cursorTransport = (JsonAsyncHttpPinotClientTransport) _transport;

    try {
      int prevOffset = (_currentPageNumber - 1) * _pageSize;
      CursorAwareBrokerResponse response = cursorTransport.fetchPreviousPage(_brokerHostPort, getCursorId(),
          prevOffset, _pageSize);
      if (response.hasExceptions() && _failOnExceptions) {
        throw new PinotClientException("Query had processing exceptions: \n" + response.getExceptions());
      }

      _currentPage = new CursorResultSetGroup(response);
      _currentPageNumber--;
      return _currentPage;
    } catch (PinotClientException e) {
      throw new PinotClientException("Failed to fetch previous page", e);
    }
  }

  @Override
  public CursorResultSetGroup seekToPage(int pageNumber) throws PinotClientException {
    checkNotClosed();
    if (pageNumber <= 0) {
      throw new IllegalArgumentException("Page number must be positive (1-based)");
    }

    if (!(_transport instanceof JsonAsyncHttpPinotClientTransport)) {
      throw new UnsupportedOperationException("Cursor operations not supported by this transport type");
    }

    JsonAsyncHttpPinotClientTransport cursorTransport = (JsonAsyncHttpPinotClientTransport) _transport;

    try {
      int seekOffset = (pageNumber - 1) * _pageSize;
      CursorAwareBrokerResponse response = cursorTransport.seekToPage(_brokerHostPort, getCursorId(),
          seekOffset, _pageSize);
      if (response.hasExceptions() && _failOnExceptions) {
        throw new PinotClientException("Query had processing exceptions: \n" + response.getExceptions());
      }

      _currentPage = new CursorResultSetGroup(response);
      _currentPageNumber = pageNumber - 1; // Store 0-based internally
      return _currentPage;
    } catch (PinotClientException e) {
      throw new PinotClientException("Failed to seek to page " + pageNumber, e);
    }
  }

  @Override
  public CompletableFuture<CursorResultSetGroup> nextAsync() {
    checkNotClosed();
    if (!hasNext()) {
      return CompletableFuture.failedFuture(new IllegalStateException("No next page available"));
    }

    if (!(_transport instanceof JsonAsyncHttpPinotClientTransport)) {
      return CompletableFuture.failedFuture(
          new UnsupportedOperationException("Cursor operations not supported by this transport type"));
    }

    JsonAsyncHttpPinotClientTransport cursorTransport = (JsonAsyncHttpPinotClientTransport) _transport;

    int nextOffset = (_currentPageNumber + 1) * _pageSize;
    return cursorTransport.fetchNextPageAsync(_brokerHostPort, getCursorId(), nextOffset, _pageSize)
        .thenApply(response -> {
          if (response.hasExceptions() && _failOnExceptions) {
            throw new RuntimeException("Query had processing exceptions: \n" + response.getExceptions());
          }

          _currentPage = new CursorResultSetGroup(response);
          _currentPageNumber++;
          return _currentPage;
        });
  }

  @Override
  public CompletableFuture<CursorResultSetGroup> previousAsync() {
    checkNotClosed();
    if (!hasPrevious()) {
      return CompletableFuture.failedFuture(new IllegalStateException("No previous page available"));
    }

    if (!(_transport instanceof JsonAsyncHttpPinotClientTransport)) {
      return CompletableFuture.failedFuture(
          new UnsupportedOperationException("Cursor operations not supported by this transport type"));
    }

    JsonAsyncHttpPinotClientTransport cursorTransport = (JsonAsyncHttpPinotClientTransport) _transport;

    int prevOffset = (_currentPageNumber - 1) * _pageSize;
    return cursorTransport.fetchPreviousPageAsync(_brokerHostPort, getCursorId(), prevOffset, _pageSize)
        .thenApply(response -> {
          if (response.hasExceptions() && _failOnExceptions) {
            throw new RuntimeException("Query had processing exceptions: \n" + response.getExceptions());
          }

          _currentPage = new CursorResultSetGroup(response);
          _currentPageNumber--;
          return _currentPage;
        });
  }

  @Override
  public CompletableFuture<CursorResultSetGroup> seekToPageAsync(int pageNumber) {
    checkNotClosed();
    if (pageNumber <= 0) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("Page number must be positive (1-based)"));
    }

    if (!(_transport instanceof JsonAsyncHttpPinotClientTransport)) {
      return CompletableFuture.failedFuture(
          new UnsupportedOperationException("Cursor operations not supported by this transport type"));
    }

    JsonAsyncHttpPinotClientTransport cursorTransport = (JsonAsyncHttpPinotClientTransport) _transport;

    int seekOffset = (pageNumber - 1) * _pageSize;
    return cursorTransport.seekToPageAsync(_brokerHostPort, getCursorId(), seekOffset, _pageSize)
        .thenApply(response -> {
          if (response.hasExceptions() && _failOnExceptions) {
            throw new RuntimeException("Query had processing exceptions: \n" + response.getExceptions());
          }

          _currentPage = new CursorResultSetGroup(response);
          _currentPageNumber = pageNumber - 1; // Store 0-based internally
          return _currentPage;
        });
  }

  @Override
  public String getCursorId() {
    checkNotClosed();
    return _currentPage.getCursorId();
  }

  @Override
  public int getCurrentPageNumber() {
    checkNotClosed();
    return _currentPageNumber + 1; // Convert 0-based internal to 1-based public API
  }

  @Override
  public long getTotalRows() {
    checkNotClosed();
    Long numRows = _currentPage.getTotalRows();
    return numRows != null ? numRows : -1;
  }

  @Override
  public int getPageSize() {
    return _pageSize;
  }

  @Override
  public boolean isExpired() {
    if (_closed) {
      return true;
    }

    Long expirationTime = _currentPage.getExpirationTimeMs();
    if (expirationTime == null) {
      return false;
    }

    return System.currentTimeMillis() > expirationTime;
  }

  @Override
  public void close() throws PinotClientException {
    if (_closed) {
      return;
    }

    // Get cursor ID before marking as closed
    String cursorId = null;
    try {
      cursorId = getCursorId();
    } catch (RuntimeException e) {
      // Ignore if we can't get cursor ID
    }

    _closed = true;

    // TODO: Implement server-side cursor cleanup
    // This would involve sending a DELETE request to the broker to clean up cursor resources
    // For now, cursors will expire naturally based on their expiration time
    LOGGER.debug("Cursor {} closed, server-side cleanup not yet implemented", cursorId);
  }

  private void checkNotClosed() {
    if (_closed) {
      throw new IllegalStateException("Cursor is closed");
    }
  }
}
