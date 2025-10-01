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
import java.util.function.IntSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of ResultCursor that manages cursor-based pagination.
 *
 * This class encapsulates the cursor state and navigation logic,
 * keeping the Connection and CursorResultSetGroup classes focused
 * on their primary responsibilities. The cursor starts with the first
 * page already loaded.
 *
 * Thread Safety: All navigation methods are synchronized to ensure
 * safe concurrent access to cursor state.
 */
public class ResultCursorImpl implements ResultCursor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ResultCursorImpl.class);

  private final PinotClientTransport<?> _transport;
  private final String _brokerHostPort;
  private final boolean _failOnExceptions;
  private final int _pageSize;

  // Synchronization lock for cursor state
  private final Object _stateLock = new Object();

  // Volatile for visibility across threads
  private volatile CursorResultSetGroup _currentPage;
  private volatile int _currentPageNumber;
  private volatile boolean _closed;

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
    _pageSize = initialResponse.getNumRows() != null
        ? initialResponse.getNumRows().intValue() : -1;
    // Initialize state - no synchronization needed in constructor
    _currentPage = new CursorResultSetGroup(initialResponse);
    _currentPageNumber = 0;
    _closed = false;
  }

  @Override
  public CursorResultSetGroup getCurrentPage() {
    synchronized (_stateLock) {
      checkNotClosed();
      return _currentPage;
    }
  }

  @Override
  public boolean hasNext() {
    synchronized (_stateLock) {
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
  }

  @Override
  public boolean hasPrevious() {
    synchronized (_stateLock) {
      checkNotClosed();
      CursorAwareBrokerResponse response = _currentPage.getCursorResponse();
      Long offset = response.getOffset();
      return offset != null && offset > 0;
    }
  }

  @Override
  public CursorResultSetGroup next() throws PinotClientException {
    return executeNavigation(
        () -> {
          CursorAwareBrokerResponse response = _currentPage.getCursorResponse();
          Long offset = response.getOffset();
          Integer numRows = response.getNumRows();
          Long totalRows = response.getNumRowsResultSet();
          if (offset == null || numRows == null || totalRows == null
              || (offset + numRows) >= totalRows) {
            throw new IllegalStateException("No next page available");
          }
        },
        () -> _currentPageNumber * _pageSize,
        (transport, cursorId, offset) -> transport.fetchNextPage(_brokerHostPort, cursorId, offset, _pageSize),
        () -> _currentPageNumber++,
        "Failed to fetch next page"
    );
  }

  @Override
  public CursorResultSetGroup previous() throws PinotClientException {
    return executeNavigation(
        () -> {
          CursorAwareBrokerResponse response = _currentPage.getCursorResponse();
          Long offset = response.getOffset();
          if (offset == null || offset <= 0) {
            throw new IllegalStateException("No previous page available");
          }
        },
        () -> (_currentPageNumber - 1) * _pageSize,
        (transport, cursorId, offset) -> transport.fetchPreviousPage(_brokerHostPort, cursorId, offset, _pageSize),
        () -> _currentPageNumber--,
        "Failed to fetch previous page"
    );
  }

  @Override
  public CursorResultSetGroup seekToPage(int pageNumber) throws PinotClientException {
    return executeNavigation(
        () -> {
          if (pageNumber <= 0) {
            throw new IllegalArgumentException("Page number must be positive (1-based)");
          }
        },
        () -> (pageNumber - 1) * _pageSize,
        (transport, cursorId, offset) -> transport.seekToPage(_brokerHostPort, cursorId, pageNumber, _pageSize),
        () -> _currentPageNumber = pageNumber - 1,
        "Failed to seek to page " + pageNumber
    );
  }

  @Override
  public CompletableFuture<CursorResultSetGroup> nextAsync() {
    return executeNavigationAsync(
        () -> {
          CursorAwareBrokerResponse response = _currentPage.getCursorResponse();
          Long offset = response.getOffset();
          Integer numRows = response.getNumRows();
          Long totalRows = response.getNumRowsResultSet();
          if (offset == null || numRows == null || totalRows == null
              || (offset + numRows) >= totalRows) {
            throw new IllegalStateException("No next page available");
          }
        },
        () -> _currentPageNumber * _pageSize,
        (transport, cursorId, offset) -> transport.fetchNextPageAsync(_brokerHostPort, cursorId, offset, _pageSize),
        () -> _currentPageNumber++
    );
  }

  @Override
  public CompletableFuture<CursorResultSetGroup> previousAsync() {
    return executeNavigationAsync(
        () -> {
          CursorAwareBrokerResponse response = _currentPage.getCursorResponse();
          Long offset = response.getOffset();
          if (offset == null || offset <= 0) {
            throw new IllegalStateException("No previous page available");
          }
        },
        () -> (_currentPageNumber - 1) * _pageSize,
        (transport, cursorId, offset) -> transport.fetchPreviousPageAsync(_brokerHostPort, cursorId, offset, _pageSize),
        () -> _currentPageNumber--
    );
  }

  @Override
  public CompletableFuture<CursorResultSetGroup> seekToPageAsync(int pageNumber) {
    return executeNavigationAsync(
        () -> {
          if (pageNumber <= 0) {
            throw new IllegalArgumentException("Page number must be positive (1-based)");
          }
        },
        () -> (pageNumber - 1) * _pageSize,
        (transport, cursorId, offset) -> transport.seekToPageAsync(_brokerHostPort, cursorId, pageNumber, _pageSize),
        () -> _currentPageNumber = pageNumber - 1
    );
  }

  private CursorResultSetGroup executeNavigation(
      Runnable validator,
      IntSupplier offsetCalculator,
      TransportFunction transportFunction,
      Runnable stateUpdater,
      String errorMessage) throws PinotClientException {
    int offset;
    String cursorId;
    synchronized (_stateLock) {
      checkNotClosed();
      validator.run();
      offset = offsetCalculator.getAsInt();
      cursorId = _currentPage.getCursorId();
    }

    JsonAsyncHttpPinotClientTransport transport = getValidatedTransport();

    try {
      CursorAwareBrokerResponse response = transportFunction.apply(transport, cursorId, offset);
      return updateStateAndReturn(response, stateUpdater);
    } catch (PinotClientException e) {
      throw new PinotClientException(errorMessage, e);
    }
  }

  private CompletableFuture<CursorResultSetGroup> executeNavigationAsync(
      Runnable validator,
      IntSupplier offsetCalculator,
      AsyncTransportFunction transportFunction,
      Runnable stateUpdater) {
    try {
      int offset;
      String cursorId;
      synchronized (_stateLock) {
        checkNotClosed();
        validator.run();
        offset = offsetCalculator.getAsInt();
        cursorId = _currentPage.getCursorId();
      }

      JsonAsyncHttpPinotClientTransport transport = getValidatedTransport();
      return transportFunction.apply(transport, cursorId, offset)
          .thenApply(response -> updateStateAndReturn(response, stateUpdater));
    } catch (IllegalStateException | IllegalArgumentException | UnsupportedOperationException e) {
      return CompletableFuture.failedFuture(e);
    }
  }

  private JsonAsyncHttpPinotClientTransport getValidatedTransport() {
    if (!(_transport instanceof JsonAsyncHttpPinotClientTransport)) {
      throw new UnsupportedOperationException("Cursor operations not supported by this transport type");
    }
    return (JsonAsyncHttpPinotClientTransport) _transport;
  }

  @FunctionalInterface
  private interface TransportFunction {
    CursorAwareBrokerResponse apply(JsonAsyncHttpPinotClientTransport transport, String cursorId, int offset)
        throws PinotClientException;
  }

  @FunctionalInterface
  private interface AsyncTransportFunction {
    CompletableFuture<CursorAwareBrokerResponse> apply(JsonAsyncHttpPinotClientTransport transport,
        String cursorId, int offset);
  }

  /**
   * Common method to update cursor state and return result for both sync and async operations.
   * This eliminates code duplication between navigation methods.
   */
  private CursorResultSetGroup updateStateAndReturn(CursorAwareBrokerResponse response, Runnable stateUpdater) {
    if (response.hasExceptions() && _failOnExceptions) {
      throw new PinotClientException("Query had processing exceptions: \n" + response.getExceptions());
    }

    synchronized (_stateLock) {
      if (_closed) {
        throw new IllegalStateException("Cursor was closed during operation");
      }
      _currentPage = new CursorResultSetGroup(response);
      stateUpdater.run();
    }
    return _currentPage;
  }

  @Override
  public String getCursorId() {
    synchronized (_stateLock) {
      checkNotClosed();
      return _currentPage.getCursorId();
    }
  }

  @Override
  public int getCurrentPageNumber() {
    synchronized (_stateLock) {
      checkNotClosed();
      return _currentPageNumber + 1;
    }
  }

  @Override
  public long getTotalRows() {
    synchronized (_stateLock) {
      checkNotClosed();
      Long numRows = _currentPage.getTotalRows();
      return numRows != null ? numRows : -1;
    }
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

    long expirationTime;
    synchronized (_stateLock) {
      if (_closed) {
        return true;
      }
      expirationTime = _currentPage.getExpirationTimeMs();
    }

    return System.currentTimeMillis() > expirationTime;
  }

  @Override
  public void close() throws PinotClientException {
    if (_closed) {
      return;
    }

    // Get cursor ID before marking as closed
    String cursorId;
    synchronized (_stateLock) {
      cursorId = _currentPage.getCursorId();
      _closed = true;
    }

    // Server-side cursor cleanup could be implemented in the future
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
