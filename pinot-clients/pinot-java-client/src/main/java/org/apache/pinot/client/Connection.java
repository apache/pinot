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

import com.google.common.collect.Iterables;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A connection to Pinot, normally created through calls to the {@link ConnectionFactory}.
 */
public class Connection {
  public static final String FAIL_ON_EXCEPTIONS = "failOnExceptions";
  private static final Logger LOGGER = LoggerFactory.getLogger(Connection.class);

  private final PinotClientTransport<?> _transport;
  private final BrokerSelector _brokerSelector;
  private final boolean _failOnExceptions;

  Connection(List<String> brokerList, PinotClientTransport<?> transport) {
    this(new Properties(), new SimpleBrokerSelector(brokerList), transport);
  }

  Connection(Properties properties, List<String> brokerList, PinotClientTransport<?> transport) {
    this(properties, new SimpleBrokerSelector(brokerList), transport);
    LOGGER.info("Created connection to broker list {}", brokerList);
  }

  Connection(BrokerSelector brokerSelector, PinotClientTransport<?> transport) {
    this(new Properties(), brokerSelector, transport);
  }

  Connection(Properties properties, BrokerSelector brokerSelector, PinotClientTransport<?> transport) {
    _brokerSelector = brokerSelector;
    _transport = transport;

    // Default fail Pinot query if response contains any exception.
    _failOnExceptions = Boolean.parseBoolean(properties.getProperty(FAIL_ON_EXCEPTIONS, "TRUE"));
  }

  /**
   * Creates a prepared statement, to escape query parameters.
   *
   * @param query The query for which to create a prepared statement
   * @return A prepared statement for this connection
   */
  public PreparedStatement prepareStatement(String query) {
    return new PreparedStatement(this, query);
  }

  /**
   * Executes a query.
   *
   * @param query The query to execute
   * @return The result of the query
   * @throws PinotClientException If an exception occurs while processing the query
   */
  public ResultSetGroup execute(String query) {
    return execute((Iterable<String>) null, query);
  }

  /**
   * Executes a query.
   *
   * @param tableName Name of the table to execute the query on
   * @param query The query to execute
   * @return The result of the query
   * @throws PinotClientException If an exception occurs while processing the query
   */
  public ResultSetGroup execute(@Nullable String tableName, String query)
      throws PinotClientException {
    return execute(tableName == null ? null : List.of(tableName), query);
  }

  /**
   * Executes a query.
   *
   * @param tableNames Names of all the tables to execute the query on
   * @param query The query to execute
   * @return The result of the query
   * @throws PinotClientException If an exception occurs while processing the query
   */
  public ResultSetGroup execute(@Nullable Iterable<String> tableNames, String query)
      throws PinotClientException {
    String[] resultTableNames = (tableNames == null) ? resolveTableName(query)
        : Iterables.toArray(tableNames, String.class);
    String brokerHostPort = _brokerSelector.selectBroker(resultTableNames);
    if (brokerHostPort == null) {
      throw new PinotClientException("Could not find broker to query " + ((tableNames == null) ? "with no tables"
          : "for table(s): " + Iterables.toString(tableNames)));
    }
    BrokerResponse response = _transport.executeQuery(brokerHostPort, query);
    if (response.hasExceptions() && _failOnExceptions) {
      throw new PinotClientException("Query had processing exceptions: \n" + response.getExceptions());
    }
    return new ResultSetGroup(response);
  }

  /**
   * Executes a query asynchronously.
   *
   * @param query The query to execute
   * @return A future containing the result of the query
   * @throws PinotClientException If an exception occurs while processing the query
   */
  public CompletableFuture<ResultSetGroup> executeAsync(String query)
      throws PinotClientException {
    return executeAsync((Iterable<String>) null, query);
  }

  /**
   * Executes a query asynchronously.
   *
   * @param tableName Name of the table to execute the query on
   * @param query The query to execute
   * @return A future containing the result of the query
   * @throws PinotClientException If an exception occurs while processing the query
   */
  public CompletableFuture<ResultSetGroup> executeAsync(@Nullable String tableName, String query)
      throws PinotClientException {
    return executeAsync(tableName == null ? null : List.of(tableName), query);
  }

  /**
   * Executes a query asynchronously.
   *
   * @param tableNames Names of all the tables to execute the query on
   * @param query The query to execute
   * @return A future containing the result of the query
   * @throws PinotClientException If an exception occurs while processing the query
   */
  public CompletableFuture<ResultSetGroup> executeAsync(@Nullable Iterable<String> tableNames, String query)
      throws PinotClientException {
    String[] resultTableNames = (tableNames == null) ? resolveTableName(query)
        : Iterables.toArray(tableNames, String.class);
    String brokerHostPort = _brokerSelector.selectBroker(resultTableNames);
    if (brokerHostPort == null) {
      throw new PinotClientException("Could not find broker to query for statement: " + query);
    }
    return _transport.executeQueryAsync(brokerHostPort, query).thenApply(ResultSetGroup::new);
  }

  /**
   * Returns the name of all the tables used in a sql query.
   *
   * @return name of all the tables used in a sql query.
   */
  @Nullable
  public static String[] resolveTableName(String query) {
    try {
      return TableNameExtractor.resolveTableName(query);
    } catch (Exception e) {
      LOGGER.warn("Failed to extract table names for query: {}, fall back to default broker selector", query, e);
      return null;
    }
  }

  /**
   * Returns the list of brokers to which this connection can connect to.
   *
   * @return The list of brokers to which this connection can connect to.
   */
  List<String> getBrokerList() {
    return _brokerSelector.getBrokers();
  }

  /**
   * Close the connection for further processing
   *
   * @throws PinotClientException when connection is already closed
   */
  public void close()
      throws PinotClientException {
    _transport.close();
    _brokerSelector.close();
  }

  /**
   * Provides access to the underlying transport mechanism for this connection.
   * There may be client metrics useful for monitoring and other observability goals.
   *
   * @return pinot client transport.
   */
  public PinotClientTransport<?> getTransport() {
    return _transport;
  }

  // ========== Cursor Methods (Default implementations for backward compatibility) ==========

  /**
   * Opens a cursor for paginated query execution using the Cursor Handle Pattern.
   * The returned cursor starts with the first page already loaded.
   *
   * @param query the query to execute
   * @param pageSize the number of rows per page
   * @return ResultCursor with the first page loaded and ready for navigation
   * @throws PinotClientException If an exception occurs while processing the query
   */
  public ResultCursor openCursor(String query, int pageSize) throws PinotClientException {
    if (!(_transport instanceof JsonAsyncHttpPinotClientTransport)) {
      throw new UnsupportedOperationException("Cursor operations not supported by this connection type");
    }

    // Select broker for cursor operation
    String brokerHostPort = _brokerSelector.selectBroker((String) null);
    if (brokerHostPort == null) {
      throw new PinotClientException("Could not find broker to execute cursor query");
    }

    try {
      CursorAwareBrokerResponse initialResponse = _transport.executeQueryWithCursor(brokerHostPort, query, pageSize);
      if (initialResponse.hasExceptions() && _failOnExceptions) {
        throw new PinotClientException("Query had processing exceptions: \n" + initialResponse.getExceptions());
      }

      return new ResultCursorImpl(_transport, brokerHostPort, initialResponse, _failOnExceptions);
    } catch (UnsupportedOperationException e) {
      throw new UnsupportedOperationException("Cursor operations not supported by this connection type", e);
    } catch (PinotClientException e) {
      throw new PinotClientException("Failed to open cursor", e);
    }
  }

  /**
   * Executes a query with cursor support for pagination.
   *
   * @deprecated Use openCursor(String, int) instead for better resource management
   * @param query the query to execute
   * @param numRows the number of rows per page
   * @return CursorResultSetGroup containing the first page and cursor metadata
   * @throws PinotClientException If an exception occurs while processing the query
   */
  @Deprecated
  public CursorResultSetGroup executeCursorQuery(String query, int numRows) throws PinotClientException {
    if (!(_transport instanceof JsonAsyncHttpPinotClientTransport)) {
      throw new UnsupportedOperationException("Cursor operations not supported by this connection type");
    }

    // Select broker for the query
    String[] tableNames = resolveTableName(query);
    String brokerHostPort = _brokerSelector.selectBroker(tableNames);
    if (brokerHostPort == null) {
      throw new PinotClientException("Could not find broker to execute cursor query");
    }

    try {
      // Execute query with cursor support
      CursorAwareBrokerResponse response = _transport.executeQueryWithCursor(brokerHostPort, query, numRows);
      if (response.hasExceptions() && _failOnExceptions) {
        throw new PinotClientException("Query had processing exceptions: \n" + response.getExceptions());
      }

      // Create cursor result set group
      return new CursorResultSetGroup(response);
    } catch (UnsupportedOperationException e) {
      throw new UnsupportedOperationException("Cursor operations not supported by this connection type", e);
    } catch (PinotClientException e) {
      throw new PinotClientException("Failed to execute cursor query", e);
    }
  }

  /**
   * Executes a query with cursor support asynchronously.
   *
   * @param query The SQL query to execute
   * @param pageSize The number of rows per page
   * @return CompletableFuture containing CursorResultSet
   */
  public CompletableFuture<CursorResultSetGroup> executeCursorQueryAsync(String query, int pageSize) {
    if (!(_transport instanceof JsonAsyncHttpPinotClientTransport)) {
      return CompletableFuture.failedFuture(
          new UnsupportedOperationException("Cursor operations not supported by this connection type"));
    }
    JsonAsyncHttpPinotClientTransport cursorTransport = (JsonAsyncHttpPinotClientTransport) _transport;

    // Select broker for the query
    String[] tableNames = resolveTableName(query);
    String brokerHostPort = _brokerSelector.selectBroker(tableNames);
    if (brokerHostPort == null) {
      return CompletableFuture.failedFuture(new PinotClientException("Could not find broker to execute cursor query"));
    }

    return cursorTransport.executeQueryWithCursorAsync(brokerHostPort, query, pageSize)
        .thenApply(response -> {
          if (response.hasExceptions() && _failOnExceptions) {
            throw new RuntimeException("Query had processing exceptions: \n" + response.getExceptions());
          }
          return new CursorResultSetGroup(response);
        });
  }

  /**
   * Fetches the next page for a cursor.
   *
   * @param cursorId The cursor identifier
   * @return CursorResultSet containing the next page
   * @throws PinotClientException If an exception occurs while processing the query
   */
  public CursorResultSetGroup fetchNext(String cursorId) throws PinotClientException {
    if (!(_transport instanceof JsonAsyncHttpPinotClientTransport)) {
      throw new UnsupportedOperationException("Cursor operations not supported by this connection type");
    }

    JsonAsyncHttpPinotClientTransport cursorTransport = (JsonAsyncHttpPinotClientTransport) _transport;

    // Select broker for cursor operation - could use cursor metadata or select new one
    String brokerHostPort = _brokerSelector.selectBroker((String) null);
    if (brokerHostPort == null) {
      throw new PinotClientException("Could not find broker to fetch next page");
    }

    try {
      CursorAwareBrokerResponse response = cursorTransport.fetchNextPage(brokerHostPort, cursorId);
      if (response.hasExceptions() && _failOnExceptions) {
        throw new PinotClientException("Query had processing exceptions: \n" + response.getExceptions());
      }
      return new CursorResultSetGroup(response);
    } catch (Exception e) {
      throw new PinotClientException("Failed to fetch next page", e);
    }
  }

  /**
   * Fetches the next page for a cursor asynchronously.
   *
   * @param cursorId The cursor identifier
   * @return CompletableFuture containing the next page
   */
  public CompletableFuture<CursorResultSetGroup> fetchNextAsync(String cursorId) {
    if (!(_transport instanceof JsonAsyncHttpPinotClientTransport)) {
      return CompletableFuture.failedFuture(
          new UnsupportedOperationException("Cursor operations not supported by this connection type"));
    }

    JsonAsyncHttpPinotClientTransport cursorTransport = (JsonAsyncHttpPinotClientTransport) _transport;

    // Select broker for cursor operation
    String brokerHostPort = _brokerSelector.selectBroker((String) null);
    if (brokerHostPort == null) {
      return CompletableFuture.failedFuture(
          new PinotClientException("Could not find broker to fetch next page"));
    }

    return cursorTransport.fetchNextPageAsync(brokerHostPort, cursorId)
        .thenApply(response -> {
          if (response.hasExceptions() && _failOnExceptions) {
            throw new RuntimeException("Query had processing exceptions: \n" + response.getExceptions());
          }
          return new CursorResultSetGroup(response);
        });
  }

  /**
   * Fetches the previous page for a cursor.
   *
   * @param cursorId The cursor identifier
   * @return CursorResultSet containing the previous page
   * @throws PinotClientException If an exception occurs while processing the query
   */
  public CursorResultSetGroup fetchPrevious(String cursorId) throws PinotClientException {
    if (!(_transport instanceof JsonAsyncHttpPinotClientTransport)) {
      throw new UnsupportedOperationException("Cursor operations not supported by this connection type");
    }

    JsonAsyncHttpPinotClientTransport cursorTransport = (JsonAsyncHttpPinotClientTransport) _transport;

    // Select broker for cursor operation
    String brokerHostPort = _brokerSelector.selectBroker((String) null);
    if (brokerHostPort == null) {
      throw new PinotClientException("Could not find broker to fetch previous page");
    }

    try {
      CursorAwareBrokerResponse response = cursorTransport.fetchPreviousPage(brokerHostPort, cursorId);
      if (response.hasExceptions() && _failOnExceptions) {
        throw new PinotClientException("Query had processing exceptions: \n" + response.getExceptions());
      }
      return new CursorResultSetGroup(response);
    } catch (Exception e) {
      throw new PinotClientException("Failed to fetch previous page", e);
    }
  }

  /**
   * Fetches the previous page for a cursor asynchronously.
   *
   * @param cursorId The cursor identifier
   * @return CompletableFuture containing the previous page
   */
  public CompletableFuture<CursorResultSetGroup> fetchPreviousAsync(String cursorId) {
    if (!(_transport instanceof JsonAsyncHttpPinotClientTransport)) {
      return CompletableFuture.failedFuture(
          new UnsupportedOperationException("Cursor operations not supported by this connection type"));
    }

    JsonAsyncHttpPinotClientTransport cursorTransport = (JsonAsyncHttpPinotClientTransport) _transport;

    // Select broker for cursor operation
    String brokerHostPort = _brokerSelector.selectBroker((String) null);
    if (brokerHostPort == null) {
      return CompletableFuture.failedFuture(
          new PinotClientException("Could not find broker to fetch previous page"));
    }

    return cursorTransport.fetchPreviousPageAsync(brokerHostPort, cursorId)
        .thenApply(response -> {
          if (response.hasExceptions() && _failOnExceptions) {
            throw new RuntimeException("Query had processing exceptions: \n" + response.getExceptions());
          }
          return new CursorResultSetGroup(response);
        });
  }

  /**
   * Seeks to a specific page for a cursor.
   *
   * @param cursorId The cursor identifier
   * @param pageNumber The page number to seek to (0-based)
   * @return CursorResultSet containing the specified page
   * @throws PinotClientException If an exception occurs while processing the query
   */
  public CursorResultSetGroup seekToPage(String cursorId, int pageNumber) throws PinotClientException {
    if (!(_transport instanceof JsonAsyncHttpPinotClientTransport)) {
      throw new UnsupportedOperationException("Cursor operations not supported by this connection type");
    }

    JsonAsyncHttpPinotClientTransport cursorTransport = (JsonAsyncHttpPinotClientTransport) _transport;

    // Select broker for cursor operation
    String brokerHostPort = _brokerSelector.selectBroker((String) null);
    if (brokerHostPort == null) {
      throw new PinotClientException("Could not find broker to seek to page");
    }

    try {
      CursorAwareBrokerResponse response = cursorTransport.seekToPage(brokerHostPort, cursorId, pageNumber);
      if (response.hasExceptions() && _failOnExceptions) {
        throw new PinotClientException("Query had processing exceptions: \n" + response.getExceptions());
      }
      return new CursorResultSetGroup(response);
    } catch (Exception e) {
      throw new PinotClientException("Failed to seek to page " + pageNumber, e);
    }
  }

  /**
   * Seeks to a specific page for a cursor asynchronously.
   *
   * @param cursorId The cursor identifier
   * @param pageNumber The page number to seek to (0-based)
   * @return CompletableFuture containing the specified page
   */
  public CompletableFuture<CursorResultSetGroup> seekToPageAsync(String cursorId, int pageNumber) {
    if (!(_transport instanceof JsonAsyncHttpPinotClientTransport)) {
      return CompletableFuture.failedFuture(
          new UnsupportedOperationException("Cursor operations not supported by this connection type"));
    }

    JsonAsyncHttpPinotClientTransport cursorTransport = (JsonAsyncHttpPinotClientTransport) _transport;

    // Select broker for cursor operation
    String brokerHostPort = _brokerSelector.selectBroker((String) null);
    if (brokerHostPort == null) {
      return CompletableFuture.failedFuture(
          new PinotClientException("Could not find broker to seek to page"));
    }

    return cursorTransport.seekToPageAsync(brokerHostPort, cursorId, pageNumber)
        .thenApply(response -> {
          if (response.hasExceptions() && _failOnExceptions) {
            throw new RuntimeException("Query had processing exceptions: \n" + response.getExceptions());
          }
          return new CursorResultSetGroup(response);
        });
  }
}
