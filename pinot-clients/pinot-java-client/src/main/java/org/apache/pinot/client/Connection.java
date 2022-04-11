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

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A connection to Pinot, normally created through calls to the {@link ConnectionFactory}.
 */
public class Connection {
  public static final String FAIL_ON_EXCEPTIONS = "failOnExceptions";
  private static final Logger LOGGER = LoggerFactory.getLogger(Connection.class);

  private final PinotClientTransport _transport;
  private final BrokerSelector _brokerSelector;
  private final boolean _failOnExceptions;

  Connection(List<String> brokerList, PinotClientTransport transport) {
    this(new Properties(), new SimpleBrokerSelector(brokerList), transport);
  }

  Connection(Properties properties, List<String> brokerList, PinotClientTransport transport) {
    this(properties, new SimpleBrokerSelector(brokerList), transport);
    LOGGER.info("Created connection to broker list {}", brokerList);
  }

  Connection(BrokerSelector brokerSelector, PinotClientTransport transport) {
    this(new Properties(), brokerSelector, transport);
  }

  Connection(Properties properties, BrokerSelector brokerSelector, PinotClientTransport transport) {
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
   * Creates a prepared statement, to escape query parameters.
   *
   * @param request The request for which to create a prepared statement.
   * @return A prepared statement for this connection.
   */
  @Deprecated
  public PreparedStatement prepareStatement(Request request) {
    return new PreparedStatement(this, request);
  }

  /**
   * Executes a query.
   *
   * @param query The query to execute
   * @return The result of the query
   * @throws PinotClientException If an exception occurs while processing the query
   */
  public ResultSetGroup execute(String query) {
    return execute(null, query);
  }

  /**
   * Executes a Pinot Request.
   * @param request The request to execute
   * @return The result of the query
   * @throws PinotClientException If an exception occurs while processing the query
   */
  @Deprecated
  public ResultSetGroup execute(Request request)
      throws PinotClientException {
    return execute(null, request);
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
    String brokerHostPort = _brokerSelector.selectBroker(tableName);
    if (brokerHostPort == null) {
      throw new PinotClientException("Could not find broker to query for table: " + tableName);
    }
    BrokerResponse response = _transport.executeQuery(brokerHostPort, query);
    if (response.hasExceptions() && _failOnExceptions) {
      throw new PinotClientException("Query had processing exceptions: \n" + response.getExceptions());
    }
    return new ResultSetGroup(response);
  }

  /**
   * Executes a Pinot Request.
   *
   * @param request The request to execute
   * @return The result of the query
   * @throws PinotClientException If an exception occurs while processing the query
   */
  @Deprecated
  public ResultSetGroup execute(@Nullable String tableName, Request request)
      throws PinotClientException {
    return execute(tableName, request.getQuery());
  }

  /**
   * Executes a query asynchronously.
   *
   * @param query The query to execute
   * @return A future containing the result of the query
   * @throws PinotClientException If an exception occurs while processing the query
   */
  public Future<ResultSetGroup> executeAsync(String query)
      throws PinotClientException {
    String brokerHostPort = _brokerSelector.selectBroker(null);
    if (brokerHostPort == null) {
      throw new PinotClientException("Could not find broker to query for statement: " + query);
    }
    return new ResultSetGroupFuture(_transport.executeQueryAsync(brokerHostPort, query));
  }

  /**
   * Executes a Pinot Request asynchronously.
   *
   * @param request The request to execute
   * @return A future containing the result of the query
   * @throws PinotClientException If an exception occurs while processing the query
   */
  @Deprecated
  public Future<ResultSetGroup> executeAsync(Request request)
      throws PinotClientException {
    return executeAsync(request.getQuery());
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

  private static class ResultSetGroupFuture implements Future<ResultSetGroup> {
    private final Future<BrokerResponse> _responseFuture;

    public ResultSetGroupFuture(Future<BrokerResponse> responseFuture) {
      _responseFuture = responseFuture;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return _responseFuture.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
      return _responseFuture.isCancelled();
    }

    @Override
    public boolean isDone() {
      return _responseFuture.isDone();
    }

    @Override
    public ResultSetGroup get()
        throws InterruptedException, ExecutionException {
      try {
        return get(60000L, TimeUnit.MILLISECONDS);
      } catch (TimeoutException e) {
        throw new ExecutionException(e);
      }
    }

    @Override
    public ResultSetGroup get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      BrokerResponse response = _responseFuture.get(timeout, unit);
      return new ResultSetGroup(response);
    }
  }
}
