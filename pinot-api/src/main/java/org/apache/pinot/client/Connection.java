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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A connection to Pinot, normally created through calls to the {@link ConnectionFactory}.
 */
public class Connection {
  private static final Logger LOGGER = LoggerFactory.getLogger(Connection.class);
  private final PinotClientTransport _transport;
  private BrokerSelector _brokerSelector;
  private List<String> _brokerList;

  Connection(List<String> brokerList, PinotClientTransport transport) {
    _brokerList = brokerList;
    LOGGER.info("Creating connection to broker list {}", brokerList);
    _brokerSelector = new SimpleBrokerSelector(brokerList);
    _transport = transport;
  }

  Connection(BrokerSelector brokerSelector, PinotClientTransport transport) {
    _brokerSelector = brokerSelector;
    _transport = transport;
  }

  /**
   * Creates a prepared statement, to escape query parameters.
   *
   * @param statement The statement for which to create a prepared statement.
   * @return A prepared statement for this connection.
   */
  public PreparedStatement prepareStatement(String statement) {
    return new PreparedStatement(this, statement);
  }

  /**
   * Executes a PQL statement.
   * @param statement The statement to execute
   * @return The result of the query
   * @throws PinotClientException If an exception occurs while processing the query
   */
  public ResultSetGroup execute(String statement)
      throws PinotClientException {
    return execute(null, new Request("pql", statement));
  }

  /**
   * Executes a Pinot Request.
   * @param request The statement to execute
   * @return The result of the query
   * @throws PinotClientException If an exception occurs while processing the query
   */
  public ResultSetGroup execute(Request request)
      throws PinotClientException {
    return execute(null, request);
  }

  /**
   * Executes a PQL statement.
   *
   * @param statement The statement to execute
   * @return The result of the query
   * @throws PinotClientException If an exception occurs while processing the query
   */
  public ResultSetGroup execute(String tableName, String statement)
      throws PinotClientException {
    return execute(tableName, new Request("pql", statement));
  }

  /**
   * Executes a Pinot Request.
   *
   * @param request The statement to execute
   * @return The result of the query
   * @throws PinotClientException If an exception occurs while processing the query
   */
  public ResultSetGroup execute(String tableName, Request request)
      throws PinotClientException {
    String brokerHostPort = _brokerSelector.selectBroker(tableName);
    if (brokerHostPort == null) {
      throw new PinotClientException(
          "Could not find broker to query for table: " + (tableName == null ? "null" : tableName));
    }
    BrokerResponse response = _transport.executeQuery(brokerHostPort, request);
    if (response.hasExceptions()) {
      throw new PinotClientException("Query had processing exceptions: \n" + response.getExceptions());
    }
    return new ResultSetGroup(response);
  }

  /**
   * Executes a PQL statement asynchronously.
   *
   * @param statement The statement to execute
   * @return A future containing the result of the query
   * @throws PinotClientException If an exception occurs while processing the query
   */
  public Future<ResultSetGroup> executeAsync(String statement)
      throws PinotClientException {
    return executeAsync(new Request("pql", statement));
  }

  /**
   * Executes a Pinot Request asynchronously.
   *
   * @param request The statement to execute
   * @return A future containing the result of the query
   * @throws PinotClientException If an exception occurs while processing the query
   */
  public Future<ResultSetGroup> executeAsync(Request request)
      throws PinotClientException {
    String brokerHostPort = _brokerSelector.selectBroker(null);
    if (brokerHostPort == null) {
      throw new PinotClientException(
          "Could not find broker to query for statement: " + (request.getQuery() == null ? "null"
              : request.getQuery()));
    }
    final Future<BrokerResponse> responseFuture = _transport.executeQueryAsync(brokerHostPort, request.getQuery());
    return new ResultSetGroupFuture(responseFuture);
  }

  /**
   * Returns the list of brokers to which this connection can connect to.
   *
   * @return The list of brokers to which this connection can connect to.
   */
  List<String> getBrokerList() {
    return _brokerList;
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
        return get(1000L, TimeUnit.DAYS);
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
