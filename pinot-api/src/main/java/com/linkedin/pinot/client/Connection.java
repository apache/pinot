/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.client;

import java.util.List;
import java.util.Random;
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
  private final List<String> _brokerList;
  private final PinotClientTransport _transport;
  private final Random _random = new Random();

  Connection(List<String> brokerList, PinotClientTransport transport) {
    LOGGER.info("Creating connection to broker list {}", brokerList);
    _brokerList = brokerList;
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
   *
   * @param statement The statement to execute
   * @return The result of the query
   * @throws PinotClientException If an exception occurs while processing the query
   */
  public ResultSetGroup execute(String statement) throws PinotClientException {
    BrokerResponse response = _transport.executeQuery(pickRandomBroker(), statement);

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
  public Future<ResultSetGroup> executeAsync(String statement) throws PinotClientException {
    final Future<BrokerResponse> responseFuture = _transport.executeQueryAsync(pickRandomBroker(), statement);

    return new ResultSetGroupFuture(responseFuture);
  }

  private String pickRandomBroker() {
    return _brokerList.get(_random.nextInt(_brokerList.size()));
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
    public ResultSetGroup get() throws InterruptedException, ExecutionException {
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
