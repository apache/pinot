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

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for prepared statement escaping
 *
 */
public class PreparedStatementTest {
  private final DummyPinotClientTransport _dummyPinotClientTransport = new DummyPinotClientTransport();

  @Test
  public void testPreparedStatementWithDynamicBroker() {
    // Create a connection with dynamic broker selector.
    BrokerSelector mockBrokerSelector = Mockito.mock(BrokerSelector.class);
    Mockito.when(mockBrokerSelector.selectBroker(Mockito.anyString()))
        .thenAnswer(i -> i.getArgument(0));
    Connection connection = new Connection(mockBrokerSelector, _dummyPinotClientTransport);

    PreparedStatement preparedStatement = connection.prepareStatement("SELECT foo FROM bar WHERE baz = ?");
    preparedStatement.setString(0, "'hello'");
    preparedStatement.execute();
    Assert.assertEquals("SELECT foo FROM bar WHERE baz = '''hello'''", _dummyPinotClientTransport.getLastQuery());
    Assert.assertEquals("bar", _dummyPinotClientTransport.getLastBrokerAddress());

    preparedStatement = connection.prepareStatement("SELECT bar FROM foo WHERE baz = ?");
    preparedStatement.setString(0, "'world'");
    preparedStatement.executeAsync();
    Assert.assertEquals("SELECT bar FROM foo WHERE baz = '''world'''", _dummyPinotClientTransport.getLastQuery());
    Assert.assertEquals("foo", _dummyPinotClientTransport.getLastBrokerAddress());
  }

  @Test
  public void testPreparedStatementEscaping() {
    // Create a prepared statement that has to quote a string appropriately
    Connection connection =
        ConnectionFactory.fromHostList(Collections.singletonList("dummy"), _dummyPinotClientTransport);
    PreparedStatement preparedStatement = connection.prepareStatement("SELECT foo FROM bar WHERE baz = ?");
    preparedStatement.setString(0, "'hello'");
    preparedStatement.execute();

    // Check that the query sent is appropriately escaped
    Assert.assertEquals("SELECT foo FROM bar WHERE baz = '''hello'''", _dummyPinotClientTransport.getLastQuery());
  }

  static class DummyPinotClientTransport implements PinotClientTransport {
    private String _lastBrokerAddress;
    private String _lastQuery;

    @Override
    public BrokerResponse executeQuery(String brokerAddress, String query)
        throws PinotClientException {
      _lastBrokerAddress = brokerAddress;
      _lastQuery = query;
      return BrokerResponse.empty();
    }

    @Override
    public CompletableFuture<BrokerResponse> executeQueryAsync(String brokerAddress, String query)
        throws PinotClientException {
      return CompletableFuture.completedFuture(executeQuery(brokerAddress, query));
    }

    @Override
    public BrokerResponse executeQuery(String brokerAddress, Request request)
        throws PinotClientException {
      return executeQuery(brokerAddress, request.getQuery());
    }

    @Override
    public CompletableFuture<BrokerResponse> executeQueryAsync(String brokerAddress, Request request)
        throws PinotClientException {
      return executeQueryAsync(brokerAddress, request.getQuery());
    }

    public String getLastQuery() {
      return _lastQuery;
    }

    public String getLastBrokerAddress() {
      return _lastBrokerAddress;
    }

    @Override
    public void close()
        throws PinotClientException {
    }
  }

  class DummyPinotClientTransportFactory implements PinotClientTransportFactory {
    @Override
    public PinotClientTransport buildTransport() {
      return _dummyPinotClientTransport;
    }
  }
}
