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
import java.util.concurrent.Future;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for prepared statement escaping
 *
 */
public class PreparedStatementTest {
  private final DummyPinotClientTransport _dummyPinotClientTransport = new DummyPinotClientTransport();

  @Test
  public void testPreparedStatementEscaping() {
    // Create a prepared statement that has to quote a string appropriately
    Connection connection =
        ConnectionFactory.fromHostList(Collections.singletonList("dummy"), _dummyPinotClientTransport);
    PreparedStatement preparedStatement =
        connection.prepareStatement(new Request("sql", "SELECT foo FROM bar WHERE baz = ?"));
    preparedStatement.setString(0, "'hello'");
    preparedStatement.execute();

    // Check that the query sent is appropriately escaped
    Assert.assertEquals("SELECT foo FROM bar WHERE baz = '''hello'''", _dummyPinotClientTransport.getLastQuery());
  }

  static class DummyPinotClientTransport implements PinotClientTransport {
    private String _lastQuery;

    @Override
    public BrokerResponse executeQuery(String brokerAddress, String query)
        throws PinotClientException {
      _lastQuery = query;
      return BrokerResponse.empty();
    }

    @Override
    public Future<BrokerResponse> executeQueryAsync(String brokerAddress, String query)
        throws PinotClientException {
      _lastQuery = query;
      return null;
    }

    @Override
    public BrokerResponse executeQuery(String brokerAddress, Request request)
        throws PinotClientException {
      _lastQuery = request.getQuery();
      return BrokerResponse.empty();
    }

    @Override
    public Future<BrokerResponse> executeQueryAsync(String brokerAddress, Request request)
        throws PinotClientException {
      _lastQuery = request.getQuery();
      return null;
    }

    public String getLastQuery() {
      return _lastQuery;
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
