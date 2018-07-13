/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.util.concurrent.Future;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Tests for prepared statement escaping
 *
 */
public class PreparedStatementTest {
  @Test
  public void testPreparedStatementEscaping() {
    // Create a prepared statement that has to quote a string appropriately
    Connection connection = ConnectionFactory.fromHostList("dummy");
    PreparedStatement preparedStatement = connection.prepareStatement("SELECT foo FROM bar WHERE baz = ?");
    preparedStatement.setString(0, "'hello'");
    preparedStatement.execute();

    // Check that the query sent is appropriately escaped
    Assert.assertEquals("SELECT foo FROM bar WHERE baz = '''hello'''", _dummyPinotClientTransport.getLastQuery());
  }

  private DummyPinotClientTransport _dummyPinotClientTransport = new DummyPinotClientTransport();
  private PinotClientTransportFactory _previousTransportFactory = null;

  class DummyPinotClientTransport implements PinotClientTransport {
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

    public String getLastQuery() {
      return _lastQuery;
    }
  }

  class DummyPinotClientTransportFactory implements PinotClientTransportFactory {
    @Override
    public PinotClientTransport buildTransport() {
      return _dummyPinotClientTransport;
    }
  }

  @BeforeClass
  public void overridePinotClientTransport() {
    _previousTransportFactory = ConnectionFactory._transportFactory;
    ConnectionFactory._transportFactory = new DummyPinotClientTransportFactory();
  }

  @AfterClass
  public void resetPinotClientTransport() {
    ConnectionFactory._transportFactory = _previousTransportFactory;
  }
}
