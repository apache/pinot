package org.apache.pinot.client;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.concurrent.Future;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class PinotStatementTest {
  private DummyPinotClientTransport _dummyPinotClientTransport = new DummyPinotClientTransport();
  private PinotClientTransportFactory _previousTransportFactory = null;

  @Test
  public void testStatement() throws Exception {
    PinotConnection connection = new PinotConnection(Collections.singletonList("dummy"), _dummyPinotClientTransport);
    Statement statement = new PinotStatement(connection);
    ResultSet resultSet = statement.executeQuery("dummy");
    Assert.assertNotEquals(resultSet, null);
    Assert.assertEquals(statement.getConnection(), connection);
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
  }

  class DummyPinotClientTransportFactory implements PinotClientTransportFactory {
    @Override
    public PinotClientTransport buildTransport() {
      return _dummyPinotClientTransport;
    }
  }
}
