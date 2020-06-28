package org.apache.pinot.client;

import java.sql.Statement;
import java.util.Collections;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class PinotConnectionTest {
  private DummyPinotClientTransport
      _dummyPinotClientTransport = new DummyPinotClientTransport();
  private PinotClientTransportFactory _previousTransportFactory = null;

  @Test
  public void createStatementTest() throws Exception {
    PinotConnection pinotConnection  = new PinotConnection(Collections.singletonList("dummy"), _dummyPinotClientTransport);
    Statement statement = pinotConnection.createStatement();
    Assert.assertNotNull(statement);
  }

  @BeforeClass
  public void overridePinotClientTransport() {
    _previousTransportFactory = ConnectionFactory._transportFactory;
    ConnectionFactory._transportFactory = new DummyPinotClientTransportFactory(_dummyPinotClientTransport);
  }

  @AfterClass
  public void resetPinotClientTransport() {
    ConnectionFactory._transportFactory = _previousTransportFactory;
  }
}
