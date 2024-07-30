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

import java.sql.DatabaseMetaData;
import java.sql.Statement;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class PinotConnectionTest {
  private DummyPinotClientTransport _dummyPinotClientTransport;
  private DummyPinotControllerTransport _dummyPinotControllerTransport;

  @BeforeMethod
  public void beforeTestMethod() {
    _dummyPinotClientTransport = new DummyPinotClientTransport();
    _dummyPinotControllerTransport = DummyPinotControllerTransport.create();
  }

  @Test
  public void createStatementTest()
      throws Exception {
    PinotConnection pinotConnection =
        new PinotConnection("dummy", _dummyPinotClientTransport, "dummy", _dummyPinotControllerTransport);
    Statement statement = pinotConnection.createStatement();
    Assert.assertNotNull(statement);
  }

  @Test
  public void getMetaDataTest()
          throws Exception {
    try (PinotConnection pinotConnection =
                 new PinotConnection("dummy", _dummyPinotClientTransport, "dummy", _dummyPinotControllerTransport)) {
      DatabaseMetaData metaData = pinotConnection.getMetaData();
      Assert.assertNotNull(metaData);
      Assert.assertTrue(metaData.getDatabaseMajorVersion() > 0);
      Assert.assertTrue(metaData.getDatabaseMinorVersion() >= 0);
      Assert.assertEquals(metaData.getDatabaseProductName(), "APACHE_PINOT");
      Assert.assertEquals(metaData.getDriverName(), "APACHE_PINOT_DRIVER");
      Assert.assertNotNull(metaData.getConnection());
    }
  }

  @Test
  public void isClosedTest()
          throws Exception {
    PinotConnection pinotConnection =
                 new PinotConnection("dummy", _dummyPinotClientTransport, "dummy", _dummyPinotControllerTransport);
    Assert.assertFalse(pinotConnection.isClosed());
    pinotConnection.close();
    Assert.assertTrue(pinotConnection.isClosed());
  }

  @Test
  public void setUserAgentTest()
      throws Exception {
    StringBuilder appId = new StringBuilder("appId-");
    for (int i = 0; i < 256; i++) {
       appId.append(i);
    }
    DummyPinotControllerTransport userAgentPinotControllerTransport = DummyPinotControllerTransport
        .create(appId.toString());

    PinotConnection pinotConnection =
        new PinotConnection("dummy", _dummyPinotClientTransport, "dummy", userAgentPinotControllerTransport);
    Statement statement = pinotConnection.createStatement();
    Assert.assertNotNull(statement);
  }

  @Test
  public void unsetUserAgentTest()
      throws Exception {
    DummyPinotControllerTransport userAgentPinotControllerTransport = DummyPinotControllerTransport
        .create(null);

    PinotConnection pinotConnection =
        new PinotConnection("dummy", _dummyPinotClientTransport, "dummy", userAgentPinotControllerTransport);
    Statement statement = pinotConnection.createStatement();
    Assert.assertNotNull(statement);
  }
}
