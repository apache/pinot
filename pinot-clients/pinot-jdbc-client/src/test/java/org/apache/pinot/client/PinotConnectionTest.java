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
import java.util.Map;
import java.util.Properties;
import org.apache.pinot.spi.utils.CommonConstants;
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

  @Test
  public void testValidQueryOptions() {
    Properties props = new Properties();
    props.put(CommonConstants.Broker.Request.QueryOptionKey.ENABLE_NULL_HANDLING, "true");
    props.put(CommonConstants.Broker.Request.QueryOptionKey.USE_MULTISTAGE_ENGINE, "true");
    props.put(CommonConstants.Broker.Request.QueryOptionKey.TIMEOUT_MS, 3000);
    props.put(CommonConstants.Broker.Request.QueryOptionKey.MAX_EXECUTION_THREADS, 20);
    PinotConnection pinotConnection =
            new PinotConnection(props, "dummy", _dummyPinotClientTransport, "dummy",
                    _dummyPinotControllerTransport);
    Map<String, Object> queryOptions = pinotConnection.getQueryOptions();
    Assert.assertEquals(queryOptions.size(), 4);
    Assert.assertEquals(queryOptions.get(CommonConstants.Broker.Request.QueryOptionKey.ENABLE_NULL_HANDLING), true);
    Assert.assertEquals(queryOptions.get(CommonConstants.Broker.Request.QueryOptionKey.USE_MULTISTAGE_ENGINE), true);
    Assert.assertEquals(queryOptions.get(CommonConstants.Broker.Request.QueryOptionKey.TIMEOUT_MS), 3000);
    Assert.assertEquals(queryOptions.get(CommonConstants.Broker.Request.QueryOptionKey.MAX_EXECUTION_THREADS), 20);
  }

  @Test
  public void testInvalidQueryOptions() {
    Properties props = new Properties();
    props.put("unknown", "true");
    PinotConnection pinotConnection =
            new PinotConnection(props, "dummy", _dummyPinotClientTransport, "dummy",
                    _dummyPinotControllerTransport);
    Map<String, Object> queryOptions = pinotConnection.getQueryOptions();
    Assert.assertEquals(queryOptions.size(), 0);
  }

  @Test
  public void testSomeInvalidSomeValidQueryOptions() {
    Properties props = new Properties();
    props.put("unknown", "true");
    // lower case of property name also should considered as wrong query option.
    props.put("enablenullhandling", "true");
    props.put(CommonConstants.Broker.Request.QueryOptionKey.TIMEOUT_MS, 5000);
    props.put(CommonConstants.Broker.Request.QueryOptionKey.MAX_EXECUTION_THREADS, 30);
    PinotConnection pinotConnection =
            new PinotConnection(props, "dummy", _dummyPinotClientTransport, "dummy",
                    _dummyPinotControllerTransport);
    Map<String, Object> queryOptions = pinotConnection.getQueryOptions();
    Assert.assertEquals(queryOptions.size(), 3);
    Assert.assertEquals(queryOptions.get(CommonConstants.Broker.Request.QueryOptionKey.TIMEOUT_MS), 5000);
    Assert.assertEquals(queryOptions.get(CommonConstants.Broker.Request.QueryOptionKey.MAX_EXECUTION_THREADS), 30);
  }
}
