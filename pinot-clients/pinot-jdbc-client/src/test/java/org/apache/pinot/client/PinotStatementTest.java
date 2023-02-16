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

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import org.apache.pinot.client.utils.DriverUtils;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PinotStatementTest {
  private static final String BASIC_TEST_QUERY = "SELECT * FROM dummy";
  private DummyPinotClientTransport _dummyPinotClientTransport = new DummyPinotClientTransport();
  private DummyPinotControllerTransport _dummyPinotControllerTransport = DummyPinotControllerTransport.create();

  @Test
  public void testExecuteQuery()
      throws Exception {
    PinotConnection connection =
        new PinotConnection("dummy", _dummyPinotClientTransport, "dummy", _dummyPinotControllerTransport);
    Statement statement = new PinotStatement(connection);
    ResultSet resultSet = statement.executeQuery(BASIC_TEST_QUERY);
    Assert.assertNotNull(resultSet);
    Assert.assertEquals(statement.getConnection(), connection);
  }

  @Test
  public void testSetEnableNullHandling()
      throws Exception {
    Properties props = new Properties();
    props.put(QueryOptionKey.ENABLE_NULL_HANDLING, "true");
    PinotConnection pinotConnection =
        new PinotConnection(props, "dummy", _dummyPinotClientTransport, "dummy", _dummyPinotControllerTransport);
    Statement statement = pinotConnection.createStatement();
    Assert.assertNotNull(statement);
    statement.executeQuery(BASIC_TEST_QUERY);
    String expectedSql = DriverUtils.createSetQueryOptionString(QueryOptionKey.ENABLE_NULL_HANDLING) + BASIC_TEST_QUERY;
    Assert.assertEquals(expectedSql, _dummyPinotClientTransport.getLastQuery().substring(0, expectedSql.length()));
  }

  @Test
  public void testSetDisableNullHandling()
      throws Exception {
    Properties props = new Properties();
    props.put(QueryOptionKey.ENABLE_NULL_HANDLING, "false");
    PinotConnection pinotConnection =
        new PinotConnection(props, "dummy", _dummyPinotClientTransport, "dummy", _dummyPinotControllerTransport);
    Statement statement = pinotConnection.createStatement();
    Assert.assertNotNull(statement);
    statement.executeQuery(BASIC_TEST_QUERY);
    String expectedSql = BASIC_TEST_QUERY;
    Assert.assertEquals(expectedSql, _dummyPinotClientTransport.getLastQuery().substring(0, expectedSql.length()));
  }

  @Test
  public void testPresetEnableNullHandling()
      throws Exception {
    Properties props = new Properties();
    props.put(QueryOptionKey.ENABLE_NULL_HANDLING, "true");
    PinotConnection pinotConnection =
        new PinotConnection(props, "dummy", _dummyPinotClientTransport, "dummy", _dummyPinotControllerTransport);
    Statement statement = pinotConnection.createStatement();
    Assert.assertNotNull(statement);
    String presetSql = DriverUtils.createSetQueryOptionString(QueryOptionKey.ENABLE_NULL_HANDLING) + BASIC_TEST_QUERY;
    statement.executeQuery(presetSql);
    Assert.assertEquals(presetSql, _dummyPinotClientTransport.getLastQuery().substring(0, presetSql.length()));
  }

  @Test
  public void testSetUseMultistageEngine()
      throws Exception {
    Properties props = new Properties();
    props.put(QueryOptionKey.USE_MULTISTAGE_ENGINE, "true");
    PinotConnection pinotConnection =
        new PinotConnection(props, "dummy", _dummyPinotClientTransport, "dummy", _dummyPinotControllerTransport);
    Statement statement = pinotConnection.createStatement();
    Assert.assertNotNull(statement);
    statement.executeQuery(BASIC_TEST_QUERY);
    String expectedSql =
        DriverUtils.createSetQueryOptionString(QueryOptionKey.USE_MULTISTAGE_ENGINE) + BASIC_TEST_QUERY;
    Assert.assertEquals(expectedSql, _dummyPinotClientTransport.getLastQuery().substring(0, expectedSql.length()));
  }

  @Test
  public void testSetAllPossibleQueryOptions()
      throws Exception {
    Properties props = new Properties();
    for (String option : PinotConnection.POSSIBLE_QUERY_OPTIONS) {
      props.put(option, "true");
    }
    PinotConnection pinotConnection =
        new PinotConnection(props, "dummy", _dummyPinotClientTransport, "dummy", _dummyPinotControllerTransport);
    Statement statement = pinotConnection.createStatement();
    Assert.assertNotNull(statement);
    statement.executeQuery(BASIC_TEST_QUERY);
    String resultingQuery = _dummyPinotClientTransport.getLastQuery();
    for (String option : PinotConnection.POSSIBLE_QUERY_OPTIONS) {
      Assert.assertTrue(resultingQuery.contains(DriverUtils.createSetQueryOptionString(option)));
    }
  }
}
