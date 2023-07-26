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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Properties;
import org.apache.commons.codec.binary.Hex;
import org.apache.pinot.client.utils.DateTimeUtils;
import org.apache.pinot.client.utils.DriverUtils;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PinotPreparedStatementTest {
  public static final String QUERY =
      "SELECT * FROM dummy WHERE name = ? and age = ? and score = ? and ts = ? and eligible = ? and sub_score = ?";
  public static final String DATE_QUERY = "SELECT * FROM dummy WHERE date = ? and updated_at = ? and created_at = ?";
  public static final String SINGLE_STRING_QUERY = "SELECT * FROM dummy WHERE value = ?";
  private static final String BASIC_TEST_QUERY = "SELECT * FROM dummy";
  private DummyPinotClientTransport _dummyPinotClientTransport = new DummyPinotClientTransport();
  private DummyPinotControllerTransport _dummyPinotControllerTransport = DummyPinotControllerTransport.create();

  @Test
  public void testSetAndClearValues()
      throws Exception {
    PinotConnection connection =
        new PinotConnection(new Properties(), "dummy", _dummyPinotClientTransport, "dummy",
            _dummyPinotControllerTransport);
    PreparedStatement preparedStatement = connection.prepareStatement(QUERY);

    preparedStatement.setString(1, "foo");
    preparedStatement.setInt(2, 20);
    preparedStatement.setDouble(3, 98.1);
    preparedStatement.setLong(4, 123456789L);
    preparedStatement.setBoolean(5, true);
    preparedStatement.setFloat(6, 1.4f);
    preparedStatement.executeQuery();

    String lastExecutedQuery = _dummyPinotClientTransport.getLastQuery();

    Assert.assertEquals(lastExecutedQuery.substring(0, lastExecutedQuery.indexOf("LIMIT")).trim(),
        "SELECT * FROM dummy WHERE name = 'foo' and age = 20 and score = 98.1 and ts = 123456789 and eligible = "
            + "'true' and sub_score = 1.4");

    preparedStatement.clearParameters();
    preparedStatement.setString(1, "");
    preparedStatement.setInt(2, 0);
    preparedStatement.setDouble(3, 0.0);
    preparedStatement.setLong(4, 0);
    preparedStatement.setBoolean(5, false);
    preparedStatement.setFloat(6, 0);
    preparedStatement.executeQuery();

    lastExecutedQuery = _dummyPinotClientTransport.getLastQuery();

    Assert.assertEquals(lastExecutedQuery.substring(0, lastExecutedQuery.indexOf("LIMIT")).trim(),
        "SELECT * FROM dummy WHERE name = '' and age = 0 and score = 0.0 and ts = 0 and eligible = 'false' and "
            + "sub_score = 0.0");
  }

  @Test
  public void testSetDateTime()
      throws Exception {
    PinotConnection connection =
        new PinotConnection("dummy", _dummyPinotClientTransport, "dummy", _dummyPinotControllerTransport);
    PreparedStatement preparedStatement = connection.prepareStatement(DATE_QUERY);

    Long currentTimestamp = System.currentTimeMillis();
    preparedStatement.setDate(1, new Date(currentTimestamp));
    preparedStatement.setTime(2, new Time(currentTimestamp));
    preparedStatement.setTimestamp(3, new Timestamp(currentTimestamp));
    preparedStatement.executeQuery();

    String expectedDate = DateTimeUtils.dateToString(new Date(currentTimestamp));
    String expectedTime = DateTimeUtils.timeStampToString(new Timestamp(currentTimestamp));
    String lastExecutedQuery = _dummyPinotClientTransport.getLastQuery();

    Assert.assertEquals(lastExecutedQuery.substring(0, lastExecutedQuery.indexOf("LIMIT")).trim(), String
        .format("SELECT * FROM dummy WHERE date = '%s' and updated_at = '%s' and created_at = '%s'", expectedDate,
            expectedTime, expectedTime));
  }

  @Test
  public void testSetAdditionalDataTypes()
      throws Exception {
    PinotConnection connection =
        new PinotConnection("dummy", _dummyPinotClientTransport, "dummy", _dummyPinotControllerTransport);
    PreparedStatement preparedStatement = connection.prepareStatement(SINGLE_STRING_QUERY);

    String value = "1234567891011121314151617181920";
    preparedStatement.setBigDecimal(1, new BigDecimal(value));
    preparedStatement.executeQuery();

    String lastExecutedQuery = _dummyPinotClientTransport.getLastQuery();
    Assert.assertEquals(lastExecutedQuery.substring(0, lastExecutedQuery.indexOf("LIMIT")).trim(),
        String.format("SELECT * FROM dummy WHERE value = '%s'", value));

    preparedStatement.clearParameters();
    preparedStatement.setBytes(1, value.getBytes());
    preparedStatement.executeQuery();
    lastExecutedQuery = _dummyPinotClientTransport.getLastQuery();
    Assert.assertEquals(lastExecutedQuery.substring(0, lastExecutedQuery.indexOf("LIMIT")).trim(),
        String.format("SELECT * FROM dummy WHERE value = '%s'", Hex.encodeHexString(value.getBytes())));
  }

  @Test
  public void testSetEnableNullHandling()
      throws Exception {
    Properties props = new Properties();
    props.put(QueryOptionKey.ENABLE_NULL_HANDLING, "true");
    PinotConnection pinotConnection =
        new PinotConnection(props, "dummy", _dummyPinotClientTransport, "dummy", _dummyPinotControllerTransport);
    PreparedStatement preparedStatement = pinotConnection.prepareStatement(BASIC_TEST_QUERY);
    preparedStatement.executeQuery();
    String expectedSql =
        DriverUtils.createSetQueryOptionString(QueryOptionKey.ENABLE_NULL_HANDLING, true) + BASIC_TEST_QUERY;
    Assert.assertEquals(_dummyPinotClientTransport.getLastQuery().substring(0, expectedSql.length()), expectedSql);
  }

  @Test
  public void testSetEnableNullHandling2()
      throws Exception {
    Properties props = new Properties();
    props.put(QueryOptionKey.ENABLE_NULL_HANDLING, "true");
    PinotConnection pinotConnection =
        new PinotConnection(props, "dummy", _dummyPinotClientTransport, "dummy", _dummyPinotControllerTransport);
    PreparedStatement preparedStatement = pinotConnection.prepareStatement("");
    preparedStatement.executeQuery(BASIC_TEST_QUERY);
    String expectedSql =
        DriverUtils.createSetQueryOptionString(QueryOptionKey.ENABLE_NULL_HANDLING, true) + BASIC_TEST_QUERY;
    Assert.assertEquals(_dummyPinotClientTransport.getLastQuery().substring(0, expectedSql.length()), expectedSql);
  }
}
