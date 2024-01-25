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

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.ResultSetMetaData;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.client.utils.DateTimeUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests deserialization of a ResultSet given hardcoded Pinot results.
 *
 */
public class PinotResultSetTest {
  public static final String TEST_RESULT_SET_RESOURCE = "result_table.json";
  private DummyJsonTransport _dummyJsonTransport = new DummyJsonTransport();

  @Test
  public void testFetchValues()
      throws Exception {
    ResultSetGroup resultSetGroup = getResultSet(TEST_RESULT_SET_RESOURCE);
    ResultSet resultSet = resultSetGroup.getResultSet(0);
    PinotResultSet pinotResultSet = new PinotResultSet(resultSet);

    int currentRow = 0;
    while (pinotResultSet.next()) {
      Assert.assertEquals(pinotResultSet.getInt(1), resultSet.getInt(currentRow, 0));
      Assert.assertEquals(pinotResultSet.getLong(2), resultSet.getLong(currentRow, 1));
      Assert.assertEquals(pinotResultSet.getFloat(3), resultSet.getFloat(currentRow, 2));
      Assert.assertEquals(pinotResultSet.getDouble(4), resultSet.getDouble(currentRow, 3));
      Assert.assertEquals(pinotResultSet.getString(5), resultSet.getString(currentRow, 4));
      currentRow++;
    }
  }

  @Test
  public void testCursorMovement()
      throws Exception {
    ResultSetGroup resultSetGroup = getResultSet(TEST_RESULT_SET_RESOURCE);
    ResultSet resultSet = resultSetGroup.getResultSet(0);
    PinotResultSet pinotResultSet = new PinotResultSet(resultSet);

    int currentRow = 0;
    int targetRow = 7;
    while (pinotResultSet.next() && currentRow < targetRow) {
      currentRow++;
    }

    Assert.assertEquals(pinotResultSet.getInt(1), resultSet.getInt(targetRow, 0));

    pinotResultSet.first();
    Assert.assertTrue(pinotResultSet.isFirst());
    Assert.assertEquals(pinotResultSet.getInt(1), resultSet.getInt(0, 0));

    pinotResultSet.last();
    Assert.assertTrue(pinotResultSet.isLast());
    Assert.assertEquals(pinotResultSet.getInt(1), resultSet.getInt(resultSet.getRowCount() - 1, 0));

    pinotResultSet.previous();
    Assert.assertEquals(pinotResultSet.getInt(1), resultSet.getInt(resultSet.getRowCount() - 2, 0));

    pinotResultSet.first();
    pinotResultSet.previous();
    Assert.assertTrue(pinotResultSet.isBeforeFirst());

    pinotResultSet.last();
    pinotResultSet.next();
    Assert.assertTrue(pinotResultSet.isAfterLast());

    pinotResultSet.first();
    pinotResultSet.absolute(7);
    Assert.assertEquals(pinotResultSet.getInt(1), resultSet.getInt(7, 0));

    pinotResultSet.relative(-5);
    Assert.assertEquals(pinotResultSet.getInt(1), resultSet.getInt(2, 0));

    pinotResultSet.relative(1);
    Assert.assertEquals(pinotResultSet.getInt(1), resultSet.getInt(3, 0));
  }

  @Test
  public void testFetchStreams()
      throws Exception {
    ResultSetGroup resultSetGroup = getResultSet(TEST_RESULT_SET_RESOURCE);
    ResultSet resultSet = resultSetGroup.getResultSet(0);
    PinotResultSet pinotResultSet = new PinotResultSet(resultSet);

    int currentRow = 0;

    while (pinotResultSet.next()) {
      Assert.assertEquals(IOUtils.toString(pinotResultSet.getAsciiStream(5)), resultSet.getString(currentRow, 4));
      Assert.assertEquals(IOUtils.toString(pinotResultSet.getUnicodeStream(5)), resultSet.getString(currentRow, 4));
      Assert.assertEquals(IOUtils.toString(pinotResultSet.getCharacterStream(5)), resultSet.getString(currentRow, 4));
      currentRow++;
    }
  }

  @Test
  public void testFetchDates()
      throws Exception {
    ResultSetGroup resultSetGroup = getResultSet(TEST_RESULT_SET_RESOURCE);
    ResultSet resultSet = resultSetGroup.getResultSet(0);
    PinotResultSet pinotResultSet = new PinotResultSet(resultSet);

    int currentRow = 0;
    while (pinotResultSet.next()) {
      Date date = DateTimeUtils.getDateFromString(resultSet.getString(currentRow, 5), Calendar.getInstance());
      long expectedTimeMillis = date.getTime();
      Assert.assertEquals(pinotResultSet.getDate(6).getTime(), expectedTimeMillis);
      currentRow++;
    }
  }

  @Test
  public void testFetchBigDecimals()
    throws Exception {
    ResultSetGroup resultSetGroup = getResultSet(TEST_RESULT_SET_RESOURCE);
    ResultSet resultSet = resultSetGroup.getResultSet(0);
    PinotResultSet pinotResultSet = new PinotResultSet(resultSet);

    int currentRow = 0;
    while (pinotResultSet.next()) {
      Assert.assertEquals(pinotResultSet.getBigDecimal(7), new BigDecimal(resultSet.getString(currentRow, 6)));
      currentRow++;
    }
  }

  @Test
  public void testFindColumn()
      throws Exception {
    ResultSetGroup resultSetGroup = getResultSet(TEST_RESULT_SET_RESOURCE);
    ResultSet resultSet = resultSetGroup.getResultSet(0);
    PinotResultSet pinotResultSet = new PinotResultSet(resultSet);

    for (int i = 0; i < resultSet.getColumnCount(); i++) {
      Assert.assertEquals(pinotResultSet.findColumn(resultSet.getColumnName(i)), i + 1);
    }
  }

  @Test
  public void testGetResultMetadata()
      throws Exception {
    ResultSetGroup resultSetGroup = getResultSet(TEST_RESULT_SET_RESOURCE);
    ResultSet resultSet = resultSetGroup.getResultSet(0);
    PinotResultSet pinotResultSet = new PinotResultSet(resultSet);
    ResultSetMetaData pinotResultSetMetadata = pinotResultSet.getMetaData();

    for (int i = 0; i < resultSet.getColumnCount(); i++) {
      Assert.assertEquals(pinotResultSetMetadata.getColumnTypeName(i + 1), resultSet.getColumnDataType(i));
    }
  }

  @Test
  public void testGetCalculatedScale() {
    PinotResultSet pinotResultSet = new PinotResultSet();
    int calculatedResult;

    calculatedResult = pinotResultSet.getCalculatedScale("1");
    Assert.assertEquals(calculatedResult, 0);

    calculatedResult = pinotResultSet.getCalculatedScale("1.0");
    Assert.assertEquals(calculatedResult, 1);

    calculatedResult = pinotResultSet.getCalculatedScale("1.2");
    Assert.assertEquals(calculatedResult, 1);

    calculatedResult = pinotResultSet.getCalculatedScale("1.23");
    Assert.assertEquals(calculatedResult, 2);

    calculatedResult = pinotResultSet.getCalculatedScale("1.234");
    Assert.assertEquals(calculatedResult, 3);

    calculatedResult = pinotResultSet.getCalculatedScale("-1.234");
    Assert.assertEquals(calculatedResult, 3);
  }

  private ResultSetGroup getResultSet(String resourceName) {
    _dummyJsonTransport._resource = resourceName;
    Connection connection = ConnectionFactory.fromHostList(Collections.singletonList("dummy"), _dummyJsonTransport);
    return connection.execute("dummy");
  }

  class DummyJsonTransport implements PinotClientTransport {
    public String _resource;

    @Override
    public BrokerResponse executeQuery(String brokerAddress, String query)
        throws PinotClientException {
      try {
        StringBuilder builder = new StringBuilder();
        InputStream stream = getClass().getClassLoader().getResourceAsStream(_resource);
        int lastByte = stream.read();
        while (lastByte != -1) {
          builder.append((char) lastByte);
          lastByte = stream.read();
        }
        String jsonText = builder.toString();
        return BrokerResponse.fromJson(JsonUtils.stringToJsonNode(jsonText));
      } catch (Exception e) {
        Assert.fail("Unexpected exception", e);
        return null;
      }
    }

    @Override
    public CompletableFuture<BrokerResponse> executeQueryAsync(String brokerAddress, String query)
        throws PinotClientException {
      return null;
    }

    @Override
    public void close()
        throws PinotClientException {
    }
  }

  class DummyJsonTransportFactory implements PinotClientTransportFactory {
    @Override
    public PinotClientTransport buildTransport() {
      return _dummyJsonTransport;
    }
  }
}
