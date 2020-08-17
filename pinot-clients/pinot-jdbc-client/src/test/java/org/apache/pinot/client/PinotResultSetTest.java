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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.Future;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.client.utils.DateTimeUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Tests deserialization of a ResultSet given hardcoded Pinot results.
 *
 */
public class PinotResultSetTest {
  public static final String TEST_RESULT_SET_RESOURCE = "selection.json";
  public static final String DATE_FORMAT = "yyyy-mm-dd";
  private DummyJsonTransport _dummyJsonTransport = new DummyJsonTransport();
  private PinotClientTransportFactory _previousTransportFactory = null;

  @Test
  public void testFetchValues()
      throws Exception {
    ResultSetGroup resultSetGroup = getResultSet(TEST_RESULT_SET_RESOURCE);
    ResultSet resultSet = resultSetGroup.getResultSet(0);
    PinotResultSet pinotResultSet = new PinotResultSet(resultSet);

    int currentRow = 0;
    while (pinotResultSet.next()) {
      Assert.assertEquals(pinotResultSet.getInt(1), resultSet.getInt(currentRow, 0));
      Assert.assertEquals(pinotResultSet.getLong(1), resultSet.getLong(currentRow, 0));
      Assert.assertEquals(pinotResultSet.getString(1), resultSet.getString(currentRow, 0));
      Assert.assertEquals(pinotResultSet.getDouble(79), resultSet.getDouble(currentRow, 78));
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
    int targetRow = 10;
    while (pinotResultSet.next() && currentRow < targetRow) {
      currentRow++;
    }

    Assert.assertEquals(pinotResultSet.getInt(10), resultSet.getInt(targetRow, 9));

    pinotResultSet.first();
    Assert.assertTrue(pinotResultSet.isFirst());
    Assert.assertEquals(pinotResultSet.getInt(10), resultSet.getInt(0, 9));

    pinotResultSet.last();
    Assert.assertTrue(pinotResultSet.isLast());
    Assert.assertEquals(pinotResultSet.getInt(10), resultSet.getInt(resultSet.getRowCount() - 1, 9));

    pinotResultSet.previous();
    Assert.assertEquals(pinotResultSet.getInt(10), resultSet.getInt(resultSet.getRowCount() - 2, 9));

    pinotResultSet.first();
    pinotResultSet.previous();
    Assert.assertTrue(pinotResultSet.isBeforeFirst());

    pinotResultSet.last();
    pinotResultSet.next();
    Assert.assertTrue(pinotResultSet.isAfterLast());

    pinotResultSet.first();
    pinotResultSet.absolute(18);
    Assert.assertEquals(pinotResultSet.getInt(10), resultSet.getInt(18, 9));

    pinotResultSet.relative(-5);
    Assert.assertEquals(pinotResultSet.getInt(10), resultSet.getInt(13, 9));

    pinotResultSet.relative(1);
    Assert.assertEquals(pinotResultSet.getInt(10), resultSet.getInt(14, 9));
  }

  @Test
  public void testFetchStreams()
      throws Exception {
    ResultSetGroup resultSetGroup = getResultSet(TEST_RESULT_SET_RESOURCE);
    ResultSet resultSet = resultSetGroup.getResultSet(0);
    PinotResultSet pinotResultSet = new PinotResultSet(resultSet);

    int currentRow = 0;

    while (pinotResultSet.next()) {
      Assert.assertEquals(IOUtils.toString(pinotResultSet.getAsciiStream(30)), resultSet.getString(currentRow, 29));
      Assert.assertEquals(IOUtils.toString(pinotResultSet.getUnicodeStream(30)), resultSet.getString(currentRow, 29));
      Assert.assertEquals(IOUtils.toString(pinotResultSet.getCharacterStream(30)), resultSet.getString(currentRow, 29));
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
      Date date = DateTimeUtils.getDateFromString(resultSet.getString(currentRow, 51), Calendar.getInstance());
      long expectedTimeMillis = date.getTime();
      Assert.assertEquals(pinotResultSet.getDate(52).getTime(), expectedTimeMillis);
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

  private ResultSetGroup getResultSet(String resourceName) {
    _dummyJsonTransport._resource = resourceName;
    Connection connection = ConnectionFactory.fromHostList("dummy");
    return connection.execute("dummy");
  }

  @BeforeClass
  public void overridePinotClientTransport() {
    _previousTransportFactory = ConnectionFactory._transportFactory;
    ConnectionFactory._transportFactory = new DummyJsonTransportFactory();
  }

  @AfterClass
  public void resetPinotClientTransport() {
    ConnectionFactory._transportFactory = _previousTransportFactory;
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
        return BrokerResponse.fromJson(new ObjectMapper().readTree(jsonText));
      } catch (Exception e) {
        Assert.fail("Unexpected exception", e);
        return null;
      }
    }

    @Override
    public Future<BrokerResponse> executeQueryAsync(String brokerAddress, String query)
        throws PinotClientException {
      return null;
    }

    @Override
    public BrokerResponse executeQuery(String brokerAddress, Request request)
        throws PinotClientException {
      return executeQuery(brokerAddress, request.getQuery());
    }

    @Override
    public Future<BrokerResponse> executeQueryAsync(String brokerAddress, Request request)
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
