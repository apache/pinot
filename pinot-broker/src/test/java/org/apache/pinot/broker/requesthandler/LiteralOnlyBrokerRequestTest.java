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
package org.apache.pinot.broker.requesthandler;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.broker.AllowAllAccessControlFactory;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.eventlistener.query.BrokerQueryEventListenerFactory;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.broker.requesthandler.BaseSingleStageBrokerRequestHandler.isLiteralOnlyQuery;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class LiteralOnlyBrokerRequestTest {
  private static final AccessControlFactory ACCESS_CONTROL_FACTORY = new AllowAllAccessControlFactory();
  private static final Random RANDOM = new Random(System.currentTimeMillis());
  private static final long ONE_HOUR_IN_MS = TimeUnit.HOURS.toMillis(1);

  @BeforeClass
  public void setUp() {
    BrokerMetrics.register(mock(BrokerMetrics.class));
    BrokerQueryEventListenerFactory.init(new PinotConfiguration());
  }

  @Test
  public void testStringLiteralBrokerRequestFromSQL() {
    assertTrue(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery("SELECT 'a'")));
    assertTrue(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery("SELECT 'a', 'b'")));
    assertTrue(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery("SELECT 'a' FROM myTable")));
    assertTrue(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery("SELECT 'a', 'b' FROM myTable")));
  }

  @Test
  public void testSelectStarBrokerRequestFromSQL() {
    assertTrue(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery("SELECT '*'")));
    assertTrue(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery("SELECT '*' FROM myTable")));
    assertFalse(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery("SELECT *")));
    assertFalse(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery("SELECT * FROM myTable")));
  }

  @Test
  public void testNumberLiteralBrokerRequestFromSQL() {
    assertTrue(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery("SELECT 1")));
    assertTrue(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery("SELECT 1, '2', 3")));
    assertTrue(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery("SELECT 1 FROM myTable")));
    assertTrue(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery("SELECT 1, '2', 3 FROM myTable")));
  }

  @Test
  public void testLiteralOnlyTransformBrokerRequestFromSQL() {
    assertTrue(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery("SELECT now()")));
    assertTrue(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery("SELECT ago('PT1H')")));
    assertTrue(isLiteralOnlyQuery(
        CalciteSqlParser.compileToPinotQuery("SELECT now(), fromDateTime('2020-01-01 UTC', 'yyyy-MM-dd z')")));
    assertTrue(isLiteralOnlyQuery(
        CalciteSqlParser.compileToPinotQuery("SELECT ago('PT1H'), fromDateTime('2020-01-01 UTC', 'yyyy-MM-dd z')")));
    assertTrue(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery("SELECT now() FROM myTable")));
    assertTrue(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery("SELECT ago('PT1H') FROM myTable")));
    assertTrue(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "SELECT now(), fromDateTime('2020-01-01 UTC', 'yyyy-MM-dd z') FROM myTable")));
    assertTrue(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "SELECT ago('PT1H'), fromDateTime('2020-01-01 UTC', 'yyyy-MM-dd z') FROM myTable")));
    assertFalse(
        isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery("SELECT count(*) from foo where bar > ago('PT1H')")));
    assertTrue(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "SELECT encodeUrl('key1=value 1&key2=value@!$2&key3=value%3'),"
            + " decodeUrl('key1%3Dvalue+1%26key2%3Dvalue%40%21%242%26key3%3Dvalue%253') FROM myTable")));
    assertFalse(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "SELECT count(*) from foo " + "where bar = encodeUrl('key1=value 1&key2=value@!$2&key3=value%3')")));
    assertFalse(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery("SELECT count(*) from foo "
        + "where bar = decodeUrl('key1%3Dvalue+1%26key2%3Dvalue%40%21%242%26key3%3Dvalue%253')")));
    assertTrue(isLiteralOnlyQuery(
        CalciteSqlParser.compileToPinotQuery("SELECT toUtf8('hello!')," + " fromUtf8(toUtf8('hello!')) FROM myTable")));
    assertFalse(isLiteralOnlyQuery(
        CalciteSqlParser.compileToPinotQuery("SELECT reverse(fromUtf8(foo))," + " toUtf8('hello!') FROM myTable")));
    assertTrue(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "SELECT toBase64(toUtf8('hello!'))," + " fromBase64('aGVsbG8h') FROM myTable")));
    assertFalse(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "SELECT reverse(toBase64(foo))," + " toBase64(fromBase64('aGVsbG8h')) FROM myTable")));
    assertFalse(isLiteralOnlyQuery(
        CalciteSqlParser.compileToPinotQuery("SELECT fromBase64(toBase64(to_utf8(foo))) FROM myTable")));
    assertFalse(isLiteralOnlyQuery(
        CalciteSqlParser.compileToPinotQuery("SELECT count(*) from foo " + "where bar = toBase64(toASCII('hello!'))")));
    assertFalse(isLiteralOnlyQuery(
        CalciteSqlParser.compileToPinotQuery("SELECT count(*) from foo " + "where bar = fromBase64('aGVsbG8h')")));
    assertFalse(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "SELECT count(*) from foo " + "where bar = fromUtf8(fromBase64('aGVsbG8h'))")));
    assertFalse(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "SELECT count(*) from myTable where regexpReplace(col1, \"b(..)\", \"X$1Y\")  = " + "\"fooXarYXazY\"")));
    assertFalse(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "SELECT count(*) from myTable where regexpReplace(col1, \"b(..)\", \"X$1Y\", 10)  = " + "\"fooXarYXazY\"")));
    assertFalse(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "SELECT count(*) from myTable where regexpReplace(col1, \"b(..)\", \"X$1Y\", 10 , 1)  = "
            + "\"fooXarYXazY\"")));
    assertFalse(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "SELECT count(*) from myTable where regexpReplace(col1, \"b(..)\", \"X$1Y\", 10 , 1, " + "\"i\")  = "
            + "\"fooXarYXazY\"")));
    assertFalse(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "SELECT count(*) from myTable where regexpReplace(col1, \"b(..)\", \"X$1Y\", 10 , 1, " + "\"m\")  = "
            + "\"fooXarYXazY\"")));
    assertTrue(isLiteralOnlyQuery(
        CalciteSqlParser.compileToPinotQuery("select isSubnetOf('1.2.3.128/0', '192.168.5.1') from mytable")));
    assertTrue(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "select isSubnetOf('1.2.3.128/0', rtrim('192.168.5.1      ')) from mytable")));
    assertTrue(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "select isSubnetOf('123:db8:85a3::8a2e:370:7334/72', '124:db8:85a3::8a2e:370:7334') from mytable")));
    assertFalse(
        isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery("select isSubnetOf('1.2.3.128/0', foo) from mytable")));
    assertFalse(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "select count(*) from mytable where isSubnetOf('7890:db8:113::8a2e:370:7334/127', ltrim('   "
            + "7890:db8:113::8a2e:370:7336'))")));
    assertFalse(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "select count(*) from mytable where isSubnetOf('7890:db8:113::8a2e:370:7334/127', "
            + "'7890:db8:113::8a2e:370:7336')")));
    assertFalse(isLiteralOnlyQuery(
        CalciteSqlParser.compileToPinotQuery("select count(*) from mytable where isSubnetOf(foo, bar)")));
  }

  @Test
  public void testLiteralOnlyWithAsBrokerRequestFromSQL() {
    assertTrue(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "SELECT now() AS currentTs, fromDateTime('2020-01-01 UTC', 'yyyy-MM-dd z') AS firstDayOf2020")));
    assertTrue(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "SELECT ago('PT1H') AS currentTs, fromDateTime('2020-01-01 UTC', 'yyyy-MM-dd z') AS firstDayOf2020")));
    assertTrue(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "SELECT encodeUrl('key1=value 1&key2=value@!$2&key3=value%3') AS encoded, "
            + "decodeUrl('key1%3Dvalue+1%26key2%3Dvalue%40%21%242%26key3%3Dvalue%253') AS decoded")));
    assertTrue(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "SELECT toUtf8('hello!') AS encoded, " + "fromUtf8(toUtf8('hello!')) AS decoded")));
    assertTrue(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "SELECT toBase64(toUtf8('hello!')) AS encoded, " + "fromBase64('aGVsbG8h') AS decoded")));
    assertTrue(isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "select isSubnetOf('1.2.3.128/0', '192.168.5.1') AS booleanCol from mytable")));
  }

  @Test
  public void testBrokerRequestHandler()
      throws Exception {
    SingleConnectionBrokerRequestHandler requestHandler =
        new SingleConnectionBrokerRequestHandler(new PinotConfiguration(), "testBrokerId", null, ACCESS_CONTROL_FACTORY,
            null, null, null, null, mock(ServerRoutingStatsManager.class));

    long randNum = RANDOM.nextLong();
    byte[] randBytes = new byte[12];
    RANDOM.nextBytes(randBytes);
    String ranStr = BytesUtils.toHexString(randBytes);
    BrokerResponse brokerResponse = requestHandler.handleRequest(String.format("SELECT %d, '%s'", randNum, ranStr));
    assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnName(0), String.format("%d", randNum));
    assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnDataType(0), DataSchema.ColumnDataType.LONG);
    assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnName(1), String.format("'%s'", ranStr));
    assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnDataType(1),
        DataSchema.ColumnDataType.STRING);
    assertEquals(brokerResponse.getResultTable().getRows().size(), 1);
    assertEquals(brokerResponse.getResultTable().getRows().get(0).length, 2);
    assertEquals(brokerResponse.getResultTable().getRows().get(0)[0], randNum);
    assertEquals(brokerResponse.getResultTable().getRows().get(0)[1], ranStr);
    assertEquals(brokerResponse.getTotalDocs(), 0);
  }

  @Test
  public void testBrokerRequestHandlerWithAsFunction()
      throws Exception {
    SingleConnectionBrokerRequestHandler requestHandler =
        new SingleConnectionBrokerRequestHandler(new PinotConfiguration(), "testBrokerId", null, ACCESS_CONTROL_FACTORY,
            null, null, null, null, mock(ServerRoutingStatsManager.class));
    long currentTsMin = System.currentTimeMillis();
    BrokerResponse brokerResponse = requestHandler.handleRequest(
        "SELECT now() AS currentTs, fromDateTime('2020-01-01 UTC', 'yyyy-MM-dd z') AS firstDayOf2020");
    long currentTsMax = System.currentTimeMillis();
    assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnName(0), "currentTs");
    assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnDataType(0), DataSchema.ColumnDataType.LONG);
    assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnName(1), "firstDayOf2020");
    assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnDataType(1), DataSchema.ColumnDataType.LONG);
    assertEquals(brokerResponse.getResultTable().getRows().size(), 1);
    assertEquals(brokerResponse.getResultTable().getRows().get(0).length, 2);
    assertTrue(Long.parseLong(brokerResponse.getResultTable().getRows().get(0)[0].toString()) > currentTsMin);
    assertTrue(Long.parseLong(brokerResponse.getResultTable().getRows().get(0)[0].toString()) < currentTsMax);
    assertEquals(brokerResponse.getResultTable().getRows().get(0)[1], 1577836800000L);
    assertEquals(brokerResponse.getTotalDocs(), 0);

    long oneHourAgoTsMin = System.currentTimeMillis() - ONE_HOUR_IN_MS;
    brokerResponse = requestHandler.handleRequest(
        "SELECT ago('PT1H') AS oneHourAgoTs, fromDateTime('2020-01-01 UTC', 'yyyy-MM-dd z') AS firstDayOf2020");
    long oneHourAgoTsMax = System.currentTimeMillis() - ONE_HOUR_IN_MS;
    assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnName(0), "oneHourAgoTs");
    assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnDataType(0), DataSchema.ColumnDataType.LONG);
    assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnName(1), "firstDayOf2020");
    assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnDataType(1), DataSchema.ColumnDataType.LONG);
    assertEquals(brokerResponse.getResultTable().getRows().size(), 1);
    assertEquals(brokerResponse.getResultTable().getRows().get(0).length, 2);
    assertTrue(Long.parseLong(brokerResponse.getResultTable().getRows().get(0)[0].toString()) >= oneHourAgoTsMin);
    assertTrue(Long.parseLong(brokerResponse.getResultTable().getRows().get(0)[0].toString()) <= oneHourAgoTsMax);
    assertEquals(brokerResponse.getResultTable().getRows().get(0)[1], 1577836800000L);
    assertEquals(brokerResponse.getTotalDocs(), 0);

    brokerResponse = requestHandler.handleRequest(
        "SELECT encodeUrl('key1=value 1&key2=value@!$2&key3=value%3') AS encoded, "
            + "decodeUrl('key1%3Dvalue+1%26key2%3Dvalue%40%21%242%26key3%3Dvalue%253') AS decoded");
    assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnName(0), "encoded");
    assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnDataType(0),
        DataSchema.ColumnDataType.STRING);
    assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnName(1), "decoded");
    assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnDataType(1),
        DataSchema.ColumnDataType.STRING);
    assertEquals(brokerResponse.getResultTable().getRows().size(), 1);
    assertEquals(brokerResponse.getResultTable().getRows().get(0).length, 2);
    assertEquals(brokerResponse.getResultTable().getRows().get(0)[0].toString(),
        "key1%3Dvalue+1%26key2%3Dvalue%40%21%242%26key3%3Dvalue%253");
    assertEquals(brokerResponse.getResultTable().getRows().get(0)[1].toString(),
        "key1=value 1&key2=value@!$2&key3=value%3");
    assertEquals(brokerResponse.getTotalDocs(), 0);

    brokerResponse = requestHandler.handleRequest(
        "SELECT toBase64(toUtf8('hello!')) AS encoded, fromUtf8(fromBase64('aGVsbG8h')) AS decoded");
    ResultTable resultTable = brokerResponse.getResultTable();
    DataSchema dataSchema = resultTable.getDataSchema();
    List<Object[]> rows = resultTable.getRows();
    assertEquals(dataSchema.getColumnName(0), "encoded");
    assertEquals(dataSchema.getColumnDataType(0), DataSchema.ColumnDataType.STRING);
    assertEquals(dataSchema.getColumnName(1), "decoded");
    assertEquals(dataSchema.getColumnDataType(1), DataSchema.ColumnDataType.STRING);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0).length, 2);
    assertEquals(rows.get(0)[0].toString(), "aGVsbG8h");
    assertEquals(rows.get(0)[1].toString(), "hello!");
    assertEquals(brokerResponse.getTotalDocs(), 0);

    brokerResponse = requestHandler.handleRequest("SELECT fromUtf8(fromBase64(toBase64(toUtf8('nested')))) AS output");
    resultTable = brokerResponse.getResultTable();
    dataSchema = resultTable.getDataSchema();
    rows = resultTable.getRows();
    assertEquals(dataSchema.getColumnName(0), "output");
    assertEquals(dataSchema.getColumnDataType(0), DataSchema.ColumnDataType.STRING);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0).length, 1);
    assertEquals(rows.get(0)[0].toString(), "nested");
    assertEquals(brokerResponse.getTotalDocs(), 0);

    brokerResponse = requestHandler.handleRequest(
        "SELECT toBase64(toUtf8('this is a long string that will encode to more than 76 characters using base64')) "
            + "AS encoded");
    resultTable = brokerResponse.getResultTable();
    dataSchema = resultTable.getDataSchema();
    rows = resultTable.getRows();
    assertEquals(dataSchema.getColumnName(0), "encoded");
    assertEquals(dataSchema.getColumnDataType(0), DataSchema.ColumnDataType.STRING);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0).length, 1);
    assertEquals(rows.get(0)[0].toString(),
        "dGhpcyBpcyBhIGxvbmcgc3RyaW5nIHRoYXQgd2lsbCBlbmNvZGUgdG8gbW9yZSB0aGFuIDc2IGNoYXJhY3RlcnMgdXNpbmcgYmFzZTY0");
    assertEquals(brokerResponse.getTotalDocs(), 0);

    brokerResponse = requestHandler.handleRequest("SELECT fromUtf8(fromBase64("
        + "'dGhpcyBpcyBhIGxvbmcgc3RyaW5nIHRoYXQgd2lsbCBlbmNvZGUgdG8gbW9yZSB0aGFuIDc2IGNoYXJhY3RlcnMgdXNpbmcgYmFzZTY0'"
        + ")) AS decoded");
    resultTable = brokerResponse.getResultTable();
    dataSchema = resultTable.getDataSchema();
    rows = resultTable.getRows();
    assertEquals(dataSchema.getColumnName(0), "decoded");
    assertEquals(dataSchema.getColumnDataType(0), DataSchema.ColumnDataType.STRING);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0).length, 1);
    assertEquals(rows.get(0)[0].toString(),
        "this is a long string that will encode to more than 76 characters using base64");
    assertEquals(brokerResponse.getTotalDocs(), 0);

    brokerResponse = requestHandler.handleRequest("SELECT fromBase64(0) AS decoded");
    assertEquals(brokerResponse.getExceptions().get(0).getErrorCode(), QueryErrorCode.SQL_PARSING.getId());

    brokerResponse = requestHandler.handleRequest(
        "SELECT isSubnetOf('2001:db8:85a3::8a2e:370:7334/62', '2001:0db8:85a3:0003:ffff:ffff:ffff:ffff') "
            + "AS booleanCol");
    resultTable = brokerResponse.getResultTable();
    dataSchema = resultTable.getDataSchema();
    rows = resultTable.getRows();
    assertEquals(dataSchema.getColumnName(0), "booleanCol");
    assertEquals(dataSchema.getColumnDataType(0), DataSchema.ColumnDataType.BOOLEAN);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0).length, 1);
    assertTrue((boolean) rows.get(0)[0]);
    assertEquals(brokerResponse.getTotalDocs(), 0);

    // first argument must be in prefix format
    brokerResponse = requestHandler.handleRequest(
        "SELECT isSubnetOf('2001:db8:85a3::8a2e:370:7334', '2001:0db8:85a3:0003:ffff:ffff:ffff:ffff') AS booleanCol");
    assertEquals(brokerResponse.getExceptions().get(0).getErrorCode(), QueryErrorCode.SQL_PARSING.getId());

    // first argument must be in prefix format
    brokerResponse =
        requestHandler.handleRequest("SELECT isSubnetOf('105.25.245.115', '105.25.245.115') AS booleanCol");
    assertEquals(brokerResponse.getExceptions().get(0).getErrorCode(), QueryErrorCode.SQL_PARSING.getId());

    // second argument should not be a prefix
    brokerResponse = requestHandler.handleRequest("SELECT isSubnetOf('1.2.3.128/26', '3.175.47.239/26') AS booleanCol");
    assertEquals(brokerResponse.getExceptions().get(0).getErrorCode(), QueryErrorCode.SQL_PARSING.getId());

    // second argument should not be a prefix
    brokerResponse = requestHandler.handleRequest("SELECT isSubnetOf('5f3f:bfdb:1bbe:a824:6bf9:0fbb:d358:1889/64', "
        + "'4275:386f:b2b5:0664:04aa:d7bd:0589:6909/64') AS booleanCol");
    assertEquals(brokerResponse.getExceptions().get(0).getErrorCode(), QueryErrorCode.SQL_PARSING.getId());

    // invalid prefix length
    brokerResponse = requestHandler.handleRequest(
        "SELECT isSubnetOf('2001:4801:7825:103:be76:4eff::/129', '2001:4801:7825:103:be76:4eff::') AS booleanCol");
    assertEquals(brokerResponse.getExceptions().get(0).getErrorCode(), QueryErrorCode.SQL_PARSING.getId());

    // invalid prefix length
    brokerResponse =
        requestHandler.handleRequest("SELECT isSubnetOf('170.189.0.175/33', '170.189.0.175') AS booleanCol");
    assertEquals(brokerResponse.getExceptions().get(0).getErrorCode(), QueryErrorCode.SQL_PARSING.getId());
  }

  /** Tests for EXPLAIN PLAN for literal only queries. */
  @Test
  public void testExplainPlanLiteralOnly()
      throws Exception {
    SingleConnectionBrokerRequestHandler requestHandler =
        new SingleConnectionBrokerRequestHandler(new PinotConfiguration(), "testBrokerId", null, ACCESS_CONTROL_FACTORY,
            null, null, null, null, mock(ServerRoutingStatsManager.class));

    // Test 1: select constant
    BrokerResponse brokerResponse = requestHandler.handleRequest("EXPLAIN PLAN FOR SELECT 1.5, 'test'");

    checkExplainResultSchema(brokerResponse.getResultTable().getDataSchema(),
        new String[]{"Operator", "Operator_Id", "Parent_Id"}, new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT
        });

    assertEquals(brokerResponse.getResultTable().getRows().size(), 1);
    assertEquals(brokerResponse.getResultTable().getRows().get(0), new Object[]{"BROKER_EVALUATE", 0, -1});
    assertEquals(brokerResponse.getTotalDocs(), 0);

    // Test 2: invoke compile time function -> literal only
    requestHandler.handleRequest(
        "EXPLAIN PLAN FOR SELECT 6+8 AS addition, fromDateTime('2020-01-01 UTC', 'yyyy-MM-dd z') AS firstDayOf2020");

    checkExplainResultSchema(brokerResponse.getResultTable().getDataSchema(),
        new String[]{"Operator", "Operator_Id", "Parent_Id"}, new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT
        });

    assertEquals(brokerResponse.getResultTable().getRows().size(), 1);
    assertEquals(brokerResponse.getResultTable().getRows().get(0), new Object[]{"BROKER_EVALUATE", 0, -1});

    assertEquals(brokerResponse.getTotalDocs(), 0);
  }

  private void checkExplainResultSchema(DataSchema schema, String[] columnNames,
      DataSchema.ColumnDataType[] columnTypes) {
    for (int i = 0; i < columnNames.length; i++) {
      assertEquals(schema.getColumnName(i), columnNames[i]);
      assertEquals(schema.getColumnDataType(i), columnTypes[i]);
    }
  }
}
