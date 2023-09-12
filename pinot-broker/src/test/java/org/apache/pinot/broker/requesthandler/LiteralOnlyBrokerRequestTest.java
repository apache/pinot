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

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Collections;
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
import org.apache.pinot.spi.eventlistener.query.PinotBrokerQueryEventListenerUtils;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;


public class LiteralOnlyBrokerRequestTest {
  private static final AccessControlFactory ACCESS_CONTROL_FACTORY = new AllowAllAccessControlFactory();
  private static final Random RANDOM = new Random(System.currentTimeMillis());
  private static final long ONE_HOUR_IN_MS = TimeUnit.HOURS.toMillis(1);

  @Test
  public void testStringLiteralBrokerRequestFromSQL() {
    Assert.assertTrue(BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery("SELECT 'a'")));
    Assert.assertTrue(
        BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery("SELECT 'a', 'b'")));
    Assert.assertTrue(
        BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery("SELECT 'a' FROM myTable")));
    Assert.assertTrue(BaseBrokerRequestHandler
        .isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery("SELECT 'a', 'b' FROM myTable")));
  }

  @Test
  public void testSelectStarBrokerRequestFromSQL() {
    Assert.assertTrue(BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery("SELECT '*'")));
    Assert.assertTrue(
        BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery("SELECT '*' FROM myTable")));
    Assert.assertFalse(BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery("SELECT *")));
    Assert.assertFalse(
        BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery("SELECT * FROM myTable")));
  }

  @Test
  public void testNumberLiteralBrokerRequestFromSQL() {
    Assert.assertTrue(BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery("SELECT 1")));
    Assert.assertTrue(
        BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery("SELECT 1, '2', 3")));
    Assert.assertTrue(
        BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery("SELECT 1 FROM myTable")));
    Assert.assertTrue(BaseBrokerRequestHandler
        .isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery("SELECT 1, '2', 3 FROM myTable")));
  }

  @Test
  public void testLiteralOnlyTransformBrokerRequestFromSQL() {
    Assert
        .assertTrue(BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery("SELECT now()")));
    Assert.assertTrue(
        BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery("SELECT ago('PT1H')")));
    Assert.assertTrue(BaseBrokerRequestHandler.isLiteralOnlyQuery(
        CalciteSqlParser.compileToPinotQuery("SELECT now(), fromDateTime('2020-01-01 UTC', 'yyyy-MM-dd z')")));
    Assert.assertTrue(BaseBrokerRequestHandler.isLiteralOnlyQuery(
        CalciteSqlParser.compileToPinotQuery("SELECT ago('PT1H'), fromDateTime('2020-01-01 UTC', 'yyyy-MM-dd z')")));
    Assert.assertTrue(
        BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery("SELECT now() FROM myTable")));
    Assert.assertTrue(BaseBrokerRequestHandler
        .isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery("SELECT ago('PT1H') FROM myTable")));
    Assert.assertTrue(BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser
        .compileToPinotQuery("SELECT now(), fromDateTime('2020-01-01 UTC', 'yyyy-MM-dd z') FROM myTable")));
    Assert.assertTrue(BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser
        .compileToPinotQuery("SELECT ago('PT1H'), fromDateTime('2020-01-01 UTC', 'yyyy-MM-dd z') FROM myTable")));
    Assert.assertFalse(BaseBrokerRequestHandler
        .isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery("SELECT count(*) from foo where bar > ago('PT1H')")));
    Assert.assertTrue(BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser
        .compileToPinotQuery("SELECT encodeUrl('key1=value 1&key2=value@!$2&key3=value%3'),"
            + " decodeUrl('key1%3Dvalue+1%26key2%3Dvalue%40%21%242%26key3%3Dvalue%253') FROM myTable")));
    Assert.assertFalse(BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser
        .compileToPinotQuery("SELECT count(*) from foo "
            + "where bar = encodeUrl('key1=value 1&key2=value@!$2&key3=value%3')")));
    Assert.assertFalse(BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser
        .compileToPinotQuery("SELECT count(*) from foo "
            + "where bar = decodeUrl('key1%3Dvalue+1%26key2%3Dvalue%40%21%242%26key3%3Dvalue%253')")));
    Assert.assertTrue(BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "SELECT toUtf8('hello!')," + " fromUtf8(toUtf8('hello!')) FROM myTable")));
    Assert.assertFalse(BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "SELECT reverse(fromUtf8(foo))," + " toUtf8('hello!') FROM myTable")));
    Assert.assertTrue(BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "SELECT toBase64(toUtf8('hello!'))," + " fromBase64('aGVsbG8h') FROM myTable")));
    Assert.assertFalse(BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "SELECT reverse(toBase64(foo))," + " toBase64(fromBase64('aGVsbG8h')) FROM myTable")));
    Assert.assertFalse(BaseBrokerRequestHandler.isLiteralOnlyQuery(
        CalciteSqlParser.compileToPinotQuery("SELECT fromBase64(toBase64(to_utf8(foo))) FROM myTable")));
    Assert.assertFalse(BaseBrokerRequestHandler.isLiteralOnlyQuery(
        CalciteSqlParser.compileToPinotQuery("SELECT count(*) from foo " + "where bar = toBase64(toASCII('hello!'))")));
    Assert.assertFalse(BaseBrokerRequestHandler.isLiteralOnlyQuery(
        CalciteSqlParser.compileToPinotQuery("SELECT count(*) from foo " + "where bar = fromBase64('aGVsbG8h')")));
    Assert.assertFalse(BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "SELECT count(*) from foo " + "where bar = fromUtf8(fromBase64('aGVsbG8h'))")));
    Assert.assertFalse(BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser
        .compileToPinotQuery("SELECT count(*) from myTable where regexpReplace(col1, \"b(..)\", \"X$1Y\")  = "
            + "\"fooXarYXazY\"")));
    Assert.assertFalse(BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser
        .compileToPinotQuery("SELECT count(*) from myTable where regexpReplace(col1, \"b(..)\", \"X$1Y\", 10)  = "
            + "\"fooXarYXazY\"")));
    Assert.assertFalse(BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser
        .compileToPinotQuery("SELECT count(*) from myTable where regexpReplace(col1, \"b(..)\", \"X$1Y\", 10 , 1)  = "
            + "\"fooXarYXazY\"")));
    Assert.assertFalse(BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser
        .compileToPinotQuery("SELECT count(*) from myTable where regexpReplace(col1, \"b(..)\", \"X$1Y\", 10 , 1, "
            + "\"i\")  = "
            + "\"fooXarYXazY\"")));
    Assert.assertFalse(BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser
        .compileToPinotQuery("SELECT count(*) from myTable where regexpReplace(col1, \"b(..)\", \"X$1Y\", 10 , 1, "
            + "\"m\")  = "
            + "\"fooXarYXazY\"")));
    Assert.assertTrue(BaseBrokerRequestHandler.isLiteralOnlyQuery(
        CalciteSqlParser.compileToPinotQuery("select isSubnetOf('1.2.3.128/0', '192.168.5.1') from mytable")));
    Assert.assertTrue(BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "select isSubnetOf('1.2.3.128/0', rtrim('192.168.5.1      ')) from mytable")));
    Assert.assertTrue(BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "select isSubnetOf('123:db8:85a3::8a2e:370:7334/72', '124:db8:85a3::8a2e:370:7334') from mytable")));
    Assert.assertFalse(BaseBrokerRequestHandler.isLiteralOnlyQuery(
        CalciteSqlParser.compileToPinotQuery("select isSubnetOf('1.2.3.128/0', foo) from mytable")));
    Assert.assertFalse(BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "select count(*) from mytable where isSubnetOf('7890:db8:113::8a2e:370:7334/127', ltrim('   "
            + "7890:db8:113::8a2e:370:7336'))")));
    Assert.assertFalse(BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "select count(*) from mytable where isSubnetOf('7890:db8:113::8a2e:370:7334/127', "
            + "'7890:db8:113::8a2e:370:7336')")));
    Assert.assertFalse(BaseBrokerRequestHandler.isLiteralOnlyQuery(
        CalciteSqlParser.compileToPinotQuery("select count(*) from mytable where isSubnetOf(foo, bar)")));
  }

  @Test
  public void testLiteralOnlyWithAsBrokerRequestFromSQL() {
    Assert.assertTrue(BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "SELECT now() AS currentTs, fromDateTime('2020-01-01 UTC', 'yyyy-MM-dd z') AS firstDayOf2020")));
    Assert.assertTrue(BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "SELECT ago('PT1H') AS currentTs, fromDateTime('2020-01-01 UTC', 'yyyy-MM-dd z') AS firstDayOf2020")));
    Assert.assertTrue(BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "SELECT encodeUrl('key1=value 1&key2=value@!$2&key3=value%3') AS encoded, "
            + "decodeUrl('key1%3Dvalue+1%26key2%3Dvalue%40%21%242%26key3%3Dvalue%253') AS decoded")));
    Assert.assertTrue(BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "SELECT toUtf8('hello!') AS encoded, " + "fromUtf8(toUtf8('hello!')) AS decoded")));
    Assert.assertTrue(BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "SELECT toBase64(toUtf8('hello!')) AS encoded, " + "fromBase64('aGVsbG8h') AS decoded")));
    Assert.assertTrue(BaseBrokerRequestHandler.isLiteralOnlyQuery(CalciteSqlParser.compileToPinotQuery(
        "select isSubnetOf('1.2.3.128/0', '192.168.5.1') AS booleanCol from mytable")));
  }

  @Test
  public void testBrokerRequestHandler()
      throws Exception {
    SingleConnectionBrokerRequestHandler requestHandler =
        new SingleConnectionBrokerRequestHandler(new PinotConfiguration(), "testBrokerId", null, ACCESS_CONTROL_FACTORY,
            null, null, new BrokerMetrics("", PinotMetricUtils.getPinotMetricsRegistry(), true, Collections.emptySet()),
            null, null, mock(ServerRoutingStatsManager.class),
                PinotBrokerQueryEventListenerUtils.getBrokerQueryEventListener());

    long randNum = RANDOM.nextLong();
    byte[] randBytes = new byte[12];
    RANDOM.nextBytes(randBytes);
    String ranStr = BytesUtils.toHexString(randBytes);
    JsonNode request = JsonUtils.stringToJsonNode(String.format("{\"sql\":\"SELECT %d, '%s'\"}", randNum, ranStr));
    RequestContext requestStats = Tracing.getTracer().createRequestScope();
    BrokerResponse brokerResponse = requestHandler.handleRequest(request, null, requestStats, null);
    Assert.assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnName(0), String.format("%d", randNum));
    Assert.assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnDataType(0),
        DataSchema.ColumnDataType.LONG);
    Assert.assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnName(1), ranStr);
    Assert.assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnDataType(1),
        DataSchema.ColumnDataType.STRING);
    Assert.assertEquals(brokerResponse.getResultTable().getRows().size(), 1);
    Assert.assertEquals(brokerResponse.getResultTable().getRows().get(0).length, 2);
    Assert.assertEquals(brokerResponse.getResultTable().getRows().get(0)[0], randNum);
    Assert.assertEquals(brokerResponse.getResultTable().getRows().get(0)[1], ranStr);
    Assert.assertEquals(brokerResponse.getTotalDocs(), 0);
  }

  @Test
  public void testBrokerRequestHandlerWithAsFunction()
      throws Exception {
    SingleConnectionBrokerRequestHandler requestHandler =
        new SingleConnectionBrokerRequestHandler(new PinotConfiguration(), "testBrokerId", null, ACCESS_CONTROL_FACTORY,
            null, null, new BrokerMetrics("", PinotMetricUtils.getPinotMetricsRegistry(), true, Collections.emptySet()),
            null, null, mock(ServerRoutingStatsManager.class),
                PinotBrokerQueryEventListenerUtils.getBrokerQueryEventListener());
    long currentTsMin = System.currentTimeMillis();
    JsonNode request = JsonUtils.stringToJsonNode(
        "{\"sql\":\"SELECT now() as currentTs, fromDateTime('2020-01-01 UTC', 'yyyy-MM-dd z') as firstDayOf2020\"}");
    RequestContext requestStats = Tracing.getTracer().createRequestScope();
    BrokerResponse brokerResponse = requestHandler.handleRequest(request, null, requestStats, null);
    long currentTsMax = System.currentTimeMillis();
    Assert.assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnName(0), "currentTs");
    Assert.assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnDataType(0),
        DataSchema.ColumnDataType.LONG);
    Assert.assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnName(1), "firstDayOf2020");
    Assert.assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnDataType(1),
        DataSchema.ColumnDataType.LONG);
    Assert.assertEquals(brokerResponse.getResultTable().getRows().size(), 1);
    Assert.assertEquals(brokerResponse.getResultTable().getRows().get(0).length, 2);
    Assert.assertTrue(Long.parseLong(brokerResponse.getResultTable().getRows().get(0)[0].toString()) > currentTsMin);
    Assert.assertTrue(Long.parseLong(brokerResponse.getResultTable().getRows().get(0)[0].toString()) < currentTsMax);
    Assert.assertEquals(brokerResponse.getResultTable().getRows().get(0)[1], 1577836800000L);
    Assert.assertEquals(brokerResponse.getTotalDocs(), 0);

    long oneHourAgoTsMin = System.currentTimeMillis() - ONE_HOUR_IN_MS;
    request = JsonUtils.stringToJsonNode(
        "{\"sql\":\"SELECT ago('PT1H') as oneHourAgoTs, fromDateTime('2020-01-01 UTC', 'yyyy-MM-dd z') as "
            + "firstDayOf2020\"}");
    requestStats = Tracing.getTracer().createRequestScope();
    brokerResponse = requestHandler.handleRequest(request, null, requestStats, null);
    long oneHourAgoTsMax = System.currentTimeMillis() - ONE_HOUR_IN_MS;
    Assert.assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnName(0), "oneHourAgoTs");
    Assert.assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnDataType(0),
        DataSchema.ColumnDataType.LONG);
    Assert.assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnName(1), "firstDayOf2020");
    Assert.assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnDataType(1),
        DataSchema.ColumnDataType.LONG);
    Assert.assertEquals(brokerResponse.getResultTable().getRows().size(), 1);
    Assert.assertEquals(brokerResponse.getResultTable().getRows().get(0).length, 2);
    Assert
        .assertTrue(Long.parseLong(brokerResponse.getResultTable().getRows().get(0)[0].toString()) >= oneHourAgoTsMin);
    Assert
        .assertTrue(Long.parseLong(brokerResponse.getResultTable().getRows().get(0)[0].toString()) <= oneHourAgoTsMax);
    Assert.assertEquals(brokerResponse.getResultTable().getRows().get(0)[1], 1577836800000L);
    Assert.assertEquals(brokerResponse.getTotalDocs(), 0);

    request = JsonUtils.stringToJsonNode(
        "{\"sql\":\"SELECT encodeUrl('key1=value 1&key2=value@!$2&key3=value%3') AS encoded, "
            + "decodeUrl('key1%3Dvalue+1%26key2%3Dvalue%40%21%242%26key3%3Dvalue%253') AS decoded\"}");
    requestStats = Tracing.getTracer().createRequestScope();
    brokerResponse = requestHandler.handleRequest(request, null, requestStats, null);
    System.out.println(brokerResponse.getResultTable());
    Assert.assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnName(0), "encoded");
    Assert.assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnDataType(0),
        DataSchema.ColumnDataType.STRING);
    Assert.assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnName(1), "decoded");
    Assert.assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnDataType(1),
        DataSchema.ColumnDataType.STRING);
    Assert.assertEquals(brokerResponse.getResultTable().getRows().size(), 1);
    Assert.assertEquals(brokerResponse.getResultTable().getRows().get(0).length, 2);
    Assert.assertEquals(brokerResponse.getResultTable().getRows().get(0)[0].toString(),
        "key1%3Dvalue+1%26key2%3Dvalue%40%21%242%26key3%3Dvalue%253");
    Assert.assertEquals(brokerResponse.getResultTable().getRows().get(0)[1].toString(),
        "key1=value 1&key2=value@!$2&key3=value%3");
    Assert.assertEquals(brokerResponse.getTotalDocs(), 0);

    request = JsonUtils.stringToJsonNode(
        "{\"sql\":\"SELECT toBase64(toUtf8('hello!')) AS encoded, " + "fromUtf8(fromBase64('aGVsbG8h')) AS decoded\"}");
    requestStats = Tracing.getTracer().createRequestScope();
    brokerResponse = requestHandler.handleRequest(request, null, requestStats, null);
    ResultTable resultTable = brokerResponse.getResultTable();
    DataSchema dataSchema = resultTable.getDataSchema();
    List<Object[]> rows = resultTable.getRows();
    Assert.assertEquals(dataSchema.getColumnName(0), "encoded");
    Assert.assertEquals(dataSchema.getColumnDataType(0), DataSchema.ColumnDataType.STRING);
    Assert.assertEquals(dataSchema.getColumnName(1), "decoded");
    Assert.assertEquals(dataSchema.getColumnDataType(1), DataSchema.ColumnDataType.STRING);
    Assert.assertEquals(rows.size(), 1);
    Assert.assertEquals(rows.get(0).length, 2);
    Assert.assertEquals(rows.get(0)[0].toString(), "aGVsbG8h");
    Assert.assertEquals(rows.get(0)[1].toString(), "hello!");
    Assert.assertEquals(brokerResponse.getTotalDocs(), 0);

    request = JsonUtils.stringToJsonNode(
        "{\"sql\":\"SELECT fromUtf8(fromBase64(toBase64(toUtf8('nested')))) AS output\"}");
    requestStats = Tracing.getTracer().createRequestScope();
    brokerResponse = requestHandler.handleRequest(request, null, requestStats, null);
    resultTable = brokerResponse.getResultTable();
    dataSchema = resultTable.getDataSchema();
    rows = resultTable.getRows();
    Assert.assertEquals(dataSchema.getColumnName(0), "output");
    Assert.assertEquals(dataSchema.getColumnDataType(0), DataSchema.ColumnDataType.STRING);
    Assert.assertEquals(rows.size(), 1);
    Assert.assertEquals(rows.get(0).length, 1);
    Assert.assertEquals(rows.get(0)[0].toString(), "nested");
    Assert.assertEquals(brokerResponse.getTotalDocs(), 0);

    request = JsonUtils.stringToJsonNode(
        "{\"sql\":\"SELECT toBase64(toUtf8('this is a long string that will encode to more than 76 characters using "
            + "base64'))"
            + " AS encoded\"}");
    requestStats = Tracing.getTracer().createRequestScope();
    brokerResponse = requestHandler.handleRequest(request, null, requestStats, null);
    resultTable = brokerResponse.getResultTable();
    dataSchema = resultTable.getDataSchema();
    rows = resultTable.getRows();
    Assert.assertEquals(dataSchema.getColumnName(0), "encoded");
    Assert.assertEquals(dataSchema.getColumnDataType(0), DataSchema.ColumnDataType.STRING);
    Assert.assertEquals(rows.size(), 1);
    Assert.assertEquals(rows.get(0).length, 1);
    Assert.assertEquals(rows.get(0)[0].toString(),
        "dGhpcyBpcyBhIGxvbmcgc3RyaW5nIHRoYXQgd2lsbCBlbmNvZGUgdG8gbW9yZSB0aGFuIDc2IGNoYXJhY3RlcnMgdXNpbmcgYmFzZTY0");
    Assert.assertEquals(brokerResponse.getTotalDocs(), 0);

    request = JsonUtils.stringToJsonNode("{\"sql\":\"SELECT fromUtf8(fromBase64"
        + "('dGhpcyBpcyBhIGxvbmcgc3RyaW5nIHRoYXQgd2lsbCBlbmNvZGUgdG8gbW9yZSB0aGFuIDc2IGNoYXJhY3RlcnMgdXNpbmcgYmFzZTY0"
        + "')) AS decoded\"}");
    requestStats = Tracing.getTracer().createRequestScope();
    brokerResponse = requestHandler.handleRequest(request, null, requestStats, null);
    resultTable = brokerResponse.getResultTable();
    dataSchema = resultTable.getDataSchema();
    rows = resultTable.getRows();
    Assert.assertEquals(dataSchema.getColumnName(0), "decoded");
    Assert.assertEquals(dataSchema.getColumnDataType(0), DataSchema.ColumnDataType.STRING);
    Assert.assertEquals(rows.size(), 1);
    Assert.assertEquals(rows.get(0).length, 1);
    Assert.assertEquals(rows.get(0)[0].toString(),
        "this is a long string that will encode to more than 76 characters using base64");
    Assert.assertEquals(brokerResponse.getTotalDocs(), 0);

    request = JsonUtils.stringToJsonNode("{\"sql\":\"SELECT fromBase64" + "(0) AS decoded\"}");
    requestStats = Tracing.getTracer().createRequestScope();
    brokerResponse = requestHandler.handleRequest(request, null, requestStats, null);
    Assert.assertTrue(
        brokerResponse.getProcessingExceptions().get(0).getMessage().contains("IllegalArgumentException"));

    request = JsonUtils.stringToJsonNode(
        "{\"sql\":\"SELECT isSubnetOf('2001:db8:85a3::8a2e:370:7334/62', '2001:0db8:85a3:0003:ffff:ffff:ffff:ffff')"
            + " as booleanCol\"}");
    requestStats = Tracing.getTracer().createRequestScope();
    brokerResponse = requestHandler.handleRequest(request, null, requestStats, null);
    resultTable = brokerResponse.getResultTable();
    dataSchema = resultTable.getDataSchema();
    rows = resultTable.getRows();
    Assert.assertEquals(dataSchema.getColumnName(0), "booleanCol");
    Assert.assertEquals(dataSchema.getColumnDataType(0), DataSchema.ColumnDataType.BOOLEAN);
    Assert.assertEquals(rows.size(), 1);
    Assert.assertEquals(rows.get(0).length, 1);
    Assert.assertTrue((boolean) rows.get(0)[0]);
    Assert.assertEquals(brokerResponse.getTotalDocs(), 0);

    // first argument must be in prefix format
    request = JsonUtils.stringToJsonNode(
        "{\"sql\":\"SELECT isSubnetOf('2001:db8:85a3::8a2e:370:7334', '2001:0db8:85a3:0003:ffff:ffff:ffff:ffff') as"
            + " booleanCol\"}");
    requestStats = Tracing.getTracer().createRequestScope();
    brokerResponse = requestHandler.handleRequest(request, null, requestStats, null);
    Assert.assertTrue(
        brokerResponse.getProcessingExceptions().get(0).getMessage().contains("IllegalArgumentException"));

    // first argument must be in prefix format
    request = JsonUtils.stringToJsonNode(
        "{\"sql\":\"SELECT isSubnetOf('105.25.245.115', '105.25.245.115') as" + " booleanCol\"}");
    requestStats = Tracing.getTracer().createRequestScope();
    brokerResponse = requestHandler.handleRequest(request, null, requestStats, null);
    Assert.assertTrue(
        brokerResponse.getProcessingExceptions().get(0).getMessage().contains("IllegalArgumentException"));

    // second argument should not be a prefix
    request = JsonUtils.stringToJsonNode(
        "{\"sql\":\"SELECT isSubnetOf('1.2.3.128/26', '3.175.47.239/26') as" + " booleanCol\"}");
    requestStats = Tracing.getTracer().createRequestScope();
    brokerResponse = requestHandler.handleRequest(request, null, requestStats, null);
    Assert.assertTrue(
        brokerResponse.getProcessingExceptions().get(0).getMessage().contains("IllegalArgumentException"));

    // second argument should not be a prefix
    request = JsonUtils.stringToJsonNode(
        "{\"sql\":\"SELECT isSubnetOf('5f3f:bfdb:1bbe:a824:6bf9:0fbb:d358:1889/64', "
            + "'4275:386f:b2b5:0664:04aa:d7bd:0589:6909/64') as"
            + " booleanCol\"}");
    requestStats = Tracing.getTracer().createRequestScope();
    brokerResponse = requestHandler.handleRequest(request, null, requestStats, null);
    Assert.assertTrue(
        brokerResponse.getProcessingExceptions().get(0).getMessage().contains("IllegalArgumentException"));

    // invalid prefix length
    request = JsonUtils.stringToJsonNode(
        "{\"sql\":\"SELECT isSubnetOf('2001:4801:7825:103:be76:4eff::/129', '2001:4801:7825:103:be76:4eff::') as"
            + " booleanCol\"}");
    requestStats = Tracing.getTracer().createRequestScope();
    brokerResponse = requestHandler.handleRequest(request, null, requestStats, null);
    Assert.assertTrue(
        brokerResponse.getProcessingExceptions().get(0).getMessage().contains("IllegalArgumentException"));

    // invalid prefix length
    request = JsonUtils.stringToJsonNode(
        "{\"sql\":\"SELECT isSubnetOf('170.189.0.175/33', '170.189.0.175') as" + " booleanCol\"}");
    requestStats = Tracing.getTracer().createRequestScope();
    brokerResponse = requestHandler.handleRequest(request, null, requestStats, null);
    Assert.assertTrue(
        brokerResponse.getProcessingExceptions().get(0).getMessage().contains("IllegalArgumentException"));
  }

  /** Tests for EXPLAIN PLAN for literal only queries. */
  @Test
  public void testExplainPlanLiteralOnly()
      throws Exception {
    SingleConnectionBrokerRequestHandler requestHandler =
        new SingleConnectionBrokerRequestHandler(new PinotConfiguration(), "testBrokerId", null, ACCESS_CONTROL_FACTORY,
            null, null, new BrokerMetrics("", PinotMetricUtils.getPinotMetricsRegistry(), true, Collections.emptySet()),
            null, null, mock(ServerRoutingStatsManager.class),
                PinotBrokerQueryEventListenerUtils.getBrokerQueryEventListener());

    // Test 1: select constant
    JsonNode request = JsonUtils.stringToJsonNode("{\"sql\":\"EXPLAIN PLAN FOR SELECT 1.5, 'test'\"}");
    RequestContext requestStats = Tracing.getTracer().createRequestScope();
    BrokerResponse brokerResponse = requestHandler.handleRequest(request, null, requestStats, null);

    checkExplainResultSchema(brokerResponse.getResultTable().getDataSchema(),
        new String[]{"Operator", "Operator_Id", "Parent_Id"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT,
            DataSchema.ColumnDataType.INT
        });

    Assert.assertEquals(brokerResponse.getResultTable().getRows().size(), 1);
    Assert.assertEquals(brokerResponse.getResultTable().getRows().get(0),
        new Object[]{"BROKER_EVALUATE", 0, -1});
    Assert.assertEquals(brokerResponse.getTotalDocs(), 0);

    // Test 2: invoke compile time function -> literal only
    long currentTsMin = System.currentTimeMillis();
    request = JsonUtils.stringToJsonNode(
        "{\"sql\":\"EXPLAIN PLAN FOR SELECT 6+8 as addition, fromDateTime('2020-01-01 UTC', 'yyyy-MM-dd z') as "
            + "firstDayOf2020\"}");
    requestStats = Tracing.getTracer().createRequestScope();
    brokerResponse = requestHandler.handleRequest(request, null, requestStats, null);

    checkExplainResultSchema(brokerResponse.getResultTable().getDataSchema(),
        new String[]{"Operator", "Operator_Id", "Parent_Id"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT,
            DataSchema.ColumnDataType.INT
        });

    Assert.assertEquals(brokerResponse.getResultTable().getRows().size(), 1);
    Assert.assertEquals(brokerResponse.getResultTable().getRows().get(0),
        new Object[]{"BROKER_EVALUATE", 0, -1});

    Assert.assertEquals(brokerResponse.getTotalDocs(), 0);
  }

  private void checkExplainResultSchema(DataSchema schema, String[] columnNames,
      DataSchema.ColumnDataType[] columnTypes) {
    for (int i = 0; i < columnNames.length; i++) {
      Assert.assertEquals(schema.getColumnName(i), columnNames[i]);
      Assert.assertEquals(schema.getColumnDataType(i), columnTypes[i]);
    }
  }
}
