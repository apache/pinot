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
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.broker.api.RequestStatistics;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.broker.AllowAllAccessControlFactory;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.PinotMetricUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.testng.Assert;
import org.testng.annotations.Test;


public class LiteralOnlyBrokerRequestTest {
  private static final AccessControlFactory ACCESS_CONTROL_FACTORY = new AllowAllAccessControlFactory();
  private static final Random RANDOM = new Random(System.currentTimeMillis());
  private static final CalciteSqlCompiler SQL_COMPILER = new CalciteSqlCompiler();

  @Test
  public void testStringLiteralBrokerRequestFromSQL() {
    Assert.assertTrue(BaseBrokerRequestHandler.isLiteralOnlyQuery(SQL_COMPILER.compileToBrokerRequest("SELECT 'a'")));
    Assert.assertTrue(
        BaseBrokerRequestHandler.isLiteralOnlyQuery(SQL_COMPILER.compileToBrokerRequest("SELECT 'a', 'b'")));
    Assert.assertTrue(
        BaseBrokerRequestHandler.isLiteralOnlyQuery(SQL_COMPILER.compileToBrokerRequest("SELECT 'a' FROM myTable")));
    Assert.assertTrue(BaseBrokerRequestHandler
        .isLiteralOnlyQuery(SQL_COMPILER.compileToBrokerRequest("SELECT 'a', 'b' FROM myTable")));
  }

  @Test
  public void testSelectStarBrokerRequestFromSQL() {
    Assert.assertTrue(BaseBrokerRequestHandler.isLiteralOnlyQuery(SQL_COMPILER.compileToBrokerRequest("SELECT '*'")));
    Assert.assertTrue(
        BaseBrokerRequestHandler.isLiteralOnlyQuery(SQL_COMPILER.compileToBrokerRequest("SELECT '*' FROM myTable")));
    Assert.assertFalse(BaseBrokerRequestHandler.isLiteralOnlyQuery(SQL_COMPILER.compileToBrokerRequest("SELECT *")));
    Assert.assertFalse(
        BaseBrokerRequestHandler.isLiteralOnlyQuery(SQL_COMPILER.compileToBrokerRequest("SELECT * FROM myTable")));
  }

  @Test
  public void testNumberLiteralBrokerRequestFromSQL() {
    Assert.assertTrue(BaseBrokerRequestHandler.isLiteralOnlyQuery(SQL_COMPILER.compileToBrokerRequest("SELECT 1")));
    Assert.assertTrue(
        BaseBrokerRequestHandler.isLiteralOnlyQuery(SQL_COMPILER.compileToBrokerRequest("SELECT 1, '2', 3")));
    Assert.assertTrue(
        BaseBrokerRequestHandler.isLiteralOnlyQuery(SQL_COMPILER.compileToBrokerRequest("SELECT 1 FROM myTable")));
    Assert.assertTrue(BaseBrokerRequestHandler
        .isLiteralOnlyQuery(SQL_COMPILER.compileToBrokerRequest("SELECT 1, '2', 3 FROM myTable")));
  }

  @Test
  public void testLiteralOnlyTransformBrokerRequestFromSQL() {
    Assert.assertTrue(BaseBrokerRequestHandler.isLiteralOnlyQuery(SQL_COMPILER.compileToBrokerRequest("SELECT now()")));
    Assert.assertTrue(BaseBrokerRequestHandler.isLiteralOnlyQuery(
        SQL_COMPILER.compileToBrokerRequest("SELECT now(), fromDateTime('2020-01-01 UTC', 'yyyy-MM-dd z')")));
    Assert.assertTrue(
        BaseBrokerRequestHandler.isLiteralOnlyQuery(SQL_COMPILER.compileToBrokerRequest("SELECT now() FROM myTable")));
    Assert.assertTrue(BaseBrokerRequestHandler.isLiteralOnlyQuery(SQL_COMPILER
        .compileToBrokerRequest("SELECT now(), fromDateTime('2020-01-01 UTC', 'yyyy-MM-dd z') FROM myTable")));
  }

  @Test
  public void testLiteralOnlyWithAsBrokerRequestFromSQL() {
    Assert.assertTrue(BaseBrokerRequestHandler.isLiteralOnlyQuery(SQL_COMPILER.compileToBrokerRequest(
        "SELECT now() AS currentTs, fromDateTime('2020-01-01 UTC', 'yyyy-MM-dd z') AS firstDayOf2020")));
  }

  @Test
  public void testBrokerRequestHandler()
      throws Exception {
    SingleConnectionBrokerRequestHandler requestHandler =
        new SingleConnectionBrokerRequestHandler(new PinotConfiguration(), null, ACCESS_CONTROL_FACTORY, null, null,
            new BrokerMetrics("", PinotMetricUtils.getPinotMetricsRegistry(), true, Collections.emptySet()), null);
    long randomSmallNumber = RANDOM.nextInt();
    long randomLargeNumber = Long.MAX_VALUE - Math.abs(RANDOM.nextInt());
    String randomString = RandomStringUtils.randomAlphanumeric(10);
    JsonNode request = JsonUtils.stringToJsonNode(
        String.format("{\"sql\":\"SELECT %d, %d, '%s'\"}", randomSmallNumber, randomLargeNumber, randomString));
    RequestStatistics requestStats = new RequestStatistics();
    BrokerResponseNative brokerResponse =
        (BrokerResponseNative) requestHandler.handleRequest(request, null, requestStats);
    ResultTable resultTable = brokerResponse.getResultTable();
    DataSchema dataSchema = resultTable.getDataSchema();
    Assert.assertEquals(dataSchema.getColumnNames(),
        new String[]{Long.toString(randomSmallNumber), Long.toString(randomLargeNumber), randomString});
    Assert.assertEquals(dataSchema.getColumnDataTypes(),
        new ColumnDataType[]{ColumnDataType.LONG, ColumnDataType.LONG, ColumnDataType.STRING});
    List<Object[]> rows = resultTable.getRows();
    Assert.assertEquals(rows.size(), 1);
    Assert.assertEquals(rows.get(0), new Object[]{randomSmallNumber, Long.toString(randomLargeNumber), randomString});
    Assert.assertEquals(brokerResponse.getTotalDocs(), 0);
  }

  @Test
  public void testBrokerRequestHandlerWithAsFunction()
      throws Exception {
    SingleConnectionBrokerRequestHandler requestHandler =
        new SingleConnectionBrokerRequestHandler(new PinotConfiguration(), null, ACCESS_CONTROL_FACTORY, null, null,
            new BrokerMetrics("", PinotMetricUtils.getPinotMetricsRegistry(), true, Collections.emptySet()), null);
    long currentTsMin = System.currentTimeMillis();
    JsonNode request = JsonUtils.stringToJsonNode(
        "{\"sql\":\"SELECT now() as currentTs, fromDateTime('2020-01-01 UTC', 'yyyy-MM-dd z') as firstDayOf2020\"}");
    RequestStatistics requestStats = new RequestStatistics();
    BrokerResponseNative brokerResponse =
        (BrokerResponseNative) requestHandler.handleRequest(request, null, requestStats);
    long currentTsMax = System.currentTimeMillis();
    Assert.assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnName(0), "currentTs");
    Assert.assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnDataType(0), ColumnDataType.LONG);
    Assert.assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnName(1), "firstDayOf2020");
    Assert.assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnDataType(1), ColumnDataType.LONG);
    Assert.assertEquals(brokerResponse.getResultTable().getRows().size(), 1);
    Assert.assertEquals(brokerResponse.getResultTable().getRows().get(0).length, 2);
    Assert.assertTrue(Long.parseLong(brokerResponse.getResultTable().getRows().get(0)[0].toString()) > currentTsMin);
    Assert.assertTrue(Long.parseLong(brokerResponse.getResultTable().getRows().get(0)[0].toString()) < currentTsMax);
    Assert.assertEquals(brokerResponse.getResultTable().getRows().get(0)[1], 1577836800000L);
    Assert.assertEquals(brokerResponse.getTotalDocs(), 0);
  }
}
