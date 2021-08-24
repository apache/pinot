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
package org.apache.pinot.queries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.response.broker.SelectionResults;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests Response Format = sql for selection, distinct, aggregations and aggregation group bys
 */
public class InterSegmentResultTableMultiValueQueriesTest extends BaseMultiValueQueriesTest {
  private static final String FILTER = " WHERE column6 != 2147483647";
  private static final String SV_GROUP_BY = " GROUP BY column8";
  private static final String MV_GROUP_BY = " GROUP BY column7";

  @Test
  public void testCountMV() {
    DataSchema dataSchema;
    List<Object[]> rows;
    int expectedResultsSize;
    String query = "SELECT COUNTMV(column6) FROM testTable";
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(CommonConstants.Broker.Request.QueryOptionKey.RESPONSE_FORMAT, CommonConstants.Broker.Request.SQL);

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    dataSchema = new DataSchema(new String[]{"countmv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.LONG});
    rows = new ArrayList<>();
    rows.add(new Object[]{426752L});
    expectedResultsSize = 1;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 400000L, 0L, 400000L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{62480L});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 62480L, 1101664L, 62480L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + FILTER + SV_GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column8", "countmv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.LONG});
    rows = new ArrayList<>();
    rows.add(new Object[]{"674022574", 43600L});
    expectedResultsSize = 10;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 174560L, 426752L, 349120L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + FILTER + MV_GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column7", "countmv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.LONG});
    rows = new ArrayList<>();
    rows.add(new Object[]{"469", 29256L});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 174560L, 426752L, 349120L, 400000L, rows, expectedResultsSize,
            dataSchema);
  }

  @Test
  public void testMaxMV() {
    DataSchema dataSchema;
    List<Object[]> rows;
    int expectedResultsSize;
    String query = "SELECT MAXMV(column6) FROM testTable";
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(CommonConstants.Broker.Request.QueryOptionKey.RESPONSE_FORMAT, CommonConstants.Broker.Request.SQL);

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    dataSchema = new DataSchema(new String[]{"maxmv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{2147483647.0});
    expectedResultsSize = 1;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 400000L, 0L, 400000L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{2147483647.0});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 62480L, 1101664L, 62480L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + FILTER + SV_GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column8", "maxmv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{"674022574", 5400497.0});
    expectedResultsSize = 10;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 174560L, 426752L, 349120L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + FILTER + MV_GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column7", "maxmv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{"225", 5400497.0});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 174560L, 426752L, 349120L, 400000L, rows, expectedResultsSize,
            dataSchema);
  }

  @Test
  public void testMinMV() {
    DataSchema dataSchema;
    List<Object[]> rows;
    int expectedResultsSize;
    String query = "SELECT MINMV(column6) FROM testTable";
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(CommonConstants.Broker.Request.QueryOptionKey.RESPONSE_FORMAT, CommonConstants.Broker.Request.SQL);

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    dataSchema = new DataSchema(new String[]{"minmv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{1001.0});
    expectedResultsSize = 1;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 400000L, 0L, 400000L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{1009.0});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 62480L, 1101664L, 62480L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + FILTER + SV_GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column8", "minmv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{"674022574", 1001.0});
    expectedResultsSize = 10;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 174560L, 426752L, 349120L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + FILTER + MV_GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column7", "minmv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{"469", 1001.0});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 174560L, 426752L, 349120L, 400000L, rows, expectedResultsSize,
            dataSchema);
  }

  @Test
  public void testSumMV() {
    DataSchema dataSchema;
    List<Object[]> rows;
    int expectedResultsSize;
    String query = "SELECT SUMMV(column6) FROM testTable";
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(CommonConstants.Broker.Request.QueryOptionKey.RESPONSE_FORMAT, CommonConstants.Broker.Request.SQL);

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    dataSchema = new DataSchema(new String[]{"summv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{484324601810280.0});
    expectedResultsSize = 1;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 400000L, 0L, 400000L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{114652613591912.0});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 62480L, 1101664L, 62480L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + FILTER + SV_GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column8", "summv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{"716819109", 39334429344.0});
    expectedResultsSize = 10;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 174560L, 426752L, 349120L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + FILTER + MV_GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column7", "summv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{"363", 58236246660.0});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 174560L, 426752L, 349120L, 400000L, rows, expectedResultsSize,
            dataSchema);
  }

  @Test
  public void testAvgMV() {
    DataSchema dataSchema;
    List<Object[]> rows;
    int expectedResultsSize;
    String query = "SELECT AVGMV(column6) FROM testTable";
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(CommonConstants.Broker.Request.QueryOptionKey.RESPONSE_FORMAT, CommonConstants.Broker.Request.SQL);

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    dataSchema = new DataSchema(new String[]{"avgmv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{1134908803.7320974});
    expectedResultsSize = 1;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 400000L, 0L, 400000L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{1835029026.759155});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 62480L, 1101664L, 62480L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + FILTER + SV_GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column8", "avgmv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{"118380643", 5392989.0});
    expectedResultsSize = 10;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 174560L, 426752L, 349120L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + FILTER + MV_GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column7", "avgmv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{"380", 3694440.5});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 174560L, 426752L, 349120L, 400000L, rows, expectedResultsSize,
            dataSchema);
  }

  @Test
  public void testMinMaxRangeMV() {
    DataSchema dataSchema;
    List<Object[]> rows;
    int expectedResultsSize;
    String query = "SELECT MINMAXRANGEMV(column6) FROM testTable";
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(CommonConstants.Broker.Request.QueryOptionKey.RESPONSE_FORMAT, CommonConstants.Broker.Request.SQL);

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    dataSchema = new DataSchema(new String[]{"minmaxrangemv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{2147482646.0});
    expectedResultsSize = 1;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 400000L, 0L, 400000L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{2147482638.0});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 62480L, 1101664L, 62480L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + FILTER + SV_GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column8", "minmaxrangemv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{"674022574", 5399496.0});
    expectedResultsSize = 10;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 174560L, 426752L, 349120L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + FILTER + MV_GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column7", "minmaxrangemv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{"225", 5399488.0});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 174560L, 426752L, 349120L, 400000L, rows, expectedResultsSize,
            dataSchema);
  }

  @Test
  public void testDistinctCountMV() {
    DataSchema dataSchema;
    List<Object[]> rows;
    int expectedResultsSize;
    String query = "SELECT DISTINCTCOUNTMV(column6) FROM testTable";
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(CommonConstants.Broker.Request.QueryOptionKey.RESPONSE_FORMAT, CommonConstants.Broker.Request.SQL);

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    dataSchema = new DataSchema(new String[]{"distinctcountmv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT});
    rows = new ArrayList<>();
    rows.add(new Object[]{18499});
    expectedResultsSize = 1;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 400000L, 0L, 400000L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{1186});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 62480L, 1101664L, 62480L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + FILTER + SV_GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column8", "distinctcountmv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT});
    rows = new ArrayList<>();
    rows.add(new Object[]{"674022574", 4783});
    expectedResultsSize = 10;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 174560L, 426752L, 349120L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + FILTER + MV_GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column7", "distinctcountmv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT});
    rows = new ArrayList<>();
    rows.add(new Object[]{"363", 3433});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 174560L, 426752L, 349120L, 400000L, rows, expectedResultsSize,
            dataSchema);
  }

  @Test
  public void testDistinctCountHLLMV() {
    DataSchema dataSchema;
    List<Object[]> rows;
    int expectedResultsSize;
    String query = "SELECT DISTINCTCOUNTHLLMV(column6) FROM testTable";
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(CommonConstants.Broker.Request.QueryOptionKey.RESPONSE_FORMAT, CommonConstants.Broker.Request.SQL);

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    dataSchema = new DataSchema(new String[]{"distinctcounthllmv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.LONG});
    rows = new ArrayList<>();
    rows.add(new Object[]{20039L});
    expectedResultsSize = 1;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 400000L, 0L, 400000L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{1296L});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 62480L, 1101664L, 62480L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + FILTER + SV_GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column8", "distinctcounthllmv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.LONG});
    rows = new ArrayList<>();
    rows.add(new Object[]{"674022574", 4715L});
    expectedResultsSize = 10;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 174560L, 426752L, 349120L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + FILTER + MV_GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column7", "distinctcounthllmv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.LONG});
    rows = new ArrayList<>();
    rows.add(new Object[]{"363", 3490L});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 174560L, 426752L, 349120L, 400000L, rows, expectedResultsSize,
            dataSchema);
  }

  @Test
  public void testDistinctCountRawHLLMV() {
    DataSchema dataSchema;
    List<Object[]> rows;
    int expectedResultsSize;
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(CommonConstants.Broker.Request.QueryOptionKey.RESPONSE_FORMAT, CommonConstants.Broker.Request.SQL);
    String query = "SELECT DISTINCTCOUNTRAWHLLMV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    dataSchema = new DataSchema(new String[]{"distinctcountrawhllmv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING});
    rows = new ArrayList<>();
    Object[] expectedRow0 = new Object[]{20039L};
    expectedResultsSize = 1;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 400000L, 0L, 400000L, 400000L, rows, expectedResultsSize,
            dataSchema);
    Object[] row0 = brokerResponse.getResultTable().getRows().get(0);
    Assert.assertEquals(
        ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(BytesUtils.toBytes(row0[0].toString())).cardinality(),
        expectedRow0[0]);

    brokerResponse = getBrokerResponseForPqlQuery(query + getFilter(), queryOptions);
    expectedRow0 = new Object[]{1296L};
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 62480L, 1101664L, 62480L, 400000L, rows, expectedResultsSize,
            dataSchema);
    row0 = brokerResponse.getResultTable().getRows().get(0);
    Assert.assertEquals(
        ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(BytesUtils.toBytes(row0[0].toString())).cardinality(),
        expectedRow0[0]);

    brokerResponse = getBrokerResponseForPqlQuery(query + FILTER + SV_GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column8", "distinctcountrawhllmv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING});
    expectedRow0 = new Object[]{"674022574", 4715L};
    expectedResultsSize = 10;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 174560L, 426752L, 349120L, 400000L, rows, expectedResultsSize,
            dataSchema);
    row0 = brokerResponse.getResultTable().getRows().get(0);
    Assert.assertEquals(row0[0], expectedRow0[0]);
    Assert.assertEquals(
        ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(BytesUtils.toBytes(row0[1].toString())).cardinality(),
        expectedRow0[1]);

    brokerResponse = getBrokerResponseForPqlQuery(query + FILTER + MV_GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column7", "distinctcountrawhllmv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING});
    expectedRow0 = new Object[]{"363", 3490L};
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 174560L, 426752L, 349120L, 400000L, rows, expectedResultsSize,
            dataSchema);
    row0 = brokerResponse.getResultTable().getRows().get(0);
    Assert.assertEquals(row0[0], expectedRow0[0]);
    Assert.assertEquals(
        ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(BytesUtils.toBytes(row0[1].toString())).cardinality(),
        expectedRow0[1]);
  }

  @Test
  public void testPercentile50MV() {
    List<String> queries = Arrays
        .asList("SELECT PERCENTILE50MV(column6) FROM testTable", "SELECT PERCENTILEMV(column6, 50) FROM testTable",
            "SELECT PERCENTILEMV(column6, '50') FROM testTable", "SELECT PERCENTILEMV(column6, \"50\") FROM testTable");

    DataSchema dataSchema;
    List<Object[]> rows;
    int expectedResultsSize;
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(CommonConstants.Broker.Request.QueryOptionKey.RESPONSE_FORMAT, CommonConstants.Broker.Request.SQL);
    for (int i = 0; i < queries.size(); i++) {
      String query = queries.get(i);
      BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);

      // Schema for first query is different from schema of the rest of the queries because the second query is using
      // PERCENTILEEMV
      // function that works off decimal values.
      dataSchema = i == 0 ? new DataSchema(new String[]{"percentile50mv(column6)"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE})
          : new DataSchema(new String[]{"percentilemv(column6, 50.0)"},
              new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE});

      rows = new ArrayList<>();
      rows.add(new Object[]{2147483647.0});
      expectedResultsSize = 1;
      QueriesTestUtils
          .testInterSegmentResultTable(brokerResponse, 400000L, 0L, 400000L, 400000L, rows, expectedResultsSize,
              dataSchema);

      brokerResponse = getBrokerResponseForPqlQuery(query + getFilter(), queryOptions);
      rows = new ArrayList<>();
      rows.add(new Object[]{2147483647.0});
      QueriesTestUtils
          .testInterSegmentResultTable(brokerResponse, 62480L, 1101664L, 62480L, 400000L, rows, expectedResultsSize,
              dataSchema);

      brokerResponse = getBrokerResponseForPqlQuery(query + FILTER + SV_GROUP_BY, queryOptions);

      // Schema for first query is different from schema of the rest of the queries because the second query is using
      // PERCENTILEEMV
      // function that works off decimal values.
      dataSchema = i == 0 ? new DataSchema(new String[]{"column8", "percentile50mv(column6)"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE})
          : new DataSchema(new String[]{"column8", "percentilemv(column6, 50.0)"},
              new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});

      rows = new ArrayList<>();
      rows.add(new Object[]{"118380643", 5392989.0});
      expectedResultsSize = 10;
      QueriesTestUtils
          .testInterSegmentResultTable(brokerResponse, 174560L, 426752L, 349120L, 400000L, rows, expectedResultsSize,
              dataSchema);

      brokerResponse = getBrokerResponseForPqlQuery(query + FILTER + MV_GROUP_BY, queryOptions);

      // Schema for first query is different from schema of the rest of the queries because the second query is using
      // PERCENTILEEMV
      // function that works off decimal values.
      dataSchema = i == 0 ? new DataSchema(new String[]{"column7", "percentile50mv(column6)"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE})
          : new DataSchema(new String[]{"column7", "percentilemv(column6, 50.0)"},
              new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});

      rows = new ArrayList<>();
      rows.add(new Object[]{"341", 5084850.0});
      QueriesTestUtils
          .testInterSegmentResultTable(brokerResponse, 174560L, 426752L, 349120L, 400000L, rows, expectedResultsSize,
              dataSchema);
    }
  }

  @Test
  public void testPercentile90MV() {
    DataSchema dataSchema;
    List<Object[]> rows;
    int expectedResultsSize;
    String query = "SELECT PERCENTILE90MV(column6) FROM testTable";
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(CommonConstants.Broker.Request.QueryOptionKey.RESPONSE_FORMAT, CommonConstants.Broker.Request.SQL);

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    dataSchema = new DataSchema(new String[]{"percentile90mv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{2147483647.0});
    expectedResultsSize = 1;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 400000L, 0L, 400000L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{2147483647.0});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 62480L, 1101664L, 62480L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + FILTER + SV_GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column8", "percentile90mv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{"118380643", 5392989.0});
    expectedResultsSize = 10;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 174560L, 426752L, 349120L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + FILTER + MV_GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column7", "percentile90mv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{"371", 5380174.0});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 174560L, 426752L, 349120L, 400000L, rows, expectedResultsSize,
            dataSchema);
  }

  @Test
  public void testPercentile95MV() {
    DataSchema dataSchema;
    List<Object[]> rows;
    int expectedResultsSize;
    String query = "SELECT PERCENTILE95MV(column6) FROM testTable";
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(CommonConstants.Broker.Request.QueryOptionKey.RESPONSE_FORMAT, CommonConstants.Broker.Request.SQL);

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    dataSchema = new DataSchema(new String[]{"percentile95mv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{2147483647.0});
    expectedResultsSize = 1;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 400000L, 0L, 400000L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{2147483647.0});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 62480L, 1101664L, 62480L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + FILTER + SV_GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column8", "percentile95mv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{"118380643", 5392989.0});
    expectedResultsSize = 10;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 174560L, 426752L, 349120L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + FILTER + MV_GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column7", "percentile95mv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{"371", 5380174.0});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 174560L, 426752L, 349120L, 400000L, rows, expectedResultsSize,
            dataSchema);
  }

  @Test
  public void testPercentile99MV() {
    DataSchema dataSchema;
    List<Object[]> rows;
    int expectedResultsSize;
    String query = "SELECT PERCENTILE99MV(column6) FROM testTable";
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(CommonConstants.Broker.Request.QueryOptionKey.RESPONSE_FORMAT, CommonConstants.Broker.Request.SQL);

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    dataSchema = new DataSchema(new String[]{"percentile99mv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{2147483647.0});
    expectedResultsSize = 1;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 400000L, 0L, 400000L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{2147483647.0});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 62480L, 1101664L, 62480L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + FILTER + SV_GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column8", "percentile99mv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{"2057094396", 5393010.0});
    expectedResultsSize = 10;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 174560L, 426752L, 349120L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + FILTER + MV_GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column7", "percentile99mv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{"476", 5394180.0});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 174560L, 426752L, 349120L, 400000L, rows, expectedResultsSize,
            dataSchema);
  }

  @Test
  public void testPercentileEst50MV() {
    DataSchema dataSchema;
    List<Object[]> rows;
    int expectedResultsSize;
    String query = "SELECT PERCENTILEEST50MV(column6) FROM testTable";
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(CommonConstants.Broker.Request.QueryOptionKey.RESPONSE_FORMAT, CommonConstants.Broker.Request.SQL);

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    dataSchema = new DataSchema(new String[]{"percentileest50mv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.LONG});
    rows = new ArrayList<>();
    rows.add(new Object[]{2147483647L});
    expectedResultsSize = 1;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 400000L, 0L, 400000L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{2147483647L});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 62480L, 1101664L, 62480L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + FILTER + SV_GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column8", "percentileest50mv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.LONG});
    rows = new ArrayList<>();
    rows.add(new Object[]{"118380643", 5392989L});
    expectedResultsSize = 10;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 174560L, 426752L, 349120L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + FILTER + MV_GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column7", "percentileest50mv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.LONG});
    rows = new ArrayList<>();
    rows.add(new Object[]{"341", 5084850L});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 174560L, 426752L, 349120L, 400000L, rows, expectedResultsSize,
            dataSchema);
  }

  @Test
  public void testPercentileEst90MV() {
    DataSchema dataSchema;
    List<Object[]> rows;
    int expectedResultsSize;
    String query = "SELECT PERCENTILEEST90MV(column6) FROM testTable";
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(CommonConstants.Broker.Request.QueryOptionKey.RESPONSE_FORMAT, CommonConstants.Broker.Request.SQL);

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    dataSchema = new DataSchema(new String[]{"percentileest90mv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.LONG});
    rows = new ArrayList<>();
    rows.add(new Object[]{2147483647L});
    expectedResultsSize = 1;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 400000L, 0L, 400000L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{2147483647L});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 62480L, 1101664L, 62480L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + FILTER + SV_GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column8", "percentileest90mv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.LONG});
    rows = new ArrayList<>();
    rows.add(new Object[]{"118380643", 5392989L});
    expectedResultsSize = 10;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 174560L, 426752L, 349120L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + FILTER + MV_GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column7", "percentileest90mv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.LONG});
    rows = new ArrayList<>();
    rows.add(new Object[]{"371", 5380174L});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 174560L, 426752L, 349120L, 400000L, rows, expectedResultsSize,
            dataSchema);
  }

  @Test
  public void testPercentileEst95MV() {
    DataSchema dataSchema;
    List<Object[]> rows;
    int expectedResultsSize;
    String query = "SELECT PERCENTILEEST95MV(column6) FROM testTable";
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(CommonConstants.Broker.Request.QueryOptionKey.RESPONSE_FORMAT, CommonConstants.Broker.Request.SQL);

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    dataSchema = new DataSchema(new String[]{"percentileest95mv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.LONG});
    rows = new ArrayList<>();
    rows.add(new Object[]{2147483647L});
    expectedResultsSize = 1;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 400000L, 0L, 400000L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{2147483647L});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 62480L, 1101664L, 62480L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + FILTER + SV_GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column8", "percentileest95mv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.LONG});
    rows = new ArrayList<>();
    rows.add(new Object[]{"118380643", 5392989L});
    expectedResultsSize = 10;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 174560L, 426752L, 349120L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + FILTER + MV_GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column7", "percentileest95mv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.LONG});
    rows = new ArrayList<>();
    rows.add(new Object[]{"371", 5380174L});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 174560L, 426752L, 349120L, 400000L, rows, expectedResultsSize,
            dataSchema);
  }

  @Test
  public void testPercentileEst99MV() {
    DataSchema dataSchema;
    List<Object[]> rows;
    int expectedResultsSize;
    String query = "SELECT PERCENTILEEST99MV(column6) FROM testTable";
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(CommonConstants.Broker.Request.QueryOptionKey.RESPONSE_FORMAT, CommonConstants.Broker.Request.SQL);

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    dataSchema = new DataSchema(new String[]{"percentileest99mv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.LONG});
    rows = new ArrayList<>();
    rows.add(new Object[]{2147483647L});
    expectedResultsSize = 1;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 400000L, 0L, 400000L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{2147483647L});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 62480L, 1101664L, 62480L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + FILTER + SV_GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column8", "percentileest99mv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.LONG});
    rows = new ArrayList<>();
    rows.add(new Object[]{"2057094396", 5393010L});
    expectedResultsSize = 10;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 174560L, 426752L, 349120L, 400000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + FILTER + MV_GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column7", "percentileest99mv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.LONG});
    rows = new ArrayList<>();
    rows.add(new Object[]{"476", 5394180L});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 174560L, 426752L, 349120L, 400000L, rows, expectedResultsSize,
            dataSchema);
  }

  /**
   * Tests Selection with SelectionResults and also ResultTable
   */
  @Test
  public void testSelection() {
    // select *
    String query = "SELECT * FROM testTable";
    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    SelectionResults selectionResults = brokerResponse.getSelectionResults();
    query = "SELECT * FROM testTable option(responseFormat=sql)";
    BrokerResponseNative brokerResponseSQL = getBrokerResponseForPqlQuery(query);
    ResultTable resultTable = brokerResponseSQL.getResultTable();

    Assert.assertEquals(resultTable.getDataSchema().getColumnNames().length, 10);
    Assert.assertEquals(resultTable.getDataSchema().getColumnNames(), new String[]{
        "column1", "column10", "column2", "column3", "column5", "column6", "column7", "column8", "column9",
        "daysSinceEpoch"
    });
    Assert.assertEquals(resultTable.getDataSchema().getColumnDataTypes(), new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT,
        DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT_ARRAY,
        DataSchema.ColumnDataType.INT_ARRAY, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT,
        DataSchema.ColumnDataType.INT
    });
    Assert.assertEquals(resultTable.getRows().size(), 10);
    Assert.assertEquals(resultTable.getDataSchema().getColumnNames(), selectionResults.getColumns().toArray());

    // select * limit infinite
    query = "SELECT * FROM testTable limit 50";
    brokerResponse = getBrokerResponseForPqlQuery(query);
    selectionResults = brokerResponse.getSelectionResults();
    query = "SELECT * FROM testTable LIMIT 50 option(responseFormat=sql)";
    brokerResponseSQL = getBrokerResponseForPqlQuery(query);
    resultTable = brokerResponseSQL.getResultTable();
    Assert.assertEquals(resultTable.getDataSchema().getColumnNames().length, 10);
    Assert.assertEquals(resultTable.getDataSchema().getColumnNames(), new String[]{
        "column1", "column10", "column2", "column3", "column5", "column6", "column7", "column8", "column9",
        "daysSinceEpoch"
    });
    Assert.assertEquals(resultTable.getRows().size(), 50);
    Assert.assertEquals(resultTable.getDataSchema().getColumnNames(), selectionResults.getColumns().toArray());

    // select 1
    query = "SELECT column6 FROM testTable";
    brokerResponse = getBrokerResponseForPqlQuery(query);
    selectionResults = brokerResponse.getSelectionResults();
    query = "SELECT column6 FROM testTable option(responseFormat=sql)";
    brokerResponseSQL = getBrokerResponseForPqlQuery(query);
    resultTable = brokerResponseSQL.getResultTable();
    Assert.assertEquals(resultTable.getDataSchema().getColumnNames().length, 1);
    Assert.assertEquals(resultTable.getDataSchema().getColumnNames(), new String[]{"column6"});
    Assert.assertEquals(resultTable.getDataSchema().getColumnDataTypes(),
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT_ARRAY});
    Assert.assertEquals(resultTable.getRows().size(), 10);
    Assert.assertEquals(resultTable.getDataSchema().getColumnNames(), selectionResults.getColumns().toArray());

    // select 3
    query = "SELECT column1, column6, column7 FROM testTable";
    brokerResponse = getBrokerResponseForPqlQuery(query);
    selectionResults = brokerResponse.getSelectionResults();
    query = "SELECT column1, column6, column7 FROM testTable option(responseFormat=sql)";
    brokerResponseSQL = getBrokerResponseForPqlQuery(query);
    resultTable = brokerResponseSQL.getResultTable();
    Assert.assertEquals(resultTable.getDataSchema().getColumnNames().length, 3);
    Assert.assertEquals(resultTable.getDataSchema().getColumnNames(), new String[]{"column1", "column6", "column7"});
    Assert.assertEquals(resultTable.getDataSchema().getColumnDataTypes(), new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT_ARRAY, DataSchema.ColumnDataType.INT_ARRAY
    });
    Assert.assertEquals(resultTable.getRows().size(), 10);
    Assert.assertEquals(resultTable.getDataSchema().getColumnNames(), selectionResults.getColumns().toArray());
  }
}
