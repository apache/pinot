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

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.google.common.collect.Lists;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
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
import org.apache.pinot.core.query.request.context.ThreadTimer;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests Response Format = sql for selection, distinct, aggregations and aggregation group bys
 */
public class InterSegmentResultTableSingleValueQueriesTest extends BaseSingleValueQueriesTest {
  private static final String GROUP_BY = " group by column9";

  @Test
  public void testCount() {
    DataSchema dataSchema;
    List<Object[]> rows;
    int expectedResultsSize;

    String query = "SELECT COUNT(*) FROM testTable";
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(QueryOptionKey.RESPONSE_FORMAT, Request.SQL);

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    dataSchema =
        new DataSchema(new String[]{"count(*)"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.LONG});
    rows = new ArrayList<>();
    rows.add(new Object[]{120000L});
    expectedResultsSize = 1;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 120000L, 0L, 0L, 120000L, rows, expectedResultsSize, dataSchema);

    // filter
    brokerResponse = getBrokerResponseForPqlQuery(query + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{24516L});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 24516L, 336536L, 0L, 120000L, rows, expectedResultsSize,
            dataSchema);

    // group by
    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column9", "count(*)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.LONG});
    rows = new ArrayList<>();
    rows.add(new Object[]{"296467636", 64420L});
    expectedResultsSize = 10;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 120000L, 0L, 120000L, 120000L, rows, expectedResultsSize,
            dataSchema);

    // filter + group by
    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{"296467636", 17080L});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 24516L, 336536L, 24516L, 120000L, rows, expectedResultsSize,
            dataSchema);

    // empty results
    brokerResponse =
        getBrokerResponseForPqlQuery(query + GROUP_BY + " where column5='non-existent-value'", queryOptions);
    rows = new ArrayList<>();
    expectedResultsSize = 0;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 0, 0, 0, 120000L, rows, expectedResultsSize, dataSchema);
  }

  @Test
  public void testMax() {
    DataSchema dataSchema;
    List<Object[]> rows;
    int expectedResultsSize;

    String query = "SELECT MAX(column1), MAX(column3) FROM testTable";
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(QueryOptionKey.RESPONSE_FORMAT, Request.SQL);

    // Query should be answered by MetadataBasedAggregationOperator, so check if numEntriesScannedInFilter and
    // numEntriesScannedPostFilter are 0
    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    dataSchema = new DataSchema(new String[]{"max(column1)", "max(column3)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{2146952047.0, 2147419555.0});
    expectedResultsSize = 1;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 120000L, 0L, 0L, 120000L, rows, expectedResultsSize, dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{2146952047.0, 999813884.0});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 24516L, 336536L, 49032L, 120000L, rows, expectedResultsSize,
            dataSchema);

    query = "select max(column1) from testTable";
    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column9", "max(column1)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{"296467636", 2146952047.0});
    expectedResultsSize = 10;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 120000L, 0L, 240000L, 120000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{"296467636", 2146952047.0});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 24516L, 336536L, 49032L, 120000L, rows, expectedResultsSize,
            dataSchema);
  }

  @Test
  public void testMin() {
    DataSchema dataSchema;
    List<Object[]> rows;
    int expectedResultsSize;
    String query = "SELECT MIN(column1), MIN(column3) FROM testTable";
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(QueryOptionKey.RESPONSE_FORMAT, Request.SQL);

    // Query should be answered by MetadataBasedAggregationOperator, so check if numEntriesScannedInFilter and
    // numEntriesScannedPostFilter are 0
    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    dataSchema = new DataSchema(new String[]{"min(column1)", "min(column3)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{240528.0, 17891.0});
    expectedResultsSize = 1;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 120000L, 0L, 0L, 120000L, rows, expectedResultsSize, dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{101116473.0, 20396372.0});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 24516L, 336536L, 49032L, 120000L, rows, expectedResultsSize,
            dataSchema);

    query = "SELECT MIN(column3) FROM testTable";
    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column9", "min(column3)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{"296467636", 17891.0});
    expectedResultsSize = 10;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 120000L, 0L, 240000L, 120000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{"296467636", 20396372.0});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 24516L, 336536L, 49032L, 120000L, rows, expectedResultsSize,
            dataSchema);
  }

  @Test
  public void testSum() {
    DataSchema dataSchema;
    List<Object[]> rows;
    int expectedResultsSize;
    String query = "SELECT SUM(column1), SUM(column3) FROM testTable";
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(QueryOptionKey.RESPONSE_FORMAT, Request.SQL);

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    dataSchema = new DataSchema(new String[]{"sum(column1)", "sum(column3)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{129268741751388.0, 129156636756600.0});
    expectedResultsSize = 1;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 120000L, 0L, 240000L, 120000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{27503790384288.0, 12429178874916.0});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 24516L, 336536L, 49032L, 120000L, rows, expectedResultsSize,
            dataSchema);

    query = "SELECT SUM(column3) FROM testTable";
    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column9", "sum(column3)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{"296467636", 69225631719808.0});
    expectedResultsSize = 10;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 120000L, 0L, 240000L, 120000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{"296467636", 8606725456500.0});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 24516L, 336536L, 49032L, 120000L, rows, expectedResultsSize,
            dataSchema);
  }

  @Test
  public void testAvg() {
    DataSchema dataSchema;
    List<Object[]> rows;
    int expectedResultsSize;
    String query = "SELECT AVG(column1), AVG(column3) FROM testTable";
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(QueryOptionKey.RESPONSE_FORMAT, Request.SQL);

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    dataSchema = new DataSchema(new String[]{"avg(column1)", "avg(column3)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{1077239514.5949, 1076305306.305});
    expectedResultsSize = 1;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 120000L, 0L, 240000L, 120000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{1121871038.680372, 506982332.9627998});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 24516L, 336536L, 49032L, 120000L, rows, expectedResultsSize,
            dataSchema);

    query = "select avg(column3) from testTable";
    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column9", "avg(column3)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{"1642909995", 2141451242.0});
    expectedResultsSize = 10;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 120000L, 0L, 240000L, 120000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{"438926263", 999309554.0});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 24516L, 336536L, 49032L, 120000L, rows, expectedResultsSize,
            dataSchema);
  }

  @Test
  public void testMinMaxRange() {
    DataSchema dataSchema;
    List<Object[]> rows;
    int expectedResultsSize;

    String query = "SELECT MINMAXRANGE(column1), MINMAXRANGE(column3) FROM testTable";
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(QueryOptionKey.RESPONSE_FORMAT, Request.SQL);

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    dataSchema = new DataSchema(new String[]{"minmaxrange(column1)", "minmaxrange(column3)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{2146711519.0, 2147401664.0});
    expectedResultsSize = 1;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 120000L, 0L, 0L, 120000L, rows, expectedResultsSize, dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{2045835574.0, 979417512.0});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 24516L, 336536L, 49032L, 120000L, rows, expectedResultsSize,
            dataSchema);

    query = "SELECT MINMAXRANGE(column1) FROM testTable";
    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column9", "minmaxrange(column1)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{"296467636", 2146711519.0});
    expectedResultsSize = 10;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 120000L, 0L, 240000L, 120000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{"296467636", 2044094181.0});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 24516L, 336536L, 49032L, 120000L, rows, expectedResultsSize,
            dataSchema);
  }

  @Test
  public void testDistinctCount() {
    DataSchema dataSchema;
    List<Object[]> rows;
    int expectedResultsSize;
    String query = "SELECT DISTINCTCOUNT(column1), DISTINCTCOUNT(column3) FROM testTable";
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(QueryOptionKey.RESPONSE_FORMAT, Request.SQL);

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    dataSchema = new DataSchema(new String[]{"distinctcount(column1)", "distinctcount(column3)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT});
    rows = new ArrayList<>();
    rows.add(new Object[]{6582, 21910});
    expectedResultsSize = 1;
    //without filter, distinctCount must be solved using dictionary. expectedNumEntriesScannedPostFilter is 0L
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 120000L, 0L, 0L, 120000L, rows, expectedResultsSize, dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{1872, 4556});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 24516L, 336536L, 49032L, 120000L, rows, expectedResultsSize,
            dataSchema);

    query = "SELECT DISTINCTCOUNT(column3) FROM testTable";
    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column9", "distinctcount(column3)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT});
    rows = new ArrayList<>();
    rows.add(new Object[]{"296467636", 11961});
    expectedResultsSize = 10;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 120000L, 0L, 240000L, 120000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{"296467636", 3289});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 24516L, 336536L, 49032L, 120000, rows, expectedResultsSize,
            dataSchema);
  }

  @Test
  public void testDistinctCountHLL() {
    DataSchema dataSchema;
    List<Object[]> rows;
    int expectedResultsSize;
    String query = "SELECT DISTINCTCOUNTHLL(column1), DISTINCTCOUNTHLL(column3) FROM testTable";
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(QueryOptionKey.RESPONSE_FORMAT, Request.SQL);

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    dataSchema = new DataSchema(new String[]{"distinctcounthll(column1)", "distinctcounthll(column3)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG});
    rows = new ArrayList<>();
    rows.add(new Object[]{5977L, 23825L});
    expectedResultsSize = 1;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 120000L, 0L, 240000L, 120000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{1886L, 4492L});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 24516L, 336536L, 49032L, 120000L, rows, expectedResultsSize,
            dataSchema);

    query = "SELECT DISTINCTCOUNTHLL(column1) FROM testTable";
    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column9", "distinctcounthll(column1)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.LONG});
    rows = new ArrayList<>();
    rows.add(new Object[]{"296467636", 3592L});
    expectedResultsSize = 10;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 120000L, 0L, 240000L, 120000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{"296467636", 1324L});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 24516L, 336536L, 49032L, 120000L, rows, expectedResultsSize,
            dataSchema);
  }

  @Test
  public void testDistinctCountRawHLL()
      throws Exception {
    String rawHllResults = "data" + File.separator + "rawhllresults.txt";
    URL resource = getClass().getClassLoader().getResource(rawHllResults);
    File resultFile = new File(resource.getFile());
    String[] results = new String[100];
    int count = 0;
    try (InputStream inputStream = new FileInputStream(resultFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
      String line;
      while ((line = reader.readLine()) != null) {
        results[count++] = line;
      }
    }

    DataSchema dataSchema;
    int expectedResultsSize;

    // 1. test aggregation only query with SQL response format
    String query = "SELECT DISTINCTCOUNTRAWHLL(column1), DISTINCTCOUNTRAWHLL(column3) FROM testTable";
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(QueryOptionKey.RESPONSE_FORMAT, Request.SQL);

    String[] columnNames = {"distinctcountrawhll(column1)", "distinctcountrawhll(column3)"};
    DataSchema.ColumnDataType[] columnDataTypes = {DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING};
    dataSchema = new DataSchema(columnNames, columnDataTypes);

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    List<Object[]> expectedRows = new ArrayList<>();
    String hexStringHll1 = results[0];
    String hexStringHll2 = results[1];
    expectedRows.add(new Object[]{hexStringHll1, hexStringHll2});
    expectedResultsSize = expectedRows.size();
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 120000L, 0L, 240000L, 120000L, expectedRows, expectedResultsSize,
            dataSchema);

    // verify cardinality
    Object[] row = brokerResponse.getResultTable().getRows().get(0);
    HyperLogLog hll1 = ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(BytesUtils.toBytes((String) row[0]));
    HyperLogLog hll2 = ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(BytesUtils.toBytes((String) row[1]));
    Long[] expectedCardinalities = new Long[]{5977L, 23825L};
    Assert.assertEquals(hll1.cardinality(), expectedCardinalities[0].longValue());
    Assert.assertEquals(hll2.cardinality(), expectedCardinalities[1].longValue());

    // 2. test aggregation only query with SQL exec + response format
    brokerResponse = getBrokerResponseForSqlQuery(query);
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 120000L, 0L, 240000L, 120000L, expectedRows, expectedResultsSize,
            dataSchema);

    // 3. test aggregation-only query with filter
    String filter = " WHERE column1 > 100000000 AND column3 BETWEEN 20000000 AND 1000000000";
    brokerResponse = getBrokerResponseForPqlQuery(query + filter, queryOptions);
    hexStringHll1 = results[2];
    hexStringHll2 = results[3];
    expectedRows = new ArrayList<>();
    expectedRows.add(new Object[]{hexStringHll1, hexStringHll2});
    QueriesTestUtils.testInterSegmentResultTable(brokerResponse, 51796L, 193884L, 103592L, 120000L, expectedRows,
        expectedResultsSize, dataSchema);

    // verify cardinality
    row = brokerResponse.getResultTable().getRows().get(0);
    hll1 = ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(BytesUtils.toBytes((String) row[0]));
    hll2 = ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(BytesUtils.toBytes((String) row[1]));
    expectedCardinalities = new Long[]{3790L, 10535L};
    Assert.assertEquals(hll1.cardinality(), expectedCardinalities[0].longValue());
    Assert.assertEquals(hll2.cardinality(), expectedCardinalities[1].longValue());

    // 4. test aggregation-only query with filter with SQL exec and response format
    brokerResponse = getBrokerResponseForSqlQuery(query + filter);
    System.out.println(query + getFilter());
    QueriesTestUtils.testInterSegmentResultTable(brokerResponse, 51796L, 193884L, 103592L, 120000L, expectedRows,
        expectedResultsSize, dataSchema);

    // 5. test aggregation + group by query
    query = "SELECT DISTINCTCOUNTRAWHLL(column1) FROM testTable GROUP BY column9 TOP 2";
    dataSchema = new DataSchema(new String[]{"column9", "distinctcountrawhll(column1)"}, columnDataTypes);
    expectedRows = new ArrayList<>();
    expectedCardinalities = new Long[2];
    int c = 0;
    for (int i = 4; i <= 5; i++) {
      String[] s = results[i].split(" ");
      expectedRows.add(new Object[]{s[0], s[1]});
      expectedCardinalities[c++] = Long.valueOf(s[2]);
    }
    expectedResultsSize = expectedRows.size();
    brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 120000L, 0L, 240000L, 120000L, expectedRows, expectedResultsSize,
            dataSchema);
    // verify cardinality
    List<Object[]> rows = brokerResponse.getResultTable().getRows();
    for (int i = 0; i < rows.size(); i++) {
      row = rows.get(i);
      Assert.assertEquals((long) expectedCardinalities[i],
          ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(BytesUtils.toBytes((String) row[1])).cardinality());
    }

    // 6. test aggregation + group by query with sql exec and response format
    // GROUP BY column is returned with actual type INT
    query =
        "SELECT column9, DISTINCTCOUNTRAWHLL(column1) FROM testTable GROUP BY column9 ORDER BY DISTINCTCOUNTRAWHLL"
            + "(column1) DESC LIMIT 2";
    dataSchema = new DataSchema(new String[]{"column9", "distinctcountrawhll(column1)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});
    brokerResponse = getBrokerResponseForSqlQuery(query);
    for (Object[] r : expectedRows) {
      Object val = r[0];
      r[0] = Integer.valueOf((String) val);
    }
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 120000L, 0L, 240000L, 120000L, expectedRows, expectedResultsSize,
            dataSchema);

    // 7. test aggregation + filter + group by query
    query = "SELECT DISTINCTCOUNTRAWHLL(column1) FROM testTable " + filter + " GROUP BY column9 TOP 2";
    brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    expectedRows = new ArrayList<>();
    expectedCardinalities = new Long[5];
    c = 0;
    for (int i = 6; i <= 7; i++) {
      String[] s = results[i].split(" ");
      expectedRows.add(new Object[]{s[0], s[1]});
      expectedCardinalities[c++] = Long.valueOf(s[2]);
    }
    dataSchema = new DataSchema(new String[]{"column9", "distinctcountrawhll(column1)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING});
    QueriesTestUtils.testInterSegmentResultTable(brokerResponse, 51796L, 193884L, 103592L, 120000L, expectedRows,
        expectedRows.size(), dataSchema);
    // verify cardinality
    rows = brokerResponse.getResultTable().getRows();
    for (int i = 0; i < rows.size(); i++) {
      row = rows.get(i);
      long cardinality =
          ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(BytesUtils.toBytes((String) row[1])).cardinality();
      Assert.assertEquals((long) expectedCardinalities[i], cardinality);
    }

    // 8. test aggregation + filter + group by query
    query = "SELECT column9, DISTINCTCOUNTRAWHLL(column1) FROM testTable" + filter
        + " GROUP BY column9 ORDER BY DISTINCTCOUNTRAWHLL(column1) DESC LIMIT 2";
    brokerResponse = getBrokerResponseForSqlQuery(query);
    for (Object[] r : expectedRows) {
      Object val = r[0];
      r[0] = Integer.valueOf((String) val);
    }
    rows = brokerResponse.getResultTable().getRows();
    for (Object[] r : rows) {
      System.out
          .println(ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(BytesUtils.toBytes((String) r[1])).cardinality());
    }
    dataSchema = new DataSchema(new String[]{"column9", "distinctcountrawhll(column1)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});
    QueriesTestUtils.testInterSegmentResultTable(brokerResponse, 51796L, 193884L, 103592L, 120000L, expectedRows,
        expectedRows.size(), dataSchema);
  }

  @Test
  public void testPercentile50() {
    List<String> queries = Arrays.asList("SELECT PERCENTILE50(column1),PERCENTILE50(column3) FROM testTable",
        "SELECT PERCENTILE(column1, 50), PERCENTILE(column3, 50) FROM testTable",
        "SELECT PERCENTILE(column1, '50'), PERCENTILE(column3, '50') FROM testTable",
        "SELECT PERCENTILE(column1, \"50\"), PERCENTILE(column3, \"50\") FROM testTable");

    DataSchema dataSchema;
    List<Object[]> rows;
    int expectedResultsSize;
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(QueryOptionKey.RESPONSE_FORMAT, Request.SQL);
    for (int i = 0; i < queries.size(); i++) {
      String query = queries.get(i);
      BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
      dataSchema = i == 0 ? new DataSchema(new String[]{"percentile50(column1)", "percentile50(column3)"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE})
          : new DataSchema(new String[]{"percentile(column1, 50.0)", "percentile(column3, 50.0)"},
              new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE});
      rows = new ArrayList<>();
      rows.add(new Object[]{1107310944.0, 1080136306.0});
      expectedResultsSize = 1;
      QueriesTestUtils
          .testInterSegmentResultTable(brokerResponse, 120000L, 0L, 240000L, 120000L, rows, expectedResultsSize,
              dataSchema);

      brokerResponse = getBrokerResponseForPqlQuery(query + getFilter(), queryOptions);
      rows = new ArrayList<>();
      rows.add(new Object[]{1139674505.0, 505053732.0});
      QueriesTestUtils
          .testInterSegmentResultTable(brokerResponse, 24516L, 336536L, 49032L, 120000L, rows, expectedResultsSize,
              dataSchema);

      query = "SELECT PERCENTILE50(column3) FROM testTable";
      brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY, queryOptions);
      dataSchema = new DataSchema(new String[]{"column9", "percentile50(column3)"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
      rows = new ArrayList<>();
      rows.add(new Object[]{"1642909995", 2141451242.0});
      expectedResultsSize = 10;
      QueriesTestUtils
          .testInterSegmentResultTable(brokerResponse, 120000L, 0L, 240000L, 120000L, rows, expectedResultsSize,
              dataSchema);

      brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY + getFilter(), queryOptions);
      rows = new ArrayList<>();
      rows.add(new Object[]{"438926263", 999309554.0});
      QueriesTestUtils
          .testInterSegmentResultTable(brokerResponse, 24516L, 336536L, 49032L, 120000L, rows, expectedResultsSize,
              dataSchema);
    }
  }

  @Test
  public void testPercentile90() {
    DataSchema dataSchema;
    List<Object[]> rows;
    int expectedResultsSize;
    String query = "SELECT PERCENTILE90(column1), PERCENTILE90(column3) FROM testTable";
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(QueryOptionKey.RESPONSE_FORMAT, Request.SQL);

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    dataSchema = new DataSchema(new String[]{"percentile90(column1)", "percentile90(column3)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{1943040511.0, 1936611145.0});
    expectedResultsSize = 1;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 120000L, 0L, 240000L, 120000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{1936730975.0, 899534534.0});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 24516L, 336536L, 49032L, 120000L, rows, expectedResultsSize,
            dataSchema);

    query = "SELECT PERCENTILE90(column3) FROM testTable";
    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column9", "percentile90(column3)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{"2101070986", 2147278341.0});
    expectedResultsSize = 10;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 120000L, 0L, 240000L, 120000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{"438926263", 999309554.0});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 24516L, 336536L, 49032L, 120000L, rows, expectedResultsSize,
            dataSchema);
  }

  @Test
  public void testPercentile95() {
    DataSchema dataSchema;
    List<Object[]> rows;
    int expectedResultsSize;
    String query = "SELECT PERCENTILE95(column1), PERCENTILE95(column3) FROM testTable";
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(QueryOptionKey.RESPONSE_FORMAT, Request.SQL);

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    dataSchema = new DataSchema(new String[]{"percentile95(column1)", "percentile95(column3)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{2071559385.0, 2042409652.0});
    expectedResultsSize = 1;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 120000L, 0L, 240000L, 120000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{2096857943.0, 947763150.0});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 24516L, 336536L, 49032L, 120000L, rows, expectedResultsSize,
            dataSchema);

    query = "SELECT PERCENTILE95(column3) FROM testTable";
    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column9", "percentile95(column3)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{"147745543", 2147419555.0});
    expectedResultsSize = 10;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 120000L, 0L, 240000L, 120000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{"438926263", 999309554.0});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 24516L, 336536L, 49032L, 120000L, rows, expectedResultsSize,
            dataSchema);
  }

  @Test
  public void testPercentile99() {
    DataSchema dataSchema;
    List<Object[]> rows;
    int expectedResultsSize;
    String query = "SELECT PERCENTILE99(column1), PERCENTILE99(column3) FROM testTable";
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(QueryOptionKey.RESPONSE_FORMAT, Request.SQL);

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    dataSchema = new DataSchema(new String[]{"percentile99(column1)", "percentile99(column3)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{2139354437.0, 2125299552.0});
    expectedResultsSize = 1;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 120000L, 0L, 240000L, 120000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{2146232405.0, 990669195.0});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 24516L, 336536L, 49032L, 120000L, rows, expectedResultsSize,
            dataSchema);

    query = "SELECT PERCENTILE99(column3) FROM testTable";
    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column9", "percentile99(column3)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    rows = new ArrayList<>();
    rows.add(new Object[]{"147745543", 2147419555.0});
    expectedResultsSize = 10;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 120000L, 0L, 240000L, 120000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{"438926263", 999309554.0});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 24516L, 336536L, 49032L, 120000L, rows, expectedResultsSize,
            dataSchema);
  }

  @Test
  public void testPercentileEst50() {
    DataSchema dataSchema;
    List<Object[]> rows;
    int expectedResultsSize;
    String query = "SELECT PERCENTILEEST50(column1), PERCENTILEEST50(column3) FROM testTable";
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(QueryOptionKey.RESPONSE_FORMAT, Request.SQL);

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    dataSchema = new DataSchema(new String[]{"percentileest50(column1)", "percentileest50(column3)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG});
    rows = new ArrayList<>();
    rows.add(new Object[]{1107310944L, 1082130431L});
    expectedResultsSize = 1;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 120000L, 0L, 240000L, 120000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{1139674505L, 509607935L});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 24516L, 336536L, 49032L, 120000L, rows, expectedResultsSize,
            dataSchema);

    query = "SELECT PERCENTILEEST50(column3) FROM testTable";
    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column9", "percentileest50(column3)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.LONG});
    rows = new ArrayList<>();
    rows.add(new Object[]{"1642909995", 2141451242L});
    expectedResultsSize = 10;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 120000L, 0L, 240000L, 120000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{"438926263", 999309554L});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 24516L, 336536L, 49032L, 120000L, rows, expectedResultsSize,
            dataSchema);
  }

  @Test
  public void testPercentileEst90() {
    DataSchema dataSchema;
    List<Object[]> rows;
    int expectedResultsSize;
    String query = "SELECT PERCENTILEEST90(column1), PERCENTILEEST90(column3) FROM testTable";
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(QueryOptionKey.RESPONSE_FORMAT, Request.SQL);

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    dataSchema = new DataSchema(new String[]{"percentileest90(column1)", "percentileest90(column3)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG});
    rows = new ArrayList<>();
    rows.add(new Object[]{1946157055L, 1946157055L});
    expectedResultsSize = 1;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 120000L, 0L, 240000L, 120000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{1939865599L, 902299647L});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 24516L, 336536L, 49032L, 120000L, rows, expectedResultsSize,
            dataSchema);

    query = "SELECT PERCENTILEEST90(column3) FROM testTable";
    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column9", "percentileest90(column3)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.LONG});
    rows = new ArrayList<>();
    rows.add(new Object[]{"2101070986", 2147278341L});
    expectedResultsSize = 10;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 120000L, 0L, 240000L, 120000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{"438926263", 999309554L});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 24516L, 336536L, 49032L, 120000L, rows, expectedResultsSize,
            dataSchema);
  }

  @Test
  public void testPercentileEst95() {
    DataSchema dataSchema;
    List<Object[]> rows;
    int expectedResultsSize;
    String query = "SELECT PERCENTILEEST95(column1), PERCENTILEEST95(column3) FROM testTable";
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(QueryOptionKey.RESPONSE_FORMAT, Request.SQL);

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    dataSchema = new DataSchema(new String[]{"percentileest95(column1)", "percentileest95(column3)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG});
    rows = new ArrayList<>();
    rows.add(new Object[]{2080374783L, 2051014655L});
    expectedResultsSize = 1;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 120000L, 0L, 240000L, 120000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{2109734911L, 950009855L});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 24516L, 336536L, 49032L, 120000L, rows, expectedResultsSize,
            dataSchema);

    query = "SELECT PERCENTILEEST95(column3) FROM testTable";
    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column9", "percentileest95(column3)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.LONG});
    rows = new ArrayList<>();
    rows.add(new Object[]{"147745543", 2147419555L});
    expectedResultsSize = 10;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 120000L, 0L, 240000L, 120000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{"438926263", 999309554L});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 24516L, 336536L, 49032L, 120000L, rows, expectedResultsSize,
            dataSchema);
  }

  @Test
  public void testPercentileEst99() {
    DataSchema dataSchema;
    List<Object[]> rows;
    int expectedResultsSize;
    String query = "SELECT PERCENTILEEST99(column1), PERCENTILEEST99(column3) FROM testTable";
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(QueryOptionKey.RESPONSE_FORMAT, Request.SQL);

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    dataSchema = new DataSchema(new String[]{"percentileest99(column1)", "percentileest99(column3)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG});
    rows = new ArrayList<>();
    rows.add(new Object[]{2143289343L, 2143289343L});
    expectedResultsSize = 1;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 120000L, 0L, 240000L, 120000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{2146232405L, 991952895L});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 24516L, 336536L, 49032L, 120000L, rows, expectedResultsSize,
            dataSchema);

    query = "SELECT PERCENTILEEST99(column3) FROM testTable";
    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY, queryOptions);
    dataSchema = new DataSchema(new String[]{"column9", "percentileest99(column3)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.LONG});
    rows = new ArrayList<>();
    rows.add(new Object[]{"147745543", 2147419555L});
    expectedResultsSize = 10;
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 120000L, 0L, 240000L, 120000L, rows, expectedResultsSize,
            dataSchema);

    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY + getFilter(), queryOptions);
    rows = new ArrayList<>();
    rows.add(new Object[]{"438926263", 999309554L});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 24516L, 336536L, 49032L, 120000L, rows, expectedResultsSize,
            dataSchema);
  }

  /**
   * Test DISTINCT on multiple segment. Since the dataset
   * is Avro files, the only thing we currently check
   * for correctness is the actual number of DISTINCT
   * records returned
   */
  @Test
  public void testInterSegmentDistinct() {
    String query = "SELECT DISTINCT(column1) FROM testTable LIMIT 1000000";
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(QueryOptionKey.RESPONSE_FORMAT, Request.SQL);
    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    ResultTable resultTable = brokerResponse.getResultTable();
    Assert.assertEquals(resultTable.getDataSchema().size(), 1);
    Assert.assertEquals(resultTable.getDataSchema().getColumnNames(), new String[]{"column1"});
    Assert.assertEquals(resultTable.getDataSchema().getColumnDataTypes(),
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT});
    Assert.assertEquals(resultTable.getRows().size(), 6582);

    query = "SELECT DISTINCT(column1, column3) FROM testTable LIMIT 1000000";
    brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    resultTable = brokerResponse.getResultTable();
    Assert.assertEquals(resultTable.getDataSchema().size(), 2);
    Assert.assertEquals(resultTable.getDataSchema().getColumnNames(), new String[]{"column1", "column3"});
    Assert.assertEquals(resultTable.getDataSchema().getColumnDataTypes(),
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT});
    Assert.assertEquals(resultTable.getRows().size(), 21968);

    query = "SELECT DISTINCT(column1, column5, column3) FROM testTable LIMIT 1000000";
    brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    resultTable = brokerResponse.getResultTable();
    Assert.assertEquals(resultTable.getDataSchema().size(), 3);
    Assert.assertEquals(resultTable.getDataSchema().getColumnNames(), new String[]{"column1", "column5", "column3"});
    Assert.assertEquals(resultTable.getDataSchema().getColumnDataTypes(), new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT
    });
    Assert.assertEquals(resultTable.getRows().size(), 21968);
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

    Assert.assertEquals(resultTable.getDataSchema().getColumnNames().length, 11);
    Assert.assertEquals(resultTable.getDataSchema().getColumnNames(), new String[]{
        "column1", "column11", "column12", "column17", "column18", "column3", "column5", "column6", "column7",
        "column9", "daysSinceEpoch"
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

    Assert.assertEquals(resultTable.getDataSchema().getColumnNames().length, 11);
    Assert.assertEquals(resultTable.getDataSchema().getColumnNames(), new String[]{
        "column1", "column11", "column12", "column17", "column18", "column3", "column5", "column6", "column7",
        "column9", "daysSinceEpoch"
    });
    Assert.assertEquals(resultTable.getRows().size(), 50);
    Assert.assertEquals(resultTable.getDataSchema().getColumnNames(), selectionResults.getColumns().toArray());

    // select 1
    query = "SELECT column3 FROM testTable";
    brokerResponse = getBrokerResponseForPqlQuery(query);
    selectionResults = brokerResponse.getSelectionResults();
    query = "SELECT column3 FROM testTable option(responseFormat=sql)";
    brokerResponseSQL = getBrokerResponseForPqlQuery(query);
    resultTable = brokerResponseSQL.getResultTable();
    Assert.assertEquals(resultTable.getDataSchema().getColumnNames().length, 1);
    Assert.assertEquals(resultTable.getDataSchema().getColumnNames(), new String[]{"column3"});
    Assert.assertEquals(resultTable.getDataSchema().getColumnDataTypes(),
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT});
    Assert.assertEquals(resultTable.getRows().size(), 10);
    Assert.assertEquals(resultTable.getDataSchema().getColumnNames(), selectionResults.getColumns().toArray());

    // select 3
    query = "SELECT column1, column3, column11 FROM testTable";
    brokerResponse = getBrokerResponseForPqlQuery(query);
    selectionResults = brokerResponse.getSelectionResults();
    query = "SELECT column1, column3, column11 FROM testTable option(responseFormat=sql)";
    brokerResponseSQL = getBrokerResponseForPqlQuery(query);
    resultTable = brokerResponseSQL.getResultTable();
    Assert.assertEquals(resultTable.getDataSchema().getColumnNames().length, 3);
    Assert.assertEquals(resultTable.getDataSchema().getColumnNames(), new String[]{"column1", "column3", "column11"});
    Assert.assertEquals(resultTable.getDataSchema().getColumnDataTypes(), new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    Assert.assertEquals(resultTable.getRows().size(), 10);
    Assert.assertEquals(resultTable.getDataSchema().getColumnNames(), selectionResults.getColumns().toArray());

    // select + order by
    query = "SELECT column1, column3 FROM testTable ORDER BY column3 option(preserveType=true)";
    brokerResponse = getBrokerResponseForPqlQuery(query);
    selectionResults = brokerResponse.getSelectionResults();
    query = "SELECT column1, column3 FROM testTable ORDER BY column3 option(responseFormat=sql)";
    brokerResponseSQL = getBrokerResponseForPqlQuery(query);
    resultTable = brokerResponseSQL.getResultTable();
    Assert.assertEquals(resultTable.getDataSchema().getColumnNames().length, 2);
    Assert.assertEquals(resultTable.getDataSchema().getColumnNames(), new String[]{"column1", "column3"});
    Assert.assertEquals(resultTable.getDataSchema().getColumnDataTypes(),
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT});
    Assert.assertEquals(resultTable.getRows().size(), 10);
    List<Object[]> rows = Lists
        .newArrayList(new Object[]{142002934, 17891}, new Object[]{142002934, 17891}, new Object[]{142002934, 17891},
            new Object[]{142002934, 17891}, new Object[]{33273941, 84046}, new Object[]{33273941, 84046},
            new Object[]{33273941, 84046}, new Object[]{33273941, 84046}, new Object[]{1002250922, 177388},
            new Object[]{1002250922, 177388});
    Assert.assertEquals(resultTable.getDataSchema().getColumnNames(), selectionResults.getColumns().toArray());
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(selectionResults.getRows().get(i), resultTable.getRows().get(i));
      Assert.assertEquals(resultTable.getRows().get(i), rows.get(i));
    }

    query = "SELECT column1, column3 FROM testTable ORDER BY column3 option(preserveType=true)";
    brokerResponse = getBrokerResponseForPqlQuery(query);
    selectionResults = brokerResponse.getSelectionResults();
    query = "SELECT column1, column3 FROM testTable ORDER BY column3 option(responseFormat=sql)";
    brokerResponseSQL = getBrokerResponseForPqlQuery(query);
    resultTable = brokerResponseSQL.getResultTable();
    Assert.assertEquals(resultTable.getDataSchema().getColumnNames().length, 2);
    Assert.assertEquals(resultTable.getDataSchema().getColumnNames(), new String[]{"column1", "column3"});
    Assert.assertEquals(resultTable.getDataSchema().getColumnDataTypes(),
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT});
    Assert.assertEquals(resultTable.getRows().size(), 10);
    rows = Lists
        .newArrayList(new Object[]{142002934, 17891}, new Object[]{142002934, 17891}, new Object[]{142002934, 17891},
            new Object[]{142002934, 17891}, new Object[]{33273941, 84046}, new Object[]{33273941, 84046},
            new Object[]{33273941, 84046}, new Object[]{33273941, 84046}, new Object[]{1002250922, 177388},
            new Object[]{1002250922, 177388});
    Assert.assertEquals(resultTable.getDataSchema().getColumnNames(), selectionResults.getColumns().toArray());
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(selectionResults.getRows().get(i), resultTable.getRows().get(i));
      Assert.assertEquals(resultTable.getRows().get(i), rows.get(i));
    }
  }

  @Test
  public void testThreadCpuTime() {
    String query = "SELECT * FROM testTable";

    ThreadTimer.setThreadCpuTimeMeasurementEnabled(true);
    // NOTE: Need to check whether thread CPU time measurement is enabled because some environments might not support
    //       ThreadMXBean.getCurrentThreadCpuTime()
    if (ThreadTimer.isThreadCpuTimeMeasurementEnabled()) {
      BrokerResponseNative brokerResponse = getBrokerResponseForSqlQuery(query);
      Assert.assertTrue(brokerResponse.getOfflineThreadCpuTimeNs() > 0);
      Assert.assertTrue(brokerResponse.getRealtimeThreadCpuTimeNs() > 0);
    }

    ThreadTimer.setThreadCpuTimeMeasurementEnabled(false);
    BrokerResponseNative brokerResponse = getBrokerResponseForSqlQuery(query);
    Assert.assertEquals(brokerResponse.getOfflineThreadCpuTimeNs(), 0);
    Assert.assertEquals(brokerResponse.getRealtimeThreadCpuTimeNs(), 0);
  }
}
