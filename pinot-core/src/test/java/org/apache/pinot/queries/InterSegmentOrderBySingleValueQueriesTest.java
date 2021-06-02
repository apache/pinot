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

import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.response.broker.AggregationResult;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.GroupByResult;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Tests order by queries
 */
public class InterSegmentOrderBySingleValueQueriesTest extends BaseSingleValueQueriesTest {

  @Test(dataProvider = "orderBySQLResultTableProvider")
  public void testGroupByOrderBySQLResponse(String query, List<Object[]> expectedResults, long expectedNumDocsScanned,
      long expectedNumEntriesScannedInFilter, long expectedNumEntriesScannedPostFilter, long expectedNumTotalDocs,
      DataSchema expectedDataSchema) {
    BrokerResponseNative brokerResponse = getBrokerResponseForSqlQuery(query);
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, expectedNumDocsScanned, expectedNumEntriesScannedInFilter,
            expectedNumEntriesScannedPostFilter, expectedNumTotalDocs, expectedResults, expectedResults.size(),
            expectedDataSchema);
  }

  @Test(dataProvider = "orderByPQLResultProvider")
  public void testGroupByOrderByPQLResponse(String query, List<String[]> expectedGroups,
      List<List<Serializable>> expectedValues, long expectedNumDocsScanned, long expectedNumEntriesScannedInFilter,
      long expectedNumEntriesScannedPostFilter, long expectedNumTotalDocs) {
    Map<String, String> queryOptions = new HashMap<>(2);
    queryOptions.put(QueryOptionKey.GROUP_BY_MODE, Request.SQL);
    queryOptions.put(QueryOptionKey.RESPONSE_FORMAT, Request.PQL);
    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    QueriesTestUtils.testInterSegmentGroupByOrderByResultPQL(brokerResponse, expectedNumDocsScanned,
        expectedNumEntriesScannedInFilter, expectedNumEntriesScannedPostFilter, expectedNumTotalDocs, expectedGroups,
        expectedValues, false);

    queryOptions.put(QueryOptionKey.PRESERVE_TYPE, "true");
    brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    QueriesTestUtils.testInterSegmentGroupByOrderByResultPQL(brokerResponse, expectedNumDocsScanned,
        expectedNumEntriesScannedInFilter, expectedNumEntriesScannedPostFilter, expectedNumTotalDocs, expectedGroups,
        expectedValues, true);
  }

  /**
   * Tests the in-segment trim option for GroupBy OrderBy query
   */
  @Test(dataProvider = "orderBySQLResultTableProvider")
  public void testGroupByOrderByTrimOptSQLLowLimitResponse(String query, List<Object[]> expectedResults,
      long expectedNumDocsScanned, long expectedNumEntriesScannedInFilter, long expectedNumEntriesScannedPostFilter,
      long expectedNumTotalDocs, DataSchema expectedDataSchema) {
    expectedResults = expectedResults.subList(0, expectedResults.size() / 2);

    if (query.toUpperCase().contains("LIMIT")) {
      String[] keyWords = query.split(" ");
      keyWords[keyWords.length - 1] = String.valueOf(expectedResults.size());
      query = String.join(" ", keyWords);
    } else {
      query += " LIMIT " + expectedResults.size();
    }

    InstancePlanMakerImplV2 planMaker = new InstancePlanMakerImplV2(expectedResults.size(), true);
    BrokerResponseNative brokerResponse = getBrokerResponseForSqlQuery(query, planMaker);
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, expectedNumDocsScanned, expectedNumEntriesScannedInFilter,
            expectedNumEntriesScannedPostFilter, expectedNumTotalDocs, expectedResults, expectedResults.size(),
            expectedDataSchema);
  }

  /**
   * Tests the in-segment build option for GroupBy OrderBy query. (No trim)
   */
  @Test(dataProvider = "orderBySQLResultTableProvider")
  public void testGroupByOrderByTrimOptHighLimitSQLResponse(String query, List<Object[]> expectedResults,
      long expectedNumDocsScanned, long expectedNumEntriesScannedInFilter, long expectedNumEntriesScannedPostFilter,
      long expectedNumTotalDocs, DataSchema expectedDataSchema) {
    InstancePlanMakerImplV2 planMaker = new InstancePlanMakerImplV2(expectedResults.size() + 1, true);
    BrokerResponseNative brokerResponse = getBrokerResponseForSqlQuery(query, planMaker);
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, expectedNumDocsScanned, expectedNumEntriesScannedInFilter,
            expectedNumEntriesScannedPostFilter, expectedNumTotalDocs, expectedResults, expectedResults.size(),
            expectedDataSchema);
  }

  /**
   * Tests the query options for groupByMode, responseFormat.
   * pql, pql - does not execute order by, returns aggregationResults
   * pql, sql - does not execute order by, returns aggregationResults
   * sql, pql - executes order by, but returns aggregationResults. Keys across all aggregations will be same
   * sql, sql - executes order by, returns resultsTable
   */
  @Test
  public void testQueryOptions() {
    String query = "SELECT SUM(column1), MIN(column6) FROM testTable GROUP BY column11 ORDER BY column11";
    Map<String, String> queryOptions = null;

    // default PQL, PQL
    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    Assert.assertNotNull(brokerResponse.getAggregationResults());
    Assert.assertNull(brokerResponse.getResultTable());

    // PQL, PQL - don't execute order by, return aggregationResults
    queryOptions = new HashMap<>(2);
    queryOptions.put(QueryOptionKey.GROUP_BY_MODE, Request.PQL);
    queryOptions.put(QueryOptionKey.RESPONSE_FORMAT, Request.PQL);
    brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    Assert.assertNotNull(brokerResponse.getAggregationResults());
    Assert.assertNull(brokerResponse.getResultTable());

    // PQL, SQL - don't execute order by, return aggregationResults.
    query = "SELECT SUM(column1) FROM testTable GROUP BY column11 ORDER BY column11";
    queryOptions.put(QueryOptionKey.GROUP_BY_MODE, Request.PQL);
    queryOptions.put(QueryOptionKey.RESPONSE_FORMAT, Request.SQL);
    brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    Assert.assertNull(brokerResponse.getAggregationResults());
    Assert.assertNotNull(brokerResponse.getResultTable());

    // SQL, PQL - execute the order by, but return aggregationResults. Keys should be same across aggregation functions.
    query = "SELECT SUM(column1), MIN(column6) FROM testTable GROUP BY column11 ORDER BY column11";
    queryOptions.put(QueryOptionKey.GROUP_BY_MODE, Request.SQL);
    queryOptions.put(QueryOptionKey.RESPONSE_FORMAT, Request.PQL);
    brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    Assert.assertNotNull(brokerResponse.getAggregationResults());
    Assert.assertNull(brokerResponse.getResultTable());
    List<AggregationResult> aggregationResults = brokerResponse.getAggregationResults();
    Assert.assertEquals(aggregationResults.size(), 2);
    Iterator<GroupByResult> it1 = aggregationResults.get(0).getGroupByResult().iterator();
    Iterator<GroupByResult> it2 = aggregationResults.get(1).getGroupByResult().iterator();
    while (it1.hasNext() && it2.hasNext()) {
      GroupByResult groupByResult1 = it1.next();
      GroupByResult groupByResult2 = it2.next();
      Assert.assertEquals(groupByResult1.getGroup(), groupByResult2.getGroup());
    }

    // SQL, SQL - execute order by, return resultsTable
    queryOptions.put(QueryOptionKey.GROUP_BY_MODE, Request.SQL);
    queryOptions.put(QueryOptionKey.RESPONSE_FORMAT, Request.SQL);
    brokerResponse = getBrokerResponseForPqlQuery(query, queryOptions);
    Assert.assertNull(brokerResponse.getAggregationResults());
    Assert.assertNotNull(brokerResponse.getResultTable());
    DataSchema dataSchema = brokerResponse.getResultTable().getDataSchema();
    Assert.assertEquals(dataSchema.size(), 3);
    Assert.assertEquals(dataSchema.getColumnNames(), new String[]{"column11", "sum(column1)", "min(column6)"});
    Assert.assertEquals(dataSchema.getColumnDataTypes(),
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE});
  }

  /**
   * Provides various combinations of order by in ResultTable.
   * In order to calculate the expected results, the results from a group by were taken, and then ordered accordingly.
   */
  @DataProvider(name = "orderBySQLResultTableProvider")
  public Object[][] orderBySQLResultTableProvider() {

    List<Object[]> data = new ArrayList<>();
    String query;
    List<Object[]> results;
    DataSchema dataSchema;
    long numDocsScanned = 120000;
    long numEntriesScannedInFilter = 0;
    long numEntriesScannedPostFilter;
    long numTotalDocs = 120000;

    // order by one of the group by columns
    query = "SELECT column11, SUM(column1) FROM testTable GROUP BY column11 ORDER BY column11";
    results = Lists.newArrayList(new Object[]{"", 5935285005452.0}, new Object[]{"P", 88832999206836.0},
        new Object[]{"gFuH", 63202785888.0}, new Object[]{"o", 18105331533948.0}, new Object[]{"t", 16331923219264.0});
    dataSchema = new DataSchema(new String[]{"column11", "sum(column1)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    numEntriesScannedPostFilter = 240000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs, dataSchema});

    // order by one of the group by columns DESC
    query = "SELECT column11, sum(column1) FROM testTable GROUP BY column11 ORDER BY column11 DESC";
    results = Lists.newArrayList(results);
    Collections.reverse(results);
    dataSchema = new DataSchema(new String[]{"column11", "sum(column1)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    numEntriesScannedPostFilter = 240000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs, dataSchema});

    // order by one of the group by columns, LIMIT less than default
    query = "SELECT column11, Sum(column1) FROM testTable GROUP BY column11 ORDER BY column11 LIMIT 3";
    results = Lists.newArrayList(results);
    Collections.reverse(results);
    results = results.subList(0, 3);
    dataSchema = new DataSchema(new String[]{"column11", "sum(column1)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    numEntriesScannedPostFilter = 240000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs, dataSchema});

    // group by 2 dimensions, order by both, tie breaker
    query =
        "SELECT column11, column12, SUM(column1) FROM testTable GROUP BY column11, column12 ORDER BY column11, column12";
    results = Lists.newArrayList(new Object[]{"", "HEuxNvH", 3789390396216.0},
        new Object[]{"", "KrNxpdycSiwoRohEiTIlLqDHnx", 733802350944.0},
        new Object[]{"", "MaztCmmxxgguBUxPti", 1333941430664.0}, new Object[]{"", "dJWwFk", 55470665124.0000},
        new Object[]{"", "oZgnrlDEtjjVpUoFLol", 22680162504.0}, new Object[]{"P", "HEuxNvH", 21998672845052.0},
        new Object[]{"P", "KrNxpdycSiwoRohEiTIlLqDHnx", 18069909216728.0},
        new Object[]{"P", "MaztCmmxxgguBUxPti", 27177029040008.0},
        new Object[]{"P", "TTltMtFiRqUjvOG", 4462670055540.0}, new Object[]{"P", "XcBNHe", 120021767504.0});
    dataSchema = new DataSchema(new String[]{"column11", "column12", "sum(column1)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    numEntriesScannedPostFilter = 360000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs, dataSchema});

    // group by 2 columns, order by both, LIMIT more than default
    query =
        "SELECT column11, column12, SUM(column1) FROM testTable GROUP BY column11, column12 ORDER BY column11, column12 LIMIT 15";
    results = Lists.newArrayList(results);
    results.add(new Object[]{"P", "dJWwFk", 6224665921376.0});
    results.add(new Object[]{"P", "fykKFqiw", 1574451324140.0});
    results.add(new Object[]{"P", "gFuH", 860077643636.0});
    results.add(new Object[]{"P", "oZgnrlDEtjjVpUoFLol", 8345501392852.0});
    results.add(new Object[]{"gFuH", "HEuxNvH", 29872400856.0});
    dataSchema = new DataSchema(new String[]{"column11", "column12", "sum(column1)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    numEntriesScannedPostFilter = 360000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs, dataSchema});

    // group by 2 columns, order by both, one of them DESC
    query =
        "SELECT column11, column12, SUM(column1) FROM testTable GROUP BY column11, column12 ORDER BY column11, column12 DESC";
    results = Lists.newArrayList(new Object[]{"", "oZgnrlDEtjjVpUoFLol", 22680162504.0},
        new Object[]{"", "dJWwFk", 55470665124.0000}, new Object[]{"", "MaztCmmxxgguBUxPti", 1333941430664.0},
        new Object[]{"", "KrNxpdycSiwoRohEiTIlLqDHnx", 733802350944.0}, new Object[]{"", "HEuxNvH", 3789390396216.0},
        new Object[]{"P", "oZgnrlDEtjjVpUoFLol", 8345501392852.0}, new Object[]{"P", "gFuH", 860077643636.0},
        new Object[]{"P", "fykKFqiw", 1574451324140.0}, new Object[]{"P", "dJWwFk", 6224665921376.0},
        new Object[]{"P", "XcBNHe", 120021767504.0});
    dataSchema = new DataSchema(new String[]{"column11", "column12", "sum(column1)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    numEntriesScannedPostFilter = 360000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs, dataSchema});

    // order by group by column and an aggregation
    query =
        "SELECT column11, column12, SUM(column1) FROM testTable GROUP BY column11, column12 ORDER BY column11, sum(column1)";
    results = Lists.newArrayList(new Object[]{"", "oZgnrlDEtjjVpUoFLol", 22680162504.0},
        new Object[]{"", "dJWwFk", 55470665124.0000}, new Object[]{"", "KrNxpdycSiwoRohEiTIlLqDHnx", 733802350944.0},
        new Object[]{"", "MaztCmmxxgguBUxPti", 1333941430664.0}, new Object[]{"", "HEuxNvH", 3789390396216.0},
        new Object[]{"P", "XcBNHe", 120021767504.0}, new Object[]{"P", "gFuH", 860077643636.0},
        new Object[]{"P", "fykKFqiw", 1574451324140.0}, new Object[]{"P", "TTltMtFiRqUjvOG", 4462670055540.0},
        new Object[]{"P", "dJWwFk", 6224665921376.0});
    dataSchema = new DataSchema(new String[]{"column11", "column12", "sum(column1)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    numEntriesScannedPostFilter = 360000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs, dataSchema});

    // order by only aggregation, DESC, LIMIT
    query =
        "SELECT column11, column12, SUM(column1) FROM testTable GROUP BY column11, column12 ORDER BY SUM(column1) DESC LIMIT 50";
    results = Lists.newArrayList(new Object[]{"P", "MaztCmmxxgguBUxPti", 27177029040008.0},
        new Object[]{"P", "HEuxNvH", 21998672845052.0},
        new Object[]{"P", "KrNxpdycSiwoRohEiTIlLqDHnx", 18069909216728.0},
        new Object[]{"P", "oZgnrlDEtjjVpUoFLol", 8345501392852.0},
        new Object[]{"o", "MaztCmmxxgguBUxPti", 6905624581072.0}, new Object[]{"P", "dJWwFk", 6224665921376.0},
        new Object[]{"o", "HEuxNvH", 5026384681784.0}, new Object[]{"t", "MaztCmmxxgguBUxPti", 4492405624940.0},
        new Object[]{"P", "TTltMtFiRqUjvOG", 4462670055540.0}, new Object[]{"t", "HEuxNvH", 4424489490364.0},
        new Object[]{"o", "KrNxpdycSiwoRohEiTIlLqDHnx", 4051812250524.0}, new Object[]{"", "HEuxNvH", 3789390396216.0},
        new Object[]{"t", "KrNxpdycSiwoRohEiTIlLqDHnx", 3529048341192.0},
        new Object[]{"P", "fykKFqiw", 1574451324140.0}, new Object[]{"t", "dJWwFk", 1349058948804.0},
        new Object[]{"", "MaztCmmxxgguBUxPti", 1333941430664.0}, new Object[]{"o", "dJWwFk", 1152689463360.0},
        new Object[]{"t", "oZgnrlDEtjjVpUoFLol", 1039101333316.0}, new Object[]{"P", "gFuH", 860077643636.0},
        new Object[]{"", "KrNxpdycSiwoRohEiTIlLqDHnx", 733802350944.0},
        new Object[]{"o", "oZgnrlDEtjjVpUoFLol", 699381633640.0}, new Object[]{"t", "TTltMtFiRqUjvOG", 675238030848.0},
        new Object[]{"t", "fykKFqiw", 480973878052.0}, new Object[]{"t", "gFuH", 330331507792.0},
        new Object[]{"o", "TTltMtFiRqUjvOG", 203835153352.0}, new Object[]{"P", "XcBNHe", 120021767504.0},
        new Object[]{"o", "fykKFqiw", 62975165296.0}, new Object[]{"", "dJWwFk", 55470665124.0000},
        new Object[]{"gFuH", "HEuxNvH", 29872400856.0}, new Object[]{"gFuH", "MaztCmmxxgguBUxPti", 29170832184.0},
        new Object[]{"", "oZgnrlDEtjjVpUoFLol", 22680162504.0}, new Object[]{"t", "XcBNHe", 11276063956.0},
        new Object[]{"gFuH", "KrNxpdycSiwoRohEiTIlLqDHnx", 4159552848.0}, new Object[]{"o", "gFuH", 2628604920.0});
    dataSchema = new DataSchema(new String[]{"column11", "column12", "sum(column1)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    numEntriesScannedPostFilter = 360000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs, dataSchema});

    // multiple aggregations (group-by column not in select)
    query = "SELECT sum(column1), MIN(column6) FROM testTable GROUP BY column11 ORDER BY column11";
    results = Lists.newArrayList(new Object[]{5935285005452.0, 2.96467636E8}, new Object[]{88832999206836.0, 1689277.0},
        new Object[]{63202785888.0, 2.96467636E8}, new Object[]{18105331533948.0, 2.96467636E8},
        new Object[]{16331923219264.0, 1980174.0});
    dataSchema = new DataSchema(new String[]{"sum(column1)", "min(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE});
    numEntriesScannedPostFilter = 360000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs, dataSchema});

    // order by aggregation with space/tab in order by
    query =
        "SELECT column11, column12, SUM(column1) FROM testTable GROUP BY column11, column12 ORDER BY SUM  (\tcolumn1) DESC LIMIT 3";
    results = Lists.newArrayList(new Object[]{"P", "MaztCmmxxgguBUxPti", 27177029040008.0},
        new Object[]{"P", "HEuxNvH", 21998672845052.0},
        new Object[]{"P", "KrNxpdycSiwoRohEiTIlLqDHnx", 18069909216728.0});
    dataSchema = new DataSchema(new String[]{"column11", "column12", "sum(column1)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    numEntriesScannedPostFilter = 360000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs, dataSchema});

    // order by an aggregation DESC, and group by column
    query = "SELECT column12, MIN(column6) FROM testTable GROUP BY column12 ORDER BY Min(column6) DESC, column12";
    results = Lists.newArrayList(new Object[]{"XcBNHe", 329467557.0}, new Object[]{"fykKFqiw", 296467636.0},
        new Object[]{"gFuH", 296467636.0}, new Object[]{"HEuxNvH", 6043515.0},
        new Object[]{"MaztCmmxxgguBUxPti", 6043515.0}, new Object[]{"dJWwFk", 6043515.0},
        new Object[]{"KrNxpdycSiwoRohEiTIlLqDHnx", 1980174.0}, new Object[]{"TTltMtFiRqUjvOG", 1980174.0},
        new Object[]{"oZgnrlDEtjjVpUoFLol", 1689277.0});
    dataSchema = new DataSchema(new String[]{"column12", "min(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    numEntriesScannedPostFilter = 240000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs, dataSchema});

    // aggregation in order-by but not in select
    query = "SELECT column12 FROM testTable GROUP BY column12 ORDER BY Min(column6) DESC, column12";
    results = Lists
        .newArrayList(new Object[]{"XcBNHe"}, new Object[]{"fykKFqiw"}, new Object[]{"gFuH"}, new Object[]{"HEuxNvH"},
            new Object[]{"MaztCmmxxgguBUxPti"}, new Object[]{"dJWwFk"}, new Object[]{"KrNxpdycSiwoRohEiTIlLqDHnx"},
            new Object[]{"TTltMtFiRqUjvOG"}, new Object[]{"oZgnrlDEtjjVpUoFLol"});
    dataSchema =
        new DataSchema(new String[]{"column12"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING});
    numEntriesScannedPostFilter = 240000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs, dataSchema});

    // multiple aggregations in order-by but not in select
    query = "SELECT column12 FROM testTable GROUP BY column12 ORDER BY Min(column6) DESC, SUM(column1) LIMIT 3";
    results = Lists.newArrayList(new Object[]{"XcBNHe"}, new Object[]{"gFuH"}, new Object[]{"fykKFqiw"});
    dataSchema =
        new DataSchema(new String[]{"column12"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING});
    numEntriesScannedPostFilter = 360000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs, dataSchema});

    // multiple aggregations in order-by, some in select
    query =
        "SELECT column12, MIN(column6) FROM testTable GROUP BY column12 ORDER BY Min(column6) DESC, SUM(column1) LIMIT 3";
    results = Lists.newArrayList(new Object[]{"XcBNHe", 329467557.0}, new Object[]{"gFuH", 296467636.0},
        new Object[]{"fykKFqiw", 296467636.0});
    dataSchema = new DataSchema(new String[]{"column12", "min(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    numEntriesScannedPostFilter = 360000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs, dataSchema});

    // numeric dimension should follow numeric ordering
    query = "select column17, count(*) from testTable group by column17 order by column17 limit 15";
    results = Lists
        .newArrayList(new Object[]{83386499, 2924L}, new Object[]{217787432, 3892L}, new Object[]{227908817, 6564L},
            new Object[]{402773817, 7304L}, new Object[]{423049234, 6556L}, new Object[]{561673250, 7420L},
            new Object[]{635942547, 3308L}, new Object[]{638936844, 3816L}, new Object[]{939479517, 3116L},
            new Object[]{984091268, 3824L}, new Object[]{1230252339, 5620L}, new Object[]{1284373442, 7428L},
            new Object[]{1555255521, 2900L}, new Object[]{1618904660, 2744L}, new Object[]{1670085862, 3388L});
    dataSchema = new DataSchema(new String[]{"column17", "count(*)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.LONG});
    numEntriesScannedPostFilter = 120000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs, dataSchema});

    // group by UDF order by UDF
    query =
        "SELECT sub(column1, 100000), COUNT(*) FROM testTable GROUP BY sub(column1, 100000) ORDER BY sub(column1, 100000) LIMIT 3";
    results = Lists.newArrayList(new Object[]{140528.0, 28L}, new Object[]{194355.0, 12L}, new Object[]{532157.0, 12L});
    dataSchema = new DataSchema(new String[]{"sub(column1,'100000')", "count(*)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.LONG});
    numEntriesScannedPostFilter = 120000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs, dataSchema});

    // space/tab in UDF
    query =
        "SELECT sub(column1, 100000), COUNT(*) FROM testTable GROUP BY sub(column1, 100000) ORDER BY SUB(   column1, 100000\t) LIMIT 3";
    results = Lists.newArrayList(new Object[]{140528.0, 28L}, new Object[]{194355.0, 12L}, new Object[]{532157.0, 12L});
    dataSchema = new DataSchema(new String[]{"sub(column1,'100000')", "count(*)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.LONG});
    numEntriesScannedPostFilter = 120000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs, dataSchema});

    // Object type aggregation - comparable intermediate results (AVG, MINMAXRANGE)
    query = "SELECT column11, AVG(column6) FROM testTable GROUP BY column11  ORDER BY column11";
    results = Lists.newArrayList(new Object[]{"", 296467636.0}, new Object[]{"P", 909380310.3521485},
        new Object[]{"gFuH", 296467636.0}, new Object[]{"o", 296467636.0}, new Object[]{"t", 526245333.3900426});
    dataSchema = new DataSchema(new String[]{"column11", "avg(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    numEntriesScannedPostFilter = 240000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs, dataSchema});

    query = "SELECT column11, AVG(column6) FROM testTable GROUP BY column11 ORDER BY AVG(column6), column11 DESC";
    results = Lists
        .newArrayList(new Object[]{"o", 296467636.0}, new Object[]{"gFuH", 296467636.0}, new Object[]{"", 296467636.0},
            new Object[]{"t", 526245333.3900426}, new Object[]{"P", 909380310.3521485});
    dataSchema = new DataSchema(new String[]{"column11", "avg(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    numEntriesScannedPostFilter = 240000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs, dataSchema});

    // Object type aggregation - non comparable intermediate results (DISTINCTCOUNT)
    query = "SELECT column12, DISTINCTCOUNT(column11) FROM testTable GROUP BY column12 ORDER BY column12";
    results = Lists.newArrayList(new Object[]{"HEuxNvH", 5}, new Object[]{"KrNxpdycSiwoRohEiTIlLqDHnx", 5},
        new Object[]{"MaztCmmxxgguBUxPti", 5}, new Object[]{"TTltMtFiRqUjvOG", 3}, new Object[]{"XcBNHe", 2},
        new Object[]{"dJWwFk", 4}, new Object[]{"fykKFqiw", 3}, new Object[]{"gFuH", 3},
        new Object[]{"oZgnrlDEtjjVpUoFLol", 4});
    dataSchema = new DataSchema(new String[]{"column12", "distinctcount(column11)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT});
    numEntriesScannedPostFilter = 240000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs, dataSchema});

    query =
        "SELECT column12, DISTINCTCOUNT(column11) FROM testTable GROUP BY column12 ORDER BY DistinctCount(column11), column12 DESC";
    results = Lists.newArrayList(new Object[]{"XcBNHe", 2}, new Object[]{"gFuH", 3}, new Object[]{"fykKFqiw", 3},
        new Object[]{"TTltMtFiRqUjvOG", 3}, new Object[]{"oZgnrlDEtjjVpUoFLol", 4}, new Object[]{"dJWwFk", 4},
        new Object[]{"MaztCmmxxgguBUxPti", 5}, new Object[]{"KrNxpdycSiwoRohEiTIlLqDHnx", 5},
        new Object[]{"HEuxNvH", 5});
    dataSchema = new DataSchema(new String[]{"column12", "distinctcount(column11)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT});
    numEntriesScannedPostFilter = 240000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs, dataSchema});

    // percentile
    query =
        "SELECT column11, percentile90(column6) FROM testTable GROUP BY column11  ORDER BY PERCENTILE90(column6), column11 LIMIT 3";
    results = Lists.newArrayList(new Object[]{"", 2.96467636E8}, new Object[]{"gFuH", 2.96467636E8},
        new Object[]{"o", 2.96467636E8});
    dataSchema = new DataSchema(new String[]{"column11", "percentile90(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    numEntriesScannedPostFilter = 240000;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs, dataSchema});

    // empty results
    query =
        "SELECT column11, MIN(column6) FROM testTable where column12='non-existent-value' GROUP BY column11 order by column11";
    results = new ArrayList<>(0);
    dataSchema = new DataSchema(new String[]{"column11", "min(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    numDocsScanned = 0;
    numEntriesScannedPostFilter = 0;
    data.add(
        new Object[]{query, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs, dataSchema});

    return data.toArray(new Object[data.size()][]);
  }

  /**
   * Provides various combinations of order by in PQL style results (GroupByResults).
   * In order to calculate the expected results, the results from a group by were taken, and then ordered accordingly.
   */
  @DataProvider(name = "orderByPQLResultProvider")
  public Object[][] orderByPQLResultProvider() {

    List<Object[]> data = new ArrayList<>();
    String query;
    List<String[]> groups;
    List<Serializable> result1;
    List<Serializable> result2;
    List<List<Serializable>> results;
    long numDocsScanned = 120000;
    long numEntriesScannedInFilter = 0;
    long numEntriesScannedPostFilter;
    long numTotalDocs = 120000;

    // order by one of the group by columns
    query = "SELECT SUM(column1) FROM testTable GROUP BY column11 ORDER BY column11";
    groups = Lists
        .newArrayList(new String[]{""}, new String[]{"P"}, new String[]{"gFuH"}, new String[]{"o"}, new String[]{"t"});
    result1 = Lists.newArrayList(5935285005452.0, 88832999206836.0, 63202785888.0, 18105331533948.0, 16331923219264.0);
    results = new ArrayList<>();
    results.add(result1);
    numEntriesScannedPostFilter = 240000;
    data.add(
        new Object[]{query, groups, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    // order by one of the group by columns DESC
    query = "SELECT SUM(column1) FROM testTable GROUP BY column11 ORDER BY column11 DESC";
    groups = Lists.newArrayList(groups);
    Collections.reverse(groups);
    result1 = Lists.newArrayList(result1);
    Collections.reverse(result1);
    results = new ArrayList<>();
    results.add(result1);
    numEntriesScannedPostFilter = 240000;
    data.add(
        new Object[]{query, groups, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    // order by one of the group by columns, TOP less than default
    query = "SELECT SUM(column1) FROM testTable GROUP BY column11 ORDER BY column11 TOP 3";
    groups = Lists.newArrayList(groups);
    Collections.reverse(groups);
    groups = groups.subList(0, 3);
    result1 = Lists.newArrayList(result1);
    Collections.reverse(result1);
    result1 = result1.subList(0, 3);
    results = new ArrayList<>();
    results.add(result1);
    numEntriesScannedPostFilter = 240000;
    data.add(
        new Object[]{query, groups, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    // group by 2 dimensions, order by both, tie breaker
    query = "SELECT SUM(column1) FROM testTable GROUP BY column11, column12 ORDER BY column11, column12";
    groups = Lists.newArrayList(new String[]{"", "HEuxNvH"}, new String[]{"", "KrNxpdycSiwoRohEiTIlLqDHnx"},
        new String[]{"", "MaztCmmxxgguBUxPti"}, new String[]{"", "dJWwFk"}, new String[]{"", "oZgnrlDEtjjVpUoFLol"},
        new String[]{"P", "HEuxNvH"}, new String[]{"P", "KrNxpdycSiwoRohEiTIlLqDHnx"},
        new String[]{"P", "MaztCmmxxgguBUxPti"}, new String[]{"P", "TTltMtFiRqUjvOG"}, new String[]{"P", "XcBNHe"});
    result1 = Lists.newArrayList(3789390396216.0, 733802350944.0, 1333941430664.0, 55470665124.0000, 22680162504.0,
        21998672845052.0, 18069909216728.0, 27177029040008.0, 4462670055540.0, 120021767504.0);
    results = new ArrayList<>();
    results.add(result1);
    numEntriesScannedPostFilter = 360000;
    data.add(
        new Object[]{query, groups, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    // group by 2 columns, order by both, TOP more than default
    query = "SELECT SUM(column1) FROM testTable GROUP BY column11, column12 ORDER BY column11, column12 TOP 15";
    groups = Lists.newArrayList(groups);
    groups.add(new String[]{"P", "dJWwFk"});
    groups.add(new String[]{"P", "fykKFqiw"});
    groups.add(new String[]{"P", "gFuH"});
    groups.add(new String[]{"P", "oZgnrlDEtjjVpUoFLol"});
    groups.add(new String[]{"gFuH", "HEuxNvH"});
    result1 = Lists.newArrayList(result1);
    result1.add(6224665921376.0);
    result1.add(1574451324140.0);
    result1.add(860077643636.0);
    result1.add(8345501392852.0);
    result1.add(29872400856.0);
    results = new ArrayList<>();
    results.add(result1);
    numEntriesScannedPostFilter = 360000;
    data.add(
        new Object[]{query, groups, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    // group by 2 columns, order by both, one of them DESC
    query = "SELECT SUM(column1) FROM testTable GROUP BY column11, column12 ORDER BY column11, column12 DESC";
    groups = Lists.newArrayList(new String[]{"", "oZgnrlDEtjjVpUoFLol"}, new String[]{"", "dJWwFk"},
        new String[]{"", "MaztCmmxxgguBUxPti"}, new String[]{"", "KrNxpdycSiwoRohEiTIlLqDHnx"},
        new String[]{"", "HEuxNvH"}, new String[]{"P", "oZgnrlDEtjjVpUoFLol"}, new String[]{"P", "gFuH"},
        new String[]{"P", "fykKFqiw"}, new String[]{"P", "dJWwFk"}, new String[]{"P", "XcBNHe"});
    result1 = Lists.newArrayList(22680162504.0, 55470665124.0000, 1333941430664.0, 733802350944.0, 3789390396216.0,
        8345501392852.0, 860077643636.0, 1574451324140.0, 6224665921376.0, 120021767504.0);
    results = new ArrayList<>();
    results.add(result1);
    numEntriesScannedPostFilter = 360000;
    data.add(
        new Object[]{query, groups, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    // order by group by column and an aggregation
    query = "SELECT SUM(column1) FROM testTable GROUP BY column11, column12 ORDER BY column11, SUM(column1)";
    groups = Lists.newArrayList(new String[]{"", "oZgnrlDEtjjVpUoFLol"}, new String[]{"", "dJWwFk"},
        new String[]{"", "KrNxpdycSiwoRohEiTIlLqDHnx"}, new String[]{"", "MaztCmmxxgguBUxPti"},
        new String[]{"", "HEuxNvH"}, new String[]{"P", "XcBNHe"}, new String[]{"P", "gFuH"},
        new String[]{"P", "fykKFqiw"}, new String[]{"P", "TTltMtFiRqUjvOG"}, new String[]{"P", "dJWwFk"});
    result1 = Lists
        .newArrayList(22680162504.0, 55470665124.0000, 733802350944.0, 1333941430664.0, 3789390396216.0, 120021767504.0,
            860077643636.0, 1574451324140.0, 4462670055540.0, 6224665921376.0);
    results = new ArrayList<>();
    results.add(result1);
    numEntriesScannedPostFilter = 360000;
    data.add(
        new Object[]{query, groups, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    // order by only aggregation, DESC, TOP
    query = "SELECT SUM(column1) FROM testTable GROUP BY column11, column12 ORDER BY SUM(column1) DESC TOP 50";
    groups = Lists.newArrayList(new String[]{"P", "MaztCmmxxgguBUxPti"}, new String[]{"P", "HEuxNvH"},
        new String[]{"P", "KrNxpdycSiwoRohEiTIlLqDHnx"}, new String[]{"P", "oZgnrlDEtjjVpUoFLol"},
        new String[]{"o", "MaztCmmxxgguBUxPti"}, new String[]{"P", "dJWwFk"}, new String[]{"o", "HEuxNvH"},
        new String[]{"t", "MaztCmmxxgguBUxPti"}, new String[]{"P", "TTltMtFiRqUjvOG"}, new String[]{"t", "HEuxNvH"},
        new String[]{"o", "KrNxpdycSiwoRohEiTIlLqDHnx"}, new String[]{"", "HEuxNvH"},
        new String[]{"t", "KrNxpdycSiwoRohEiTIlLqDHnx"}, new String[]{"P", "fykKFqiw"}, new String[]{"t", "dJWwFk"},
        new String[]{"", "MaztCmmxxgguBUxPti"}, new String[]{"o", "dJWwFk"}, new String[]{"t", "oZgnrlDEtjjVpUoFLol"},
        new String[]{"P", "gFuH"}, new String[]{"", "KrNxpdycSiwoRohEiTIlLqDHnx"},
        new String[]{"o", "oZgnrlDEtjjVpUoFLol"}, new String[]{"t", "TTltMtFiRqUjvOG"}, new String[]{"t", "fykKFqiw"},
        new String[]{"t", "gFuH"}, new String[]{"o", "TTltMtFiRqUjvOG"}, new String[]{"P", "XcBNHe"},
        new String[]{"o", "fykKFqiw"}, new String[]{"", "dJWwFk"}, new String[]{"gFuH", "HEuxNvH"},
        new String[]{"gFuH", "MaztCmmxxgguBUxPti"}, new String[]{"", "oZgnrlDEtjjVpUoFLol"},
        new String[]{"t", "XcBNHe"}, new String[]{"gFuH", "KrNxpdycSiwoRohEiTIlLqDHnx"}, new String[]{"o", "gFuH"});
    result1 = Lists.newArrayList(27177029040008.0, 21998672845052.0, 18069909216728.0, 8345501392852.0, 6905624581072.0,
        6224665921376.0, 5026384681784.0, 4492405624940.0, 4462670055540.0, 4424489490364.0, 4051812250524.0,
        3789390396216.0, 3529048341192.0, 1574451324140.0, 1349058948804.0, 1333941430664.0, 1152689463360.0,
        1039101333316.0, 860077643636.0, 733802350944.0, 699381633640.0, 675238030848.0, 480973878052.0, 330331507792.0,
        203835153352.0, 120021767504.0, 62975165296.0, 55470665124.0000, 29872400856.0, 29170832184.0, 22680162504.0,
        11276063956.0, 4159552848.0, 2628604920.0);
    results = new ArrayList<>();
    results.add(result1);
    numEntriesScannedPostFilter = 360000;
    data.add(
        new Object[]{query, groups, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    // multiple aggregations
    query = "SELECT SUM(column1), MIN(column6) FROM testTable GROUP BY column11 ORDER BY column11";
    groups = Lists
        .newArrayList(new String[]{""}, new String[]{"P"}, new String[]{"gFuH"}, new String[]{"o"}, new String[]{"t"});
    result1 = Lists.newArrayList(5935285005452.0, 88832999206836.0, 63202785888.0, 18105331533948.0, 16331923219264.0);
    result2 = Lists.newArrayList(2.96467636E8, 1689277.0, 2.96467636E8, 2.96467636E8, 1980174.0);
    results = new ArrayList<>();
    results.add(result1);
    results.add(result2);
    numEntriesScannedPostFilter = 360000;
    data.add(
        new Object[]{query, groups, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    // order by aggregation with space/tab in order by
    query = "SELECT SUM(column1) FROM testTable GROUP BY column11, column12 ORDER BY SUM  ( column1) DESC TOP 3";
    groups = Lists.newArrayList(new String[]{"P", "MaztCmmxxgguBUxPti"}, new String[]{"P", "HEuxNvH"},
        new String[]{"P", "KrNxpdycSiwoRohEiTIlLqDHnx"});
    result1 = Lists.newArrayList(27177029040008.0, 21998672845052.0, 18069909216728.0);
    results = new ArrayList<>();
    results.add(result1);
    numEntriesScannedPostFilter = 360000;
    data.add(
        new Object[]{query, groups, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    // order by an aggregation DESC, and group by column
    query = "SELECT MIN(column6) FROM testTable GROUP BY column12 ORDER BY MIN(column6) DESC, column12";
    groups = Lists
        .newArrayList(new String[]{"XcBNHe"}, new String[]{"fykKFqiw"}, new String[]{"gFuH"}, new String[]{"HEuxNvH"},
            new String[]{"MaztCmmxxgguBUxPti"}, new String[]{"dJWwFk"}, new String[]{"KrNxpdycSiwoRohEiTIlLqDHnx"},
            new String[]{"TTltMtFiRqUjvOG"}, new String[]{"oZgnrlDEtjjVpUoFLol"});
    result1 = Lists
        .newArrayList(329467557.0, 296467636.0, 296467636.0, 6043515.0, 6043515.0, 6043515.0, 1980174.0, 1980174.0,
            1689277.0);
    results = new ArrayList<>();
    results.add(result1);
    numEntriesScannedPostFilter = 240000;
    data.add(
        new Object[]{query, groups, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    // numeric dimension should follow numeric ordering
    query = "select count(*) from testTable group by column17 order by column17 top 15";
    groups = Lists.newArrayList(new String[]{"83386499"}, new String[]{"217787432"}, new String[]{"227908817"},
        new String[]{"402773817"}, new String[]{"423049234"}, new String[]{"561673250"}, new String[]{"635942547"},
        new String[]{"638936844"}, new String[]{"939479517"}, new String[]{"984091268"}, new String[]{"1230252339"},
        new String[]{"1284373442"}, new String[]{"1555255521"}, new String[]{"1618904660"}, new String[]{"1670085862"});
    result1 = Lists
        .newArrayList(2924L, 3892L, 6564L, 7304L, 6556L, 7420L, 3308L, 3816L, 3116L, 3824L, 5620L, 7428L, 2900L, 2744L,
            3388L);
    results = new ArrayList<>();
    results.add(result1);
    numEntriesScannedPostFilter = 120000;
    data.add(
        new Object[]{query, groups, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    // group by UDF order by UDF
    query = "SELECT COUNT(*) FROM testTable GROUP BY sub(column1, 100000) TOP 3 ORDER BY sub(column1, 100000)";
    groups = Lists.newArrayList(new String[]{"140528.0"}, new String[]{"194355.0"}, new String[]{"532157.0"});
    result1 = Lists.newArrayList(28L, 12L, 12L);
    results = new ArrayList<>();
    results.add(result1);
    numEntriesScannedPostFilter = 120000;
    data.add(
        new Object[]{query, groups, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    // spaces in UDF
    query = "SELECT COUNT(*) FROM testTable GROUP BY sub(column1, 100000) TOP 3 ORDER BY SUB(   column1, 100000 )";
    groups = Lists.newArrayList(new String[]{"140528.0"}, new String[]{"194355.0"}, new String[]{"532157.0"});
    result1 = Lists.newArrayList(28L, 12L, 12L);
    results = new ArrayList<>();
    results.add(result1);
    numEntriesScannedPostFilter = 120000;
    data.add(
        new Object[]{query, groups, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    // Object type aggregation - comparable intermediate results (AVG, MINMAXRANGE)
    query = "SELECT AVG(column6) FROM testTable GROUP BY column11  ORDER BY column11";
    groups = Lists
        .newArrayList(new String[]{""}, new String[]{"P"}, new String[]{"gFuH"}, new String[]{"o"}, new String[]{"t"});
    result1 = Lists.newArrayList(296467636.0, 909380310.3521485, 296467636.0, 296467636.0, 526245333.3900426);
    results = new ArrayList<>();
    results.add(result1);
    numEntriesScannedPostFilter = 240000;
    data.add(
        new Object[]{query, groups, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    query = "SELECT AVG(column6) FROM testTable GROUP BY column11 ORDER BY AVG(column6), column11 DESC";
    groups = Lists
        .newArrayList(new String[]{"o"}, new String[]{"gFuH"}, new String[]{""}, new String[]{"t"}, new String[]{"P"});
    result1 = Lists.newArrayList(296467636.0, 296467636.0, 296467636.0, 526245333.3900426, 909380310.3521485);
    results = new ArrayList<>();
    results.add(result1);
    numEntriesScannedPostFilter = 240000;
    data.add(
        new Object[]{query, groups, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    // Object type aggregation - non comparable intermediate results (DISTINCTCOUNT)
    query = "SELECT DISTINCTCOUNT(column11) FROM testTable GROUP BY column12 ORDER BY column12";
    groups = Lists.newArrayList(new String[]{"HEuxNvH"}, new String[]{"KrNxpdycSiwoRohEiTIlLqDHnx"},
        new String[]{"MaztCmmxxgguBUxPti"}, new String[]{"TTltMtFiRqUjvOG"}, new String[]{"XcBNHe"},
        new String[]{"dJWwFk"}, new String[]{"fykKFqiw"}, new String[]{"gFuH"}, new String[]{"oZgnrlDEtjjVpUoFLol"});
    result1 = Lists.newArrayList(5, 5, 5, 3, 2, 4, 3, 3, 4);
    results = new ArrayList<>();
    results.add(result1);
    numEntriesScannedPostFilter = 240000;
    data.add(
        new Object[]{query, groups, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    query =
        "SELECT DISTINCTCOUNT(column11) FROM testTable GROUP BY column12 ORDER BY DISTINCTCOUNT(column11), column12 DESC";
    groups = Lists.newArrayList(new String[]{"XcBNHe"}, new String[]{"gFuH"}, new String[]{"fykKFqiw"},
        new String[]{"TTltMtFiRqUjvOG"}, new String[]{"oZgnrlDEtjjVpUoFLol"}, new String[]{"dJWwFk"},
        new String[]{"MaztCmmxxgguBUxPti"}, new String[]{"KrNxpdycSiwoRohEiTIlLqDHnx"}, new String[]{"HEuxNvH"});
    result1 = Lists.newArrayList(2, 3, 3, 3, 4, 4, 5, 5, 5);
    results = new ArrayList<>();
    results.add(result1);
    numEntriesScannedPostFilter = 240000;
    data.add(
        new Object[]{query, groups, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    // empty results
    query =
        "SELECT MIN(column6) FROM testTable where column12='non-existent-value' GROUP BY column11 order by column11";
    groups = Collections.emptyList();
    results = Collections.singletonList(Collections.emptyList());
    numDocsScanned = 0;
    numEntriesScannedPostFilter = 0;
    data.add(
        new Object[]{query, groups, results, numDocsScanned, numEntriesScannedInFilter, numEntriesScannedPostFilter, numTotalDocs});

    return data.toArray(new Object[data.size()][]);
  }
}
