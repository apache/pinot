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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.spi.utils.BytesUtils;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class InterSegmentAggregationMultiValueQueriesTest extends BaseMultiValueQueriesTest {
  private static final String SV_GROUP_BY = " group by column8";
  private static final String MV_GROUP_BY = " group by column7";
  private static final String ORDER_BY_ALIAS = " order by cnt_column6 DESC";

  @Test
  public void testCountMV() {
    String query = "SELECT COUNTMV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L, new String[]{"426752"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 62480L, 1101664L, 62480L, 400000L, new String[]{"62480"});

    brokerResponse = getBrokerResponseForPqlQuery(query + SV_GROUP_BY);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L, new String[]{"231056"});

    brokerResponse = getBrokerResponseForPqlQuery(query + MV_GROUP_BY);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L, new String[]{"199896"});

    query = "SELECT COUNTMV(column6) FROM testTable GROUP BY VALUEIN(column7, 363, 469, 246, 100000)";
    brokerResponse = getBrokerResponseForPqlQuery(query);
    List<String[]> groupKeys = new ArrayList<>();
    groupKeys.add(new String[]{"363"});
    groupKeys.add(new String[]{"469"});
    groupKeys.add(new String[]{"246"});
    List<String[]> aggregations = new ArrayList<>();
    aggregations.add(new String[]{"35436"});
    aggregations.add(new String[]{"33576"});
    aggregations.add(new String[]{"24300"});
    QueriesTestUtils.testInterSegmentAggregationGroupByResult(brokerResponse, 400000L, 0L, 800000L, 400000L, groupKeys,
        aggregations);

    query =
        "SELECT VALUEIN(column7, 363, 469, 246, 100000), COUNTMV(column6) FROM testTable GROUP BY VALUEIN(column7, 363, 469, 246, 100000)";
    brokerResponse = getBrokerResponseForSqlQuery(query);
    DataSchema expectedDataSchema =
        new DataSchema(new String[]{"valuein(column7,'363','469','246','100000')", "countmv(column6)"},
            new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.LONG});
    QueriesTestUtils.testInterSegmentResultTable(brokerResponse, 400000L, 0L, 800000L, 400000L, Lists
        .newArrayList(new Object[]{469, (long) 33576}, new Object[]{246, (long) 24300},
            new Object[]{363, (long) 35436}), 3, expectedDataSchema);

    query =
        "SELECT VALUEIN(column7, 363, 469, 246, 100000), COUNTMV(column6) FROM testTable GROUP BY VALUEIN(column7, 363, 469, 246, 100000) ORDER BY COUNTMV(column6)";
    brokerResponse = getBrokerResponseForSqlQuery(query);
    QueriesTestUtils.testInterSegmentResultTable(brokerResponse, 400000L, 0L, 800000L, 400000L, Lists
        .newArrayList(new Object[]{246, (long) 24300}, new Object[]{469, (long) 33576},
            new Object[]{363, (long) 35436}), 3, expectedDataSchema);

    query =
        "SELECT VALUEIN(column7, 363, 469, 246, 100000), COUNTMV(column6) FROM testTable GROUP BY VALUEIN(column7, 363, 469, 246, 100000) ORDER BY COUNTMV(column6) DESC";
    brokerResponse = getBrokerResponseForSqlQuery(query);
    QueriesTestUtils.testInterSegmentResultTable(brokerResponse, 400000L, 0L, 800000L, 400000L, Lists
        .newArrayList(new Object[]{363, (long) 35436}, new Object[]{469, (long) 33576},
            new Object[]{246, (long) 24300}), 3, expectedDataSchema);

    query =
        "SELECT VALUEIN(column7, 363, 469, 246, 100000) AS value_in_col, COUNTMV(column6) FROM testTable GROUP BY value_in_col ORDER BY COUNTMV(column6) DESC";
    expectedDataSchema = new DataSchema(new String[]{"value_in_col", "countmv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.LONG});
    brokerResponse = getBrokerResponseForSqlQuery(query);
    QueriesTestUtils.testInterSegmentResultTable(brokerResponse, 400000L, 0L, 800000L, 400000L, Lists
        .newArrayList(new Object[]{363, (long) 35436}, new Object[]{469, (long) 33576},
            new Object[]{246, (long) 24300}), 3, expectedDataSchema);

    query = "SELECT COUNTMV(column6) FROM testTable GROUP BY daysSinceEpoch";
    brokerResponse = getBrokerResponseForPqlQuery(query);
    groupKeys = new ArrayList<>();
    groupKeys.add(new String[]{"1756015683"});
    aggregations = new ArrayList<>();
    aggregations.add(new String[]{"426752"});
    QueriesTestUtils.testInterSegmentAggregationGroupByResult(brokerResponse, 400000L, 0L, 800000L, 400000L, groupKeys,
        aggregations);

    query =
        "SELECT daysSinceEpoch, COUNTMV(column6) FROM testTable GROUP BY daysSinceEpoch ORDER BY COUNTMV(column6) DESC";
    expectedDataSchema = new DataSchema(new String[]{"daysSinceEpoch", "countmv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.LONG});
    brokerResponse = getBrokerResponseForSqlQuery(query);
    List<Object[]> result = new ArrayList<>();
    result.add(new Object[]{1756015683, (long) 426752});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 400000L, 0L, 800000L, 400000L, result, 1, expectedDataSchema);

    query = "SELECT COUNTMV(column6) FROM testTable GROUP BY timeconvert(daysSinceEpoch, 'DAYS', 'HOURS')";
    brokerResponse = getBrokerResponseForPqlQuery(query);
    groupKeys = new ArrayList<>();
    groupKeys.add(new String[]{"42144376392"});
    aggregations = new ArrayList<>();
    aggregations.add(new String[]{"426752"});
    QueriesTestUtils.testInterSegmentAggregationGroupByResult(brokerResponse, 400000L, 0L, 800000L, 400000L, groupKeys,
        aggregations);

    query =
        "SELECT timeconvert(daysSinceEpoch, 'DAYS', 'HOURS'), COUNTMV(column6) FROM testTable GROUP BY timeconvert(daysSinceEpoch, 'DAYS', 'HOURS') ORDER BY COUNTMV(column6) DESC";
    expectedDataSchema = new DataSchema(new String[]{"timeconvert(daysSinceEpoch,'DAYS','HOURS')", "countmv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG});
    brokerResponse = getBrokerResponseForSqlQuery(query);
    result = new ArrayList<>();
    result.add(new Object[]{42144376392L, (long) 426752});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 400000L, 0L, 800000L, 400000L, result, 1, expectedDataSchema);
  }

  @Test
  public void testCastCountMV() {
    String query = "SELECT COUNTMV(column6) as cnt_column6 FROM testTable";
    BrokerResponseNative brokerResponse = getBrokerResponseForSqlQuery(query);
    DataSchema expectedDataSchema =
        new DataSchema(new String[]{"cnt_column6"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.LONG});
    QueriesTestUtils.testInterSegmentResultTable(brokerResponse, 400000L, 0L, 400000L, 400000L,
        Arrays.asList(new Long[][]{new Long[]{426752L}}), 1, expectedDataSchema);
    Assert.assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnName(0), "cnt_column6");

    brokerResponse = getBrokerResponseForSqlQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentResultTable(brokerResponse, 62480L, 1101664L, 62480L, 400000L,
        Arrays.asList(new Long[][]{new Long[]{62480L}}), 1, expectedDataSchema);
    Assert.assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnName(0), "cnt_column6");

    brokerResponse = getBrokerResponseForSqlQuery(query + SV_GROUP_BY + ORDER_BY_ALIAS);
    QueriesTestUtils.testInterSegmentResultTable(brokerResponse, 400000L, 0L, 800000L, 400000L,
        Arrays.asList(new Long[][]{new Long[]{231056L}}), 10, expectedDataSchema);
    Assert.assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnName(0), "cnt_column6");

    brokerResponse = getBrokerResponseForSqlQuery(query + MV_GROUP_BY + ORDER_BY_ALIAS);
    QueriesTestUtils.testInterSegmentResultTable(brokerResponse, 400000L, 0L, 800000L, 400000L,
        Arrays.asList(new Long[][]{new Long[]{199896L}}), 10, expectedDataSchema);
    Assert.assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnName(0), "cnt_column6");
  }

  @Test
  public void testMaxMV() {
    String query = "SELECT MAXMV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L,
        new String[]{"2147483647.00000"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1101664L, 62480L, 400000L,
        new String[]{"2147483647.00000"});

    brokerResponse = getBrokerResponseForPqlQuery(query + SV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"2147483647.00000"});

    brokerResponse = getBrokerResponseForPqlQuery(query + MV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"2147483647.00000"});
  }

  @Test
  public void testMinMV() {
    String query = "SELECT MINMV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L, new String[]{"1001.00000"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1101664L, 62480L, 400000L,
        new String[]{"1009.00000"});

    brokerResponse = getBrokerResponseForPqlQuery(query + SV_GROUP_BY);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L, new String[]{"1001.00000"});

    brokerResponse = getBrokerResponseForPqlQuery(query + MV_GROUP_BY);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L, new String[]{"1001.00000"});
  }

  @Test
  public void testSumMV() {
    String query = "SELECT SUMMV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L,
        new String[]{"484324601810280.00000"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1101664L, 62480L, 400000L,
        new String[]{"114652613591912.00000"});

    brokerResponse = getBrokerResponseForPqlQuery(query + SV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"402591409613620.00000"});

    brokerResponse = getBrokerResponseForPqlQuery(query + MV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"393483780531788.00000"});
  }

  @Test
  public void testAvgMV() {
    String query = "SELECT AVGMV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L,
        new String[]{"1134908803.73210"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1101664L, 62480L, 400000L,
        new String[]{"1835029026.75916"});

    brokerResponse = getBrokerResponseForPqlQuery(query + SV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"2147483647.00000"});

    brokerResponse = getBrokerResponseForPqlQuery(query + MV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"2147483647.00000"});
  }

  @Test
  public void testMinMaxRangeMV() {
    String query = "SELECT MINMAXRANGEMV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L,
        new String[]{"2147482646.00000"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1101664L, 62480L, 400000L,
        new String[]{"2147482638.00000"});

    brokerResponse = getBrokerResponseForPqlQuery(query + SV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"2147482646.00000"});

    brokerResponse = getBrokerResponseForPqlQuery(query + MV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"2147482646.00000"});
  }

  @Test
  public void testDistinctCountMV() {
    String query = "SELECT DISTINCTCOUNTMV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L, new String[]{"18499"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 62480L, 1101664L, 62480L, 400000L, new String[]{"1186"});

    brokerResponse = getBrokerResponseForPqlQuery(query + SV_GROUP_BY);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L, new String[]{"4784"});

    brokerResponse = getBrokerResponseForPqlQuery(query + MV_GROUP_BY);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L, new String[]{"3434"});
  }

  @Test
  public void testDistinctCountHLLMV() {
    String query = "SELECT DISTINCTCOUNTHLLMV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L, new String[]{"20039"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 62480L, 1101664L, 62480L, 400000L, new String[]{"1296"});

    brokerResponse = getBrokerResponseForPqlQuery(query + SV_GROUP_BY);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L, new String[]{"4715"});

    brokerResponse = getBrokerResponseForPqlQuery(query + MV_GROUP_BY);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L, new String[]{"3490"});
  }

  @Test
  public void testDistinctCountRawHLLMV() {
    String query = "SELECT DISTINCTCOUNTRAWHLLMV(column6) FROM testTable";
    Function<Serializable, String> cardinalityExtractor = value -> String
        .valueOf(ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(BytesUtils.toBytes((String) value)).cardinality());

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L, cardinalityExtractor,
            new String[]{"20039"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 62480L, 1101664L, 62480L, 400000L, cardinalityExtractor,
            new String[]{"1296"});

    brokerResponse = getBrokerResponseForPqlQuery(query + SV_GROUP_BY);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L, cardinalityExtractor,
            new String[]{"4715"});

    brokerResponse = getBrokerResponseForPqlQuery(query + MV_GROUP_BY);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L, cardinalityExtractor,
            new String[]{"3490"});
  }

  @Test
  public void testPercentile50MV() {
    List<String> queries = Arrays
        .asList("SELECT PERCENTILE50MV(column6) FROM testTable", "SELECT PERCENTILEMV(column6, 50) FROM testTable",
            "SELECT PERCENTILEMV(column6, '50') FROM testTable", "SELECT PERCENTILEMV(column6, \"50\") FROM testTable");

    for (String query : queries) {
      BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
      QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L,
          new String[]{"2147483647.00000"});

      brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
      QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1101664L, 62480L, 400000L,
          new String[]{"2147483647.00000"});

      brokerResponse = getBrokerResponseForPqlQuery(query + SV_GROUP_BY);
      QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
          new String[]{"2147483647.00000"});

      brokerResponse = getBrokerResponseForPqlQuery(query + MV_GROUP_BY);
      QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
          new String[]{"2147483647.00000"});
    }
  }

  @Test
  public void testPercentile90MV() {
    String query = "SELECT PERCENTILE90MV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L,
        new String[]{"2147483647.00000"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1101664L, 62480L, 400000L,
        new String[]{"2147483647.00000"});

    brokerResponse = getBrokerResponseForPqlQuery(query + SV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"2147483647.00000"});

    brokerResponse = getBrokerResponseForPqlQuery(query + MV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"2147483647.00000"});
  }

  @Test
  public void testPercentile95MV() {
    String query = "SELECT PERCENTILE95MV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L,
        new String[]{"2147483647.00000"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1101664L, 62480L, 400000L,
        new String[]{"2147483647.00000"});

    brokerResponse = getBrokerResponseForPqlQuery(query + SV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"2147483647.00000"});

    brokerResponse = getBrokerResponseForPqlQuery(query + MV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"2147483647.00000"});
  }

  @Test
  public void testPercentile99MV() {
    String query = "SELECT PERCENTILE99MV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L,
        new String[]{"2147483647.00000"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1101664L, 62480L, 400000L,
        new String[]{"2147483647.00000"});

    brokerResponse = getBrokerResponseForPqlQuery(query + SV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"2147483647.00000"});

    brokerResponse = getBrokerResponseForPqlQuery(query + MV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"2147483647.00000"});
  }

  @Test
  public void testPercentileEst50MV() {
    String query = "SELECT PERCENTILEEST50MV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L, new String[]{"2147483647"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1101664L, 62480L, 400000L,
        new String[]{"2147483647"});

    brokerResponse = getBrokerResponseForPqlQuery(query + SV_GROUP_BY);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L, new String[]{"2147483647"});

    brokerResponse = getBrokerResponseForPqlQuery(query + MV_GROUP_BY);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L, new String[]{"2147483647"});
  }

  @Test
  public void testPercentileEst90MV() {
    String query = "SELECT PERCENTILEEST90MV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L, new String[]{"2147483647"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1101664L, 62480L, 400000L,
        new String[]{"2147483647"});

    brokerResponse = getBrokerResponseForPqlQuery(query + SV_GROUP_BY);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L, new String[]{"2147483647"});

    brokerResponse = getBrokerResponseForPqlQuery(query + MV_GROUP_BY);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L, new String[]{"2147483647"});
  }

  @Test
  public void testPercentileEst95MV() {
    String query = "SELECT PERCENTILEEST95MV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L, new String[]{"2147483647"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1101664L, 62480L, 400000L,
        new String[]{"2147483647"});

    brokerResponse = getBrokerResponseForPqlQuery(query + SV_GROUP_BY);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L, new String[]{"2147483647"});

    brokerResponse = getBrokerResponseForPqlQuery(query + MV_GROUP_BY);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L, new String[]{"2147483647"});
  }

  @Test
  public void testPercentileEst99MV() {
    String query = "SELECT PERCENTILEEST99MV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L, new String[]{"2147483647"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1101664L, 62480L, 400000L,
        new String[]{"2147483647"});

    brokerResponse = getBrokerResponseForPqlQuery(query + SV_GROUP_BY);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L, new String[]{"2147483647"});

    brokerResponse = getBrokerResponseForPqlQuery(query + MV_GROUP_BY);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L, new String[]{"2147483647"});
  }

  @Test
  public void testPercentileRawEst50MV() {
    testPercentileRawEstAggregationFunction(50);
  }

  @Test
  public void testPercentileRawEst90MV() {
    testPercentileRawEstAggregationFunction(90);
  }

  @Test
  public void testPercentileRawEst95MV() {
    testPercentileRawEstAggregationFunction(95);
  }

  @Test
  public void testPercentileRawEst99MV() {
    testPercentileRawEstAggregationFunction(99);
  }

  private void testPercentileRawEstAggregationFunction(int percentile) {
    Function<Serializable, String> quantileExtractor = value -> String.valueOf(
        ObjectSerDeUtils.QUANTILE_DIGEST_SER_DE.deserialize(BytesUtils.toBytes((String) value))
            .getQuantile(percentile / 100.0));

    String rawQuery =
        String.format("SELECT PERCENTILERAWEST%dMV(column6) FROM testTable", percentile);

    String query =
        String.format("SELECT PERCENTILEEST%dMV(column6) FROM testTable", percentile);

    queryAndTestAggregationResult(rawQuery, getExpectedQueryResults(query), quantileExtractor);

    queryAndTestAggregationResult(rawQuery + getFilter(), getExpectedQueryResults(query + getFilter()), quantileExtractor);

    // Comparing hard coded values for group by queries, as the results are ordered differently between regular and raw.
    ExpectedQueryResult<String> expectedQueryResultsWithSVGroupBy =
        new ExpectedQueryResult<>(400000L, 0L, 800000L, 400000L, new String[]{"2147483647"});

    ExpectedQueryResult<String> expectedQueryResultsWithMVGroupBy =
        new ExpectedQueryResult<>(400000L, 0L, 800000L, 400000L, new String[]{"2147483647"});

    queryAndTestAggregationResult(rawQuery + SV_GROUP_BY, expectedQueryResultsWithSVGroupBy, quantileExtractor);

    queryAndTestAggregationResult(rawQuery + MV_GROUP_BY, expectedQueryResultsWithMVGroupBy,
        quantileExtractor);
  }

  private void testPercentileRawTDigestAggregationFunction(int percentile) {
    Function<Serializable, String> quantileExtractor = value -> String.valueOf(
        ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(BytesUtils.toBytes((String) value))
            .quantile(percentile / 100.0));

    String rawQuery =
        String.format("SELECT PERCENTILERAWTDIGEST%dMV(column6) FROM testTable", percentile);

    String query =
        String.format("SELECT PERCENTILETDIGEST%dMV(column6) FROM testTable", percentile);

    queryAndTestAggregationResultWithDelta(rawQuery, getExpectedQueryResults(query), quantileExtractor);

    queryAndTestAggregationResultWithDelta(rawQuery + getFilter(), getExpectedQueryResults(query + getFilter()), quantileExtractor);

    // Comparing hard coded values for group by queries, as the results are ordered differently between regular and raw.
    ExpectedQueryResult<String> expectedQueryResultsWithSVGroupBy =
        new ExpectedQueryResult<>(400000L, 0L, 800000L, 400000L, new String[]{"2147483647"});

    ExpectedQueryResult<String> expectedQueryResultsWithMVGroupBy =
        new ExpectedQueryResult<>(400000L, 0L, 800000L, 400000L, new String[]{"2147483647"});

    queryAndTestAggregationResultWithDelta(rawQuery + SV_GROUP_BY, expectedQueryResultsWithSVGroupBy, quantileExtractor);

    queryAndTestAggregationResultWithDelta(rawQuery + MV_GROUP_BY, expectedQueryResultsWithMVGroupBy,
        quantileExtractor);
  }

  private ExpectedQueryResult<String> getExpectedQueryResults(String query) {
    return QueriesTestUtils.buildExpectedResponse(getBrokerResponseForPqlQuery(query));
  }

  private void queryAndTestAggregationResultWithDelta(String query, ExpectedQueryResult<String> expectedQueryResults,
      Function<Serializable, String> responseMapper) {
    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);

    QueriesTestUtils
        .testInterSegmentApproximateAggregationResult(brokerResponse, expectedQueryResults.getNumDocsScanned(),
            expectedQueryResults.getNumEntriesScannedInFilter(), expectedQueryResults.getNumEntriesScannedPostFilter(),
            expectedQueryResults.getNumTotalDocs(), responseMapper, expectedQueryResults.getResults(), SerializedBytesQueriesTest.PERCENTILE_TDIGEST_DELTA);
  }

  private void queryAndTestAggregationResult(String query, ExpectedQueryResult<String> expectedQueryResults,
      Function<Serializable, String> responseMapper) {
    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, expectedQueryResults.getNumDocsScanned(),
            expectedQueryResults.getNumEntriesScannedInFilter(), expectedQueryResults.getNumEntriesScannedPostFilter(),
            expectedQueryResults.getNumTotalDocs(), responseMapper, expectedQueryResults.getResults());
  }

  @Test
  public void testPercentileRawTDigest50MV() {
    testPercentileRawTDigestAggregationFunction(50);
  }

  @Test
  public void testPercentileRawTDigest90MV() {
    testPercentileRawTDigestAggregationFunction(90);
  }

  @Test
  public void testPercentileRawTDigest95MV() {
    testPercentileRawTDigestAggregationFunction(95);
  }

  @Test
  public void testPercentileRawTDigest99MV() {
    testPercentileRawTDigestAggregationFunction(99);
  }

  @Test
  public void testNumGroupsLimit() {
    String query = "SELECT COUNT(*) FROM testTable GROUP BY column6";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    assertFalse(brokerResponse.isNumGroupsLimitReached());

    brokerResponse = getBrokerResponseForPqlQuery(query,
        new InstancePlanMakerImplV2(1000, 1000, InstancePlanMakerImplV2.DEFAULT_MIN_SEGMENT_GROUP_TRIM_SIZE,
            InstancePlanMakerImplV2.DEFAULT_MIN_SERVER_GROUP_TRIM_SIZE,
            InstancePlanMakerImplV2.DEFAULT_GROUPBY_TRIM_THRESHOLD));
    assertTrue(brokerResponse.isNumGroupsLimitReached());
  }

  @Test
  public void testGroupByMVColumns() {
    String query = "SELECT COUNT(*), column7 FROM testTable GROUP BY column7 LIMIT 1000";
    BrokerResponseNative brokerResponse = getBrokerResponseForSqlQuery(query);
    assertEquals(brokerResponse.getResultTable().getRows().size(), 359);
    query = "SELECT COUNT(*), column5 FROM testTable GROUP BY column5 LIMIT 1000";
    brokerResponse = getBrokerResponseForSqlQuery(query);
    assertEquals(brokerResponse.getResultTable().getRows().size(), 9);
    query = "SELECT COUNT(*), column3 FROM testTable GROUP BY column3 LIMIT 1000";
    brokerResponse = getBrokerResponseForSqlQuery(query);
    assertEquals(brokerResponse.getResultTable().getRows().size(), 5);
    // Test groupby with one multi-value column and one non-dictionary single value column
    query = "SELECT COUNT(*), column7, column5 FROM testTable GROUP BY column7, column5 LIMIT 1000";
    brokerResponse = getBrokerResponseForSqlQuery(query);
    assertEquals(brokerResponse.getResultTable().getRows().size(), 1000);
    query = "SELECT COUNT(*), column7, column5 FROM testTable GROUP BY column5, column7 LIMIT 1000";
    brokerResponse = getBrokerResponseForSqlQuery(query);
    assertEquals(brokerResponse.getResultTable().getRows().size(), 1000);
    // Test groupby with one multi-value column and one single value column
    query = "SELECT COUNT(*), column3, column5 FROM testTable GROUP BY column3, column5 LIMIT 1000";
    brokerResponse = getBrokerResponseForSqlQuery(query);
    assertEquals(brokerResponse.getResultTable().getRows().size(), 41);
    query = "SELECT COUNT(*), column3, column5 FROM testTable GROUP BY column5, column3 LIMIT 1000";
    brokerResponse = getBrokerResponseForSqlQuery(query);
    assertEquals(brokerResponse.getResultTable().getRows().size(), 41);
  }
}
