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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.apache.calcite.avatica.util.Base64;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.spi.utils.BytesUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class InterSegmentAggregationMultiValueRawQueriesTest extends BaseMultiValueRawQueriesTest {
  private static final String SV_GROUP_BY = " GROUP BY column8 ORDER BY value DESC LIMIT 1";
  private static final String MV_GROUP_BY = " GROUP BY column7 ORDER BY value DESC LIMIT 1";

  // Allow 5% quantile error due to the randomness of TDigest merge
  private static final double PERCENTILE_TDIGEST_DELTA = 0.05 * Integer.MAX_VALUE;
  // Allow 2% quantile error due to the randomness of KLL merge
  private static final double PERCENTILE_KLL_DELTA = 0.02 * Integer.MAX_VALUE;

  @Test
  public void testCountMV() {
    String query = "SELECT COUNTMV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    DataSchema expectedDataSchema =
        new DataSchema(new String[]{"countmv(column6)"}, new DataSchema.ColumnDataType[]
            {DataSchema.ColumnDataType.LONG});
    Object[] expectedResults = new Object[]{426752L};
    ResultTable expectedResultTable = new ResultTable(expectedDataSchema, Collections.singletonList(expectedResults));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 400000L, 400000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER);
    expectedResults[0] = 62480L;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 62480L, 519028L, 62480L, 400000L, expectedResultTable);

    String svGroupBy = " GROUP BY column8 ORDER BY COUNTMV(column6) DESC LIMIT 1";
    brokerResponse = getBrokerResponse(query + svGroupBy);
    expectedResults[0] = 231056L;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 800000L, 400000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER + svGroupBy);
    expectedResults[0] = 58440L;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 62480L, 519028L, 124960L, 400000L, expectedResultTable);

    String mvGroupBy = " GROUP BY column7 ORDER BY COUNTMV(column6) DESC LIMIT 1";
    brokerResponse = getBrokerResponse(query + mvGroupBy);
    expectedResults[0] = 199896L;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 800000L, 400000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER + mvGroupBy);
    expectedResults[0] = 53212L;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 62480L, 519028L, 124960L, 400000L, expectedResultTable);

    query = "SELECT VALUEIN(column7, 363, 469, 246, 100000), COUNTMV(column6) FROM testTable"
        + " GROUP BY VALUEIN(column7, 363, 469, 246, 100000) ORDER BY COUNTMV(column6)";
    brokerResponse = getBrokerResponse(query);
    expectedDataSchema = new DataSchema(new String[]{"valuein(column7,'363','469','246','100000')", "countmv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.LONG});
    expectedResultTable = new ResultTable(expectedDataSchema,
        Arrays.asList(new Object[]{246, 24300L}, new Object[]{469, 33576L}, new Object[]{363, 35436L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 800000L, 400000L, expectedResultTable);

    query = "SELECT VALUEIN(column7, 363, 469, 246, 100000) AS key, COUNTMV(column6) AS value FROM testTable"
        + " GROUP BY key ORDER BY value";
    brokerResponse = getBrokerResponse(query);
    expectedDataSchema =
        new DataSchema(new String[]{"key", "value"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT,
            DataSchema.ColumnDataType.LONG});
    expectedResultTable = new ResultTable(expectedDataSchema,
        Arrays.asList(new Object[]{246, 24300L}, new Object[]{469, 33576L}, new Object[]{363, 35436L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 800000L, 400000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + " DESC");
    expectedResultTable = new ResultTable(expectedDataSchema,
        Arrays.asList(new Object[]{363, 35436L}, new Object[]{469, 33576L}, new Object[]{246, 24300L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 800000L, 400000L, expectedResultTable);

    query = "SELECT daysSinceEpoch, COUNTMV(column6) FROM testTable GROUP BY daysSinceEpoch";
    brokerResponse = getBrokerResponse(query);
    expectedDataSchema = new DataSchema(new String[]{"daysSinceEpoch", "countmv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.LONG});
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{1756015683, 426752L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 800000L, 400000L, expectedResultTable);

    query = "SELECT TIMECONVERT(daysSinceEpoch, 'DAYS', 'HOURS') AS key, COUNTMV(column6) FROM testTable"
        + " GROUP BY key ORDER BY COUNTMV(column6) DESC";
    brokerResponse = getBrokerResponse(query);
    expectedDataSchema = new DataSchema(new String[]{"key", "countmv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.LONG});
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{42144376392L, 426752L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 800000L, 400000L, expectedResultTable);
  }

  @Test
  public void testMaxMV() {
    String query = "SELECT MAXMV(column6) AS value FROM testTable";

    // Without filter, query should be answered by DictionaryBasedAggregationOperator (numEntriesScannedPostFilter = 0)
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    DataSchema expectedDataSchema = new DataSchema(new String[]{"value"}, new DataSchema.ColumnDataType[]
        {DataSchema.ColumnDataType.DOUBLE});
    ResultTable expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2147483647.0}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 0L, 400000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER);
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 62480L, 519028L, 62480L, 400000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + SV_GROUP_BY);
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 800000L, 400000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER + SV_GROUP_BY);
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 62480L, 519028L, 124960L, 400000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + MV_GROUP_BY);
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 800000L, 400000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER + MV_GROUP_BY);
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 62480L, 519028L, 124960L, 400000L, expectedResultTable);
  }

  @Test
  public void testMinMV() {
    String query = "SELECT MINMV(column6) AS value FROM testTable";

    // Without filter, query should be answered by DictionaryBasedAggregationOperator (numEntriesScannedPostFilter = 0)
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    DataSchema expectedDataSchema = new DataSchema(new String[]{"value"}, new DataSchema.ColumnDataType[]
        {DataSchema.ColumnDataType.DOUBLE});
    Object[] expectedResults = new Object[]{1001.0};
    ResultTable expectedResultTable = new ResultTable(expectedDataSchema, Collections.singletonList(expectedResults));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 0L, 400000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER);
    expectedResults[0] = 1009.0;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 62480L, 519028L, 62480L, 400000L, expectedResultTable);

    String svGroupBy = " GROUP BY column8 ORDER BY value LIMIT 1";
    brokerResponse = getBrokerResponse(query + svGroupBy);
    expectedResults[0] = 1001.0;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 800000L, 400000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER + svGroupBy);
    expectedResults[0] = 1009.0;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 62480L, 519028L, 124960L, 400000L, expectedResultTable);

    String mvGroupBy = " GROUP BY column7 ORDER BY value LIMIT 1";
    brokerResponse = getBrokerResponse(query + mvGroupBy);
    expectedResults[0] = 1001.0;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 800000L, 400000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER + mvGroupBy);
    expectedResults[0] = 1009.0;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 62480L, 519028L, 124960L, 400000L, expectedResultTable);
  }

  @Test
  public void testSumMV() {
    String query = "SELECT SUMMV(column6) AS value FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    DataSchema expectedDataSchema = new DataSchema(new String[]{"value"}, new DataSchema.ColumnDataType[]
        {DataSchema.ColumnDataType.DOUBLE});
    Object[] expectedResults = new Object[]{484324601810280.0};
    ResultTable expectedResultTable = new ResultTable(expectedDataSchema, Collections.singletonList(expectedResults));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 400000L, 400000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER);
    expectedResults[0] = 114652613591912.0;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 62480L, 519028L, 62480L, 400000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + SV_GROUP_BY);
    expectedResults[0] = 402591409613620.0;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 800000L, 400000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER + SV_GROUP_BY);
    expectedResults[0] = 105976779658032.0;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 62480L, 519028L, 124960L, 400000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + MV_GROUP_BY);
    expectedResults[0] = 393483780531788.0;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 800000L, 400000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER + MV_GROUP_BY);
    expectedResults[0] = 106216645956692.0;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 62480L, 519028L, 124960L, 400000L, expectedResultTable);
  }

  @Test
  public void testAvgMV() {
    String query = "SELECT AVGMV(column6) AS value FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    DataSchema expectedDataSchema = new DataSchema(new String[]{"value"}, new DataSchema.ColumnDataType[]
        {DataSchema.ColumnDataType.DOUBLE});
    Object[] expectedResults = new Object[]{1134908803.7321};
    ResultTable expectedResultTable = new ResultTable(expectedDataSchema, Collections.singletonList(expectedResults));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 400000L, 400000L, expectedResultTable, 1e-5);

    brokerResponse = getBrokerResponse(query + FILTER);
    expectedResults[0] = 1835029026.75916;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 62480L, 519028L, 62480L, 400000L, expectedResultTable,
        1e-5);

    brokerResponse = getBrokerResponse(query + SV_GROUP_BY);
    expectedResults[0] = 2147483647.0;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 800000L, 400000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER + SV_GROUP_BY);
    expectedResults[0] = 2147483647.0;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 62480L, 519028L, 124960L, 400000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + MV_GROUP_BY);
    expectedResults[0] = 2147483647.0;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 800000L, 400000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER + MV_GROUP_BY);
    expectedResults[0] = 2147483647.0;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 62480L, 519028L, 124960L, 400000L, expectedResultTable);
  }

  @Test
  public void testMinMaxRangeMV() {
    String query = "SELECT MINMAXRANGEMV(column6) AS value FROM testTable";

    // Without filter, query should be answered by DictionaryBasedAggregationOperator (numEntriesScannedPostFilter = 0)
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    DataSchema expectedDataSchema = new DataSchema(new String[]{"value"}, new DataSchema.ColumnDataType[]
        {DataSchema.ColumnDataType.DOUBLE});
    Object[] expectedResults = new Object[]{2147482646.0};
    ResultTable expectedResultTable = new ResultTable(expectedDataSchema, Collections.singletonList(expectedResults));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 0L, 400000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER);
    expectedResults[0] = 2147482638.0;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 62480L, 519028L, 62480L, 400000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + SV_GROUP_BY);
    expectedResults[0] = 2147482646.0;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 800000L, 400000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER + SV_GROUP_BY);
    expectedResults[0] = 2147482638.0;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 62480L, 519028L, 124960L, 400000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + MV_GROUP_BY);
    expectedResults[0] = 2147482646.0;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 800000L, 400000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER + MV_GROUP_BY);
    expectedResults[0] = 2147482638.0;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 62480L, 519028L, 124960L, 400000L, expectedResultTable);
  }

  @Test
  public void testDistinctCountMV() {
    String query = "SELECT DISTINCTCOUNTMV(column6) AS value FROM testTable";

    // Without filter, query should be answered by DictionaryBasedAggregationOperator (numEntriesScannedPostFilter = 0)
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    DataSchema expectedDataSchema = new DataSchema(new String[]{"value"}, new DataSchema.ColumnDataType[]
        {DataSchema.ColumnDataType.INT});
    Object[] expectedResults = new Object[]{18499};
    ResultTable expectedResultTable = new ResultTable(expectedDataSchema, Collections.singletonList(expectedResults));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 400000L, 400000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER);
    expectedResults[0] = 1186;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 62480L, 519028L, 62480L, 400000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + SV_GROUP_BY);
    expectedResults[0] = 4784;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 800000L, 400000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER + SV_GROUP_BY);
    expectedResults[0] = 1186;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 62480L, 519028L, 124960L, 400000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + MV_GROUP_BY);
    expectedResults[0] = 3434;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 800000L, 400000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER + MV_GROUP_BY);
    expectedResults[0] = 583;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 62480L, 519028L, 124960L, 400000L, expectedResultTable);
  }

  @Test
  public void testDistinctCountHLLMV() {
    String query = "SELECT DISTINCTCOUNTHLLMV(column6) AS value FROM testTable";

    // Without filter, query should be answered by DictionaryBasedAggregationOperator (numEntriesScannedPostFilter = 0)
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    DataSchema expectedDataSchema = new DataSchema(new String[]{"value"}, new DataSchema.ColumnDataType[]
        {DataSchema.ColumnDataType.LONG});
    Object[] expectedResults = new Object[]{20039L};
    ResultTable expectedResultTable = new ResultTable(expectedDataSchema, Collections.singletonList(expectedResults));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 400000L, 400000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER);
    expectedResults[0] = 1296L;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 62480L, 519028L, 62480L, 400000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + SV_GROUP_BY);
    expectedResults[0] = 4715L;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 800000L, 400000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER + SV_GROUP_BY);
    expectedResults[0] = 1296L;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 62480L, 519028L, 124960L, 400000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + MV_GROUP_BY);
    expectedResults[0] = 3490L;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 800000L, 400000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER + MV_GROUP_BY);
    expectedResults[0] = 606L;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 62480L, 519028L, 124960L, 400000L, expectedResultTable);
  }

  @Test
  public void testDistinctCountRawHLLMV() {
    String query = "SELECT DISTINCTCOUNTRAWHLLMV(column6) AS value FROM testTable";
    Function<Object, Object> cardinalityExtractor =
        value -> ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(BytesUtils.toBytes((String) value)).cardinality();

    // Without filter, query should be answered by DictionaryBasedAggregationOperator (numEntriesScannedPostFilter = 0)
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    DataSchema expectedDataSchema = new DataSchema(new String[]{"value"}, new DataSchema.ColumnDataType[]
        {DataSchema.ColumnDataType.LONG});
    Object[] expectedResults = new Object[]{20039L};
    ResultTable expectedResultTable = new ResultTable(expectedDataSchema, Collections.singletonList(expectedResults));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 400000L, 400000L, expectedResultTable,
        cardinalityExtractor);

    brokerResponse = getBrokerResponse(query + FILTER);
    expectedResults[0] = 1296L;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 62480L, 519028L, 62480L, 400000L, expectedResultTable,
        cardinalityExtractor);

    brokerResponse = getBrokerResponse(query + SV_GROUP_BY);
    expectedResults[0] = 4715L;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 800000L, 400000L, expectedResultTable,
        cardinalityExtractor);

    brokerResponse = getBrokerResponse(query + FILTER + SV_GROUP_BY);
    expectedResults[0] = 1296L;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 62480L, 519028L, 124960L, 400000L, expectedResultTable,
        cardinalityExtractor);

    brokerResponse = getBrokerResponse(query + MV_GROUP_BY);
    expectedResults[0] = 3490L;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 800000L, 400000L, expectedResultTable,
        cardinalityExtractor);

    brokerResponse = getBrokerResponse(query + FILTER + MV_GROUP_BY);
    expectedResults[0] = 606L;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 62480L, 519028L, 124960L, 400000L, expectedResultTable,
        cardinalityExtractor);
  }

  @Test
  public void testPercentileMV() {
    List<String> queries = Arrays.asList("SELECT PERCENTILE50MV(column6) AS value FROM testTable",
        "SELECT PERCENTILEMV(column6, 50) AS value FROM testTable",
        "SELECT PERCENTILEMV(column6, '50') AS value FROM testTable",
        "SELECT PERCENTILE90MV(column6) AS value FROM testTable",
        "SELECT PERCENTILE95MV(column6) AS value FROM testTable",
        "SELECT PERCENTILE99MV(column6) AS value FROM testTable");

    DataSchema expectedDataSchema = new DataSchema(new String[]{"value"}, new DataSchema.ColumnDataType[]
        {DataSchema.ColumnDataType.DOUBLE});
    ResultTable expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2147483647.0}));
    for (String query : queries) {
      BrokerResponseNative brokerResponse = getBrokerResponse(query);
      QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 400000L, 400000L, expectedResultTable);

      brokerResponse = getBrokerResponse(query + FILTER);
      QueriesTestUtils.testInterSegmentsResult(brokerResponse, 62480L, 519028L, 62480L, 400000L, expectedResultTable);

      brokerResponse = getBrokerResponse(query + SV_GROUP_BY);
      QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 800000L, 400000L, expectedResultTable);

      brokerResponse = getBrokerResponse(query + FILTER + SV_GROUP_BY);
      QueriesTestUtils.testInterSegmentsResult(brokerResponse, 62480L, 519028L, 124960L, 400000L, expectedResultTable);

      brokerResponse = getBrokerResponse(query + MV_GROUP_BY);
      QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 800000L, 400000L, expectedResultTable);

      brokerResponse = getBrokerResponse(query + FILTER + MV_GROUP_BY);
      QueriesTestUtils.testInterSegmentsResult(brokerResponse, 62480L, 519028L, 124960L, 400000L, expectedResultTable);
    }
  }

  @Test
  public void testPercentileEstMV() {
    List<String> queries = Arrays.asList("SELECT PERCENTILEEST50MV(column6) AS value FROM testTable",
        "SELECT PERCENTILEESTMV(column6, 50) AS value FROM testTable",
        "SELECT PERCENTILEESTMV(column6, '50') AS value FROM testTable",
        "SELECT PERCENTILEEST90MV(column6) AS value FROM testTable",
        "SELECT PERCENTILEEST95MV(column6) AS value FROM testTable",
        "SELECT PERCENTILEEST99MV(column6) AS value FROM testTable");

    for (String query : queries) {
      BrokerResponseNative brokerResponse = getBrokerResponse(query);
      DataSchema expectedDataSchema = new DataSchema(new String[]{"value"}, new DataSchema.ColumnDataType[]
          {DataSchema.ColumnDataType.LONG});
      ResultTable expectedResultTable =
          new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2147483647L}));
      QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 400000L, 400000L, expectedResultTable);

      brokerResponse = getBrokerResponse(query + FILTER);
      QueriesTestUtils.testInterSegmentsResult(brokerResponse, 62480L, 519028L, 62480L, 400000L, expectedResultTable);

      brokerResponse = getBrokerResponse(query + SV_GROUP_BY);
      QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 800000L, 400000L, expectedResultTable);

      brokerResponse = getBrokerResponse(query + FILTER + SV_GROUP_BY);
      QueriesTestUtils.testInterSegmentsResult(brokerResponse, 62480L, 519028L, 124960L, 400000L, expectedResultTable);

      brokerResponse = getBrokerResponse(query + MV_GROUP_BY);
      QueriesTestUtils.testInterSegmentsResult(brokerResponse, 400000L, 0L, 800000L, 400000L, expectedResultTable);

      brokerResponse = getBrokerResponse(query + FILTER + MV_GROUP_BY);
      QueriesTestUtils.testInterSegmentsResult(brokerResponse, 62480L, 519028L, 124960L, 400000L, expectedResultTable);
    }
  }

  @Test
  public void testPercentileRawEstMV() {
    testPercentileRawEstMV(50);
    testPercentileRawEstMV(90);
    testPercentileRawEstMV(95);
    testPercentileRawEstMV(99);
  }

  private void testPercentileRawEstMV(int percentile) {
    Function<Object, Object> quantileExtractor =
        value -> ObjectSerDeUtils.QUANTILE_DIGEST_SER_DE.deserialize(BytesUtils.toBytes((String) value))
            .getQuantile(percentile / 100.0);

    String rawQuery = String.format("SELECT PERCENTILERAWEST%dMV(column6) AS value FROM testTable", percentile);
    String regularQuery = String.format("SELECT PERCENTILEEST%dMV(column6) AS value FROM testTable", percentile);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(rawQuery), getBrokerResponse(regularQuery),
        quantileExtractor);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(rawQuery + FILTER),
        getBrokerResponse(regularQuery + FILTER), quantileExtractor);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(rawQuery + SV_GROUP_BY),
        getBrokerResponse(regularQuery + SV_GROUP_BY), quantileExtractor);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(rawQuery + FILTER + SV_GROUP_BY),
        getBrokerResponse(regularQuery + FILTER + SV_GROUP_BY), quantileExtractor);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(rawQuery + MV_GROUP_BY),
        getBrokerResponse(regularQuery + MV_GROUP_BY), quantileExtractor);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(rawQuery + FILTER + MV_GROUP_BY),
        getBrokerResponse(regularQuery + FILTER + MV_GROUP_BY), quantileExtractor);
  }

  @Test
  public void testPercentileRawTDigestMV() {
    testPercentileRawTDigestMV(50);
    testPercentileRawTDigestMV(90);
    testPercentileRawTDigestMV(95);
    testPercentileRawTDigestMV(99);

    testPercentileRawTDigestCustomCompressionMV(50, 150);
    testPercentileRawTDigestCustomCompressionMV(90, 500);
    testPercentileRawTDigestCustomCompressionMV(95, 200);
    testPercentileRawTDigestCustomCompressionMV(99, 1000);
  }

  private void testPercentileRawTDigestMV(int percentile) {
    Function<Object, Object> quantileExtractor =
        value -> ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(BytesUtils.toBytes((String) value))
            .quantile(percentile / 100.0);

    String rawQuery = String.format("SELECT PERCENTILERAWTDIGEST%dMV(column6) AS value FROM testTable", percentile);
    String regularQuery = String.format("SELECT PERCENTILETDIGEST%dMV(column6) AS value FROM testTable", percentile);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(rawQuery), getBrokerResponse(regularQuery),
        quantileExtractor, PERCENTILE_TDIGEST_DELTA);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(rawQuery + FILTER),
        getBrokerResponse(regularQuery + FILTER), quantileExtractor, PERCENTILE_TDIGEST_DELTA);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(rawQuery + SV_GROUP_BY),
        getBrokerResponse(regularQuery + SV_GROUP_BY), quantileExtractor, PERCENTILE_TDIGEST_DELTA);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(rawQuery + FILTER + SV_GROUP_BY),
        getBrokerResponse(regularQuery + FILTER + SV_GROUP_BY), quantileExtractor, PERCENTILE_TDIGEST_DELTA);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(rawQuery + MV_GROUP_BY),
        getBrokerResponse(regularQuery + MV_GROUP_BY), quantileExtractor, PERCENTILE_TDIGEST_DELTA);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(rawQuery + FILTER + MV_GROUP_BY),
        getBrokerResponse(regularQuery + FILTER + MV_GROUP_BY), quantileExtractor, PERCENTILE_TDIGEST_DELTA);
  }

  private void testPercentileRawTDigestCustomCompressionMV(int percentile, int compressionFactor) {
    Function<Object, Object> quantileExtractor =
        value -> ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(BytesUtils.toBytes((String) value))
            .quantile(percentile / 100.0);

    String rawQuery = String.format("SELECT PERCENTILERAWTDIGESTMV(column6, %d, %d) AS value FROM testTable",
        percentile, compressionFactor);
    String regularQuery = String.format("SELECT PERCENTILETDIGESTMV(column6, %d, %d) AS value FROM testTable",
        percentile, compressionFactor);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(rawQuery), getBrokerResponse(regularQuery),
        quantileExtractor, PERCENTILE_TDIGEST_DELTA);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(rawQuery + FILTER),
        getBrokerResponse(regularQuery + FILTER), quantileExtractor, PERCENTILE_TDIGEST_DELTA);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(rawQuery + SV_GROUP_BY),
        getBrokerResponse(regularQuery + SV_GROUP_BY), quantileExtractor, PERCENTILE_TDIGEST_DELTA);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(rawQuery + FILTER + SV_GROUP_BY),
        getBrokerResponse(regularQuery + FILTER + SV_GROUP_BY), quantileExtractor, PERCENTILE_TDIGEST_DELTA);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(rawQuery + MV_GROUP_BY),
        getBrokerResponse(regularQuery + MV_GROUP_BY), quantileExtractor, PERCENTILE_TDIGEST_DELTA);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(rawQuery + FILTER + MV_GROUP_BY),
        getBrokerResponse(regularQuery + FILTER + MV_GROUP_BY), quantileExtractor, PERCENTILE_TDIGEST_DELTA);
  }

  @Test
  public void testPercentileRawKLLMV() {
    testPercentileRawKLLMV(50);
    testPercentileRawKLLMV(90);
    testPercentileRawKLLMV(95);
    testPercentileRawKLLMV(99);
  }

  private void testPercentileRawKLLMV(int percentile) {
    Function<Object, Object> quantileExtractor = value -> {
      try {
        KllDoublesSketch sketch = ObjectSerDeUtils.KLL_SKETCH_SER_DE.deserialize(Base64.decode((String) value));
        return sketch.getQuantile(percentile / 100.0);
      } catch (IOException e) {
        return null;
      }
    };

    String rawKllQuery = String.format("SELECT PERCENTILERAWKLLMV(column6, %d) AS value FROM testTable", percentile);
    String regularQuery = String.format("SELECT PERCENTILEMV(column6, %d) AS value FROM testTable", percentile);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(rawKllQuery), getBrokerResponse(regularQuery),
        quantileExtractor, PERCENTILE_KLL_DELTA);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(rawKllQuery + FILTER),
        getBrokerResponse(regularQuery + FILTER), quantileExtractor, PERCENTILE_KLL_DELTA);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(rawKllQuery + SV_GROUP_BY),
        getBrokerResponse(regularQuery + SV_GROUP_BY), quantileExtractor, PERCENTILE_KLL_DELTA);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(rawKllQuery + FILTER + SV_GROUP_BY),
        getBrokerResponse(regularQuery + FILTER + SV_GROUP_BY), quantileExtractor, PERCENTILE_KLL_DELTA);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(rawKllQuery + MV_GROUP_BY),
        getBrokerResponse(regularQuery + MV_GROUP_BY), quantileExtractor, PERCENTILE_KLL_DELTA);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(rawKllQuery + FILTER + MV_GROUP_BY),
        getBrokerResponse(regularQuery + FILTER + MV_GROUP_BY), quantileExtractor, PERCENTILE_KLL_DELTA);
  }

  @Test
  public void testNumGroupsLimit() {
    String query = "SELECT COUNT(*) FROM testTable GROUP BY column6";

    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    assertFalse(brokerResponse.isNumGroupsLimitReached());

    brokerResponse = getBrokerResponse(query,
        new InstancePlanMakerImplV2(1000, 1000, InstancePlanMakerImplV2.DEFAULT_MIN_SEGMENT_GROUP_TRIM_SIZE,
            InstancePlanMakerImplV2.DEFAULT_MIN_SERVER_GROUP_TRIM_SIZE,
            InstancePlanMakerImplV2.DEFAULT_GROUPBY_TRIM_THRESHOLD));
    assertTrue(brokerResponse.isNumGroupsLimitReached());
  }

  @Test
  public void testFilteredAggregations() {
    String query = "SELECT COUNT(*) FILTER(WHERE column1 > 5) FROM testTable WHERE column3 > 0";
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    DataSchema expectedDataSchema = new DataSchema(new String[]{"count(*) FILTER(WHERE column1 > '5')"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.LONG});
    ResultTable expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{370236L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 370236L, 400000L, 0L, 400000L, expectedResultTable);
  }

  @Test
  public void testGroupByMVColumns() {
    String query = "SELECT COUNT(*), column7 FROM testTable GROUP BY column7 LIMIT 1000";
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    assertEquals(brokerResponse.getResultTable().getRows().size(), 359);

    query = "SELECT COUNT(*), column5 FROM testTable GROUP BY column5 LIMIT 1000";
    brokerResponse = getBrokerResponse(query);
    assertEquals(brokerResponse.getResultTable().getRows().size(), 9);

    query = "SELECT COUNT(*), column3 FROM testTable GROUP BY column3 LIMIT 1000";
    brokerResponse = getBrokerResponse(query);
    assertEquals(brokerResponse.getResultTable().getRows().size(), 5);

    // Test group-by with one multi-value column and one non-dictionary single value column
    query = "SELECT COUNT(*), column7, column5 FROM testTable GROUP BY column7, column5 LIMIT 1000";
    brokerResponse = getBrokerResponse(query);
    assertEquals(brokerResponse.getResultTable().getRows().size(), 1000);

    query = "SELECT COUNT(*), column7, column5 FROM testTable GROUP BY column5, column7 LIMIT 1000";
    brokerResponse = getBrokerResponse(query);
    assertEquals(brokerResponse.getResultTable().getRows().size(), 1000);

    // Test group-by with one multi-value column and one single value column
    query = "SELECT COUNT(*), column3, column5 FROM testTable GROUP BY column3, column5 LIMIT 1000";
    brokerResponse = getBrokerResponse(query);
    assertEquals(brokerResponse.getResultTable().getRows().size(), 41);

    query = "SELECT COUNT(*), column3, column5 FROM testTable GROUP BY column5, column3 LIMIT 1000";
    brokerResponse = getBrokerResponse(query);
    assertEquals(brokerResponse.getResultTable().getRows().size(), 41);
  }
}
