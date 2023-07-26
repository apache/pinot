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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.spi.utils.BytesUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class InterSegmentAggregationSingleValueQueriesTest extends BaseSingleValueQueriesTest {
  private static final String GROUP_BY = " GROUP BY column9 ORDER BY v1 DESC, v2 DESC LIMIT 1";

  // Allow 5% quantile error due to the randomness of TDigest merge
  private static final double PERCENTILE_TDIGEST_DELTA = 0.05 * Integer.MAX_VALUE;
  // Allow 2% quantile error due to the randomness of KLL merge
  private static final double PERCENTILE_KLL_DELTA = 0.02 * Integer.MAX_VALUE;

  @Test
  public void testCount() {
    String query = "SELECT COUNT(*) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    DataSchema expectedDataSchema = new DataSchema(new String[]{"count(*)"}, new ColumnDataType[]{ColumnDataType.LONG});
    Object[] expectedResults = new Object[]{120000L};
    ResultTable expectedResultTable = new ResultTable(expectedDataSchema, Collections.singletonList(expectedResults));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 0L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER);
    expectedResults[0] = 24516L;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 0L, 120000L, expectedResultTable);

    String groupBy = " GROUP BY column9 ORDER BY COUNT(*) DESC LIMIT 1";
    brokerResponse = getBrokerResponse(query + groupBy);
    expectedResults[0] = 64420L;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 120000L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER + groupBy);
    expectedResults[0] = 17080L;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 24516L, 120000L, expectedResultTable);

    query = "SELECT COUNT(*) AS v1 FROM testTable";

    brokerResponse = getBrokerResponse(query);
    expectedDataSchema = new DataSchema(new String[]{"v1"}, new ColumnDataType[]{ColumnDataType.LONG});
    expectedResults = new Object[]{120000L};
    expectedResultTable = new ResultTable(expectedDataSchema, Collections.singletonList(expectedResults));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 0L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER);
    expectedResults[0] = 24516L;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 0L, 120000L, expectedResultTable);

    groupBy = " GROUP BY column9 ORDER BY v1 DESC LIMIT 1";
    brokerResponse = getBrokerResponse(query + groupBy);
    expectedResults[0] = 64420L;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 120000L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER + groupBy);
    expectedResults[0] = 17080L;
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 24516L, 120000L, expectedResultTable);
  }

  @Test
  public void testMax() {
    String query = "SELECT MAX(column1) AS v1, MAX(column3) AS v2 FROM testTable";

    // Without filter, query should be answered by NonScanBasedAggregationOperator (numEntriesScannedPostFilter = 0)
    // for dictionary based columns.
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    DataSchema expectedDataSchema =
        new DataSchema(new String[]{"v1", "v2"}, new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.DOUBLE});
    ResultTable expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2146952047.0, 2147419555.0}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 0L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2146952047.0, 999813884.0}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 49032L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + GROUP_BY);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2146952047.0, 2146630496.0}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 360000L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER + GROUP_BY);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2146952047.0, 999813884.0}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 73548L, 120000L, expectedResultTable);
  }

  @Test
  public void testMin() {
    String query = "SELECT MIN(column1) AS v1, MIN(column3) AS v2 FROM testTable";

    // Without filter, query should be answered by NonScanBasedAggregationOperator (numEntriesScannedPostFilter = 0)
    // for dictionary based columns.
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    DataSchema expectedDataSchema =
        new DataSchema(new String[]{"v1", "v2"}, new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.DOUBLE});
    ResultTable expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{240528.0, 17891.0}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 0L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{101116473.0, 20396372.0}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 49032L, 120000L, expectedResultTable);

    String groupBy = " GROUP BY column9 ORDER BY v1, v2 LIMIT 1";
    brokerResponse = getBrokerResponse(query + groupBy);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{240528.0, 17891.0}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 360000L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER + groupBy);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{101116473.0, 91804599.0}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 73548L, 120000L, expectedResultTable);
  }

  @Test
  public void testSum() {
    String query = "SELECT SUM(column1) AS v1, SUM(column3) AS v2 FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    DataSchema expectedDataSchema =
        new DataSchema(new String[]{"v1", "v2"}, new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.DOUBLE});
    ResultTable expectedResultTable = new ResultTable(expectedDataSchema,
        Collections.singletonList(new Object[]{129268741751388.0, 129156636756600.0}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 240000L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER);
    expectedResultTable = new ResultTable(expectedDataSchema,
        Collections.singletonList(new Object[]{27503790384288.0, 12429178874916.0}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 49032L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + GROUP_BY);
    expectedResultTable = new ResultTable(expectedDataSchema,
        Collections.singletonList(new Object[]{69526727335224.0, 69225631719808.0}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 360000L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER + GROUP_BY);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{19058003631876.0, 8606725456500.0}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 73548L, 120000L, expectedResultTable);
  }

  @Test
  public void testAvg() {
    String query = "SELECT AVG(column1) AS v1, AVG(column3) AS v2 FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    DataSchema expectedDataSchema =
        new DataSchema(new String[]{"v1", "v2"}, new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.DOUBLE});
    ResultTable expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{1077239514.5949, 1076305306.305}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 240000L, 120000L, expectedResultTable, 1e-5);

    brokerResponse = getBrokerResponse(query + FILTER);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{1121871038.68037, 506982332.96280}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 49032L, 120000L, expectedResultTable,
        1e-5);

    brokerResponse = getBrokerResponse(query + GROUP_BY);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2142595699.0, 334963174.0}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 360000L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER + GROUP_BY);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2142595699.0, 334963174.0}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 73548L, 120000L, expectedResultTable);
  }

  @Test
  public void testMinMaxRange() {
    String query = "SELECT MINMAXRANGE(column1) AS v1, MINMAXRANGE(column3) AS v2 FROM testTable";

    // Without filter, query should be answered by NonScanBasedAggregationOperator (numEntriesScannedPostFilter = 0)
    // for dictionary based columns.
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    DataSchema expectedDataSchema =
        new DataSchema(new String[]{"v1", "v2"}, new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.DOUBLE});
    ResultTable expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2146711519.0, 2147401664.0}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 0L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2045835574.0, 979417512.0}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 49032L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + GROUP_BY);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2146711519.0, 2146612605.0}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 360000L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER + GROUP_BY);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2044094181.0, 979417512.0}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 73548L, 120000L, expectedResultTable);
  }

  @Test
  public void testDistinctCount() {
    String query = "SELECT DISTINCTCOUNT(column1) AS v1, DISTINCTCOUNT(column3) AS v2 FROM testTable";

    // Without filter, query should be answered by NonScanBasedAggregationOperator (numEntriesScannedPostFilter = 0)
    // for dictionary based columns.
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    DataSchema expectedDataSchema =
        new DataSchema(new String[]{"v1", "v2"}, new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT});
    ResultTable expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{6582, 21910}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 0L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER);
    expectedResultTable = new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{1872, 4556}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 49032L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + GROUP_BY);
    expectedResultTable = new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{3495, 11961}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 360000L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER + GROUP_BY);
    expectedResultTable = new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{1272, 3289}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 73548L, 120000L, expectedResultTable);
  }

  @Test
  public void testDistinctCountHLL() {
    String query = "SELECT DISTINCTCOUNTHLL(column1) AS v1, DISTINCTCOUNTHLL(column3) AS v2 FROM testTable";

    // Without filter, query should be answered by NonScanBasedAggregationOperator (numEntriesScannedPostFilter = 0)
    // for dictionary based columns.
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    DataSchema expectedDataSchema =
        new DataSchema(new String[]{"v1", "v2"}, new ColumnDataType[]{ColumnDataType.LONG, ColumnDataType.LONG});
    ResultTable expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{5977L, 23825L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 0L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER);
    expectedResultTable = new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{1886L, 4492L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 49032L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + GROUP_BY);
    expectedResultTable = new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{3592L, 11889L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 360000L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER + GROUP_BY);
    expectedResultTable = new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{1324L, 3197L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 73548L, 120000L, expectedResultTable);
  }

  @Test
  public void testDistinctCountRawHLL() {
    String query = "SELECT DISTINCTCOUNTRAWHLL(column1) AS v1, DISTINCTCOUNTRAWHLL(column3) AS v2 FROM testTable";
    Function<Object, Object> cardinalityExtractor =
        value -> ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(BytesUtils.toBytes((String) value)).cardinality();

    // Without filter, query should be answered by NonScanBasedAggregationOperator (numEntriesScannedPostFilter = 0)
    // for dictionary based columns.
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    DataSchema expectedDataSchema =
        new DataSchema(new String[]{"v1", "v2"}, new ColumnDataType[]{ColumnDataType.LONG, ColumnDataType.LONG});
    ResultTable expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{5977L, 23825L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 0L, 120000L, expectedResultTable,
        cardinalityExtractor);

    brokerResponse = getBrokerResponse(query + FILTER);
    expectedResultTable = new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{1886L, 4492L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 49032L, 120000L, expectedResultTable,
        cardinalityExtractor);

    brokerResponse = getBrokerResponse(query + GROUP_BY);
    expectedResultTable = new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{3592L, 11889L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 360000L, 120000L, expectedResultTable,
        cardinalityExtractor);

    brokerResponse = getBrokerResponse(query + FILTER + GROUP_BY);
    expectedResultTable = new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{1324L, 3197L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 73548L, 120000L, expectedResultTable,
        cardinalityExtractor);
  }

  @Test
  public void testPercentile() {
    List<String> queries =
        Arrays.asList("SELECT PERCENTILE50(column1) AS v1, PERCENTILE50(column3) AS v2 FROM testTable",
            "SELECT PERCENTILE(column1, 50) AS v1, PERCENTILE(column3, 50) AS v2 FROM testTable",
            "SELECT PERCENTILE(column1, '50') AS v1, PERCENTILE(column3, '50') AS v2 FROM testTable");
    DataSchema expectedDataSchema =
        new DataSchema(new String[]{"v1", "v2"}, new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.DOUBLE});
    for (String query : queries) {
      BrokerResponseNative brokerResponse = getBrokerResponse(query);
      ResultTable expectedResultTable =
          new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{1107310944.0, 1080136306.0}));
      QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 240000L, 120000L, expectedResultTable);

      brokerResponse = getBrokerResponse(query + FILTER);
      expectedResultTable =
          new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{1139674505.0, 505053732.0}));
      QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 49032L, 120000L, expectedResultTable);

      brokerResponse = getBrokerResponse(query + GROUP_BY);
      expectedResultTable =
          new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2146791843.0, 1418523221.0}));
      QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 360000L, 120000L, expectedResultTable);

      brokerResponse = getBrokerResponse(query + FILTER + GROUP_BY);
      expectedResultTable =
          new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2142595699.0, 334963174.0}));
      QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 73548L, 120000L, expectedResultTable);
    }

    String query = "SELECT PERCENTILE90(column1) AS v1, PERCENTILE90(column3) AS v2 FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    ResultTable expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{1943040511.0, 1936611145.0}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 240000L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{1936730975.0, 899534534.0}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 49032L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + GROUP_BY);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2146791843.0, 1418523221.0}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 360000L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER + GROUP_BY);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2142595699.0, 334963174.0}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 73548L, 120000L, expectedResultTable);

    query = "SELECT PERCENTILE95(column1) AS v1, PERCENTILE95(column3) AS v2 FROM testTable";

    brokerResponse = getBrokerResponse(query);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2071559385.0, 2042409652.0}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 240000L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2096857943.0, 947763150.0}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 49032L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + GROUP_BY);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2146791843.0, 1418523221.0}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 360000L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER + GROUP_BY);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2142595699.0, 334963174.0}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 73548L, 120000L, expectedResultTable);

    query = "SELECT PERCENTILE99(column1) AS v1, PERCENTILE99(column3) AS v2 FROM testTable";

    brokerResponse = getBrokerResponse(query);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2139354437.0, 2125299552.0}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 240000L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2146232405.0, 990669195.0}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 49032L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + GROUP_BY);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2146791843.0, 1418523221.0}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 360000L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER + GROUP_BY);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2146232405.0, 990259756.0}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 73548L, 120000L, expectedResultTable);
  }

  @Test
  public void testPercentileEst() {
    String query = "SELECT PERCENTILEEST50(column1) AS v1, PERCENTILEEST50(column3) AS v2 FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    DataSchema expectedDataSchema =
        new DataSchema(new String[]{"v1", "v2"}, new ColumnDataType[]{ColumnDataType.LONG, ColumnDataType.LONG});
    ResultTable expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{1107310944L, 1082130431L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 240000L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{1139674505L, 509607935L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 49032L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + GROUP_BY);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2146791843L, 1418523221L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 360000L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER + GROUP_BY);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2142595699L, 334963174L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 73548L, 120000L, expectedResultTable);

    query = "SELECT PERCENTILEEST90(column1) AS v1, PERCENTILEEST90(column3) AS v2 FROM testTable";

    brokerResponse = getBrokerResponse(query);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{1946157055L, 1946157055L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 240000L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{1939865599L, 902299647L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 49032L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + GROUP_BY);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2146791843L, 1418523221L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 360000L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER + GROUP_BY);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2142595699L, 334963174L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 73548L, 120000L, expectedResultTable);

    query = "SELECT PERCENTILEEST95(column1) AS v1, PERCENTILEEST95(column3) AS v2 FROM testTable";

    brokerResponse = getBrokerResponse(query);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2080374783L, 2051014655L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 240000L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2109734911L, 950009855L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 49032L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + GROUP_BY);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2146791843L, 1418523221L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 360000L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER + GROUP_BY);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2142595699L, 334963174L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 73548L, 120000L, expectedResultTable);

    query = "SELECT PERCENTILEEST99(column1) AS v1, PERCENTILEEST99(column3) AS v2 FROM testTable";

    brokerResponse = getBrokerResponse(query);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2143289343L, 2143289343L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 240000L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2146232405L, 991952895L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 49032L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + GROUP_BY);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2146791843L, 1418523221L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 360000L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER + GROUP_BY);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2146232405L, 993001471L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 73548L, 120000L, expectedResultTable);
  }

  @Test
  public void testPercentileRawEst() {
    testPercentileRawEst(50);
    testPercentileRawEst(90);
    testPercentileRawEst(95);
    testPercentileRawEst(99);
  }

  private void testPercentileRawEst(int percentile) {
    Function<Object, Object> quantileExtractor =
        value -> ObjectSerDeUtils.QUANTILE_DIGEST_SER_DE.deserialize(BytesUtils.toBytes((String) value))
            .getQuantile(percentile / 100.0);

    String rawQuery =
        String.format("SELECT PERCENTILERAWEST%d(column1) AS v1, PERCENTILERAWEST%d(column3) AS v2 FROM testTable",
            percentile, percentile);
    String regularQuery =
        String.format("SELECT PERCENTILEEST%d(column1) AS v1, PERCENTILEEST%d(column3) AS v2 FROM testTable",
            percentile, percentile);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(rawQuery), getBrokerResponse(regularQuery),
        quantileExtractor);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(rawQuery + FILTER),
        getBrokerResponse(regularQuery + FILTER), quantileExtractor);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(rawQuery + GROUP_BY),
        getBrokerResponse(regularQuery + GROUP_BY), quantileExtractor);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(rawQuery + FILTER + GROUP_BY),
        getBrokerResponse(regularQuery + FILTER + GROUP_BY), quantileExtractor);
  }

  @Test
  public void testPercentileRawTDigest() {
    testPercentileRawTDigest(50);
    testPercentileRawTDigest(90);
    testPercentileRawTDigest(95);
    testPercentileRawTDigest(99);

    testPercentileRawTDigestCustomCompression(50, 150);
    testPercentileRawTDigestCustomCompression(90, 500);
    testPercentileRawTDigestCustomCompression(95, 200);
    testPercentileRawTDigestCustomCompression(99, 1000);
  }

  private void testPercentileRawTDigest(int percentile) {
    Function<Object, Object> quantileExtractor =
        value -> ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(BytesUtils.toBytes((String) value))
            .quantile(percentile / 100.0);

    String rawQuery = String.format(
        "SELECT PERCENTILERAWTDIGEST%d(column1) AS v1, PERCENTILERAWTDIGEST%d(column3) AS v2 FROM testTable",
        percentile, percentile);
    String regularQuery =
        String.format("SELECT PERCENTILETDIGEST%d(column1) AS v1, PERCENTILETDIGEST%d(column3) AS v2 FROM testTable",
            percentile, percentile);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(rawQuery), getBrokerResponse(regularQuery),
        quantileExtractor, PERCENTILE_TDIGEST_DELTA);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(rawQuery + FILTER),
        getBrokerResponse(regularQuery + FILTER), quantileExtractor, PERCENTILE_TDIGEST_DELTA);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(rawQuery + GROUP_BY),
        getBrokerResponse(regularQuery + GROUP_BY), quantileExtractor, PERCENTILE_TDIGEST_DELTA);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(rawQuery + FILTER + GROUP_BY),
        getBrokerResponse(regularQuery + FILTER + GROUP_BY), quantileExtractor, PERCENTILE_TDIGEST_DELTA);
  }

  private void testPercentileRawTDigestCustomCompression(int percentile, int compressionFactor) {
    Function<Object, Object> quantileExtractor =
        value -> ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(BytesUtils.toBytes((String) value))
            .quantile(percentile / 100.0);

    String rawQuery = String.format(
        "SELECT PERCENTILERAWTDIGEST(column1, %d, %d) AS v1, PERCENTILERAWTDIGEST(column3, %d, %d) AS v2 "
            + "FROM testTable",
        percentile, compressionFactor, percentile, compressionFactor);
    String regularQuery =
        String.format("SELECT PERCENTILETDIGEST(column1, %d, %d) AS v1, PERCENTILETDIGEST(column3, %d, %d) AS v2 "
                + "FROM testTable",
            percentile, compressionFactor, percentile, compressionFactor);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(rawQuery), getBrokerResponse(regularQuery),
        quantileExtractor, PERCENTILE_TDIGEST_DELTA);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(rawQuery + FILTER),
        getBrokerResponse(regularQuery + FILTER), quantileExtractor, PERCENTILE_TDIGEST_DELTA);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(rawQuery + GROUP_BY),
        getBrokerResponse(regularQuery + GROUP_BY), quantileExtractor, PERCENTILE_TDIGEST_DELTA);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(rawQuery + FILTER + GROUP_BY),
        getBrokerResponse(regularQuery + FILTER + GROUP_BY), quantileExtractor, PERCENTILE_TDIGEST_DELTA);
  }

  @Test
  public void testPercentileKLL() {
    String query = "SELECT PERCENTILEKLL(column1, 50) AS v1, PERCENTILEKLL(column3, 50) AS v2 FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    DataSchema expectedDataSchema =
        new DataSchema(new String[]{"v1", "v2"}, new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.DOUBLE});
    ResultTable expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{1107310944L, 1082130431L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L,
        240000L, 120000L, expectedResultTable, PERCENTILE_KLL_DELTA);

    brokerResponse = getBrokerResponse(query + FILTER);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{1139674505L, 509607935L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L,
        49032L, 120000L, expectedResultTable, PERCENTILE_KLL_DELTA);

    brokerResponse = getBrokerResponse(query + GROUP_BY);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2146791843L, 1418523221L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L,
        360000L, 120000L, expectedResultTable, PERCENTILE_KLL_DELTA);

    brokerResponse = getBrokerResponse(query + FILTER + GROUP_BY);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2142595699L, 334963174L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L,
        73548L, 120000L, expectedResultTable, PERCENTILE_KLL_DELTA);

    query = "SELECT PERCENTILEKLL(column1, 90) AS v1, PERCENTILEKLL(column3, 90) AS v2 FROM testTable";

    brokerResponse = getBrokerResponse(query);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{1946157055L, 1946157055L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L,
        240000L, 120000L, expectedResultTable, PERCENTILE_KLL_DELTA);

    brokerResponse = getBrokerResponse(query + FILTER);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{1939865599L, 902299647L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L,
        49032L, 120000L, expectedResultTable, PERCENTILE_KLL_DELTA);

    brokerResponse = getBrokerResponse(query + GROUP_BY);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2146791843L, 1418523221L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L,
        360000L, 120000L, expectedResultTable, PERCENTILE_KLL_DELTA);

    brokerResponse = getBrokerResponse(query + FILTER + GROUP_BY);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2142595699L, 334963174L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L,
        73548L, 120000L, expectedResultTable, PERCENTILE_KLL_DELTA);

    query = "SELECT PERCENTILEKLL(column1, 95) AS v1, PERCENTILEKLL(column3, 95) AS v2 FROM testTable";

    brokerResponse = getBrokerResponse(query);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2080374783L, 2051014655L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L,
        240000L, 120000L, expectedResultTable, PERCENTILE_KLL_DELTA);

    brokerResponse = getBrokerResponse(query + FILTER);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2109734911L, 950009855L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L,
        49032L, 120000L, expectedResultTable, PERCENTILE_KLL_DELTA);

    brokerResponse = getBrokerResponse(query + GROUP_BY);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2146791843L, 1418523221L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L,
        360000L, 120000L, expectedResultTable, PERCENTILE_KLL_DELTA);

    brokerResponse = getBrokerResponse(query + FILTER + GROUP_BY);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2142595699L, 334963174L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L,
        73548L, 120000L, expectedResultTable, PERCENTILE_KLL_DELTA);

    query = "SELECT PERCENTILEKLL(column1, 99) AS v1, PERCENTILEKLL(column3, 99) AS v2 FROM testTable";

    brokerResponse = getBrokerResponse(query);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2143289343L, 2143289343L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L,
        240000L, 120000L, expectedResultTable, PERCENTILE_KLL_DELTA);

    brokerResponse = getBrokerResponse(query + FILTER);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2146232405L, 991952895L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L,
        49032L, 120000L, expectedResultTable, PERCENTILE_KLL_DELTA);

    brokerResponse = getBrokerResponse(query + GROUP_BY);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2146791843L, 1418523221L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L,
        360000L, 120000L, expectedResultTable, PERCENTILE_KLL_DELTA);

    brokerResponse = getBrokerResponse(query + FILTER + GROUP_BY);
    expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2146232405L, 993001471L}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L,
        73548L, 120000L, expectedResultTable, PERCENTILE_KLL_DELTA);
  }

  @Test
  public void testNumGroupsLimit() {
    String query = "SELECT COUNT(*) FROM testTable GROUP BY column1";

    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    assertFalse(brokerResponse.isNumGroupsLimitReached());

    brokerResponse = getBrokerResponse(query,
        new InstancePlanMakerImplV2(1000, 1000, InstancePlanMakerImplV2.DEFAULT_MIN_SEGMENT_GROUP_TRIM_SIZE,
            InstancePlanMakerImplV2.DEFAULT_MIN_SERVER_GROUP_TRIM_SIZE,
            InstancePlanMakerImplV2.DEFAULT_GROUPBY_TRIM_THRESHOLD));
    assertTrue(brokerResponse.isNumGroupsLimitReached());
  }

  @Test
  public void testDistinctSum() {
    String query = "select DISTINCTSUM(column1) as v1, DISTINCTSUM(column3) as v2 from testTable";

    // Without filter, query should be answered by NonScanBasedAggregationOperator (numEntriesScannedPostFilter = 0)
    // for dictionary based columns.
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    DataSchema expectedDataSchema =
        new DataSchema(new String[]{"v1", "v2"}, new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.DOUBLE});
    ResultTable expectedResultTable =
        new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{7074556592262.0, 23553878404013.0}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 0L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER);
    expectedResultTable = new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{2062916453604.0,
        2334011146274.0}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 49032L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + GROUP_BY);
    expectedResultTable = new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{3745055692019.0,
        12836683389098.0}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 360000L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER + GROUP_BY);
    expectedResultTable = new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{1397706323624.0,
        1686328722268.0}));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 73548L, 120000L, expectedResultTable);
  }

  @Test
  public void testDistinctAvg() {
    String query = "select DISTINCTAVG(column1) as v1, DISTINCTAVG(column3) as v2 from testTable";

    // Without filter, query should be answered by NonScanBasedAggregationOperator (numEntriesScannedPostFilter = 0)
    // for dictionary based columns.
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    DataSchema expectedDataSchema =
        new DataSchema(new String[]{"v1", "v2"}, new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.DOUBLE});
    ResultTable expectedResultTable = new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{
        1074833879.1039197, 1075028681.150753
    }));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 0L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER);
    expectedResultTable = new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{
        1101985285.0448718, 512293930.26207197
    }));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 49032L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + GROUP_BY);
    expectedResultTable = new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{
        2142595699.0, 334963174.0
    }));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 120000L, 0L, 360000L, 120000L, expectedResultTable);

    brokerResponse = getBrokerResponse(query + FILTER + GROUP_BY);
    expectedResultTable = new ResultTable(expectedDataSchema, Collections.singletonList(new Object[]{
        2142595699.0, 334963174.0
    }));
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 24516L, 252256L, 73548L, 120000L, expectedResultTable);
  }
}
