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
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.SelectionResults;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.spi.utils.BytesUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class InterSegmentAggregationSingleValueQueriesTest extends BaseSingleValueQueriesTest {
  private static final String GROUP_BY = " group by column9";

  // Allow 5% quantile error due to the randomness of TDigest merge
  private static final double PERCENTILE_TDIGEST_DELTA = 0.05 * Integer.MAX_VALUE;

  @Test
  public void testCount() {
    String query = "SELECT COUNT(*) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 0L, 120000L, new String[]{"120000"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 0L, 120000L, new String[]{"24516"});

    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 120000L, 120000L, new String[]{"64420"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query + GROUP_BY);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 24516L, 120000L, new String[]{"17080"});
  }

  @Test
  public void testMax() {
    String query = "SELECT MAX(column1), MAX(column3) FROM testTable";

    // Query should be answered by MetadataBasedAggregationOperator, so check if numEntriesScannedInFilter and
    // numEntriesScannedPostFilter are 0
    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 0L, 120000L,
        new String[]{"2146952047.00000", "2147419555.00000"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 49032L, 120000L,
        new String[]{"2146952047.00000", "999813884.00000"});

    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 360000L, 120000L,
        new String[]{"2146952047.00000", "2147419555.00000"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 73548L, 120000L,
        new String[]{"2146952047.00000", "999813884.00000"});
  }

  @Test
  public void testMin() {
    String query = "SELECT MIN(column1), MIN(column3) FROM testTable";

    // Query should be answered by MetadataBasedAggregationOperator, so check if numEntriesScannedInFilter and
    // numEntriesScannedPostFilter are 0
    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 0L, 120000L,
        new String[]{"240528.00000", "17891.00000"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 49032L, 120000L,
        new String[]{"101116473.00000", "20396372.00000"});

    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 360000L, 120000L,
        new String[]{"240528.00000", "17891.00000"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 73548L, 120000L,
        new String[]{"101116473.00000", "20396372.00000"});
  }

  @Test
  public void testSum() {
    String query = "SELECT SUM(column1), SUM(column3) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 240000L, 120000L,
        new String[]{"129268741751388.00000", "129156636756600.00000"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 49032L, 120000L,
        new String[]{"27503790384288.00000", "12429178874916.00000"});

    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 360000L, 120000L,
        new String[]{"69526727335224.00000", "69225631719808.00000"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 73548L, 120000L,
        new String[]{"19058003631876.00000", "8606725456500.00000"});
  }

  @Test
  public void testAvg() {
    String query = "SELECT AVG(column1), AVG(column3) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 240000L, 120000L,
        new String[]{"1077239514.59490", "1076305306.30500"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 49032L, 120000L,
        new String[]{"1121871038.68037", "506982332.96280"});

    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 360000L, 120000L,
        new String[]{"2142595699.00000", "2141451242.00000"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 73548L, 120000L,
        new String[]{"2142595699.00000", "999309554.00000"});
  }

  @Test
  public void testMinMaxRange() {
    String query = "SELECT MINMAXRANGE(column1), MINMAXRANGE(column3) FROM testTable";

    // Query should be answered by MetadataBasedAggregationOperator, so check if numEntriesScannedInFilter and
    // numEntriesScannedPostFilter are 0
    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 0L, 120000L,
        new String[]{"2146711519.00000", "2147401664.00000"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 49032L, 120000L,
        new String[]{"2045835574.00000", "979417512.00000"});

    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 360000L, 120000L,
        new String[]{"2146711519.00000", "2146612605.00000"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 73548L, 120000L,
        new String[]{"2044094181.00000", "979417512.00000"});
  }

  @Test
  public void testDistinctCount() {
    String query = "SELECT DISTINCTCOUNT(column1), DISTINCTCOUNT(column3) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    //without filter, we should be using dictionary for distinctcount
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 0L, 120000L, new String[]{"6582", "21910"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 49032L, 120000L,
        new String[]{"1872", "4556"});

    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 360000L, 120000L,
        new String[]{"3495", "11961"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 73548L, 120000L,
        new String[]{"1272", "3289"});
  }

  @Test
  public void testDistinctCountHLL() {
    String query = "SELECT DISTINCTCOUNTHLL(column1), DISTINCTCOUNTHLL(column3) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 240000L, 120000L,
        new String[]{"5977", "23825"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 49032L, 120000L,
        new String[]{"1886", "4492"});

    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 360000L, 120000L,
        new String[]{"3592", "11889"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 73548L, 120000L,
        new String[]{"1324", "3197"});
  }

  @Test
  public void testDistinctCountRawHLL() {
    String query = "SELECT DISTINCTCOUNTRAWHLL(column1), DISTINCTCOUNTRAWHLL(column3) FROM testTable";
    Function<Serializable, String> cardinalityExtractor = value -> String
        .valueOf(ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(BytesUtils.toBytes((String) value)).cardinality());

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 240000L, 120000L, cardinalityExtractor,
            new String[]{"5977", "23825"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 49032L, 120000L, cardinalityExtractor,
            new String[]{"1886", "4492"});

    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 360000L, 120000L, cardinalityExtractor,
            new String[]{"3592", "11889"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query + GROUP_BY);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 73548L, 120000L, cardinalityExtractor,
            new String[]{"1324", "3197"});
  }

  @Test
  public void testDistinctCountHLLSketch() {
    String query = "SELECT DISTINCTCOUNTHLLSKETCH(column1), DISTINCTCOUNTHLLSKETCH(column3) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 240000L, 120000L,
        new String[]{"6543", "21864"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 49032L, 120000L,
        new String[]{"1864", "4537"});

    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 360000L, 120000L,
        new String[]{"3507", "11852"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 73548L, 120000L,
        new String[]{"1273", "3263"});
  }

  @Test
  public void testDistinctCountRawHLLSketch() {
    String query = "SELECT DISTINCTCOUNTRAWHLLSKETCH(column1), DISTINCTCOUNTRAWHLLSKETCH(column3) FROM testTable";
    Function<Serializable, String> cardinalityExtractor = value -> String
        .valueOf((long) ObjectSerDeUtils.HYPER_LOG_LOG_SKETCH_SER_DE.deserialize(BytesUtils.toBytes((String) value)).getEstimate());

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 240000L, 120000L, cardinalityExtractor,
            new String[]{"6543", "21864"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 49032L, 120000L, cardinalityExtractor,
            new String[]{"1864", "4537"});

    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 360000L, 120000L, cardinalityExtractor,
            new String[]{"3507", "11852"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query + GROUP_BY);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 73548L, 120000L, cardinalityExtractor,
            new String[]{"1273", "3263"});
  }

  @Test
  public void testDistinctCountHLLPlusPlus() {
    String query = "SELECT DISTINCTCOUNTHLLPLUSPLUS(column1), DISTINCTCOUNTHLLPLUSPLUS(column3) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 240000L, 120000L,
        new String[]{"6577", "21918"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 49032L, 120000L,
        new String[]{"1873", "4553"});

    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 360000L, 120000L,
        new String[]{"3500", "11957"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 73548L, 120000L,
        new String[]{"1272", "3287"});
  }

  @Test
  public void testDistinctCountRawHLLPlusPlus() {
    String query = "SELECT DISTINCTCOUNTRAWHLLPLUSPLUS(column1), DISTINCTCOUNTRAWHLLPLUSPLUS(column3) FROM testTable";
    Function<Serializable, String> cardinalityExtractor = value -> String
        .valueOf(ObjectSerDeUtils.HYPER_LOG_LOG_PLUS_PLUS_SER_DE.deserialize(BytesUtils.toBytes((String) value)).result());

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 240000L, 120000L, cardinalityExtractor,
            new String[]{"6577", "21918"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 49032L, 120000L, cardinalityExtractor,
            new String[]{"1873", "4553"});

    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 360000L, 120000L, cardinalityExtractor,
            new String[]{"3500", "11957"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query + GROUP_BY);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 73548L, 120000L, cardinalityExtractor,
            new String[]{"1272", "3287"});
  }

  @Test
  public void testPercentile50() {
    List<String> queries = Arrays.asList("SELECT PERCENTILE50(column1), PERCENTILE50(column3) FROM testTable",
        "SELECT PERCENTILE(column1, 50), PERCENTILE(column3, 50) FROM testTable",
        "SELECT PERCENTILE(column1, '50'), PERCENTILE(column3, '50') FROM testTable",
        "SELECT PERCENTILE(column1, \"50\"), PERCENTILE(column3, \"50\") FROM testTable");

    for (String query : queries) {
      BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
      QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 240000L, 120000L,
          new String[]{"1107310944.00000", "1080136306.00000"});

      brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
      QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 49032L, 120000L,
          new String[]{"1139674505.00000", "505053732.00000"});

      brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY);
      QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 360000L, 120000L,
          new String[]{"2146791843.00000", "2141451242.00000"});

      brokerResponse = getBrokerResponseForPqlQueryWithFilter(query + GROUP_BY);
      QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 73548L, 120000L,
          new String[]{"2142595699.00000", "999309554.00000"});
    }
  }

  @Test
  public void testPercentile90() {
    String query = "SELECT PERCENTILE90(column1), PERCENTILE90(column3) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 240000L, 120000L,
        new String[]{"1943040511.00000", "1936611145.00000"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 49032L, 120000L,
        new String[]{"1936730975.00000", "899534534.00000"});

    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 360000L, 120000L,
        new String[]{"2146791843.00000", "2147278341.00000"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 73548L, 120000L,
        new String[]{"2142595699.00000", "999309554.00000"});
  }

  @Test
  public void testPercentile95() {
    String query = "SELECT PERCENTILE95(column1), PERCENTILE95(column3) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 240000L, 120000L,
        new String[]{"2071559385.00000", "2042409652.00000"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 49032L, 120000L,
        new String[]{"2096857943.00000", "947763150.00000"});

    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 360000L, 120000L,
        new String[]{"2146791843.00000", "2147419555.00000"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 73548L, 120000L,
        new String[]{"2142595699.00000", "999309554.00000"});
  }

  @Test
  public void testPercentile99() {
    String query = "SELECT PERCENTILE99(column1), PERCENTILE99(column3) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 240000L, 120000L,
        new String[]{"2139354437.00000", "2125299552.00000"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 49032L, 120000L,
        new String[]{"2146232405.00000", "990669195.00000"});

    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 360000L, 120000L,
        new String[]{"2146791843.00000", "2147419555.00000"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 73548L, 120000L,
        new String[]{"2146232405.00000", "999309554.00000"});
  }

  @Test
  public void testPercentileEst50() {
    String query = "SELECT PERCENTILEEST50(column1), PERCENTILEEST50(column3) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 240000L, 120000L,
        new String[]{"1107310944", "1082130431"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 49032L, 120000L,
        new String[]{"1139674505", "509607935"});

    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 360000L, 120000L,
        new String[]{"2146791843", "2141451242"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 73548L, 120000L,
        new String[]{"2142595699", "999309554"});
  }

  @Test
  public void testPercentileEst90() {
    String query = "SELECT PERCENTILEEST90(column1), PERCENTILEEST90(column3) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 240000L, 120000L,
        new String[]{"1946157055", "1946157055"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 49032L, 120000L,
        new String[]{"1939865599", "902299647"});

    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 360000L, 120000L,
        new String[]{"2146791843", "2147278341"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 73548L, 120000L,
        new String[]{"2142595699", "999309554"});
  }

  @Test
  public void testPercentileEst95() {
    String query = "SELECT PERCENTILEEST95(column1), PERCENTILEEST95(column3) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 240000L, 120000L,
        new String[]{"2080374783", "2051014655"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 49032L, 120000L,
        new String[]{"2109734911", "950009855"});

    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 360000L, 120000L,
        new String[]{"2146791843", "2147419555"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 73548L, 120000L,
        new String[]{"2142595699", "999309554"});
  }

  @Test
  public void testPercentileEst99() {
    String query = "SELECT PERCENTILEEST99(column1), PERCENTILEEST99(column3) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 240000L, 120000L,
        new String[]{"2143289343", "2143289343"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 49032L, 120000L,
        new String[]{"2146232405", "991952895"});

    brokerResponse = getBrokerResponseForPqlQuery(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 360000L, 120000L,
        new String[]{"2146791843", "2147419555"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 73548L, 120000L,
        new String[]{"2146232405", "999309554"});
  }

  @Test
  public void testPercentileRawEst50() {
    testPercentileRawEstAggregationFunction(50);
  }

  @Test
  public void testPercentileRawEst90() {
    testPercentileRawEstAggregationFunction(90);
  }

  @Test
  public void testPercentileRawEst95() {
    testPercentileRawEstAggregationFunction(95);
  }

  @Test
  public void testPercentileRawEst99() {
    testPercentileRawEstAggregationFunction(99);
  }

  private void queryAndTestAggregationResult(String query, ExpectedQueryResult<String> expectedQueryResults,
      Function<Serializable, String> responseMapper) {
    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, expectedQueryResults.getNumDocsScanned(),
            expectedQueryResults.getNumEntriesScannedInFilter(), expectedQueryResults.getNumEntriesScannedPostFilter(),
            expectedQueryResults.getNumTotalDocs(), responseMapper, expectedQueryResults.getResults());
  }

  private void testPercentileRawEstAggregationFunction(int percentile) {
    Function<Serializable, String> quantileExtractor = value -> String.valueOf(
        ObjectSerDeUtils.QUANTILE_DIGEST_SER_DE.deserialize(BytesUtils.toBytes((String) value))
            .getQuantile(percentile / 100.0));

    String rawQuery =
        String.format("SELECT PERCENTILERAWEST%d(column1), PERCENTILERAWEST%d(column3) FROM testTable", percentile,
            percentile);

    String query =
        String
            .format("SELECT PERCENTILEEST%d(column1), PERCENTILEEST%d(column3) FROM testTable", percentile, percentile);

    queryAndTestAggregationResult(rawQuery, getExpectedQueryResults(query), quantileExtractor);

    queryAndTestAggregationResult(rawQuery + getFilter(), getExpectedQueryResults(query + getFilter()),
        quantileExtractor);

    queryAndTestAggregationResult(rawQuery + GROUP_BY, getExpectedQueryResults(query + GROUP_BY), quantileExtractor);

    queryAndTestAggregationResult(rawQuery + getFilter() + GROUP_BY,
        getExpectedQueryResults(query + getFilter() + GROUP_BY),
        quantileExtractor);
  }

  @Test
  public void testPercentileRawTDigest50() {
    testPercentileRawTDigestAggregationFunction(50);
  }

  @Test
  public void testPercentileRawTDigest90() {
    testPercentileRawTDigestAggregationFunction(90);
  }

  @Test
  public void testPercentileRawTDigest95() {
    testPercentileRawTDigestAggregationFunction(95);
  }

  @Test
  public void testPercentileRawTDigest99() {
    testPercentileRawTDigestAggregationFunction(99);
  }

  private void testPercentileRawTDigestAggregationFunction(int percentile) {
    Function<Serializable, String> quantileExtractor = value -> String.valueOf(
        ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(BytesUtils.toBytes((String) value))
            .quantile(percentile / 100.0));

    String rawQuery =
        String.format("SELECT PERCENTILERAWTDIGEST%d(column1), PERCENTILERAWTDIGEST%d(column3) FROM testTable",
            percentile, percentile);

    String query =
        String.format("SELECT PERCENTILETDIGEST%d(column1), PERCENTILETDIGEST%d(column3) FROM testTable", percentile,
            percentile);

    queryAndTestAggregationResultWithDelta(rawQuery, getExpectedQueryResults(query), quantileExtractor);

    queryAndTestAggregationResultWithDelta(rawQuery + getFilter(), getExpectedQueryResults(query + getFilter()),
        quantileExtractor);

    queryAndTestAggregationResultWithDelta(rawQuery + GROUP_BY, getExpectedQueryResults(query + GROUP_BY),
        quantileExtractor);

    queryAndTestAggregationResultWithDelta(rawQuery + getFilter() + GROUP_BY,
        getExpectedQueryResults(query + getFilter() + GROUP_BY),
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
            expectedQueryResults.getNumTotalDocs(), responseMapper, expectedQueryResults.getResults(),
            PERCENTILE_TDIGEST_DELTA);
  }

  @Test
  public void testNumGroupsLimit() {
    String query = "SELECT COUNT(*) FROM testTable GROUP BY column1";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    assertFalse(brokerResponse.isNumGroupsLimitReached());

    brokerResponse = getBrokerResponseForPqlQuery(query,
        new InstancePlanMakerImplV2(1000, 1000, InstancePlanMakerImplV2.DEFAULT_MIN_SEGMENT_GROUP_TRIM_SIZE,
            InstancePlanMakerImplV2.DEFAULT_MIN_SERVER_GROUP_TRIM_SIZE,
            InstancePlanMakerImplV2.DEFAULT_GROUPBY_TRIM_THRESHOLD));
    assertTrue(brokerResponse.isNumGroupsLimitReached());
  }

  /**
   * Test DISTINCT on single column multiple segment. Since the dataset
   * is Avro files, the only thing we currently check
   * for correctness is the actual number of DISTINCT
   * records returned
   */
  @Test
  public void testInterSegmentDistinctSingleColumn() {
    final String query = "SELECT DISTINCT(column1) FROM testTable LIMIT 1000000";
    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    final SelectionResults selectionResults = brokerResponse.getSelectionResults();
    Assert.assertEquals(selectionResults.getColumns().size(), 1);
    Assert.assertEquals(selectionResults.getColumns().get(0), "column1");
    Assert.assertEquals(selectionResults.getRows().size(), 6582);
  }

  /**
   * Test DISTINCT on single column multiple segment. Since the dataset
   * is Avro files, the only thing we currently check
   * for correctness is the actual number of DISTINCT
   * records returned
   */
  @Test
  public void testInterSegmentDistinctMultiColumn() {
    final String query = "SELECT DISTINCT(column1, column3) FROM testTable LIMIT 1000000";
    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    final SelectionResults selectionResults = brokerResponse.getSelectionResults();
    Assert.assertEquals(selectionResults.getColumns().size(), 2);
    Assert.assertEquals(selectionResults.getColumns().get(0), "column1");
    Assert.assertEquals(selectionResults.getColumns().get(1), "column3");
    Assert.assertEquals(selectionResults.getRows().size(), 21968);
  }
}
