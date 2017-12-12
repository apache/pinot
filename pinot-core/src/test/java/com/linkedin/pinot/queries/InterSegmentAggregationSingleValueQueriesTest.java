/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.queries;

import com.linkedin.pinot.common.response.broker.BrokerResponseNative;
import org.testng.annotations.Test;


public class InterSegmentAggregationSingleValueQueriesTest extends BaseSingleValueQueriesTest {
  private static String GROUP_BY = " group by column9";

  @Test
  public void testCount() {
    String query = "SELECT COUNT(*) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 0L, 120000L,
        new String[]{"120000"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 0L, 120000L,
        new String[]{"24516"});

    brokerResponse = getBrokerResponseForQuery(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 120000L, 120000L,
        new String[]{"64420"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 24516L, 120000L,
        new String[]{"17080"});
  }

  @Test
  public void testMax() {
    String query = "SELECT MAX(column1), MAX(column3) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    /* numEntriedScannedInFilter and numEntriedScannedPostFilter are 0 here,
     * because this query gets answered by metadataBasedAggregationOperator */
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 0L, 120000L,
        new String[]{"2146952047.00000", "2147419555.00000"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 49032L, 120000L,
        new String[]{"2146952047.00000", "999813884.00000"});

    brokerResponse = getBrokerResponseForQuery(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 360000L, 120000L,
        new String[]{"2146952047.00000", "2147419555.00000"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 73548L, 120000L,
        new String[]{"2146952047.00000", "999813884.00000"});
  }

  @Test
  public void testMin() {
    String query = "SELECT MIN(column1), MIN(column3) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    /* numEntriedScannedInFilter and numEntriedScannedPostFilter are 0 here,
     * because this query gets answered by metadataBasedAggregationOperator */
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 0L, 120000L,
        new String[]{"240528.00000", "17891.00000"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 49032L, 120000L,
        new String[]{"101116473.00000", "20396372.00000"});

    brokerResponse = getBrokerResponseForQuery(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 360000L, 120000L,
        new String[]{"240528.00000", "17891.00000"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 73548L, 120000L,
        new String[]{"101116473.00000", "20396372.00000"});
  }

  @Test
  public void testSum() {
    String query = "SELECT SUM(column1), SUM(column3) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 240000L, 120000L,
        new String[]{"129268741751388.00000", "129156636756600.00000"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 49032L, 120000L,
        new String[]{"27503790384288.00000", "12429178874916.00000"});

    brokerResponse = getBrokerResponseForQuery(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 360000L, 120000L,
        new String[]{"69526727335224.00000", "69225631719808.00000"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 73548L, 120000L,
        new String[]{"19058003631876.00000", "8606725456500.00000"});
  }

  @Test
  public void testAvg() {
    String query = "SELECT AVG(column1), AVG(column3) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 240000L, 120000L,
        new String[]{"1077239514.59490", "1076305306.30500"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 49032L, 120000L,
        new String[]{"1121871038.68037", "506982332.96280"});

    brokerResponse = getBrokerResponseForQuery(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 360000L, 120000L,
        new String[]{"2142595699.00000", "2141451242.00000"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 73548L, 120000L,
        new String[]{"2142595699.00000", "999309554.00000"});
  }

  @Test
  public void testMinMaxRange() {
    String query = "SELECT MINMAXRANGE(column1), MINMAXRANGE(column3) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    /* numEntriedScannedInFilter and numEntriedScannedPostFilter are 0 here,
     * because this query gets answered by metadataBasedAggregationOperator */
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 0L, 120000L,
        new String[]{"2146711519.00000", "2147401664.00000"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 49032L, 120000L,
        new String[]{"2045835574.00000", "979417512.00000"});

    brokerResponse = getBrokerResponseForQuery(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 360000L, 120000L,
        new String[]{"2146711519.00000", "2146612605.00000"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 73548L, 120000L,
        new String[]{"2044094181.00000", "979417512.00000"});
  }

  @Test
  public void testDistinctCount() {
    String query = "SELECT DISTINCTCOUNT(column1), DISTINCTCOUNT(column3) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 240000L, 120000L,
        new String[]{"6582", "21910"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 49032L, 120000L,
        new String[]{"1872", "4556"});

    brokerResponse = getBrokerResponseForQuery(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 360000L, 120000L,
        new String[]{"3495", "11961"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 73548L, 120000L,
        new String[]{"1272", "3289"});
  }

  @Test
  public void testDistinctCountHLL() {
    String query = "SELECT DISTINCTCOUNTHLL(column1), DISTINCTCOUNTHLL(column3) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 240000L, 120000L,
        new String[]{"5977", "23825"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 49032L, 120000L,
        new String[]{"1886", "4492"});

    brokerResponse = getBrokerResponseForQuery(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 360000L, 120000L,
        new String[]{"3592", "11889"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 73548L, 120000L,
        new String[]{"1324", "3197"});
  }

  @Test
  public void testPercentile50() {
    String query = "SELECT PERCENTILE50(column1), PERCENTILE50(column3) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 240000L, 120000L,
        new String[]{"1107310944.00000", "1080136306.00000"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 49032L, 120000L,
        new String[]{"1139674505.00000", "505053732.00000"});

    brokerResponse = getBrokerResponseForQuery(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 360000L, 120000L,
        new String[]{"2146791843.00000", "2141451242.00000"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 73548L, 120000L,
        new String[]{"2142595699.00000", "999309554.00000"});
  }

  @Test
  public void testPercentile90() {
    String query = "SELECT PERCENTILE90(column1), PERCENTILE90(column3) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 240000L, 120000L,
        new String[]{"1943040511.00000", "1936611145.00000"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 49032L, 120000L,
        new String[]{"1936730975.00000", "899534534.00000"});

    brokerResponse = getBrokerResponseForQuery(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 360000L, 120000L,
        new String[]{"2146791843.00000", "2147278341.00000"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 73548L, 120000L,
        new String[]{"2142595699.00000", "999309554.00000"});
  }

  @Test
  public void testPercentile95() {
    String query = "SELECT PERCENTILE95(column1), PERCENTILE95(column3) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 240000L, 120000L,
        new String[]{"2071559385.00000", "2042409652.00000"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 49032L, 120000L,
        new String[]{"2096857943.00000", "947763150.00000"});

    brokerResponse = getBrokerResponseForQuery(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 360000L, 120000L,
        new String[]{"2146791843.00000", "2147419555.00000"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 73548L, 120000L,
        new String[]{"2142595699.00000", "999309554.00000"});
  }

  @Test
  public void testPercentile99() {
    String query = "SELECT PERCENTILE99(column1), PERCENTILE99(column3) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 240000L, 120000L,
        new String[]{"2139354437.00000", "2125299552.00000"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 49032L, 120000L,
        new String[]{"2146232405.00000", "990669195.00000"});

    brokerResponse = getBrokerResponseForQuery(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 360000L, 120000L,
        new String[]{"2146791843.00000", "2147419555.00000"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 73548L, 120000L,
        new String[]{"2146232405.00000", "999309554.00000"});
  }

  @Test
  public void testPercentileEst50() {
    String query = "SELECT PERCENTILEEST50(column1), PERCENTILEEST50(column3) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 240000L, 120000L,
        new String[]{"1107310944", "1082130431"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 49032L, 120000L,
        new String[]{"1139674505", "509607935"});

    brokerResponse = getBrokerResponseForQuery(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 360000L, 120000L,
        new String[]{"2146791843", "2141451242"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 73548L, 120000L,
        new String[]{"2142595699", "999309554"});
  }

  @Test
  public void testPercentileEst90() {
    String query = "SELECT PERCENTILEEST90(column1), PERCENTILEEST90(column3) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 240000L, 120000L,
        new String[]{"1946157055", "1946157055"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 49032L, 120000L,
        new String[]{"1939865599", "902299647"});

    brokerResponse = getBrokerResponseForQuery(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 360000L, 120000L,
        new String[]{"2146791843", "2147278341"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 73548L, 120000L,
        new String[]{"2142595699", "999309554"});
  }

  @Test
  public void testPercentileEst95() {
    String query = "SELECT PERCENTILEEST95(column1), PERCENTILEEST95(column3) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 240000L, 120000L,
        new String[]{"2080374783", "2051014655"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 49032L, 120000L,
        new String[]{"2109734911", "950009855"});

    brokerResponse = getBrokerResponseForQuery(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 360000L, 120000L,
        new String[]{"2146791843", "2147419555"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 73548L, 120000L,
        new String[]{"2142595699", "999309554"});
  }

  @Test
  public void testPercentileEst99() {
    String query = "SELECT PERCENTILEEST99(column1), PERCENTILEEST99(column3) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 240000L, 120000L,
        new String[]{"2143289343", "2143289343"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 49032L, 120000L,
        new String[]{"2146232405", "991952895"});

    brokerResponse = getBrokerResponseForQuery(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 120000L, 0L, 360000L, 120000L,
        new String[]{"2146791843", "2147419555"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query + GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 24516L, 336536L, 73548L, 120000L,
        new String[]{"2146232405", "999309554"});
  }
}
