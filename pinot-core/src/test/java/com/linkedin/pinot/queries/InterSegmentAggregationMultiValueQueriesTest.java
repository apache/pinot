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


public class InterSegmentAggregationMultiValueQueriesTest extends BaseMultiValueQueriesTest {
  private static String SV_GROUP_BY = " group by column8";
  private static String MV_GROUP_BY = " group by column7";

  @Test
  public void testCountMV() {
    String query = "SELECT COUNTMV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L,
        new String[]{"426752"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1129720L, 62480L, 400000L,
        new String[]{"62480"});

    brokerResponse = getBrokerResponseForQuery(query + SV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"231056"});

    brokerResponse = getBrokerResponseForQuery(query + MV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"199896"});
  }

  @Test
  public void testMaxMV() {
    String query = "SELECT MAXMV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L,
        new String[]{"2147483647.00000"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1129720L, 62480L, 400000L,
        new String[]{"2147483647.00000"});

    brokerResponse = getBrokerResponseForQuery(query + SV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"2147483647.00000"});

    brokerResponse = getBrokerResponseForQuery(query + MV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"2147483647.00000"});
  }

  @Test
  public void testMinMV() {
    String query = "SELECT MINMV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L,
        new String[]{"1001.00000"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1129720L, 62480L, 400000L,
        new String[]{"1009.00000"});

    brokerResponse = getBrokerResponseForQuery(query + SV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"1001.00000"});

    brokerResponse = getBrokerResponseForQuery(query + MV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"1001.00000"});
  }

  @Test
  public void testSumMV() {
    String query = "SELECT SUMMV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L,
        new String[]{"484324601810280.00000"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1129720L, 62480L, 400000L,
        new String[]{"114652613591912.00000"});

    brokerResponse = getBrokerResponseForQuery(query + SV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"402591409613620.00000"});

    brokerResponse = getBrokerResponseForQuery(query + MV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"393483780531788.00000"});
  }

  @Test
  public void testAvgMV() {
    String query = "SELECT AVGMV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L,
        new String[]{"1134908803.73210"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1129720L, 62480L, 400000L,
        new String[]{"1835029026.75916"});

    brokerResponse = getBrokerResponseForQuery(query + SV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"2147483647.00000"});

    brokerResponse = getBrokerResponseForQuery(query + MV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"2147483647.00000"});
  }

  @Test
  public void testMinMaxRangeMV() {
    String query = "SELECT MINMAXRANGEMV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L,
        new String[]{"2147482646.00000"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1129720L, 62480L, 400000L,
        new String[]{"2147482638.00000"});

    brokerResponse = getBrokerResponseForQuery(query + SV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"2147482646.00000"});

    brokerResponse = getBrokerResponseForQuery(query + MV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"2147482646.00000"});
  }

  @Test
  public void testDistinctCountMV() {
    String query = "SELECT DISTINCTCOUNTMV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L,
        new String[]{"18499"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1129720L, 62480L, 400000L,
        new String[]{"1186"});

    brokerResponse = getBrokerResponseForQuery(query + SV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"4784"});

    brokerResponse = getBrokerResponseForQuery(query + MV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"3434"});
  }

  @Test
  public void testDistinctCountHLLMV() {
    String query = "SELECT DISTINCTCOUNTHLLMV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L,
        new String[]{"20039"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1129720L, 62480L, 400000L,
        new String[]{"1296"});

    brokerResponse = getBrokerResponseForQuery(query + SV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"4715"});

    brokerResponse = getBrokerResponseForQuery(query + MV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"3490"});
  }

  @Test
  public void testPercentile50MV() {
    String query = "SELECT PERCENTILE50MV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L,
        new String[]{"2147483647.00000"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1129720L, 62480L, 400000L,
        new String[]{"2147483647.00000"});

    brokerResponse = getBrokerResponseForQuery(query + SV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"2147483647.00000"});

    brokerResponse = getBrokerResponseForQuery(query + MV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"2147483647.00000"});
  }

  @Test
  public void testPercentile90MV() {
    String query = "SELECT PERCENTILE90MV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L,
        new String[]{"2147483647.00000"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1129720L, 62480L, 400000L,
        new String[]{"2147483647.00000"});

    brokerResponse = getBrokerResponseForQuery(query + SV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"2147483647.00000"});

    brokerResponse = getBrokerResponseForQuery(query + MV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"2147483647.00000"});
  }

  @Test
  public void testPercentile95MV() {
    String query = "SELECT PERCENTILE95MV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L,
        new String[]{"2147483647.00000"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1129720L, 62480L, 400000L,
        new String[]{"2147483647.00000"});

    brokerResponse = getBrokerResponseForQuery(query + SV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"2147483647.00000"});

    brokerResponse = getBrokerResponseForQuery(query + MV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"2147483647.00000"});
  }

  @Test
  public void testPercentile99MV() {
    String query = "SELECT PERCENTILE99MV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L,
        new String[]{"2147483647.00000"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1129720L, 62480L, 400000L,
        new String[]{"2147483647.00000"});

    brokerResponse = getBrokerResponseForQuery(query + SV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"2147483647.00000"});

    brokerResponse = getBrokerResponseForQuery(query + MV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"2147483647.00000"});
  }

  @Test
  public void testPercentileEst50MV() {
    String query = "SELECT PERCENTILEEST50MV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L,
        new String[]{"2147483647"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1129720L, 62480L, 400000L,
        new String[]{"2147483647"});

    brokerResponse = getBrokerResponseForQuery(query + SV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"2147483647"});

    brokerResponse = getBrokerResponseForQuery(query + MV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"2147483647"});
  }

  @Test
  public void testPercentileEst90MV() {
    String query = "SELECT PERCENTILEEST90MV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L,
        new String[]{"2147483647"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1129720L, 62480L, 400000L,
        new String[]{"2147483647"});

    brokerResponse = getBrokerResponseForQuery(query + SV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"2147483647"});

    brokerResponse = getBrokerResponseForQuery(query + MV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"2147483647"});
  }

  @Test
  public void testPercentileEst95MV() {
    String query = "SELECT PERCENTILEEST95MV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L,
        new String[]{"2147483647"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1129720L, 62480L, 400000L,
        new String[]{"2147483647"});

    brokerResponse = getBrokerResponseForQuery(query + SV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"2147483647"});

    brokerResponse = getBrokerResponseForQuery(query + MV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"2147483647"});
  }

  @Test
  public void testPercentileEst99MV() {
    String query = "SELECT PERCENTILEEST99MV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L,
        new String[]{"2147483647"});

    brokerResponse = getBrokerResponseForQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1129720L, 62480L, 400000L,
        new String[]{"2147483647"});

    brokerResponse = getBrokerResponseForQuery(query + SV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"2147483647"});

    brokerResponse = getBrokerResponseForQuery(query + MV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"2147483647"});
  }
}
