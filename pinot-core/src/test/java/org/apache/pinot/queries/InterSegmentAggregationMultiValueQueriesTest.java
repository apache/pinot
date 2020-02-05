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
import java.util.function.Function;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.startree.hll.HllUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class InterSegmentAggregationMultiValueQueriesTest extends BaseMultiValueQueriesTest {
  private static String SV_GROUP_BY = " group by column8";
  private static String MV_GROUP_BY = " group by column7";
  private static String ORDER_BY_ALIAS = " order by cnt_column6 DESC";

  @Test
  public void testCountMV() {
    String query = "SELECT COUNTMV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L, new String[]{"426752"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 62480L, 1089104L, 62480L, 400000L, new String[]{"62480"});

    brokerResponse = getBrokerResponseForPqlQuery(query + SV_GROUP_BY);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L, new String[]{"231056"});

    brokerResponse = getBrokerResponseForPqlQuery(query + MV_GROUP_BY);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L, new String[]{"199896"});
  }

  @Test
  public void testCastCountMV() {
    String query = "SELECT COUNTMV(column6) as cnt_column6 FROM testTable";
    BrokerResponseNative brokerResponse = getBrokerResponseForSqlQuery(query);
    DataSchema expectedDataSchema = new DataSchema(new String[]{"cnt_column6"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.LONG});
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 400000L, 0L, 400000L, 400000L, Arrays.asList(new Long[][]{new Long[]{426752L}}), 1, expectedDataSchema);
    Assert.assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnName(0), "cnt_column6");

    brokerResponse = getBrokerResponseForSqlQueryWithFilter(query);
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 62480L, 869592L, 62480L, 400000L, Arrays.asList(new Long[][]{new Long[]{62480L}}), 1, expectedDataSchema);
    Assert.assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnName(0), "cnt_column6");

    brokerResponse = getBrokerResponseForSqlQuery(query + SV_GROUP_BY + ORDER_BY_ALIAS);
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 400000L, 0L, 800000L, 400000L, Arrays.asList(new Long[][]{new Long[]{231056L}}), 10, expectedDataSchema);
    Assert.assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnName(0), "cnt_column6");

    brokerResponse = getBrokerResponseForSqlQuery(query + MV_GROUP_BY + ORDER_BY_ALIAS);
    QueriesTestUtils
        .testInterSegmentResultTable(brokerResponse, 400000L, 0L, 800000L, 400000L, Arrays.asList(new Long[][]{new Long[]{199896L}}), 10, expectedDataSchema);
    Assert.assertEquals(brokerResponse.getResultTable().getDataSchema().getColumnName(0), "cnt_column6");
  }

  @Test
  public void testMaxMV() {
    String query = "SELECT MAXMV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L,
        new String[]{"2147483647.00000"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1089104L, 62480L, 400000L,
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
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1089104L, 62480L, 400000L,
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
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1089104L, 62480L, 400000L,
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
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1089104L, 62480L, 400000L,
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
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1089104L, 62480L, 400000L,
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
        .testInterSegmentAggregationResult(brokerResponse, 62480L, 1089104L, 62480L, 400000L, new String[]{"1186"});

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
        .testInterSegmentAggregationResult(brokerResponse, 62480L, 1089104L, 62480L, 400000L, new String[]{"1296"});

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
    Function<Serializable, String> cardinalityExtractor =
        value -> String.valueOf(HllUtil.buildHllFromBytes(BytesUtils.toBytes((String) value)).cardinality());

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L, cardinalityExtractor,
            new String[]{"20039"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 62480L, 1089104L, 62480L, 400000L, cardinalityExtractor,
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
    String query = "SELECT PERCENTILE50MV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L,
        new String[]{"2147483647.00000"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1089104L, 62480L, 400000L,
        new String[]{"2147483647.00000"});

    brokerResponse = getBrokerResponseForPqlQuery(query + SV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"2147483647.00000"});

    brokerResponse = getBrokerResponseForPqlQuery(query + MV_GROUP_BY);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L,
        new String[]{"2147483647.00000"});
  }

  @Test
  public void testPercentile90MV() {
    String query = "SELECT PERCENTILE90MV(column6) FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 400000L, 400000L,
        new String[]{"2147483647.00000"});

    brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1089104L, 62480L, 400000L,
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
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1089104L, 62480L, 400000L,
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
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1089104L, 62480L, 400000L,
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
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1089104L, 62480L, 400000L,
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
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1089104L, 62480L, 400000L,
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
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1089104L, 62480L, 400000L,
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
    QueriesTestUtils.testInterSegmentAggregationResult(brokerResponse, 62480L, 1089104L, 62480L, 400000L,
        new String[]{"2147483647"});

    brokerResponse = getBrokerResponseForPqlQuery(query + SV_GROUP_BY);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L, new String[]{"2147483647"});

    brokerResponse = getBrokerResponseForPqlQuery(query + MV_GROUP_BY);
    QueriesTestUtils
        .testInterSegmentAggregationResult(brokerResponse, 400000L, 0L, 800000L, 400000L, new String[]{"2147483647"});
  }

  @Test
  public void testNumGroupsLimit() {
    String query = "SELECT COUNT(*) FROM testTable GROUP BY column6";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    assertFalse(brokerResponse.isNumGroupsLimitReached());

    brokerResponse = getBrokerResponseForPqlQuery(query, new InstancePlanMakerImplV2(1000, 1000));
    assertTrue(brokerResponse.isNumGroupsLimitReached());
  }
}
