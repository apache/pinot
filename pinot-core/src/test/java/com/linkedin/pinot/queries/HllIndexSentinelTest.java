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

import com.linkedin.pinot.common.query.gen.AvroQueryGenerator;
import com.linkedin.pinot.common.query.gen.AvroQueryGenerator.TestSimpleAggreationQuery;
import com.linkedin.pinot.core.startree.hll.HllConfig;
import com.linkedin.pinot.core.startree.hll.HllConstants;
import com.linkedin.pinot.core.startree.hll.SegmentWithHllIndexCreateHelper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Served the same purpose as QueriesSentinelTest, but extracted out as a separate class to make the logic clearer.
 * In minor places, the logic is not the same as QueriesSentinelTest.
 * E.g. different data loaded, pre-assigned query columns, etc.
 */
public class HllIndexSentinelTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(HllIndexSentinelTest.class);

  private static final String hllDeriveColumnSuffix = HllConstants.DEFAULT_HLL_DERIVE_COLUMN_SUFFIX;
  private static final int hllLog2m = HllConstants.DEFAULT_LOG2M;
  private static final double approximationThreshold = 0.001;
  private static final String tableName = "testTable";
  private final TestHelper testHelper;
  private SegmentWithHllIndexCreateHelper helper;
  private final String segmentName = "testSegment";

  private static final String AVRO_DATA = "data/test_data-sv.avro";

  private static final Set<String> columnsToDeriveHllFields =
      new HashSet<>(Arrays.asList("column1", "column2", "column3",
          "count", "weeksSinceEpochSunday", "daysSinceEpoch",
          "column17", "column18"));

  private static final HllConfig hllConfig =
      new HllConfig(hllLog2m, columnsToDeriveHllFields, hllDeriveColumnSuffix);

  public HllIndexSentinelTest()
      throws IOException {
    testHelper = new TestHelper(tableName, null/*server conf*/);
  }

  @BeforeClass
  public void setup() throws Exception {
    testHelper.init();
    helper = testHelper.buildLoadDefaultHllSegment(hllConfig);
  }

  @AfterClass
  public void tearDown()  {
    helper.cleanTempDir();
    testHelper.close();
  }

  @Test
  public void testFastHllNoGroupBy() throws Exception {
    final int baseValue = 10000000;
    final String[] filterColumns = {"column1" /* first split */, "column17" /* low priority in split */};

    for (String filterColumn: filterColumns) {
      for (String distinctCountColumn : columnsToDeriveHllFields) {
        final List<TestSimpleAggreationQuery> aggCalls = new ArrayList<>();
        aggCalls.add(new TestSimpleAggreationQuery(
            "select fasthll(" + distinctCountColumn + ") from " + tableName +
                " where " + filterColumn + " > " + baseValue + " limit 0",
            0.0));
        aggCalls.add(new TestSimpleAggreationQuery(
            "select distinctcounthll(" + distinctCountColumn + ") from " + tableName +
                " where " + filterColumn + " > " + baseValue + " limit 0",
            0.0));
        ApproximateQueryTestUtil.runApproximationQueries(
            testHelper.queryExecutor, segmentName, aggCalls, approximationThreshold, testHelper.serverMetrics);

        // correct query
        Object ret = ApproximateQueryTestUtil.runQuery(
            testHelper.queryExecutor, segmentName, new TestSimpleAggreationQuery(
                "select distinctcount(" + distinctCountColumn + ") from " + tableName +
                    " where " + filterColumn + " > " + baseValue + " limit 0",
                0.0), testHelper.serverMetrics);
        LOGGER.debug(ret.toString());
      }
    }
  }

  @Test
  public void testFastHllWithGroupBy() throws Exception {
    final int baseValue = 10000000;
    final String[] filterColumns = {"column1" /* first split */, "column17" /* low priority in split */};

    // === info about data/test_data-sv.avro data ===
    // column17: Int, cardinality: 25, has index built
    // column13: String, cardinality: 6, no hll index built
    // column1: Int, cardinality: 6583, has hll index built
    // column9: Int, cardinality: 1738, no hll index built
    final String[] gbyColumns = new String[]{"column17", "column13", "column1", "column9"};

    for (String filterColumn: filterColumns) {
      for (String gbyColumn : gbyColumns) {
        for (String distinctCountColumn : columnsToDeriveHllFields) {
          final List<AvroQueryGenerator.TestGroupByAggreationQuery> groupByCalls = new ArrayList<>();
          groupByCalls.add(new AvroQueryGenerator.TestGroupByAggreationQuery(
              "select fasthll(" + distinctCountColumn + ") from " + tableName +
                  " where " + filterColumn + " < " + baseValue +
                  " group by " + gbyColumn + " limit 0", null));
          groupByCalls.add(new AvroQueryGenerator.TestGroupByAggreationQuery(
              "select distinctcounthll(" + distinctCountColumn + ") from " + tableName +
                  " where " + filterColumn + " < " + baseValue +
                  " group by " + gbyColumn + " limit 0", null));
          ApproximateQueryTestUtil.runApproximationQueries(
              testHelper.queryExecutor, segmentName, groupByCalls, approximationThreshold, testHelper.serverMetrics);

          // correct query
          Object ret = ApproximateQueryTestUtil.runQuery(
              testHelper.queryExecutor, segmentName, new AvroQueryGenerator.TestGroupByAggreationQuery(
                  "select distinctcount(" + distinctCountColumn + ") from " + tableName +
                      " where " + filterColumn + " < " + baseValue +
                      " group by " + gbyColumn + " limit 0", null), testHelper.serverMetrics);
          LOGGER.debug(ret.toString());
        }
      }
    }
  }


}
