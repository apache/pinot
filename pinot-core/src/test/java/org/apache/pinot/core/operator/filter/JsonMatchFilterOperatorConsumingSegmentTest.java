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
package org.apache.pinot.core.operator.filter;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.query.FastFilteredCountOperator;
import org.apache.pinot.core.plan.FilterPlanNode;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.plan.maker.PlanMaker;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImplTestUtils;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Tests [JsonMatchFilterOperator] on a consuming segment that keeps indexing between query planning and execution.
/// numDocs is read at planning time, but the mutable JSON index answers with its live documents at execution time, so
/// the documents ingested in between must be ignored. Otherwise the AND with another filter returns them, and the
/// count shortcuts are off, e.g. for `NOT JSON_MATCH`, which is counted as numDocs minus the JSON_MATCH count.
public class JsonMatchFilterOperatorConsumingSegmentTest {
  private static final String JSON_COLUMN = "tags";
  private static final String TIME_COLUMN = "ts";
  private static final long TIME_VALUE = 1000L;
  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName("testTable")
      .addSingleValueDimension(JSON_COLUMN, DataType.STRING)
      .addSingleValueDimension(TIME_COLUMN, DataType.LONG)
      .build();
  private static final PlanMaker PLAN_MAKER = new InstancePlanMakerImplV2();
  // Scan-based filter on a raw column: the AND then applies the JSON index bitmap as is, without iterating it within
  // [0, numDocs)
  private static final String SCAN_FILTER = "ts >= 0 AND ts <= 2000";
  private static final String ENV_IS_STAGING = "JSON_MATCH(tags, '\"$.env\" = ''staging''')";
  private static final String ENV_IS_DEV = "JSON_MATCH(tags, '\"$.env\" = ''dev''')";

  private MutableSegment _segment;

  @BeforeMethod
  public void setUp() {
    _segment = MutableSegmentImplTestUtils.createMutableSegmentImpl(SCHEMA, Set.of(JSON_COLUMN, TIME_COLUMN), Set.of(),
        Set.of(), Map.of(JSON_COLUMN, new JsonIndexConfig()), null);
    // Documents visible at planning time: 0..2 prod, 3..4 staging, 5 dev
    index("prod", "prod", "prod", "staging", "staging", "dev");
  }

  @AfterMethod
  public void tearDown() {
    _segment.destroy();
  }

  @DataProvider
  public Object[][] filters() {
    return new Object[][]{
        {ENV_IS_STAGING, List.of(3, 4)},
        // Answered by the JSON index, over the documents that have the key with another value
        {"JSON_MATCH(tags, '\"$.env\" != ''staging''')", List.of(0, 1, 2, 5)},
        {"NOT " + ENV_IS_STAGING, List.of(0, 1, 2, 5)}
    };
  }

  @Test(dataProvider = "filters")
  public void testFilterOnlyReturnsDocsVisibleAtPlanning(String predicate, List<Integer> expectedDocIds) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT * FROM testTable WHERE " + SCAN_FILTER + " AND " + predicate);
    BaseFilterOperator filterOperator = new FilterPlanNode(new SegmentContext(_segment), queryContext).run();
    ingestAfterPlanning();
    assertEquals(TestUtils.getDocIds(filterOperator.nextBlock().getBlockDocIdSet()), expectedDocIds);
  }

  @DataProvider
  public Object[][] counts() {
    return new Object[][]{
        {ENV_IS_STAGING, 2L},
        {"JSON_MATCH(tags, '\"$.env\" != ''staging''')", 4L},
        {"NOT " + ENV_IS_STAGING, 4L},
        // AND and OR of JSON_MATCH predicates are counted from their bitmaps
        {ENV_IS_STAGING + " OR " + ENV_IS_DEV, 3L},
        {"NOT " + ENV_IS_STAGING + " AND NOT " + ENV_IS_DEV, 3L}
    };
  }

  @Test(dataProvider = "counts")
  public void testFastCountOnlyCoversDocsVisibleAtPlanning(String predicate, long expectedCount) {
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM testTable WHERE " + predicate);
    Operator<?> operator = PLAN_MAKER.makeSegmentPlanNode(new SegmentContext(_segment), queryContext).run();
    assertTrue(operator instanceof FastFilteredCountOperator, "Expected the fast count path, got: " + operator);
    ingestAfterPlanning();
    assertEquals(((AggregationResultsBlock) operator.nextBlock()).getResults().get(0), expectedCount);
  }

  /// The consuming segment keeps indexing between query planning and execution: documents 6..7 staging, 8 dev, 9 prod
  private void ingestAfterPlanning() {
    index("staging", "staging", "dev", "prod");
  }

  private void index(String... envs) {
    for (String env : envs) {
      GenericRow row = new GenericRow();
      row.putValue(JSON_COLUMN, "{\"env\":\"" + env + "\"}");
      row.putValue(TIME_COLUMN, TIME_VALUE);
      try {
        _segment.index(row, null);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
