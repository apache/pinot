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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.query.FastFilteredCountOperator;
import org.apache.pinot.core.plan.FilterPlanNode;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.plan.maker.PlanMaker;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImplTestUtils;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Tests [InvertedIndexFilterOperator] on a consuming segment that keeps indexing between query planning and
/// execution. numDocs is read at planning time, but the mutable inverted index returns its live bitmaps at execution
/// time, so the documents ingested in between must be ignored. Otherwise exclusive predicates (`<>`, `NOT IN`)
/// return rows with the excluded value, and the count shortcuts are off.
///
/// `env` is a single-value column, and the multi-value column `tags` holds the same value plus `all` in each row.
public class InvertedIndexFilterOperatorConsumingSegmentTest {
  private static final String ENV_COLUMN = "env";
  private static final String TAGS_COLUMN = "tags";
  private static final String TIME_COLUMN = "ts";
  private static final long TIME_VALUE = 1000L;
  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName("testTable")
      .addSingleValueDimension(ENV_COLUMN, DataType.STRING)
      .addMultiValueDimension(TAGS_COLUMN, DataType.STRING)
      .addSingleValueDimension(TIME_COLUMN, DataType.LONG)
      .build();
  private static final PlanMaker PLAN_MAKER = new InstancePlanMakerImplV2();
  // Scan-based filter on a raw column: the AND then applies the inverted index bitmap as is, without iterating it
  // within [0, numDocs)
  private static final String SCAN_FILTER = "ts >= 0 AND ts <= 2000";

  private MutableSegment _segment;

  @BeforeMethod
  public void setUp() {
    // Null value vectors are kept for the tests with null handling, the other tests index no null row
    _segment = MutableSegmentImplTestUtils.createMutableSegmentImpl(SCHEMA, Set.of(TIME_COLUMN), Set.of(),
        Set.of(ENV_COLUMN, TAGS_COLUMN), false, true);
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
        {"env <> 'staging'", List.of(0, 1, 2, 5)},
        {"env NOT IN ('staging', 'dev')", List.of(0, 1, 2)},
        {"env = 'staging'", List.of(3, 4)},
        {"env IN ('staging', 'dev')", List.of(3, 4, 5)},
        {"tags <> 'staging'", List.of(0, 1, 2, 5)},
        {"tags NOT IN ('staging', 'dev')", List.of(0, 1, 2)}
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

  @Test
  public void testGroupByDoesNotReturnExcludedValue() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT env, COUNT(*) FROM testTable WHERE " + SCAN_FILTER + " AND env <> 'staging' GROUP BY env");
    Operator<?> operator = PLAN_MAKER.makeSegmentPlanNode(new SegmentContext(_segment), queryContext).run();
    ingestAfterPlanning();
    AggregationGroupByResult groupByResult = ((GroupByResultsBlock) operator.nextBlock()).getAggregationGroupByResult();
    Map<String, Long> counts = new HashMap<>();
    Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = groupByResult.getGroupKeyIterator();
    while (groupKeyIterator.hasNext()) {
      GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
      counts.put((String) groupKey._keys[0], (Long) groupByResult.getResultForGroupId(0, groupKey._groupId));
    }
    assertEquals(counts, Map.of("prod", 3L, "dev", 1L));
  }

  @DataProvider
  public Object[][] counts() {
    return new Object[][]{
        {"env <> 'staging'", 4L},
        {"env NOT IN ('staging', 'dev')", 3L},
        {"NOT (env = 'staging')", 4L},
        // AND and OR of inverted index predicates are counted from their bitmaps
        {"env <> 'staging' AND env <> 'dev'", 3L},
        {"env = 'staging' OR env = 'dev'", 3L},
        // A multi-value column is counted on the union of the bitmaps, for 1, 2 and more dictionary ids
        {"tags <> 'staging'", 4L},
        {"tags NOT IN ('staging', 'dev')", 3L},
        {"tags IN ('staging', 'dev', 'prod')", 6L}
    };
  }

  @Test(dataProvider = "counts")
  public void testFastCountOnlyCoversDocsVisibleAtPlanning(String predicate, long expectedCount) {
    assertFastCount("SELECT COUNT(*) FROM testTable WHERE " + predicate, expectedCount);
  }

  @DataProvider
  public Object[][] countsWithNullHandling() {
    // The null row is UNKNOWN, so neither counted as matching nor as not matching
    return new Object[][]{
        {"env <> 'staging'", 4L},
        {"env NOT IN ('staging', 'dev')", 3L},
        {"tags <> 'staging'", 4L},
        {"tags NOT IN ('staging', 'dev')", 3L}
    };
  }

  @Test(dataProvider = "countsWithNullHandling")
  public void testFastCountWithNullHandlingOnlyCoversDocsVisibleAtPlanning(String predicate, long expectedCount) {
    // Document 6, visible at planning time, is null in both columns
    indexNullRow();
    assertFastCount("SET enableNullHandling = true; SELECT COUNT(*) FROM testTable WHERE " + predicate,
        expectedCount);
  }

  private void assertFastCount(String query, long expectedCount) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    Operator<?> operator = PLAN_MAKER.makeSegmentPlanNode(new SegmentContext(_segment), queryContext).run();
    assertTrue(operator instanceof FastFilteredCountOperator, "Expected the fast count path, got: " + operator);
    ingestAfterPlanning();
    assertEquals(((AggregationResultsBlock) operator.nextBlock()).getResults().get(0), expectedCount);
  }

  /// The consuming segment keeps indexing between query planning and execution: 2 staging, then 1 dev and 1 prod
  /// documents
  private void ingestAfterPlanning() {
    index("staging", "staging", "dev", "prod");
  }

  private void index(String... envs) {
    for (String env : envs) {
      GenericRow row = new GenericRow();
      row.putValue(ENV_COLUMN, env);
      row.putValue(TAGS_COLUMN, new Object[]{env, "all"});
      row.putValue(TIME_COLUMN, TIME_VALUE);
      index(row);
    }
  }

  private void indexNullRow() {
    GenericRow row = new GenericRow();
    row.putDefaultNullValue(ENV_COLUMN, "null");
    row.putDefaultNullValue(TAGS_COLUMN, new Object[]{"null"});
    row.putValue(TIME_COLUMN, TIME_VALUE);
    index(row);
  }

  private void index(GenericRow row) {
    try {
      _segment.index(row, null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
