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
package org.apache.pinot.query;

import com.sun.management.ThreadMXBean;
import java.lang.management.ManagementFactory;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.serde.PlanNodeSerializer;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;


/// Planning budgets for queries with large IN lists.
///
/// A planner rule or a Calcite upgrade can make planning much more expensive for these queries without any test
/// failing, because each rule that simplifies a predicate rebuilds the lists in it. These tests plan the shapes that
/// were expensive and compare their cost with a baseline query that holds the same lists in a single-table `WHERE`:
/// - Planner work, measured as the bytes that the planning thread allocates. The bytes change with the JIT state, but
///   their ratio to the baseline does not, so the budgets are ratios.
/// - Plan size, measured as the serialized bytes of the stages.
///
/// Measured ratios:
/// - Join shapes: 1.0x to 1.1x with sealed IN lists (see `SearchSealer`), 2.7x to 3.6x without.
/// - A list outside `WHERE`: 0.9x to 1.7x with sealed IN lists, about 400x without.
/// - A list next to a range: 1.0x the plan bytes, because `RexExpressionUtils` ships the values as one `IN` or
///   `NOT_IN`. With one range per value, it was 10x to 12x.
///
/// The ratios do not catch a cost per value that the baseline has too, so [#testBaselineGrowsLinearly] checks the
/// baseline itself. `SealedInListPlanningTest` checks that sealing does not change the plans.
public class LargeInListPlanningBudgetTest extends QueryEnvironmentTestBase {
  private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getPlatformMXBean(ThreadMXBean.class);
  private static final String THREE_LISTS = "SELECT col1 FROM a WHERE col3 IN (L1) AND col6 IN (L2) AND col7 IN (L3)";
  private static final String ONE_LIST = "SELECT col1 FROM a WHERE col3 IN (L1)";
  private static final String JOINS = "SELECT a.col1, a.col2, b.col2, c.col1, COUNT(*) FILTER (WHERE a.col5), "
      + "SUM(a.col3) FILTER (WHERE a.col6 > 0), MAX(a.col7) FROM a LEFT JOIN b ON a.col1 = b.col1 "
      + "LEFT JOIN c ON a.col2 = c.col2 WHERE a.col3 IN (L1) AND a.col6 IN (L2) AND a.col7 IN (L3) AND a.ts > 10 "
      + "GROUP BY a.col1, a.col2, b.col2, c.col1 ORDER BY 5 DESC LIMIT 50";
  private static final String CASE = "SELECT SUM(CASE WHEN col3 IN (L1) THEN 1 ELSE 0 END) FROM a";
  private static final int JOIN_VALUES = 10_000;
  private static final double JOIN_BUDGET = 1.5;
  private static final int OUTSIDE_WHERE_VALUES = 300;
  private static final double OUTSIDE_WHERE_BUDGET = 5;

  /// Joins, aggregates and sorts above filters with 3 lists, which makes many rules pull the predicates up and simplify
  /// them.
  @DataProvider(name = "joinShapes")
  public Object[][] joinShapes() {
    return new Object[][]{
        {"LEFT JOINs", JOINS, THREE_LISTS},
        {"LEFT JOINs with NOT IN", JOINS.replace(" IN (", " NOT IN ("), THREE_LISTS.replace(" IN (", " NOT IN (")},
        {"INNER JOINs", JOINS.replace("LEFT JOIN", "JOIN"), THREE_LISTS},
        {"3 LEFT JOINs", JOINS.replace("ON a.col2 = c.col2", "ON a.col2 = c.col2 LEFT JOIN d ON a.col1 = d.col2"),
            THREE_LISTS}
    };
  }

  @Test(dataProvider = "joinShapes", timeOut = 120_000)
  public void testJoinShapeAllocation(String name, String query, String baseline) {
    double ratio = allocationRatio(withLists(query, JOIN_VALUES), withLists(baseline, JOIN_VALUES));
    assertTrue(ratio <= JOIN_BUDGET, message(name, "planning allocates", ratio, JOIN_BUDGET));
  }

  /// A list outside the `WHERE` clause, which `SqlToRelConverter` used to expand into an `OR` of equalities and
  /// simplify against itself.
  @DataProvider(name = "listOutsideWhereShapes")
  public Object[][] listOutsideWhereShapes() {
    return new Object[][]{
        {"JOIN ON", "SELECT a.col1 FROM a JOIN b ON a.col1 = b.col1 AND b.col3 IN (L1)"},
        {"LEFT JOIN ON", "SELECT a.col1 FROM a LEFT JOIN b ON a.col1 = b.col1 AND a.col3 IN (L1)"},
        {"CASE", CASE},
        {"CASE with NOT IN", CASE.replace(" IN (", " NOT IN (")},
        {"FILTER", "SELECT COUNT(*) FILTER (WHERE col3 IN (L1)) FROM a"},
        {"SELECT list", "SELECT col3 IN (L1) FROM a"},
        {"GROUP BY", "SELECT CASE WHEN col3 IN (L1) THEN 'x' ELSE 'y' END, COUNT(*) FROM a "
            + "GROUP BY CASE WHEN col3 IN (L1) THEN 'x' ELSE 'y' END"},
        {"window", "SELECT col1, ROW_NUMBER() OVER (PARTITION BY CASE WHEN col3 IN (L1) THEN 1 ELSE 0 END "
            + "ORDER BY col6) FROM a"},
        {"CASE in HAVING", "SELECT col1, COUNT(*) FROM a GROUP BY col1 "
            + "HAVING SUM(CASE WHEN col3 IN (L1) THEN 1 ELSE 0 END) > 0"}
    };
  }

  @Test(dataProvider = "listOutsideWhereShapes", timeOut = 120_000)
  public void testListOutsideWhereAllocation(String name, String query) {
    double ratio =
        allocationRatio(withLists(query, OUTSIDE_WHERE_VALUES), withLists(ONE_LIST, OUTSIDE_WHERE_VALUES));
    assertTrue(ratio <= OUTSIDE_WHERE_BUDGET, message(name, "planning allocates", ratio, OUTSIDE_WHERE_BUDGET));
  }

  /// The budgets fail without sealed IN lists. If this test fails, the budgets above no longer catch the regressions
  /// that they are meant for.
  @Test(timeOut = 120_000)
  public void testBudgetsFailWithoutSealing() {
    String unsealed = "SET sealedInListThreshold=0; ";
    double joinRatio = allocationRatio(unsealed + withLists(JOINS, JOIN_VALUES),
        unsealed + withLists(THREE_LISTS, JOIN_VALUES));
    assertTrue(joinRatio > JOIN_BUDGET, message("LEFT JOINs without sealing, which must exceed the budget",
        "planning allocates", joinRatio, JOIN_BUDGET));
    double caseRatio = allocationRatio(unsealed + withLists(CASE, OUTSIDE_WHERE_VALUES),
        unsealed + withLists(ONE_LIST, OUTSIDE_WHERE_VALUES));
    assertTrue(caseRatio > OUTSIDE_WHERE_BUDGET, message("CASE without sealing, which must exceed the budget",
        "planning allocates", caseRatio, OUTSIDE_WHERE_BUDGET));
  }

  /// The baseline itself allocates in proportion to the number of values.
  @Test(timeOut = 120_000)
  public void testBaselineGrowsLinearly() {
    double ratio = allocationRatio(withLists(THREE_LISTS, 20_000), withLists(THREE_LISTS, 5_000));
    assertTrue(ratio <= 6, message("4x the values", "planning allocates", ratio, 6));
  }

  /// A list next to a range on the same column, which fold into one Sarg with single values and a range.
  @DataProvider(name = "listAndRangeShapes")
  public Object[][] listAndRangeShapes() {
    return new Object[][]{
        {"IN or a range", "SELECT col1 FROM a WHERE col3 IN (L1) OR col3 > 100000000"},
        {"NOT IN and a range", "SELECT col1 FROM a WHERE col3 NOT IN (L1) AND col3 > 0"}
    };
  }

  @Test(dataProvider = "listAndRangeShapes")
  public void testListAndRangePlanSize(String name, String query) {
    double ratio = (double) planBytes(withLists(query, 20_000)) / planBytes(withLists(ONE_LIST, 20_000));
    assertTrue(ratio <= 2, message(name, "the plan has", ratio, 2));
  }

  private static String message(String name, String what, double ratio, double budget) {
    return String.format("%s: %s %.2fx the bytes of the baseline (budget %.2fx)", name, what, ratio, budget);
  }

  /// Returns the bytes that planning the query allocates, divided by the bytes that planning the baseline allocates.
  private double allocationRatio(String query, String baseline) {
    if (!THREAD_MX_BEAN.isThreadAllocatedMemorySupported() || !THREAD_MX_BEAN.isThreadAllocatedMemoryEnabled()) {
      throw new SkipException("The JVM does not measure the bytes that a thread allocates");
    }
    return (double) allocatedBytes(query) / allocatedBytes(baseline);
  }

  /// Returns the bytes that the current thread allocates to plan the query: the smallest of 2 runs after a warm-up run.
  private long allocatedBytes(String query) {
    long smallest = Long.MAX_VALUE;
    for (int run = 0; run < 3; run++) {
      long before = THREAD_MX_BEAN.getCurrentThreadAllocatedBytes();
      try (QueryEnvironment.CompiledQuery compiledQuery = _queryEnvironment.compile(query)) {
        compiledQuery.planQuery(1);
      }
      long allocated = THREAD_MX_BEAN.getCurrentThreadAllocatedBytes() - before;
      if (run > 0) {
        smallest = Math.min(smallest, allocated);
      }
    }
    return smallest;
  }

  /// Plans the query and returns the serialized size of its stages.
  private long planBytes(String query) {
    try (QueryEnvironment.CompiledQuery compiledQuery = _queryEnvironment.compile(query)) {
      long bytes = 0;
      for (DispatchablePlanFragment fragment : compiledQuery.planQuery(1).getQueryPlan().getQueryStagesWithoutRoot()) {
        bytes += PlanNodeSerializer.process(fragment.getPlanFragment().getFragmentRoot()).getSerializedSize();
      }
      return bytes;
    }
  }

  /// Replaces `L1`, `L2` and `L3` with lists of distinct integers.
  private static String withLists(String query, int numValues) {
    return query.replace("(L1)", "(" + values(numValues, 7, 3) + ")")
        .replace("(L2)", "(" + values(numValues, 5, 1) + ")")
        .replace("(L3)", "(" + values(numValues, 11, 2) + ")");
  }

  private static String values(int numValues, int step, int offset) {
    return IntStream.range(0, numValues).mapToObj(i -> Long.toString((long) i * step + offset))
        .collect(Collectors.joining(", "));
  }
}
