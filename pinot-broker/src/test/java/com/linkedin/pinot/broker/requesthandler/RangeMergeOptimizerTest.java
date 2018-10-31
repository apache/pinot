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
package com.linkedin.pinot.broker.requesthandler;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Unit test for {@link RangeMergeOptimizerTest}
 */
public class RangeMergeOptimizerTest {

  private static final String TIME_COLUMN = "time";
  private RangeMergeOptimizer _optimizer;
  private FilterQueryOptimizerRequest.FilterQueryOptimizerRequestBuilder _builder;
  private Pql2Compiler _compiler;

  @BeforeClass
  public void setup() {
    _compiler = new Pql2Compiler();
    _optimizer = new RangeMergeOptimizer();
    _builder = new FilterQueryOptimizerRequest.FilterQueryOptimizerRequestBuilder();
  }

  @Test
  public void testRangeIntersection() {
    List<String> range1 = Arrays.asList("");
    List<String> range2 = Arrays.asList("");

    // One range within another
    range1.set(0, "(1\t\t100)");
    range2.set(0, "(10\t\t20]");
    testRangeOptimizer(range1, range2, "(10\t\t20]");

    // One range within another, and one range is unbounded
    range1.set(0, "(*\t\t*)");
    range2.set(0, "[1\t\t2)");
    testRangeOptimizer(range1, range2, "[1\t\t2)");

    // One range with unbounded lower
    range1.set(0, "(*\t\t5]");
    range2.set(0, "[1\t\t20)");
    testRangeOptimizer(range1, range2, "[1\t\t5]");

    // One range with unbounded upper
    range1.set(0, "(5\t\t*)");
    range2.set(0, "[1\t\t20)");
    testRangeOptimizer(range1, range2, "(5\t\t20)");

    // Partial overlap
    range1.set(0, "(1\t\t10]");
    range2.set(0, "[5\t\t20)");
    testRangeOptimizer(range1, range2, "[5\t\t10]");

    // No overlap
    range1.set(0, "(1\t\t10]");
    range2.set(0, "[20\t\t30)");
    testRangeOptimizer(range1, range2, "[20\t\t10]");

    // Single point overlap
    range1.set(0, "(1\t\t10]");
    range2.set(0, "[10\t\t30)");
    testRangeOptimizer(range1, range2, "[10\t\t10]");

    // Redundant case
    range1.set(0, "(*\t\t10]");
    range2.set(0, "(*\t\t30)");
    testRangeOptimizer(range1, range2, "(*\t\t10]");
  }

  @Test
  public void testRangeOptimizer() {

    // Query with single >
    FilterQueryTree actualTree = buildFilterQueryTree("select * from table where time > 10", true);
    FilterQueryTree expectedTree = buildFilterQueryTree("select * from table where time > 10", false);
    compareTrees(actualTree, expectedTree);

    // Query with single >=
    actualTree = buildFilterQueryTree("select * from table where time >= 10", true);
    expectedTree = buildFilterQueryTree("select * from table where time >= 10", false);
    compareTrees(actualTree, expectedTree);

    // Query with single <
    actualTree = buildFilterQueryTree("select * from table where time < 10", true);
    expectedTree = buildFilterQueryTree("select * from table where time < 10", false);
    compareTrees(actualTree, expectedTree);

    // Query with single <=
    actualTree = buildFilterQueryTree("select * from table where time <= 10", true);
    expectedTree = buildFilterQueryTree("select * from table where time <= 10", false);
    compareTrees(actualTree, expectedTree);

    // Query with >= and <=
    actualTree = buildFilterQueryTree("select * from table where time >= 10 and time <= 20 and foo = 'bar'", true);
    expectedTree = buildFilterQueryTree("select * from table where time between 10 and 20 and foo = 'bar'", false);
    compareTrees(actualTree, expectedTree);

    // Query with no intersection
    actualTree = buildFilterQueryTree("select * from table where time >= 10 and time <= 5", true);
    expectedTree = buildFilterQueryTree("select * from table where time between 10 and 5", false);
    compareTrees(actualTree, expectedTree);

    // Query with multiple ranges on time column
    actualTree = buildFilterQueryTree("select * from table where time >= 10 and time <= 20 and time <= 15", true);
    expectedTree = buildFilterQueryTree("select * from table where time between 10 and 15", false);
    compareTrees(actualTree, expectedTree);

    // Query with multiple predicates
    actualTree =
        buildFilterQueryTree("select * from table where time >= 10 and time <= 20 and time <= 15 and foo = 'bar'",
            true);
    expectedTree = buildFilterQueryTree("select * from table where time between 10 and 15 and foo = 'bar'", false);
    compareTrees(actualTree, expectedTree);

    // Query with nested predicates
    actualTree =
        buildFilterQueryTree("select * from table where (time >= 10 and time <= 20) and ((time >= 5 and time <= 15))",
            true);
    expectedTree = buildFilterQueryTree("select * from table where time between 10 and 15", false);
    compareTrees(actualTree, expectedTree);

    // Query with nested predicates where not all ranges can be merged
    actualTree = buildFilterQueryTree(
        "select * from table where (time >= 10 and time <= 20) and (foo1 = 'bar1' and (foo2 = 'bar2' and (time >= 5 and time <= 15)))",
        true);
    expectedTree = buildFilterQueryTree(
        "select * from table where time between 10 and 20 and (foo1 = 'bar1' and (foo2 = 'bar2' and (time between 5 and 15)))",
        false);
    compareTrees(actualTree, expectedTree);

    // Query with time range in different levels of tree and all ranges can be merged
    actualTree =
        buildFilterQueryTree("select * from table where (((time >= 10 and time <= 20) and time >= 5) and time <= 15)",
            true);
    expectedTree = buildFilterQueryTree("select * from table where time between 10 and 15", false);
    compareTrees(actualTree, expectedTree);

    // Query with time range of one value.
    actualTree =
        buildFilterQueryTree("select * from table where (time > 10 and time <= 20) and (time >= 20 and time <= 30)",
            true);
    expectedTree = buildFilterQueryTree("select * from table where time between 20 and 20", false);
    compareTrees(actualTree, expectedTree);

    // Query with OR predicates
    actualTree = buildFilterQueryTree(
        "select * from table where (foo1 = 'bar1' or (foo2 = 'bar2' and (time >= 10 and time <= 20)))", true);
    expectedTree =
        buildFilterQueryTree("select * from table where (foo1 = 'bar1' or (foo2 = 'bar2' and time between 10 and 20))",
            false);
    compareTrees(actualTree, expectedTree);

    // Query with same lower and upper range
    actualTree = buildFilterQueryTree("select * from table where (time >= 10 and time <= 20) and (time between 10 and 20)", true);
    expectedTree = buildFilterQueryTree("select * from table where time between 10 and 20", false);
    compareTrees(actualTree, expectedTree);

    // Query without time column
    actualTree = buildFilterQueryTree("select * from table where foo = 'bar'", true);
    expectedTree = buildFilterQueryTree("select * from table where foo = 'bar'", false);
    compareTrees(actualTree, expectedTree);
  }

  private void testRangeOptimizer(List<String> range1, List<String> range2, String expected) {
    String actual;
    actual = RangeMergeOptimizer.intersectRanges(range1, range2).get(0);
    Assert.assertEquals(actual, expected);
    actual = RangeMergeOptimizer.intersectRanges(range2, range1).get(0);
    Assert.assertEquals(actual, expected);
  }

  /**
   * Helper method to compare two filter query trees
   * @param actualTree Actual tree
   * @param expectedTree Expected tree
   */
  private void compareTrees(FilterQueryTree actualTree, FilterQueryTree expectedTree) {
    Assert.assertNotNull(actualTree);
    Assert.assertNotNull(expectedTree);

    Assert.assertEquals(actualTree.getOperator(), expectedTree.getOperator());
    Assert.assertEquals(actualTree.getColumn(), expectedTree.getColumn());
    Assert.assertEquals(actualTree.getValue(), expectedTree.getValue());

    List<FilterQueryTree> actualChildren = actualTree.getChildren();
    List<FilterQueryTree> expectedChildren = expectedTree.getChildren();

    if (expectedChildren != null) {
      Assert.assertNotNull(actualChildren);
      Assert.assertEquals(actualChildren.size(), expectedChildren.size());

      Map<String, FilterQueryTree> expectedSet = new HashMap<>(expectedChildren.size());
      for (FilterQueryTree expectedChild : expectedChildren) {
        expectedSet.put(expectedChild.getColumn(), expectedChild);
      }

      for (FilterQueryTree actualChild : actualChildren) {
        FilterQueryTree expectedChild = expectedSet.get(actualChild.getColumn());
        compareTrees(actualChild, expectedChild);
      }
    }
  }

  /**
   * Helper method to build the filter query tree for the given query
   * @param query Query for which to build the filter query tree
   * @param optimize If true, then range optimization is performed
   * @return Filter query tree for the query
   */
  private FilterQueryTree buildFilterQueryTree(String query, boolean optimize) {
    BrokerRequest brokerRequest = _compiler.compileToBrokerRequest(query);
    FilterQueryTree filterQueryTree = RequestUtils.generateFilterQueryTree(brokerRequest);

    if (optimize) {
      FilterQueryOptimizerRequest request =
          _builder.setFilterQueryTree(filterQueryTree).setTimeColumn(TIME_COLUMN).build();
      return _optimizer.optimize(request);
    } else {
      return filterQueryTree;
    }
  }
}
