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
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test for MultipleOrEqualitiesToInClauseFilterQueryTreeOptimizer.
 */
public class MultipleOrEqualitiesToInClauseFilterQueryTreeOptimizerTest {
  @Test
  public void testSimpleOrCase() {
    // a = 1 OR a = 2 OR a = 3 should be rewritten to a IN (1, 2, 3)
    checkForIdenticalFilterQueryTrees(
        "select * from a where a = 1 OR a = 2 OR a = 3",
        "select * from a where a IN (1, 2, 3)"
    );
  }

  @Test
  public void testWithOtherClause() {
    // a = 1 OR a = 2 OR (a = 3 AND b = 4) -> a IN (1, 2) OR (a = 3 AND b = 4)
    checkForIdenticalFilterQueryTrees(
        "select * from a where a = 1 OR a = 2 OR a = 3 AND b = 4",
        "select * from a where a IN (1, 2) OR (a = 3 AND b = 4)"
    );
  }

  @Test
  public void testTwoOrClauses() {
    // a = 1 OR a = 2 OR a = 3 OR b = 4 OR b = 5 OR b = 6 -> a IN (1,2,3) OR b IN (4,5,6)
    checkForIdenticalFilterQueryTrees(
        "select * from a where a = 1 OR a = 2 OR a = 3 OR b = 4 OR b = 5 OR b = 6",
        "select * from a where a IN (1,2,3) OR b IN (4,5,6)"
    );
  }

  @Test
  public void testMultipleOutOfOrderClauses() {
    // a = 1 OR a = 2 OR a = 3 OR b = 4 OR b = 5 OR b = 6 -> a IN (1,2,3) OR b IN (4,5,6)
    checkForIdenticalFilterQueryTrees(
        "select * from a where a = 1 OR b = 4 OR a = 2 OR b = 5 OR a = 3 OR b = 6",
        "select * from a where a IN (1,2,3) OR b IN (4,5,6)"
    );
  }

  @Test
  public void testDuplicatesAndPullup() {
    // a = 1 OR a = 1 -> a = 1 (equality predicate, not IN clause, eg. a IN (1))
    // This should also remove the OR node
    checkForIdenticalFilterQueryTrees(
        "select * from a where a = 1 OR a = 1",
        "select * from a where a = 1"
    );

    // (a = 1 OR a = 1) AND b = 2 -> a = 1 AND b = 2 (no OR node either)
    // These are reordered due to an implementation detail
    checkOptimizedFilterQueryTreeForQuery(
        "select * from a where (a = 1 OR a = 1) AND b = 2",
        "AND\n" +
            " b EQUALITY [2]\n" +
            " a EQUALITY [1]"
    );
  }

  @Test
  public void testEqualityAndInMerge() {
    // a = 1 OR a IN (2,3,4) -> a IN (1,2,3,4)
    checkForIdenticalFilterQueryTrees(
        "select * from a where a = 1 OR a IN (2,3,4,31)",
        "select * from a where a IN (1,2,31,3,4)"
    );
  }

  @Test
  public void testSingularInClauseDedupeAndCollapse() {
    // a IN (1,1) -> a = 1
    checkForIdenticalFilterQueryTrees(
        "select * from a where a IN (1, 1) OR a = 1",
        "select * from a where a = 1"
    );
  }

  private static final Pql2Compiler COMPILER = new Pql2Compiler();
  public static final BrokerRequestOptimizer OPTIMIZER = new BrokerRequestOptimizer();

  private String stripIds(String filterQueryTree) {
    String[] lines = filterQueryTree.split("\n");

    // Strip all FilterQueryTree ids
    for (int i = 0; i < lines.length; i++) {
      lines[i] = lines[i].replaceAll(" \\(.*", "");
    }

    return StringUtil.join("\n", lines);
  }

  private String filterQueryTreeForQuery(String query) {
    BrokerRequest brokerRequest = OPTIMIZER.optimize(COMPILER.compileToBrokerRequest(query), null /* timeColumn */);
    return RequestUtils.generateFilterQueryTree(brokerRequest).toString();
  }

  private void checkOptimizedFilterQueryTreeForQuery(String query, String optimizedFilterQueryTree) {
    String queryFilterTree = stripIds(filterQueryTreeForQuery(query));
    Assert.assertEquals(queryFilterTree, stripIds(optimizedFilterQueryTree), "Optimized filter query trees are different for query " + query);
  }

  private void checkForIdenticalFilterQueryTrees(String query, String optimizedQuery) {
    String queryFilterTree = stripIds(filterQueryTreeForQuery(query));
    String optimizedFilterQueryTree = stripIds(filterQueryTreeForQuery(optimizedQuery));

    Assert.assertEquals(queryFilterTree, stripIds(optimizedFilterQueryTree), "Optimized filter query trees are different for query " + query);
  }
}
