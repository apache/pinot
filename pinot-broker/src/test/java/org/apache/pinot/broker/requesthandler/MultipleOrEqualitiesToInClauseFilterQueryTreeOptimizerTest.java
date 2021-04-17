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
package org.apache.pinot.broker.requesthandler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.common.utils.request.FilterQueryTree;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.requesthandler.BrokerRequestOptimizer;
import org.apache.pinot.core.requesthandler.MultipleOrEqualitiesToInClauseFilterQueryTreeOptimizer;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test for MultipleOrEqualitiesToInClauseFilterQueryTreeOptimizer.
 */
public class MultipleOrEqualitiesToInClauseFilterQueryTreeOptimizerTest {

  @Test
  public void testSimpleOrCase() {
    // a = 1 OR a = 2 OR a = 3 should be rewritten to a IN (1, 2, 3)
    checkForIdenticalFilterQueryTrees("select * from a where a = 1 OR a = 2 OR a = 3",
        "select * from a where a IN (1, 2, 3)");
  }

  @Test
  public void testWithOtherClause() {
    // a = 1 OR a = 2 OR (a = 3 AND b = 4) -> a IN (1, 2) OR (a = 3 AND b = 4)
    checkForIdenticalFilterQueryTrees("select * from a where a = 1 OR a = 2 OR a = 3 AND b = 4",
        "select * from a where a IN (1, 2) OR (a = 3 AND b = 4)");
  }

  @Test
  public void testTwoOrClauses() {
    // a = 1 OR a = 2 OR a = 3 OR b = 4 OR b = 5 OR b = 6 -> a IN (1,2,3) OR b IN (4,5,6)
    checkForIdenticalFilterQueryTrees("select * from a where a = 1 OR a = 2 OR a = 3 OR b = 4 OR b = 5 OR b = 6",
        "select * from a where a IN (1,2,3) OR b IN (4,5,6)");
  }

  @Test
  public void testMultipleOutOfOrderClauses() {
    // a = 1 OR a = 2 OR a = 3 OR b = 4 OR b = 5 OR b = 6 -> a IN (1,2,3) OR b IN (4,5,6)
    checkForIdenticalFilterQueryTrees("select * from a where a = 1 OR b = 4 OR a = 2 OR b = 5 OR a = 3 OR b = 6",
        "select * from a where a IN (1,2,3) OR b IN (4,5,6)");
  }

  @Test
  public void testDuplicatesAndPullup() {
    // a = 1 OR a = 1 -> a = 1 (equality predicate, not IN clause, eg. a IN (1))
    // This should also remove the OR node
    checkForIdenticalFilterQueryTrees("select * from a where a = 1 OR a = 1", "select * from a where a = 1");

    // (a = 1 OR a = 1) AND b = 2 -> a = 1 AND b = 2 (no OR node either)
    checkOptimizedFilterQueryTreeForQuery("select * from a where (a = 1 OR a = 1) AND b = 2",
        "AND\n" + " a EQUALITY [1]\n" + " b EQUALITY [2]");
  }

  @Test
  public void testEqualityAndInMerge() {
    // a = 1 OR a IN (2,3,4) -> a IN (1,2,3,4)
    testHelper("select * from a where a = 1 OR a IN (2,3,4,31)", "select * from a where a IN (1,2,3,31,4)");
  }

  @Test
  public void testSingularInClauseDedupeAndCollapse() {
    // a IN (1,1) -> a = 1
    checkForIdenticalFilterQueryTrees("select * from a where a IN (1, 1) OR a = 1", "select * from a where a = 1");
  }

  private static final Pql2Compiler COMPILER = new Pql2Compiler();
  public static final BrokerRequestOptimizer BROKER_REQUEST_OPTIMIZER = new BrokerRequestOptimizer();

  private String stripIds(String filterQueryTree) {
    String[] lines = filterQueryTree.split("\n");

    // Strip all FilterQueryTree ids
    for (int i = 0; i < lines.length; i++) {
      lines[i] = lines[i].replaceAll(" \\(.*", "");
    }

    return StringUtil.join("\n", lines);
  }

  private String filterQueryTreeForQuery(String query) {
    BrokerRequest brokerRequest =
        BROKER_REQUEST_OPTIMIZER.optimize(COMPILER.compileToBrokerRequest(query), null /* timeColumn */);
    return RequestUtils.generateFilterQueryTree(brokerRequest).toString();
  }

  private void checkOptimizedFilterQueryTreeForQuery(String query, String optimizedFilterQueryTree) {
    String queryFilterTree = stripIds(filterQueryTreeForQuery(query));
    Assert.assertEquals(queryFilterTree, stripIds(optimizedFilterQueryTree),
        "Optimized filter query trees are different for query " + query);
  }

  private void checkForIdenticalFilterQueryTrees(String query, String optimizedQuery) {
    String queryFilterTree = stripIds(filterQueryTreeForQuery(query));
    String optimizedFilterQueryTree = stripIds(filterQueryTreeForQuery(optimizedQuery));

    Assert.assertEquals(queryFilterTree, stripIds(optimizedFilterQueryTree),
        "Optimized filter query trees are different for query " + query);
  }

  /**
   * {@link MultipleOrEqualitiesToInClauseFilterQueryTreeOptimizer} returns
   * the same FilterQueryTree (both instance and contents) which was originally
   * passed to it as part of BrokerRequest if the query is not rewritten
   */
  @Test
  public void testNoQueryRewrite() {
    String query = "SELECT * FROM foo WHERE A IN (1, 2, 3)";
    testNoQueryRewriteHelper(query);

    query = "SELECT * FROM foo WHERE A = 1";
    testNoQueryRewriteHelper(query);

    query = "SELECT * FROM foo WHERE A = 1 OR A > 7";
    testNoQueryRewriteHelper(query);

    query = "SELECT * FROM foo WHERE A IN (1, 2, 3, 4) OR A > 7";
    testNoQueryRewriteHelper(query);

    query = "SELECT * FROM foo WHERE a = 100 OR (a = 200 AND b = 300)";
    testNoQueryRewriteHelper(query);

    // NOTE: this query should however be optimized by FlattenOptimizer to get rid of first predicate
    query = "SELECT * FROM foo WHERE a = 100 OR (a = 100 AND b = 300)";
    testNoQueryRewriteHelper(query);

    query = "SELECT * FROM foo WHERE A = 200 OR B = 300";
    testNoQueryRewriteHelper(query);

    query = "SELECT * FROM foo WHERE a IN (1, 2, 3, 4) OR b = 100";
    testNoQueryRewriteHelper(query);

    query = "SELECT * FROM foo WHERE a IN (1, 2, 3, 4) OR b IN (1, 2, 3, 4)";
    testNoQueryRewriteHelper(query);

    query = "SELECT * FROM foo WHERE a IN (1, 2, 3, 4) OR b = 100 OR (a > 100 and b = 25)";
    testNoQueryRewriteHelper(query);

    query = "SELECT * FROM foo WHERE a IN (1, 2, 3, 4) OR a > 10 OR b = 100";
    testNoQueryRewriteHelper(query);

    query = "SELECT * FROM foo WHERE a > 20 AND a < 100";
    testNoQueryRewriteHelper(query);

    query = "SELECT * FROM foo WHERE (a IN (1, 2, 3) OR b IN (4, 5, 6)) AND (a < 20 OR b > 2)";
    testNoQueryRewriteHelper(query);
  }

  private void testNoQueryRewriteHelper(String query) {
    BrokerRequest brokerRequest = COMPILER.compileToBrokerRequest(query);
    // get the FilterQueryTree before running optimizer
    FilterQueryTree filterQueryTreeBeforeOptimization = RequestUtils.generateFilterQueryTree(brokerRequest);

    BROKER_REQUEST_OPTIMIZER.optimize(brokerRequest, null);
    FilterQueryTree optimizedFilterQueryTree = RequestUtils.generateFilterQueryTree(brokerRequest);

    // compare both FilterQueryTrees to confirm that filter query
    // tree didn't change at all -- this verifies that for cases where we don't
    // have to rewrite the tree, we return the original tree
    compareFilterQueryTreeIgnoringOrder(filterQueryTreeBeforeOptimization, optimizedFilterQueryTree);
  }

  @Test
  public void testORRootOperatorWithSingleUniqueColumn() {
    // tests for single unique column with EQ/IN filter operators

    String queryToOptimize = "SELECT * FROM foo WHERE a IN (1)";
    String goodQuery = "SELECT * FROM foo WHERE a = 1";
    testHelper(queryToOptimize, goodQuery);

    // 1. multiple EQs to single IN rewrite
    queryToOptimize = "SELECT * FROM foo WHERE A = 100 OR A = 200 OR A = 300";
    goodQuery = "SELECT * FROM foo WHERE A IN (100, 200, 300)";
    testHelper(queryToOptimize, goodQuery);

    // 2. multiple INs to single IN rewrite
    queryToOptimize = "SELECT * FROM foo WHERE A IN (100) OR A IN (200) OR A IN (300)";
    goodQuery = "SELECT * FROM foo WHERE A IN (100, 200, 300)";
    testHelper(queryToOptimize, goodQuery);

    // 3. rewrite EQ and IN as EQ
    queryToOptimize = "SELECT * FROM foo WHERE A = 100 OR A IN (100)";
    goodQuery = "SELECT * FROM foo WHERE A = 100";
    testHelper(queryToOptimize, goodQuery);

    // 4. rewrite EQ and EQ as EQ
    queryToOptimize = "SELECT * FROM foo WHERE A = 100 OR A = 100";
    goodQuery = "SELECT * FROM foo WHERE A = 100";
    testHelper(queryToOptimize, goodQuery);

    // 5. rewrite EQ and IN as IN with deduplication
    queryToOptimize = "SELECT * FROM foo WHERE A = 100 OR A = 200 OR A = 300 OR A IN (200, 300, 400, 500, 600)";
    goodQuery = "SELECT * FROM foo WHERE A IN (100, 200, 300, 400, 500, 600)";
    testHelper(queryToOptimize, goodQuery);

    // tests for single unique column with both EQ/IN and non EQ/IN operators

    // 6. rewrite IN as EQ along with no rewrite for range on the same column
    queryToOptimize = "SELECT * FROM foo WHERE a IN (1) OR a > 7";
    goodQuery = "SELECT * FROM foo WHERE a = 1 OR a > 7";
    testHelper(queryToOptimize, goodQuery);

    // 7. rewrite EQ and EQ as EQ along with no rewrite for range on the same column
    queryToOptimize = "SELECT * FROM foo WHERE a = 1 OR a = 1 OR a > 7";
    goodQuery = "SELECT * FROM foo WHERE a = 1 OR a > 7";
    testHelper(queryToOptimize, goodQuery);

    // 8. rewrite EQ and EQ as IN along with no rewrite for range on the same column
    queryToOptimize = "SELECT * FROM foo WHERE a = 1 OR a = 2 OR a > 7";
    goodQuery = "SELECT * FROM foo WHERE a IN (1, 2) OR a > 7";
    testHelper(queryToOptimize, goodQuery);

    // 9. rewrite EQ and IN as IN along with no rewrite for range on the same column
    queryToOptimize = "SELECT * FROM foo WHERE a = 1 OR a IN (2, 3, 4) OR a > 7";
    goodQuery = "SELECT * FROM foo WHERE a IN (1, 2, 3, 4) OR a > 7";
    testHelper(queryToOptimize, goodQuery);

    // 10. rewrite EQ and IN as IN along with no rewrite for range on the same column
    queryToOptimize = "SELECT * FROM foo WHERE a = 1 OR a IN (2, 3, 4) OR (a > 7 AND a < 20)";
    goodQuery = "SELECT * FROM foo WHERE a IN (1, 2, 3, 4) OR (a > 7 AND a < 20)";
    testHelper(queryToOptimize, goodQuery);
  }

  @Test
  public void testORRootOperatorWithMultipleColumns() {
    // tests for multiple columns with EQ/IN filter operators

    String queryToOptimize = "SELECT * FROM foo WHERE a IN (1)";
    String goodQuery = "SELECT * FROM foo WHERE a = 1";
    testHelper(queryToOptimize, goodQuery);

    queryToOptimize = "SELECT * FROM foo WHERE a IN (1, 1, 1)";
    goodQuery = "SELECT * FROM foo WHERE a = 1";
    testHelper(queryToOptimize, goodQuery);

    queryToOptimize = "SELECT * FROM foo WHERE a IN (1, 2, 3, 4, 1, 2)";
    goodQuery = "SELECT * FROM foo WHERE a IN (1, 2, 3, 4)";
    testHelper(queryToOptimize, goodQuery);

    queryToOptimize = "SELECT * FROM foo WHERE a IN (1) OR b IN (2)";
    goodQuery = "SELECT * FROM foo WHERE a = 1 OR b = 2";
    testHelper(queryToOptimize, goodQuery);

    queryToOptimize = "SELECT * FROM foo WHERE a IN (1, 2, 3, 4) OR b IN (4, 5, 6, 7) OR a IN (2, 5, 6)";
    goodQuery = "SELECT * FROM foo WHERE a IN (1, 2, 3, 4, 5, 6) OR b IN (4, 5, 6, 7)";
    testHelper(queryToOptimize, goodQuery);

    queryToOptimize = "SELECT * FROM foo WHERE a IN (100) OR b = 300";
    goodQuery = "SELECT * FROM foo WHERE a = 100 OR b = 300";
    testHelper(queryToOptimize, goodQuery);

    queryToOptimize = "SELECT * FROM foo WHERE a = 100 OR a = 200 OR b = 300";
    goodQuery = "SELECT * FROM foo WHERE a IN (100, 200) OR b = 300";
    testHelper(queryToOptimize, goodQuery);

    queryToOptimize = "SELECT * FROM foo WHERE a = 1 OR a = 2 OR a IN (4, 5, 6, 7) OR b = 3";
    goodQuery = "SELECT * FROM foo WHERE a IN (1, 2, 4, 5, 6, 7) OR b = 3";
    testHelper(queryToOptimize, goodQuery);

    queryToOptimize = "SELECT * FROM foo WHERE a = 1 OR a = 1 OR b = 3";
    goodQuery = "SELECT * FROM foo WHERE a = 1 OR b = 3";
    testHelper(queryToOptimize, goodQuery);

    // tests for multiple columns with both EQ/IN and non EQ/IN operators

    queryToOptimize = "SELECT * FROM foo WHERE a IN (1) OR b IN (2) OR c > 20";
    goodQuery = "SELECT * FROM foo WHERE a = 1 OR b = 2 OR c > 20";
    testHelper(queryToOptimize, goodQuery);

    queryToOptimize = "SELECT * FROM foo WHERE a IN (1) OR b IN (2) OR c > 20 OR a > 200";
    goodQuery = "SELECT * FROM foo WHERE a = 1 OR b = 2 OR a > 200 OR c > 20";
    testHelper(queryToOptimize, goodQuery);

    queryToOptimize = "SELECT * FROM foo WHERE a IN (1) OR b IN (2) OR (c > 20 AND a > 200)";
    goodQuery = "SELECT * FROM foo WHERE a = 1 OR b = 2 OR (c > 20 AND a > 200)";
    testHelper(queryToOptimize, goodQuery);

    // nested rewrite
    queryToOptimize = "SELECT * FROM foo WHERE a = 1 OR b = 2 OR (c IN (20) AND a > 200)";
    goodQuery = "SELECT * FROM foo WHERE a = 1 OR b = 2 OR (c = 20 AND a > 200)";
    testHelper(queryToOptimize, goodQuery);

    // nested rewrite
    queryToOptimize = "SELECT * FROM foo WHERE a = 1 OR b = 2 OR (c IN (20) AND a IN (200))";
    goodQuery = "SELECT * FROM foo WHERE a = 1 OR b = 2 OR (c = 20 AND a = 200)";
    testHelper(queryToOptimize, goodQuery);

    // nested rewrite
    queryToOptimize = "SELECT * FROM foo WHERE a IN (1) OR a IN (2) OR b IN (2) OR (c IN (100) AND d IN (200))";
    goodQuery = "SELECT * FROM foo WHERE a IN (1, 2) OR b = 2 OR (c = 100 AND d = 200)";
    testHelper(queryToOptimize, goodQuery);

    // nested rewrite
    queryToOptimize = "SELECT * FROM foo WHERE a = 1 OR a IN (2, 3, 4) OR (a > 7 AND a < 20 AND b IN (2))";
    goodQuery = "SELECT * FROM foo WHERE a IN (1, 2, 3, 4) OR (a > 7 AND a < 20 AND b = 2)";
    testHelper(queryToOptimize, goodQuery);

    // nested rewrite
    queryToOptimize = "SELECT * FROM foo WHERE (a IN (1, 2, 3, 4) AND b > 20) OR (a > 7 AND a < 20 AND b IN (2))";
    goodQuery = "SELECT * FROM foo WHERE (a IN (1, 2, 3, 4) AND b > 20) OR (a > 7 AND a < 20 AND b = 2)";
    testHelper(queryToOptimize, goodQuery);

    // nested rewrite
    queryToOptimize =
        "SELECT * FROM foo WHERE (a IN (1, 2, 3, 4) AND (c = 20 OR c = 30 OR c = 40 OR b > 20)) OR (a > 7 AND a < 20 AND b IN (2))";
    goodQuery =
        "SELECT * FROM foo WHERE (a IN (1, 2, 3, 4) AND (c IN (20, 30, 40) OR b > 20)) OR (a > 7 AND a < 20 AND b = 2)";
    testHelper(queryToOptimize, goodQuery);
  }

  @Test
  public void testANDRootOperator() {
    String queryToOptimize = "SELECT * FROM foo WHERE a IN (1) AND b = 3";
    String goodQuery = "SELECT * FROM foo WHERE a = 1 AND b = 3";
    testHelper(queryToOptimize, goodQuery);

    queryToOptimize = "SELECT * FROM foo WHERE a IN (1) AND b IN (3)";
    goodQuery = "SELECT * FROM foo WHERE a = 1 AND b = 3";
    testHelper(queryToOptimize, goodQuery);

    queryToOptimize = "SELECT * FROM foo WHERE (a = 1 OR a = 2) AND b = 3";
    goodQuery = "SELECT * FROM foo WHERE a IN (1, 2) AND b = 3";
    testHelper(queryToOptimize, goodQuery);

    queryToOptimize = "SELECT * FROM foo WHERE (a = 1 OR a = 2) AND b = 3 AND c > 7";
    goodQuery = "SELECT * FROM foo WHERE a IN (1, 2) AND b = 3 AND c > 7";
    testHelper(queryToOptimize, goodQuery);

    queryToOptimize = "SELECT * FROM foo WHERE (a IN (1) OR b IN (3)) AND (b < 20 OR c > 20)";
    goodQuery = "SELECT * FROM foo WHERE (a = 1 OR b = 3) AND (b < 20 OR c > 20)";
    testHelper(queryToOptimize, goodQuery);

    queryToOptimize = "SELECT * FROM foo WHERE (a = 1 OR a IN (1, 2, 3) OR b IN (3)) AND (b < 20 OR c > 20)";
    goodQuery = "SELECT * FROM foo WHERE (a IN (1, 2, 3) OR b = 3) AND (b < 20 OR c > 20)";
    testHelper(queryToOptimize, goodQuery);

    queryToOptimize = "SELECT * FROM foo WHERE (a = 1 OR a IN (1, 2, 3) OR b IN (3)) AND (c IN (20) OR d > 100)";
    goodQuery = "SELECT * FROM foo WHERE (a IN (1, 2, 3) OR b = 3) AND (c = 20 OR d > 100)";
    testHelper(queryToOptimize, goodQuery);
  }

  /**
   * Runs a candidate query to optimize/rewrite and another "good" query
   * which need not be optimized.
   *
   * Verification is done by first checking that "good" query is indeed good
   * and that no optimization/rewrite is required on it.
   *
   * Secondly, we compile good query and get its broker request and FilterQueryTree
   *
   * We then compile the candidate query and get its broker request and FilterQueryTree.
   * Optimize this one and compare the resulting FilterQuery with that of good query.
   *
   * @param queryToOptimize candidate query to optimize
   * @param goodQuery good query that is not rewritten
   */
  private void testHelper(String queryToOptimize, String goodQuery) {
    // first check that goodQuery is not rewritten
    testNoQueryRewriteHelper(goodQuery);

    // compile good query, get broker request and filter query tree
    BrokerRequest goodQueryBrokerRequest = COMPILER.compileToBrokerRequest(goodQuery);
    FilterQueryTree filterQueryTree = RequestUtils.generateFilterQueryTree(goodQueryBrokerRequest);

    // compile candidate query, get broker request and optimize it
    BrokerRequest brokerRequestToOptimize = COMPILER.compileToBrokerRequest(queryToOptimize);
    // this would update the broker request with optimized filter query
    BROKER_REQUEST_OPTIMIZER.optimize(brokerRequestToOptimize, null);
    FilterQueryTree optimizedFilterQueryTree = RequestUtils.generateFilterQueryTree(brokerRequestToOptimize);

    // compare the optimized filter query tree with filter query tree of good query
    compareFilterQueryTreeIgnoringOrder(filterQueryTree, optimizedFilterQueryTree);
  }

  private void compareFilterQueryTreeIgnoringOrder(FilterQueryTree fq1, FilterQueryTree fq2) {
    Assert.assertNotNull(fq1);
    Assert.assertNotNull(fq2);

    Assert.assertEquals(fq1.getOperator(), fq2.getOperator());
    Assert.assertEquals(fq1.getColumn(), fq2.getColumn());

    List<FilterQueryTree> children1 = fq1.getChildren();
    List<FilterQueryTree> children2 = fq2.getChildren();

    if (children1 == null) {
      Assert.assertNull(children2);
    } else {
      Assert.assertNotNull(children2);
    }

    if (children1 == null) {
      // this is a leaf level ROOT operator so we need to only compare predicate
      // values ignoring order since we have already compared the operator name and column name
      // above
      compareValues(fq1.getValue(), fq2.getValue());
    } else {
      Assert.assertEquals(children1.size(), children2.size());
      // this is a non leaf ROOT operator (OR, AND)

      // STEP 1: get its leaf operator children -- IN, EQ, >, <, BETWEEN etc
      List<FilterQueryTree> childrenThatAreLeafOperators1 = getChildrenThatAreLeafOperators(fq1);
      List<FilterQueryTree> childrenThatAreLeafOperators2 = getChildrenThatAreLeafOperators(fq2);
      // compare them by sorting on column and comparing the predicate values ignoring order
      compareLeafOperators(childrenThatAreLeafOperators1, childrenThatAreLeafOperators2);

      // STEP 2: get its non-leaf operator children -- AND, OR
      List<FilterQueryTree> childrenThatAreNonLeafOperators1 = getChildrenThatAreNonLeafOperators(fq1);
      List<FilterQueryTree> childrenThatAreNonLeafOperators2 = getChildrenThatAreNonLeafOperators(fq2);

      // compare the size
      Assert.assertEquals(childrenThatAreNonLeafOperators1.size(), childrenThatAreNonLeafOperators2.size());

      // the optimizer adds them in the same order so we simply need to recurse
      // and let the rest of this code compare the subtrees ignoring order
      for (int i = 0; i < childrenThatAreNonLeafOperators1.size(); i++) {
        FilterQueryTree f1 = childrenThatAreNonLeafOperators1.get(i);
        FilterQueryTree f2 = childrenThatAreNonLeafOperators2.get(i);
        compareFilterQueryTreeIgnoringOrder(f1, f2);
      }
    }
  }

  private void compareLeafOperators(List<FilterQueryTree> childrenThatAreLeafOperators1,
      List<FilterQueryTree> childrenThatAreLeafOperators2) {
    Assert.assertNotNull(childrenThatAreLeafOperators1);
    Assert.assertNotNull(childrenThatAreLeafOperators2);
    Assert.assertEquals(childrenThatAreLeafOperators1.size(), childrenThatAreLeafOperators2.size());
    Comparator<FilterQueryTree> comparator = new Comparator<FilterQueryTree>() {
      @Override
      public int compare(FilterQueryTree o1, FilterQueryTree o2) {
        return o1.getColumn().compareTo(o2.getColumn());
      }
    };

    Collections.sort(childrenThatAreLeafOperators1, comparator);
    Collections.sort(childrenThatAreLeafOperators2, comparator);

    for (int i = 0; i < childrenThatAreLeafOperators1.size(); i++) {
      FilterQueryTree fq1 = childrenThatAreLeafOperators1.get(i);
      FilterQueryTree fq2 = childrenThatAreLeafOperators2.get(i);
      // this will straight go into comparing a root operator that is leaf
      compareFilterQueryTreeIgnoringOrder(fq1, fq2);
    }
  }

  private List<FilterQueryTree> getChildrenThatAreNonLeafOperators(FilterQueryTree filterQueryTree) {
    List<FilterQueryTree> result = new ArrayList<>();
    for (FilterQueryTree child : filterQueryTree.getChildren()) {
      if (child.getOperator() == FilterOperator.AND || child.getOperator() == FilterOperator.OR) {
        result.add(child);
      }
    }
    return result;
  }

  private List<FilterQueryTree> getChildrenThatAreLeafOperators(FilterQueryTree filterQueryTree) {
    List<FilterQueryTree> result = new ArrayList<>();
    for (FilterQueryTree child : filterQueryTree.getChildren()) {
      if (child.getOperator() != FilterOperator.AND && child.getOperator() != FilterOperator.OR) {
        result.add(child);
      }
    }
    return result;
  }

  private void compareValues(List<String> values1, List<String> values2) {
    Assert.assertNotNull(values1);
    Assert.assertNotNull(values2);
    Assert.assertEquals(values1.size(), values2.size());
    Set<String> set = new HashSet<>(values1);
    for (String val : values2) {
      Assert.assertTrue(set.contains(val));
    }
  }
}
