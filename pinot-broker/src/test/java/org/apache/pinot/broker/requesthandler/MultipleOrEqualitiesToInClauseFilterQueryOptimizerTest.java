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

import java.util.Map;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.FilterQuery;
import org.apache.pinot.common.request.FilterQueryMap;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MultipleOrEqualitiesToInClauseFilterQueryOptimizerTest {

  private static final Pql2Compiler COMPILER = new Pql2Compiler();
  private static final MultipleOrEqualitiesToInClauseFilterQueryOptimizer OPTIMIZER = new MultipleOrEqualitiesToInClauseFilterQueryOptimizer();

  /**
   * {@link MultipleOrEqualitiesToInClauseFilterQueryOptimizer} returns
   * the same instance of FilterQuery and FilterQueryMap which was originally
   * passed to it as part of BrokerRequest. Same instance indicates a
   * NO-OP optimization
   */
  @Test
  public void testNoOptimization() {
    String query = "SELECT * FROM foo WHERE A IN (1, 2, 3)";
    testNoOptimizationHelper(query);

    query = "SELECT * FROM foo WHERE A = 1";
    testNoOptimizationHelper(query);

    query = "SELECT * FROM foo WHERE A = 1 OR A > 7";
    testNoOptimizationHelper(query);

    query = "SELECT * FROM foo WHERE A IN (1, 2, 3, 4) OR A > 7";
    testNoOptimizationHelper(query);

    query = "SELECT * FROM foo WHERE a = 100 OR (a = 200 AND b = 300)";
    testNoOptimizationHelper(query);

    // NOTE: this query should however be optimized by FlattenOptimizer to get rid of first predicate
    query = "SELECT * FROM foo WHERE a = 100 OR (a = 100 AND b = 300)";
    testNoOptimizationHelper(query);

    query = "SELECT * FROM foo WHERE A = 200 OR B = 300";
    testNoOptimizationHelper(query);

    query = "SELECT * FROM foo WHERE a IN (1, 2, 3, 4) OR b = 100";
    testNoOptimizationHelper(query);

    query = "SELECT * FROM foo WHERE a IN (1, 2, 3, 4) OR b IN (1, 2, 3, 4)";
    testNoOptimizationHelper(query);

    query = "SELECT * FROM foo WHERE a IN (1, 2, 3, 4) OR b = 100 OR (a > 100 and b = 25)";
    testNoOptimizationHelper(query);

    query = "SELECT * FROM foo WHERE a IN (1, 2, 3, 4) OR a > 10 OR b = 100";
    testNoOptimizationHelper(query);

    query = "SELECT * FROM foo WHERE a > 20 AND a < 100";
    testNoOptimizationHelper(query);

    query = "SELECT * FROM foo WHERE a > 20 AND a < 100";
    testNoOptimizationHelper(query);

    query = "SELECT * FROM foo WHERE (a IN (1, 2, 3) OR b IN (4, 5, 6)) AND (a < 20 OR b > 2)";
    testNoOptimizationHelper(query);
  }

  private void testNoOptimizationHelper(String query) {
    BrokerRequest brokerRequest = COMPILER.compileToBrokerRequest(query);

    FilterQuery filterQuery = brokerRequest.getFilterQuery();
    FilterQueryMap filterQueryMap = brokerRequest.getFilterSubQueryMap();
    Map<Integer, FilterQuery> map = filterQueryMap.getFilterQueryMap();

    FilterQueryOptimizer.FilterQueryOptimizationResult optimizationResult =
        OPTIMIZER.optimize(brokerRequest.getFilterQuery(), brokerRequest.getFilterSubQueryMap());

    // this verifies the == behavior to ensure that same instance of FilterQuery
    // is returned if optimization wasn't needed.
    Assert.assertSame(filterQuery, optimizationResult.getFilterQuery());
    Assert.assertSame(filterQueryMap, optimizationResult.getFilterQueryMap());
    Assert.assertSame(map, optimizationResult.getFilterQueryMap().getFilterQueryMap());
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

    String queryToOptimize = "SELECT * FROM foo WHERE a IN (1) OR b IN (2)";
    String goodQuery = "SELECT * FROM foo WHERE a = 1 OR b = 2";
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
    goodQuery = "SELECT * FROM foo WHERE a = 1 OR b = 2 OR c > 20 OR a > 200";
    testHelper(queryToOptimize, goodQuery);

    queryToOptimize = "SELECT * FROM foo WHERE a IN (1) OR b IN (2) OR (c > 20 AND a > 200)";
    goodQuery = "SELECT * FROM foo WHERE a = 1 OR b = 2 OR (c > 20 AND a > 200)";
    testHelper(queryToOptimize, goodQuery);

    // nested rewrite
    queryToOptimize = "SELECT * FROM foo WHERE a IN (1) OR a IN (2) OR b IN (2) OR (c IN (100) AND d IN (200))";
    goodQuery = "SELECT * FROM foo WHERE a IN (1, 2) OR b = 2 OR (c = 100 AND d = 200)";
    testHelper(queryToOptimize, goodQuery);
  }

  @Test
  public void testANDRootOperator() {
    String queryToOptimize = "SELECT * FROM foo WHERE a IN (1) AND b = 3";
    String goodQuery = "SELECT * FROM foo WHERE a = 1 AND b = 3";
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
   * Runs a query to optimize/rewrite and another "good" query which need not
   * be optimized.
   *
   * Verification is done by first checking that "good" query is indeed good
   * and that no optimization/rewrite is required on it.
   *
   * Secondly, we compile good query and get its broker request, FilterQuery
   * and FilterQueryMap
   *
   * We then compile the candidate query and get its broker request, FilterQuery
   * and FilterQueryMap. Optimize this one and compare the resulting FilterQuery
   * and FilterQueryMap with that of good query.
   *
   * @param queryToOptimize candidate query to optimize
   * @param goodQuery good query that is not rewritten
   */
  private void testHelper(String queryToOptimize, String goodQuery) {
    BrokerRequest brokerRequest1 = COMPILER.compileToBrokerRequest(queryToOptimize);
    BrokerRequest brokerRequest2 = COMPILER.compileToBrokerRequest(goodQuery);

    FilterQueryOptimizer.FilterQueryOptimizationResult optimizationResult = OPTIMIZER.optimize(brokerRequest1.getFilterQuery(), brokerRequest1.getFilterSubQueryMap());
    testNoOptimizationHelper(goodQuery);

    FilterQuery optimizedFilterQuery = optimizationResult.getFilterQuery();
    FilterQueryMap optimizedFilterQueryMap = optimizationResult.getFilterQueryMap();

    FilterQuery filterQuery = brokerRequest2.getFilterQuery();
    Assert.assertTrue(optimizedFilterQuery.equals(filterQuery));

    FilterQueryMap filterQueryMap = brokerRequest2.getFilterSubQueryMap();
    Assert.assertTrue(optimizedFilterQueryMap.equals(filterQueryMap));
  }
}