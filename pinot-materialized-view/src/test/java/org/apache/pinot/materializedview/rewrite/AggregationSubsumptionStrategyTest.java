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
package org.apache.pinot.materializedview.rewrite;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.materializedview.metadata.MaterializedViewDefinitionMetadata;
import org.apache.pinot.materializedview.rewrite.MaterializedViewMetadataCache.MaterializedViewCacheEntry;
import org.apache.pinot.materializedview.rewrite.strategy.AggregationSubsumptionStrategy;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class AggregationSubsumptionStrategyTest {

  private AggregationSubsumptionStrategy _strategy;

  @BeforeClass
  public void setUp() {
    _strategy = new AggregationSubsumptionStrategy();
  }

  private MaterializedViewCacheEntry createEntry(String viewTableName, String baseTable, String definedSql) {
    MaterializedViewDefinitionMetadata definition = new MaterializedViewDefinitionMetadata(
        viewTableName,
        Collections.singletonList(baseTable),
        definedSql,
        new HashMap<>(),
        null);
    PinotQuery compiledQuery = CalciteSqlParser.compileToPinotQuery(definedSql);
    return new MaterializedViewCacheEntry(definition, compiledQuery, 1L, java.util.Map.of());
  }

  // -----------------------------------------------------------------------
  //  Basic aggregation matching
  // -----------------------------------------------------------------------

  @Test
  public void testBasicAggregationMatch() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getMaterializedViewTableNameWithType(), "mv_orders_OFFLINE");
    assertEquals(result.getCost(), 6.0);
    assertEquals(result.getMaterializedViewQuery().getDataSource().getTableName(), "mv_orders_OFFLINE");
  }

  @Test
  public void testSelectSubset() {
    String definedSql =
        "SELECT city, SUM(revenue) AS sum_rev, COUNT(revenue) AS cnt FROM orders GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 6.0);
    assertEquals(result.getMaterializedViewQuery().getSelectList().size(), 2);
  }

  // -----------------------------------------------------------------------
  //  GROUP BY order insensitivity
  // -----------------------------------------------------------------------

  @Test
  public void testGroupByOrderInsensitive() {
    String definedSql =
        "SELECT city, state, SUM(revenue) AS sum_rev FROM orders GROUP BY city, state";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, state, SUM(revenue) FROM orders GROUP BY state, city");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 6.0);
  }

  // -----------------------------------------------------------------------
  //  Alias handling
  // -----------------------------------------------------------------------

  @Test
  public void testAliasInsensitiveMatching() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) AS total_revenue FROM orders GROUP BY city");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 6.0);
  }

  @Test
  public void testRewrittenSelectPreservesUserAlias() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) AS r_sum FROM orders GROUP BY city");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    PinotQuery rewritten = result.getMaterializedViewQuery();

    List<Expression> selectList = rewritten.getSelectList();
    assertEquals(selectList.size(), 2);

    assertEquals(selectList.get(0).getIdentifier().getName(), "city");

    // SUM(revenue) AS r_sum → SUM(sum_rev) AS r_sum
    Function aliasFunc = selectList.get(1).getFunctionCall();
    assertNotNull(aliasFunc);
    assertEquals(aliasFunc.getOperator(), "as");
    Function innerAgg = aliasFunc.getOperands().get(0).getFunctionCall();
    assertNotNull(innerAgg);
    assertEquals(innerAgg.getOperator(), "sum");
    assertEquals(innerAgg.getOperands().get(0).getIdentifier().getName(), "sum_rev");
    assertEquals(aliasFunc.getOperands().get(1).getIdentifier().getName(), "r_sum");
  }

  @Test
  public void testRewrittenSelectNoAlias() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    List<Expression> selectList = result.getMaterializedViewQuery().getSelectList();
    assertEquals(selectList.size(), 2);

    assertEquals(selectList.get(0).getIdentifier().getName(), "city");
    // SUM(revenue) → SUM(sum_rev) AS sum(revenue) — implicit alias preserves the user's
    // expected result column name when there is no explicit AS clause.
    Function aliasFunc = selectList.get(1).getFunctionCall();
    assertNotNull(aliasFunc);
    assertEquals(aliasFunc.getOperator(), "as");
    Function reAgg = aliasFunc.getOperands().get(0).getFunctionCall();
    assertNotNull(reAgg);
    assertEquals(reAgg.getOperator(), "sum");
    assertEquals(reAgg.getOperands().get(0).getIdentifier().getName(), "sum_rev");
    assertEquals(aliasFunc.getOperands().get(1).getIdentifier().getName(), "sum(revenue)");
  }

  // -----------------------------------------------------------------------
  //  GROUP BY mismatch
  // -----------------------------------------------------------------------

  @Test
  public void testNoMatchUserHasMoreGroupByColumns() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, state, SUM(revenue) FROM orders GROUP BY city, state");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  @Test
  public void testFinerMaterializedViewGranularityBasicSum() {
    String definedSql =
        "SELECT city, state, SUM(revenue) AS sum_rev FROM orders GROUP BY city, state";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result, "Finer MV granularity should match via re-aggregation");
    assertEquals(result.getCost(), 6.0);

    PinotQuery rewritten = result.getMaterializedViewQuery();
    assertNotNull(rewritten.getGroupByList(), "GROUP BY should be retained");
    assertEquals(rewritten.getGroupByList().size(), 1);
    assertEquals(rewritten.getGroupByList().get(0).getIdentifier().getName(), "city");

    List<Expression> selectList = rewritten.getSelectList();
    assertEquals(selectList.size(), 2);
    assertEquals(selectList.get(0).getIdentifier().getName(), "city");
    // SUM(revenue) → SUM(sum_rev) AS sum(revenue) — implicit alias preserves result column name.
    Function aliasFunc = selectList.get(1).getFunctionCall();
    assertNotNull(aliasFunc);
    assertEquals(aliasFunc.getOperator(), "as");
    Function reAgg = aliasFunc.getOperands().get(0).getFunctionCall();
    assertNotNull(reAgg);
    assertEquals(reAgg.getOperator(), "sum");
    assertEquals(reAgg.getOperands().get(0).getIdentifier().getName(), "sum_rev");
    assertEquals(aliasFunc.getOperands().get(1).getIdentifier().getName(), "sum(revenue)");
  }

  @Test
  public void testWholeTableUserAgainstGroupedMaterializedView() {
    // User query has no GROUP BY but contains an aggregation; MV is grouped.  Engine should
    // re-aggregate the MV's per-group rows into one whole-table result on the MV side.  This
    // pins the fix for groupByMatches accepting the userIsWholeTable && !mvIsWholeTable case.
    String definedSql =
        "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery("SELECT SUM(revenue) FROM orders");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result, "User whole-table over grouped MV should re-aggregate");
    PinotQuery rewritten = result.getMaterializedViewQuery();
    assertTrue(rewritten.getGroupByList() == null || rewritten.getGroupByList().isEmpty(),
        "Rewritten query must not have GROUP BY (whole-table re-aggregation)");
    List<Expression> selectList = rewritten.getSelectList();
    assertEquals(selectList.size(), 1);
    // SUM(revenue) → SUM(sum_rev) AS sum(revenue) — implicit alias preserves result column name.
    Function aliasFunc = selectList.get(0).getFunctionCall();
    assertNotNull(aliasFunc);
    assertEquals(aliasFunc.getOperator(), "as");
    Function reAgg = aliasFunc.getOperands().get(0).getFunctionCall();
    assertNotNull(reAgg);
    assertEquals(reAgg.getOperator(), "sum");
    assertEquals(reAgg.getOperands().get(0).getIdentifier().getName(), "sum_rev");
    assertEquals(aliasFunc.getOperands().get(1).getIdentifier().getName(), "sum(revenue)");
  }

  @Test
  public void testNoMatchDifferentGroupByColumns() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT state, SUM(revenue) FROM orders GROUP BY state");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  // -----------------------------------------------------------------------
  //  HAVING remap
  // -----------------------------------------------------------------------

  @Test
  public void testHavingKeptAsHaving() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city HAVING SUM(revenue) > 1000");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    PinotQuery rewritten = result.getMaterializedViewQuery();

    assertNotNull(rewritten.getGroupByList());
    assertNotNull(rewritten.getHavingExpression());
    assertNull(rewritten.getFilterExpression());

    // HAVING SUM(revenue) > 1000 → HAVING SUM(sum_rev) > 1000
    assertTrue(rewritten.getHavingExpression().toString().contains("sum_rev"));
  }

  @Test
  public void testNoMatchHavingReferencesUnknownAgg() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city HAVING AVG(revenue) > 100");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result, "HAVING references AVG(revenue) which is not in MV projection");
  }

  // -----------------------------------------------------------------------
  //  ORDER BY remap
  // -----------------------------------------------------------------------

  @Test
  public void testOrderByRemappedToMaterializedViewColumn() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city ORDER BY SUM(revenue) DESC");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    PinotQuery rewritten = result.getMaterializedViewQuery();

    List<Expression> orderByList = rewritten.getOrderByList();
    assertNotNull(orderByList);
    assertEquals(orderByList.size(), 1);
    assertTrue(orderByList.get(0).toString().contains("sum_rev"));
  }

  @Test
  public void testOrderByDimensionColumn() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city ORDER BY city ASC");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertNotNull(result.getMaterializedViewQuery().getOrderByList());
  }

  @Test
  public void testNoMatchOrderByUnknownExpression() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city ORDER BY AVG(revenue)");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  // -----------------------------------------------------------------------
  //  Residual WHERE filter
  // -----------------------------------------------------------------------

  @Test
  public void testResidualWhereOnGroupByColumn() {
    String definedSql =
        "SELECT city, SUM(revenue) AS sum_rev FROM orders WHERE region = 'US' GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders WHERE region = 'US' AND city = 'NYC' GROUP BY city");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 7.0);
    assertNotNull(result.getMaterializedViewQuery().getFilterExpression());
  }

  @Test
  public void testNoMatchResidualWhereOnNonGroupByColumn() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders WHERE status = 'active' GROUP BY city");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result, "Residual filter on non-GROUP-BY column 'status' is invalid for aggregation MV");
  }

  // -----------------------------------------------------------------------
  //  Shape rejection
  // -----------------------------------------------------------------------

  @Test
  public void testNoMatchScanUserVsAggMaterializedView() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery("SELECT city FROM orders");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  @Test
  public void testNoMatchAggUserVsScanMaterializedView() {
    String definedSql = "SELECT city, revenue FROM orders";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  // -----------------------------------------------------------------------
  //  SELECT expression not in MV
  // -----------------------------------------------------------------------

  @Test
  public void testNoMatchSelectExprNotInMaterializedView() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, AVG(revenue) FROM orders GROUP BY city");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  // -----------------------------------------------------------------------
  //  Null compiled query
  // -----------------------------------------------------------------------

  @Test
  public void testNullCompiledQuery() {
    MaterializedViewDefinitionMetadata definition = new MaterializedViewDefinitionMetadata(
        "mv_broken_OFFLINE",
        Collections.singletonList("orders"),
        null,
        new HashMap<>(),
        null);
    MaterializedViewCacheEntry entry = new MaterializedViewCacheEntry(
        definition, null, 1L, java.util.Map.of());

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  // -----------------------------------------------------------------------
  //  Rewrite structure verification
  // -----------------------------------------------------------------------

  @Test
  public void testRewrittenQueryRetainsGroupBy() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    PinotQuery rewritten = result.getMaterializedViewQuery();

    assertNotNull(rewritten.getGroupByList());
    assertEquals(rewritten.getGroupByList().size(), 1);
    assertEquals(rewritten.getGroupByList().get(0).getIdentifier().getName(), "city");
    assertNull(rewritten.getHavingExpression());
    assertNull(rewritten.getFilterExpression());
  }

  @Test
  public void testRewrittenQueryHavingAndResidualSeparate() {
    String definedSql =
        "SELECT city, SUM(revenue) AS sum_rev FROM orders WHERE region = 'US' GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders WHERE region = 'US' AND city = 'NYC' "
            + "GROUP BY city HAVING SUM(revenue) > 1000");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    PinotQuery rewritten = result.getMaterializedViewQuery();

    assertNotNull(rewritten.getGroupByList());

    // HAVING kept as HAVING with re-aggregation reference
    assertNotNull(rewritten.getHavingExpression());
    assertTrue(rewritten.getHavingExpression().toString().contains("sum_rev"));

    // Residual WHERE on dimension column
    assertNotNull(rewritten.getFilterExpression());
    assertTrue(rewritten.getFilterExpression().toString().contains("city"));
  }

  @Test
  public void testMatchWhenUserAndConjunctsReordered() {
    // MV defined with two AND conjuncts; user writes the same conjuncts in the opposite order.
    // AND is commutative, so this must match with the no-residual cost and a null rewritten filter.
    String definedSql =
        "SELECT city, SUM(revenue) AS sum_rev FROM orders "
            + "WHERE region = 'US' AND status = 'active' GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders "
            + "WHERE status = 'active' AND region = 'US' GROUP BY city");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result, "AND is commutative: reordered conjuncts must still match");
    assertEquals(result.getCost(), 6.0, "Set-equal filters -> no-residual cost");
    assertNull(result.getMaterializedViewQuery().getFilterExpression(),
        "Set-equal filters should leave the rewritten MV query with no residual filter");
  }

  @Test
  public void testNoFilterWhenFiltersEqualAndNoHaving() {
    String definedSql =
        "SELECT city, SUM(revenue) AS sum_rev FROM orders WHERE region = 'US' GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders WHERE region = 'US' GROUP BY city");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 6.0);
    assertNull(result.getMaterializedViewQuery().getFilterExpression());
    assertNotNull(result.getMaterializedViewQuery().getGroupByList());
  }

  // =======================================================================
  //  Equal granularity with function mismatch (equivalence-based rewrite)
  // =======================================================================

  @Test
  public void testEqualGranularitySketchMismatchUsesAggregationRewrite() {
    String definedSql =
        "SELECT city, DISTINCTCOUNTRAWHLL(user_id) AS raw_hll FROM orders GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, DISTINCTCOUNTHLL(user_id) FROM orders GROUP BY city");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result, "Equal granularity with sketch mismatch should match via aggregation rewrite");
    assertEquals(result.getCost(), 6.0);

    PinotQuery rewritten = result.getMaterializedViewQuery();

    // GROUP BY must be retained (not removed as in scan rewrite)
    assertNotNull(rewritten.getGroupByList());
    assertEquals(rewritten.getGroupByList().size(), 1);
    assertEquals(rewritten.getGroupByList().get(0).getIdentifier().getName(), "city");

    List<Expression> selectList = rewritten.getSelectList();
    assertEquals(selectList.size(), 2);
    assertEquals(selectList.get(0).getIdentifier().getName(), "city");

    // DISTINCTCOUNTHLL(user_id) → DISTINCTCOUNTHLL(raw_hll) AS distinctcounthll(user_id)
    Function aliasFunc = selectList.get(1).getFunctionCall();
    assertNotNull(aliasFunc);
    assertEquals(aliasFunc.getOperator(), "as");
    Function reAgg = aliasFunc.getOperands().get(0).getFunctionCall();
    assertNotNull(reAgg);
    assertEquals(reAgg.getOperator(), "distinctcounthll");
    assertEquals(reAgg.getOperands().get(0).getIdentifier().getName(), "raw_hll");
    assertEquals(aliasFunc.getOperands().get(1).getIdentifier().getName(), "distinctcounthll(user_id)");
  }

  /// Simulates the broker's `handleHLLLog2mOverride` injecting a
  /// `Literal(8)` operand into the user query's DISTINCTCOUNTHLL.
  /// Verifies that:
  ///
  ///   - The match still succeeds (operandsMatch ignores literals).
  ///   - The rewritten query preserves the injected log2m parameter.
  ///
  @Test
  public void testEqualGranularitySketchWithBrokerInjectedLog2m() {
    String definedSql =
        "SELECT city, DISTINCTCOUNTRAWHLL(user_id) AS raw_hll FROM orders GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, DISTINCTCOUNTHLL(user_id) FROM orders GROUP BY city");

    // Simulate handleHLLLog2mOverride: append Literal(8) to DISTINCTCOUNTHLL operands
    for (Expression selectExpr : userQuery.getSelectList()) {
      Function func = selectExpr.getFunctionCall();
      if (func != null && "distinctcounthll".equalsIgnoreCase(func.getOperator())) {
        func.addToOperands(RequestUtils.getLiteralExpression(8));
      }
    }

    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);
    assertNotNull(result, "Should match despite broker-injected log2m literal");
    assertEquals(result.getCost(), 6.0);

    PinotQuery rewritten = result.getMaterializedViewQuery();
    assertNotNull(rewritten.getGroupByList());

    List<Expression> selectList = rewritten.getSelectList();
    assertEquals(selectList.size(), 2);

    // DISTINCTCOUNTHLL(user_id, 8) → DISTINCTCOUNTHLL(raw_hll, 8) AS distinctcounthll(user_id, 8)
    Function aliasFunc = selectList.get(1).getFunctionCall();
    assertNotNull(aliasFunc);
    assertEquals(aliasFunc.getOperator(), "as");
    Function reAgg = aliasFunc.getOperands().get(0).getFunctionCall();
    assertNotNull(reAgg);
    assertEquals(reAgg.getOperator(), "distinctcounthll");
    assertEquals(reAgg.getOperandsSize(), 2, "Rewritten expression must retain trailing literal");
    assertEquals(reAgg.getOperands().get(0).getIdentifier().getName(), "raw_hll");
    assertTrue(reAgg.getOperands().get(1).getLiteral() != null,
        "Second operand must be the preserved log2m literal");
    assertEquals(aliasFunc.getOperands().get(1).getIdentifier().getName(), "distinctcounthll(user_id, 8)");
  }

  // =======================================================================
  //  Finer MV granularity tests (aggregation rewrite)
  // =======================================================================

  @Test
  public void testFinerMaterializedViewCountToSum() {
    String definedSql =
        "SELECT city, state, COUNT(revenue) AS cnt FROM orders GROUP BY city, state";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, COUNT(revenue) FROM orders GROUP BY city");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result, "COUNT -> SUM re-aggregation should work");
    assertEquals(result.getCost(), 6.0);

    PinotQuery rewritten = result.getMaterializedViewQuery();
    List<Expression> selectList = rewritten.getSelectList();
    // COUNT(revenue) → SUM(cnt) AS count(revenue)
    Function aliasFunc = selectList.get(1).getFunctionCall();
    assertNotNull(aliasFunc);
    assertEquals(aliasFunc.getOperator(), "as");
    Function reAgg = aliasFunc.getOperands().get(0).getFunctionCall();
    assertNotNull(reAgg);
    assertEquals(reAgg.getOperator(), "sum");
    assertEquals(reAgg.getOperands().get(0).getIdentifier().getName(), "cnt");
    assertEquals(aliasFunc.getOperands().get(1).getIdentifier().getName(), "count(revenue)");
  }

  @Test
  public void testFinerMaterializedViewSketchMergeDistinctCountHll() {
    String definedSql =
        "SELECT city, state, DISTINCTCOUNTRAWHLL(user_id) AS raw_hll "
            + "FROM orders GROUP BY city, state";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, DISTINCTCOUNTHLL(user_id) FROM orders GROUP BY city");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result, "DISTINCTCOUNTHLL from DISTINCTCOUNTRAWHLL should match");
    assertEquals(result.getCost(), 6.0);

    PinotQuery rewritten = result.getMaterializedViewQuery();
    List<Expression> selectList = rewritten.getSelectList();
    // DISTINCTCOUNTHLL(user_id) → DISTINCTCOUNTHLL(raw_hll) AS distinctcounthll(user_id)
    Function aliasFunc = selectList.get(1).getFunctionCall();
    assertNotNull(aliasFunc);
    assertEquals(aliasFunc.getOperator(), "as");
    Function reAgg = aliasFunc.getOperands().get(0).getFunctionCall();
    assertNotNull(reAgg);
    assertEquals(reAgg.getOperator(), "distinctcounthll");
    assertEquals(reAgg.getOperands().get(0).getIdentifier().getName(), "raw_hll");
    assertEquals(aliasFunc.getOperands().get(1).getIdentifier().getName(), "distinctcounthll(user_id)");
  }

  @Test
  public void testFinerMaterializedViewMixedAggregationTypes() {
    String definedSql =
        "SELECT city, state, SUM(revenue) AS sum_rev, COUNT(revenue) AS cnt, "
            + "MAX(revenue) AS max_rev FROM orders GROUP BY city, state";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue), COUNT(revenue), MAX(revenue) FROM orders GROUP BY city");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 6.0);

    PinotQuery rewritten = result.getMaterializedViewQuery();
    List<Expression> selectList = rewritten.getSelectList();
    assertEquals(selectList.size(), 4);

    // city -> city (dimension, no alias wrap since name matches MV column)
    assertEquals(selectList.get(0).getIdentifier().getName(), "city");
    // SUM(revenue) → SUM(sum_rev) AS sum(revenue)
    Function sumAlias = selectList.get(1).getFunctionCall();
    assertEquals(sumAlias.getOperator(), "as");
    assertEquals(sumAlias.getOperands().get(0).getFunctionCall().getOperator(), "sum");
    assertEquals(sumAlias.getOperands().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(),
        "sum_rev");
    assertEquals(sumAlias.getOperands().get(1).getIdentifier().getName(), "sum(revenue)");
    // COUNT(revenue) → SUM(cnt) AS count(revenue)
    Function countAlias = selectList.get(2).getFunctionCall();
    assertEquals(countAlias.getOperator(), "as");
    assertEquals(countAlias.getOperands().get(0).getFunctionCall().getOperator(), "sum");
    assertEquals(countAlias.getOperands().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(),
        "cnt");
    assertEquals(countAlias.getOperands().get(1).getIdentifier().getName(), "count(revenue)");
    // MAX(revenue) → MAX(max_rev) AS max(revenue)
    Function maxAlias = selectList.get(3).getFunctionCall();
    assertEquals(maxAlias.getOperator(), "as");
    assertEquals(maxAlias.getOperands().get(0).getFunctionCall().getOperator(), "max");
    assertEquals(maxAlias.getOperands().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(),
        "max_rev");
    assertEquals(maxAlias.getOperands().get(1).getIdentifier().getName(), "max(revenue)");
  }

  @Test
  public void testFinerMaterializedViewPreservesUserAlias() {
    String definedSql =
        "SELECT city, state, SUM(revenue) AS sum_rev FROM orders GROUP BY city, state";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) AS total FROM orders GROUP BY city");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    PinotQuery rewritten = result.getMaterializedViewQuery();
    List<Expression> selectList = rewritten.getSelectList();

    // SUM(revenue) AS total → SUM(sum_rev) AS total
    Function aliasFunc = selectList.get(1).getFunctionCall();
    assertNotNull(aliasFunc);
    assertEquals(aliasFunc.getOperator(), "as");
    // Inner: SUM(sum_rev)
    Function innerAgg = aliasFunc.getOperands().get(0).getFunctionCall();
    assertEquals(innerAgg.getOperator(), "sum");
    assertEquals(innerAgg.getOperands().get(0).getIdentifier().getName(), "sum_rev");
    // Alias: total
    assertEquals(aliasFunc.getOperands().get(1).getIdentifier().getName(), "total");
  }

  @Test
  public void testFinerMaterializedViewWithHavingKeptAsHaving() {
    String definedSql =
        "SELECT city, state, SUM(revenue) AS sum_rev FROM orders GROUP BY city, state";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city HAVING SUM(revenue) > 1000");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 6.0);

    PinotQuery rewritten = result.getMaterializedViewQuery();
    // GROUP BY retained
    assertNotNull(rewritten.getGroupByList());
    assertEquals(rewritten.getGroupByList().size(), 1);
    // HAVING kept as HAVING (not converted to WHERE)
    assertNotNull(rewritten.getHavingExpression());
    // HAVING should reference SUM(sum_rev) > 1000
    String havingStr = rewritten.getHavingExpression().toString();
    assertTrue(havingStr.contains("sum_rev"), "HAVING should reference re-aggregated MV column");
  }

  @Test
  public void testFinerMaterializedViewWithResidualWhere() {
    String definedSql =
        "SELECT city, state, SUM(revenue) AS sum_rev FROM orders WHERE region = 'US' "
            + "GROUP BY city, state";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders WHERE region = 'US' AND city = 'NYC' GROUP BY city");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 7.0);

    PinotQuery rewritten = result.getMaterializedViewQuery();
    assertNotNull(rewritten.getGroupByList());
    assertNotNull(rewritten.getFilterExpression());
    assertTrue(rewritten.getFilterExpression().toString().contains("city"));
  }

  @Test
  public void testFinerMaterializedViewRejectUnsupportedFunction() {
    String definedSql =
        "SELECT city, state, SUM(revenue) AS sum_rev FROM orders GROUP BY city, state";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, AVG(revenue) FROM orders GROUP BY city");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result, "AVG has no equivalence rule registered");
  }

  /// Regression test: the MV stores AVG(revenue) as a pre-computed column. Even though the user
  /// query's AVG(revenue) is an exact key in the MV projection map, there is no
  /// AggregationEquivalence rule for AVG (it is not re-aggregatable from a pre-computed average).
  /// The old code fell through to a bare-column rewrite (avg_rev), producing wrong results.
  /// The fix requires a rule to exist before accepting the MV candidate.
  @Test
  public void testRejectAvgEvenWhenMaterializedViewHasExactAvgColumn() {
    // MV materialises AVG(revenue) directly — but there is no distributive re-aggregation rule.
    String definedSql =
        "SELECT city, state, AVG(revenue) AS avg_rev FROM orders GROUP BY city, state";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, AVG(revenue) FROM orders GROUP BY city");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result,
        "AVG has no re-aggregation rule; MV with exact AVG column must be rejected, not silently rewritten "
            + "to a bare identifier that would compute wrong results over finer-granularity groups");
  }

  @Test
  public void testFinerMaterializedViewRejectSketchFunctionMismatch() {
    String definedSql =
        "SELECT city, state, SUM(revenue) AS sum_rev FROM orders GROUP BY city, state";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, DISTINCTCOUNTHLL(revenue) FROM orders GROUP BY city");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result, "MV stores SUM, not DISTINCTCOUNTRAWHLL — sketch merge not possible");
  }

  @Test
  public void testFinerMaterializedViewWithOrderBy() {
    String definedSql =
        "SELECT city, state, SUM(revenue) AS sum_rev FROM orders GROUP BY city, state";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city ORDER BY SUM(revenue) DESC");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 6.0);

    PinotQuery rewritten = result.getMaterializedViewQuery();
    assertNotNull(rewritten.getOrderByList());
    assertEquals(rewritten.getOrderByList().size(), 1);
    // ORDER BY should reference SUM(sum_rev), wrapped in ordering
    String orderByStr = rewritten.getOrderByList().get(0).toString();
    assertTrue(orderByStr.contains("sum_rev"));
  }

  @Test
  public void testFinerMaterializedViewCountStarWithOrderByAlias() {
    String definedSql = "SELECT DaysSinceEpoch, Carrier, SUM(ArrDelay) AS sum_ArrDelay, "
        + "COUNT(*) AS flight_count FROM airlineStats GROUP BY DaysSinceEpoch, Carrier";
    MaterializedViewCacheEntry entry = createEntry("airlineStatsMv_OFFLINE", "airlineStats", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT Carrier, SUM(ArrDelay) AS total_delay, COUNT(*) AS flights FROM airlineStats "
            + "GROUP BY Carrier ORDER BY total_delay DESC LIMIT 10");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result, "Quickstart SUM/COUNT query should match the daily carrier MV");
    PinotQuery rewritten = result.getMaterializedViewQuery();
    assertEquals(rewritten.getSelectList().size(), 3);
    assertEquals(rewritten.getSelectList().get(1).getFunctionCall().getOperands().get(0)
        .getFunctionCall().getOperands().get(0).getIdentifier().getName(), "sum_ArrDelay");
    assertEquals(rewritten.getSelectList().get(2).getFunctionCall().getOperands().get(0)
        .getFunctionCall().getOperands().get(0).getIdentifier().getName(), "flight_count");
    assertTrue(rewritten.getOrderByList().get(0).toString().contains("sum_ArrDelay"));
  }

  @Test
  public void testFinerMaterializedViewSketchWithOrderByAlias() {
    String definedSql = "SELECT DaysSinceEpoch, Carrier, DISTINCTCOUNTRAWHLL(FlightNum) AS raw_hll_FlightNum "
        + "FROM airlineStats GROUP BY DaysSinceEpoch, Carrier";
    MaterializedViewCacheEntry entry = createEntry("airlineStatsMv_OFFLINE", "airlineStats", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT Carrier, DISTINCTCOUNTHLL(FlightNum) AS approx_flight_nums FROM airlineStats "
            + "GROUP BY Carrier ORDER BY approx_flight_nums DESC LIMIT 10");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result, "Quickstart sketch query should match the daily carrier raw-sketch MV");
    PinotQuery rewritten = result.getMaterializedViewQuery();
    assertEquals(rewritten.getSelectList().size(), 2);
    assertEquals(rewritten.getSelectList().get(1).getFunctionCall().getOperands().get(0)
        .getFunctionCall().getOperands().get(0).getIdentifier().getName(), "raw_hll_FlightNum");
    assertTrue(rewritten.getOrderByList().get(0).toString().contains("raw_hll_FlightNum"));
  }
}
