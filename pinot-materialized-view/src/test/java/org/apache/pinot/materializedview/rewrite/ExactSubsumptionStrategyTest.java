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

import java.util.HashMap;
import java.util.List;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.materializedview.metadata.MaterializedViewDefinitionMetadata;
import org.apache.pinot.materializedview.rewrite.MaterializedViewMetadataCache.MaterializedViewCacheEntry;
import org.apache.pinot.materializedview.rewrite.strategy.ExactSubsumptionStrategy;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class ExactSubsumptionStrategyTest {

  private ExactSubsumptionStrategy _strategy;

  @BeforeClass
  public void setUp() {
    _strategy = new ExactSubsumptionStrategy();
  }

  private MaterializedViewCacheEntry createEntry(String viewTableName, String baseTable, String definedSql) {
    MaterializedViewDefinitionMetadata definition = new MaterializedViewDefinitionMetadata(
        viewTableName,
        List.of(baseTable),
        definedSql,
        new HashMap<>(),
        null);
    PinotQuery compiledQuery = CalciteSqlParser.compileToPinotQuery(definedSql);
    return new MaterializedViewCacheEntry(definition, compiledQuery, 1L, java.util.Map.of());
  }

  @Test
  public void testExactMatch() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_revenue FROM orders GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getMaterializedViewTableNameWithType(), "mv_orders_OFFLINE");
    assertEquals(result.getCost(), 0.0);
    assertEquals(result.getMaterializedViewQuery().getDataSource().getTableName(), "mv_orders_OFFLINE");
  }

  @Test
  public void testNoMatchDifferentSelect() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_revenue FROM orders GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, AVG(revenue) FROM orders GROUP BY city");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  @Test
  public void testNoMatchDifferentGroupBy() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_revenue FROM orders GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city, state");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  @Test
  public void testNoMatchWhenUserHasExtraFilter() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_revenue FROM orders GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders WHERE status = 'active' GROUP BY city");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result, "Exact strategy should reject any residual WHERE filter");
  }

  @Test
  public void testNoMatchWhenUserExtendsFilterWithAnd() {
    String definedSql =
        "SELECT city, SUM(revenue) AS sum_revenue FROM orders WHERE region = 'US' GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders WHERE region = 'US' AND status = 'active' GROUP BY city");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result, "Exact strategy should reject any residual WHERE filter");
  }

  @Test
  public void testNoMatchUserFilterSubsetOfMaterializedView() {
    String definedSql =
        "SELECT city, SUM(revenue) AS sum_revenue FROM orders WHERE region = 'US' AND status = 'active' "
            + "GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders WHERE region = 'US' GROUP BY city");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  @Test
  public void testExactMatchWithAndConjunctsReordered() {
    String definedSql =
        "SELECT city, SUM(revenue) AS sum_revenue FROM orders WHERE region = 'US' AND status = 'active' "
            + "GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    /// User writes the same two AND conjuncts in the opposite order.
    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders WHERE status = 'active' AND region = 'US' GROUP BY city");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result, "AND is commutative: reordered conjuncts must still match exactly");
    assertEquals(result.getCost(), 0.0);
    assertNull(result.getMaterializedViewQuery().getFilterExpression(),
        "Exact match drops the filter — MV already enforces it");
  }

  @Test
  public void testNoMatchCompletelyDifferentFilter() {
    String definedSql =
        "SELECT city, SUM(revenue) AS sum_revenue FROM orders WHERE region = 'US' GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders WHERE region = 'EU' GROUP BY city");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  @Test
  public void testNullCompiledQuery() {
    MaterializedViewDefinitionMetadata definition = new MaterializedViewDefinitionMetadata(
        "mv_broken_OFFLINE",
        List.of("orders"),
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

  @Test
  public void testExactMatchNoGroupBy() {
    String definedSql = "SELECT * FROM orders WHERE status = 'active'";
    MaterializedViewCacheEntry entry = createEntry("mv_active_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(definedSql);
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 0.0);
  }

  @Test
  public void testExactMatchIgnoresSelectOrder() {
    String viewSql = "SELECT a, b, c FROM orders";
    String querySql = "SELECT c, a, b FROM orders";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", viewSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(querySql);
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 0.0);
  }

  @Test
  public void testExactMatchIgnoresAliasDifference() {
    String viewSql = "SELECT a, SUM(b) AS b_sum FROM orders GROUP BY a";
    String querySql = "SELECT a, SUM(b) AS total_b FROM orders GROUP BY a";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", viewSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(querySql);
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 0.0);
  }

  @Test
  public void testRewrittenSelectPreservesUserAlias() {
    String viewSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    String querySql = "SELECT city, SUM(revenue) AS r_sum FROM orders GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", viewSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(querySql);
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    PinotQuery rewritten = result.getMaterializedViewQuery();
    assertEquals(rewritten.getDataSource().getTableName(), "mv_orders_OFFLINE");
    assertNull(rewritten.getFilterExpression());

    List<org.apache.pinot.common.request.Expression> selectList = rewritten.getSelectList();
    assertEquals(selectList.size(), 2);

    /// "city" column (no alias in user query) -> simple identifier "city"
    assertEquals(selectList.get(0).getIdentifier().getName(), "city");

    /// "SUM(revenue) AS r_sum" -> rewritten to "sum_rev AS r_sum"
    org.apache.pinot.common.request.Function aliasFunc = selectList.get(1).getFunctionCall();
    assertNotNull(aliasFunc);
    assertEquals(aliasFunc.getOperator(), "as");
    assertEquals(aliasFunc.getOperands().get(0).getIdentifier().getName(), "sum_rev");
    assertEquals(aliasFunc.getOperands().get(1).getIdentifier().getName(), "r_sum");
  }

  @Test
  public void testRewrittenSelectNoAlias() {
    String viewSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    String querySql = "SELECT city, SUM(revenue) FROM orders GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", viewSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(querySql);
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    PinotQuery rewritten = result.getMaterializedViewQuery();
    List<org.apache.pinot.common.request.Expression> selectList = rewritten.getSelectList();
    assertEquals(selectList.size(), 2);

    /// "city" identifier matches MV column name "city" — no alias wrapping required.
    assertEquals(selectList.get(0).getIdentifier().getName(), "city");
    /// SUM(revenue) (no explicit AS) -> "sum_rev AS sum(revenue)"; the implicit alias preserves the
    /// user's expected result-column name so clients reading by column name aren't silently broken.
    org.apache.pinot.common.request.Function aliasFunc = selectList.get(1).getFunctionCall();
    assertNotNull(aliasFunc);
    assertEquals(aliasFunc.getOperator(), "as");
    assertEquals(aliasFunc.getOperands().get(0).getIdentifier().getName(), "sum_rev");
    assertEquals(aliasFunc.getOperands().get(1).getIdentifier().getName(), "sum(revenue)");
  }

  /// Pins the wrong-result fix: `ORDER BY count(*)` against an MV that does NOT project
  /// `count(*)` must NOT match.  `collectReferencedColumns(asc(count(*)))` returns the empty
  /// set (the `*` identifier is filtered out), so the prior `coversReferencedColumns({}, ...)`
  /// returned true vacuously, and `remapExpression` left `count(*)` unchanged in the rewritten
  /// MV query — producing a count over the pre-aggregated MV bucket rows (one per group)
  /// instead of the original base-table rows.  The strengthened `orderByCompatible` now
  /// rejects function-shaped sort keys absent from the projection map.
  @Test
  public void testOrderByAggregateNotInProjectionRejected() {
    String viewSql = "SELECT city, SUM(revenue) AS sum_revenue FROM orders GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", viewSql);

    /// User adds ORDER BY count(*) — an aggregate NOT in the MV's SELECT.  Rewriting against
    /// the pre-aggregated MV would silently change the meaning of count(*).
    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city ORDER BY count(*)");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result, "MV without count(*) projection must not match a user query that ORDER BYs count(*)");
  }

  /// Pins the positive case: `ORDER BY` an aggregate that IS in the MV's SELECT remains
  /// rewritable — `count_star` here maps `count(*) → cs_count`, so the strengthened check
  /// finds `count(*)` as a key in the projection map and accepts the match.
  @Test
  public void testOrderByAggregateInProjectionAccepted() {
    String viewSql = "SELECT city, count(*) AS cs_count FROM orders GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", viewSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, count(*) FROM orders GROUP BY city ORDER BY count(*) DESC");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result, "MV that projects count(*) must accept a user query that ORDER BYs count(*)");
    PinotQuery rewritten = result.getMaterializedViewQuery();
    List<org.apache.pinot.common.request.Expression> orderBy = rewritten.getOrderByList();
    assertNotNull(orderBy);
    /// After remap, ORDER BY count(*) DESC becomes ORDER BY cs_count DESC.
    org.apache.pinot.common.request.Expression sortKey =
        orderBy.get(0).getFunctionCall().getOperands().get(0);
    assertEquals(sortKey.getIdentifier().getName(), "cs_count");
  }

  /// Pins the canonical-stripper fix: `ORDER BY <col> NULLS LAST` compiles to a nested
  /// `nullsLast(asc(<col>))` wrapper.  An `asc`-only stripper would leave the wrapper in place
  /// and over-reject a bare-identifier sort key.  Using `CalciteSqlParser.removeOrderByFunctions`
  /// (the same helper used by `AggregationSubsumptionStrategy`) handles all four wrapper kinds
  /// uniformly.
  @Test
  public void testOrderByWithNullsLastAndAliasedMaterializedViewColumn() {
    String viewSql = "SELECT base_col AS mv_col FROM orders";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", viewSql);

    PinotQuery userQuery =
        CalciteSqlParser.compileToPinotQuery("SELECT base_col FROM orders ORDER BY base_col NULLS LAST");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result, "Nested asc/nullsLast wrapper must not block matching against an aliased MV column");
  }
}
