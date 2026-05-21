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
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.materializedview.metadata.MaterializedViewDefinitionMetadata;
import org.apache.pinot.materializedview.rewrite.MaterializedViewMetadataCache.MaterializedViewCacheEntry;
import org.apache.pinot.materializedview.rewrite.strategy.ScanSubsumptionStrategy;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class ScanSubsumptionStrategyTest {

  private ScanSubsumptionStrategy _strategy;

  @BeforeClass
  public void setUp() {
    _strategy = new ScanSubsumptionStrategy();
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
  //  Projection subset matching
  // -----------------------------------------------------------------------

  @Test
  public void testProjectionSubset() {
    String viewSql = "SELECT a, b, c FROM orders";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", viewSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery("SELECT a FROM orders");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 2.0);
    assertEquals(result.getMaterializedViewQuery().getDataSource().getTableName(), "mv_orders_OFFLINE");
    assertEquals(result.getMaterializedViewQuery().getSelectList().size(), 1);
  }

  @Test
  public void testProjectionSubsetMultipleColumns() {
    String viewSql = "SELECT a, b, c FROM orders";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", viewSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery("SELECT a, c FROM orders");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 2.0);
  }

  @Test
  public void testProjectionSubsetDifferentOrder() {
    String viewSql = "SELECT a, b, c FROM orders";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", viewSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery("SELECT c, a FROM orders");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 2.0);
  }

  @Test
  public void testNoMatchColumnNotInMaterializedView() {
    String viewSql = "SELECT a, b FROM orders";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", viewSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery("SELECT a, d FROM orders");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  // -----------------------------------------------------------------------
  //  WHERE / residual filter
  // -----------------------------------------------------------------------

  @Test
  public void testProjectionSubsetWithResidualFilter() {
    String viewSql = "SELECT a, b, c FROM orders WHERE region = 'US'";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", viewSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT a FROM orders WHERE region = 'US' AND b = 'active'");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 3.0);
    assertNotNull(result.getMaterializedViewQuery().getFilterExpression());
  }

  @Test
  public void testNoMatchResidualFilterReferencesColumnNotInMaterializedView() {
    String viewSql = "SELECT a, b FROM orders";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", viewSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT a FROM orders WHERE c = 1");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  @Test
  public void testMatchWhenUserAndConjunctsReordered() {
    // MV has a two-conjunct filter; user writes the same two conjuncts in the opposite order.
    // AND is commutative, so this must match with zero residual and the base (no-residual) cost.
    String viewSql = "SELECT a, b, c FROM orders WHERE region = 'US' AND status = 'active'";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", viewSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT a FROM orders WHERE status = 'active' AND region = 'US'");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result, "AND is commutative: reordered conjuncts must match");
    assertEquals(result.getCost(), 2.0, "Set-equal filters -> no-residual cost");
    assertNull(result.getMaterializedViewQuery().getFilterExpression(),
        "No residual — the rewritten MV query should have no filter");
  }

  @Test
  public void testMatchWhenUserNestedAndEquivalentToFlatMaterializedViewAnd() {
    // MV has a flat 3-way AND; user's filter is nested and reordered. flattenAnd normalizes both.
    String viewSql = "SELECT a, b, c FROM orders WHERE region = 'US' AND status = 'active' AND tier = 'gold'";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", viewSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT a FROM orders WHERE tier = 'gold' AND (status = 'active' AND region = 'US')");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result, "Nested AND with same leaves must match a flat AND");
    assertEquals(result.getCost(), 2.0);
    assertNull(result.getMaterializedViewQuery().getFilterExpression());
  }

  @Test
  public void testProjectionSubsetMaterializedViewNoFilterUserNoFilter() {
    String viewSql = "SELECT a, b, c FROM orders";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", viewSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery("SELECT a, b FROM orders");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 2.0);
    assertNull(result.getMaterializedViewQuery().getFilterExpression());
  }

  // -----------------------------------------------------------------------
  //  Shape rejection
  // -----------------------------------------------------------------------

  @Test
  public void testNoMatchWhenQueryHasGroupBy() {
    String viewSql = "SELECT city, revenue FROM orders";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", viewSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  @Test
  public void testNoMatchWhenMaterializedViewHasGroupBy() {
    String viewSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", viewSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery("SELECT city FROM orders");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  // -----------------------------------------------------------------------
  //  ORDER BY
  // -----------------------------------------------------------------------

  @Test
  public void testNoMatchOrderByColumnNotInMaterializedView() {
    String viewSql = "SELECT a, b FROM orders";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", viewSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT a FROM orders ORDER BY c");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  @Test
  public void testMatchOrderByColumnInMaterializedView() {
    String viewSql = "SELECT a, b, c FROM orders";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", viewSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT a FROM orders ORDER BY b");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 2.0);
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

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery("SELECT a FROM orders");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  // -----------------------------------------------------------------------
  //  SELECT rewrite verification
  // -----------------------------------------------------------------------

  @Test
  public void testRewrittenSelectUsesOriginalColumnNames() {
    String viewSql = "SELECT a, b, c FROM orders";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", viewSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery("SELECT b, a FROM orders");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    List<org.apache.pinot.common.request.Expression> rewrittenSelect =
        result.getMaterializedViewQuery().getSelectList();
    assertEquals(rewrittenSelect.size(), 2);
    assertEquals(rewrittenSelect.get(0).getIdentifier().getName(), "b");
    assertEquals(rewrittenSelect.get(1).getIdentifier().getName(), "a");
  }

  @Test
  public void testRewrittenSelectPreservesUserAlias() {
    String viewSql = "SELECT a, b AS col_b FROM orders";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", viewSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery("SELECT b AS my_b FROM orders");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    List<org.apache.pinot.common.request.Expression> selectList =
        result.getMaterializedViewQuery().getSelectList();
    assertEquals(selectList.size(), 1);

    // "b AS my_b" -> rewritten to "col_b AS my_b"
    org.apache.pinot.common.request.Function aliasFunc = selectList.get(0).getFunctionCall();
    assertNotNull(aliasFunc);
    assertEquals(aliasFunc.getOperator(), "as");
    assertEquals(aliasFunc.getOperands().get(0).getIdentifier().getName(), "col_b");
    assertEquals(aliasFunc.getOperands().get(1).getIdentifier().getName(), "my_b");
  }

  @Test
  public void testRewrittenSelectImplicitAliasPreservesUserColumnName() {
    String viewSql = "SELECT a, b AS col_b FROM orders";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", viewSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery("SELECT b FROM orders");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    List<org.apache.pinot.common.request.Expression> selectList =
        result.getMaterializedViewQuery().getSelectList();
    assertEquals(selectList.size(), 1);

    // "b" (no user alias) -> "col_b AS b" so the client sees the original column name "b" in
    // the result schema rather than the MV-side "col_b".  Without the implicit alias, clients
    // reading the result by column name would silently break.
    org.apache.pinot.common.request.Function aliasFunc = selectList.get(0).getFunctionCall();
    assertNotNull(aliasFunc);
    assertEquals(aliasFunc.getOperator(), "as");
    assertEquals(aliasFunc.getOperands().get(0).getIdentifier().getName(), "col_b");
    assertEquals(aliasFunc.getOperands().get(1).getIdentifier().getName(), "b");
  }

  @Test
  public void testRewrittenSelectNoAliasWhenUserNameMatchesViewName() {
    String viewSql = "SELECT a, b FROM orders";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", viewSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery("SELECT b FROM orders");
    MaterializedViewRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    List<org.apache.pinot.common.request.Expression> selectList =
        result.getMaterializedViewQuery().getSelectList();
    assertEquals(selectList.size(), 1);

    // User's identifier "b" exactly matches the MV column name "b", so no alias wrapping is
    // needed — the natural result column name already matches the user's expectation.
    assertEquals(selectList.get(0).getIdentifier().getName(), "b");
  }
}
