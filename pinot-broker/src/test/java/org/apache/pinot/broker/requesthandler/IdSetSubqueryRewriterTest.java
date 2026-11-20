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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Litmus;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests [IdSetSubqueryRewriter] on parsed queries, with a fake runner that returns a given IdSet per subquery.
public class IdSetSubqueryRewriterTest {
  private static final String EMPTY = IdSetSubqueryRewriter.EMPTY_ID_SET;

  @Test
  public void testRewritesInSubqueryInFilters() {
    FakeRunner runner = new FakeRunner().returning("SELECT IDSET(col1) FROM b", "set1")
        .returning("SELECT IDSET(col3) FROM b WHERE col2 = 'x'", "set2");
    List<String> subqueries = assertRewrite(
        "SELECT col1 FROM a WHERE IN_SUBQUERY(col1, 'SELECT IDSET(col1) FROM b') = 1 "
            + "AND NOT IN_SUBQUERY(col3, 'SELECT IDSET(col3) FROM b WHERE col2 = ''x''')",
        "SELECT col1 FROM a WHERE IN_ID_SET(col1, 'set1') = 1 AND NOT IN_ID_SET(col3, 'set2')", false, runner);
    assertEquals(subqueries, List.of("SELECT IDSET(col1) FROM b", "SELECT IDSET(col3) FROM b WHERE col2 = 'x'"));
    assertEquals(runner._ran, subqueries);
    assertEquals(runner._validated, List.of());
  }

  @Test
  public void testRewritesInSubqueryInEveryClause() {
    FakeRunner runner = new FakeRunner().returning("q1", "set1").returning("q2", "set2").returning("q3", "set3")
        .returning("q4", "set4").returning("q5", "set5");
    assertRewrite(
        "SELECT IN_SUBQUERY(a.col1, 'q1'), COUNT(*) FROM a JOIN b ON a.col2 = b.col2 AND IN_SUBQUERY(b.col3, 'q2') = 1 "
            + "GROUP BY 1 HAVING IN_SUBQUERY(MAX(a.col3), 'q3') = 1 "
            + "ORDER BY CASE WHEN IN_SUBQUERY(MIN(b.col1), 'q4') = 1 THEN 1 ELSE IN_SUBQUERY(1, 'q5') END",
        "SELECT IN_ID_SET(a.col1, 'set1'), COUNT(*) FROM a JOIN b ON a.col2 = b.col2 AND IN_ID_SET(b.col3, 'set2') = 1 "
            + "GROUP BY 1 HAVING IN_ID_SET(MAX(a.col3), 'set3') = 1 "
            + "ORDER BY CASE WHEN IN_ID_SET(MIN(b.col1), 'set4') = 1 THEN 1 ELSE IN_ID_SET(1, 'set5') END",
        false, runner);
    assertEquals(runner._ran.size(), 5);
  }

  @Test
  public void testRewritesInSubqueryInNestedQueries() {
    FakeRunner runner = new FakeRunner().returning("q1", "set1").returning("q2", "set2").returning("q3", "set3")
        .returning("q4", "set4");
    assertRewrite(
        "WITH w AS (SELECT col1 FROM b WHERE IN_SUBQUERY(col1, 'q1') = 1) "
            + "SELECT col1 FROM (SELECT col1 FROM a WHERE IN_SUBQUERY(col1, 'q2') = 1) "
            + "WHERE col1 IN (SELECT col1 FROM w) "
            + "AND col1 NOT IN (SELECT col1 FROM c WHERE IN_SUBQUERY(col1, 'q3') = 0) "
            + "UNION ALL SELECT col1 FROM d WHERE IN_SUBQUERY(col1, 'q4') = 1",
        "WITH w AS (SELECT col1 FROM b WHERE IN_ID_SET(col1, 'set1') = 1) "
            + "SELECT col1 FROM (SELECT col1 FROM a WHERE IN_ID_SET(col1, 'set2') = 1) "
            + "WHERE col1 IN (SELECT col1 FROM w) "
            + "AND col1 NOT IN (SELECT col1 FROM c WHERE IN_ID_SET(col1, 'set3') = 0) "
            + "UNION ALL SELECT col1 FROM d WHERE IN_ID_SET(col1, 'set4') = 1",
        false, runner);
    assertEquals(runner._ran.size(), 4);
  }

  @Test
  public void testMatchesEveryNameOfInSubquery() {
    FakeRunner runner = new FakeRunner().returning("q1", "set1").returning("q2", "set2").returning("q3", "set3");
    assertRewrite("SELECT col1 FROM a WHERE INSUBQUERY(col1, 'q1') = 1 AND inSubquery(col2, 'q2') = 1 "
            + "AND in_sub_query(col3, 'q3') = 1",
        "SELECT col1 FROM a WHERE IN_ID_SET(col1, 'set1') = 1 AND IN_ID_SET(col2, 'set2') = 1 "
            + "AND IN_ID_SET(col3, 'set3') = 1", false, runner);
  }

  @Test
  public void testUsesTheEmptyIdSetForNoResult() {
    FakeRunner runner = new FakeRunner().returning("q1", null);
    assertRewrite("SELECT col1 FROM a WHERE IN_SUBQUERY(col1, 'q1') = 0",
        "SELECT col1 FROM a WHERE IN_ID_SET(col1, '" + EMPTY + "') = 0", false, runner);
  }

  @Test
  public void testRunsScalarSubqueries() {
    String subquery1 = "SELECT IDSET(col1) FROM b WHERE col3 > 1";
    String subquery2 = "SELECT IDSET(col2, 'expectedInsertions=10') FROM (SELECT col2 FROM c UNION SELECT col2 FROM d)";
    String subquery3 = "WITH w AS (SELECT col3 FROM b) SELECT IDSET(col3) FROM w";
    String subquery4 = "SELECT IDSET(col1) FROM b\n  -- a comment\n  GROUP BY col2\n  ORDER BY col2 LIMIT 1";
    // The text of a scalar subquery includes its parentheses
    List<String> expected =
        List.of("(" + subquery1 + ")", "(" + subquery2 + ")", "(" + subquery3 + ")", "(\n  " + subquery4 + "\n)");
    FakeRunner runner = new FakeRunner().returning(expected.get(0), "set1").returning(expected.get(1), "set2")
        .returning(expected.get(2), "set3").returning(expected.get(3), "set4");
    List<String> subqueries = assertRewrite(
        "SET timeoutMs = 1000;\nSELECT col1 FROM a WHERE IN_ID_SET(col1, " + expected.get(0) + ") = 1\n"
            + "AND IN_ID_SET(col2, " + expected.get(1) + ") = 1 AND inIdSet(col3, " + expected.get(2) + ") = 1\n"
            + "AND IN_ID_SET(col1, " + expected.get(3) + ") = 0",
        "SELECT col1 FROM a WHERE IN_ID_SET(col1, 'set1') = 1 AND IN_ID_SET(col2, 'set2') = 1 "
            + "AND inIdSet(col3, 'set3') = 1 AND IN_ID_SET(col1, 'set4') = 0", false, runner);
    assertEquals(subqueries, expected);
    assertEquals(runner._validated, expected);
    assertEquals(runner._ran, expected);
  }

  @Test
  public void testLeavesCorrelatedScalarSubqueries() {
    String correlated = "(SELECT IDSET(b.col1) FROM b WHERE b.col2 = a.col2 AND IN_SUBQUERY(b.col3, 'q1') = 1)";
    FakeRunner runner = new FakeRunner().correlated(correlated).returning("q1", "set1");
    List<String> subqueries = assertRewrite("SELECT col1 FROM a WHERE IN_ID_SET(a.col1, " + correlated + ") = 1",
        "SELECT col1 FROM a WHERE IN_ID_SET(a.col1, (SELECT IDSET(b.col1) FROM b WHERE b.col2 = a.col2 "
            + "AND IN_ID_SET(b.col3, 'set1') = 1)) = 1", false, runner);
    // The IN_SUBQUERY in the correlated subquery still runs first
    assertEquals(subqueries, List.of("q1"));
    assertEquals(runner._validated, List.of(correlated));
    assertEquals(runner._ran, List.of("q1"));
  }

  @Test
  public void testLeavesScalarSubqueriesWhoseTextDiffers() {
    // The parser read another text than the given one, e.g. after the broker sanitized it
    String sql = "SELECT col1 FROM a WHERE IN_ID_SET(col1, (SELECT IDSET(col1) FROM b)) = 1";
    SqlNode query = parse(sql);
    FakeRunner runner = new FakeRunner();
    List<String> subqueries =
        IdSetSubqueryRewriter.rewrite(sql.replace("FROM b", "FROM c"), query, false, runner);
    assertEquals(subqueries, List.of());
    assertEquals(runner._validated, List.of());
    assertTrue(query.equalsDeep(parse(sql), Litmus.THROW));
  }

  @Test
  public void testExplainRunsNoSubqueries() {
    String scalarSubquery = "(SELECT IDSET(col1) FROM b)";
    String correlated = "(SELECT IDSET(b.col1) FROM b WHERE b.col2 = a.col2)";
    FakeRunner runner = new FakeRunner().correlated(correlated);
    List<String> subqueries = assertRewrite(
        "EXPLAIN PLAN FOR SELECT col1 FROM a WHERE IN_SUBQUERY(col1, 'q1') = 1 AND IN_ID_SET(col2, "
            + scalarSubquery + ") = 1 AND IN_ID_SET(col3, " + correlated + ") = 1",
        "SELECT col1 FROM a WHERE IN_ID_SET(col1, '" + EMPTY + "') = 1 AND IN_ID_SET(col2, '" + EMPTY + "') = 1 "
            + "AND IN_ID_SET(col3, " + correlated + ") = 1", true, runner);
    assertEquals(subqueries, List.of("q1", scalarSubquery));
    assertEquals(runner._validated, List.of(scalarSubquery, correlated));
    assertEquals(runner._ran, List.of());
  }

  @Test
  public void testLeavesQueriesWithoutIdSetSubqueries() {
    FakeRunner runner = new FakeRunner();
    String sql = "SELECT col1 FROM a WHERE IN_ID_SET(col1, 'AwAAAA==') = 1 AND col2 IN (SELECT col2 FROM b) "
        + "AND col3 = (SELECT MAX(col3) FROM c)";
    assertEquals(assertRewrite(sql, sql, false, runner), List.of());
    assertEquals(runner._validated, List.of());
    assertEquals(runner._ran, List.of());
  }

  @Test
  public void testRejectsInvalidInSubqueryCalls() {
    for (String sql : List.of("SELECT col1 FROM a WHERE IN_SUBQUERY(col1) = 1",
        "SELECT col1 FROM a WHERE IN_SUBQUERY(col1, 'q1', 'q2') = 1",
        "SELECT col1 FROM a WHERE IN_SUBQUERY(col1, col2) = 1",
        "SELECT col1 FROM a WHERE IN_SUBQUERY(col1, (SELECT 'q1')) = 1")) {
      QueryException exception = expectThrows(QueryException.class,
          () -> IdSetSubqueryRewriter.rewrite(sql, parse(sql), false, new FakeRunner()));
      assertEquals(exception.getErrorCode(), QueryErrorCode.QUERY_VALIDATION);
      assertTrue(exception.getMessage().contains("IN_SUBQUERY takes an expression and a string literal"),
          exception.getMessage());
    }
  }

  @Test
  public void testRunsEachSubqueryOnce() {
    String scalarSubquery = "(SELECT IDSET(col1) FROM b)";
    FakeRunner runner = new FakeRunner().returning("q1", "set1").returning(scalarSubquery, "set2");
    List<String> subqueries = assertRewrite(
        "SELECT col1 FROM a WHERE IN_SUBQUERY(col1, 'q1') = 1 OR IN_SUBQUERY(col2, 'q1') = 1 "
            + "OR IN_ID_SET(col1, " + scalarSubquery + ") = 1 OR IN_ID_SET(col2, " + scalarSubquery + ") = 1",
        "SELECT col1 FROM a WHERE IN_ID_SET(col1, 'set1') = 1 OR IN_ID_SET(col2, 'set1') = 1 "
            + "OR IN_ID_SET(col1, 'set2') = 1 OR IN_ID_SET(col2, 'set2') = 1", false, runner);
    assertEquals(subqueries, List.of("q1", scalarSubquery));
    assertEquals(runner._ran, List.of("q1", scalarSubquery));
    assertEquals(runner._validated, List.of(scalarSubquery));
  }

  @Test
  public void testLeavesScalarSubqueriesThatNameWithItems() {
    // On its own, a subquery that names the WITH item b would read table b
    String namesWithItem = "(SELECT IDSET(col1) FROM b)";
    String other = "(SELECT IDSET(col3) FROM c)";
    FakeRunner runner = new FakeRunner().returning(other, "set1");
    assertRewrite("WITH b AS (SELECT col1 FROM d) SELECT col1 FROM a WHERE IN_ID_SET(col1, " + namesWithItem
            + ") = 1 AND IN_ID_SET(col3, " + other + ") = 1",
        "WITH b AS (SELECT col1 FROM d) SELECT col1 FROM a WHERE IN_ID_SET(col1, " + namesWithItem
            + ") = 1 AND IN_ID_SET(col3, 'set1') = 1", false, runner);
    assertEquals(runner._validated, List.of(other));

    // The WITH items stay in scope in a subquery that stays, so the subqueries in it stay too
    String correlated = "(SELECT IDSET(c.col1) FROM c WHERE c.col2 = a.col2 AND IN_ID_SET(c.col3, " + namesWithItem
        + ") = 1)";
    runner = new FakeRunner();
    String sql = "WITH b AS (SELECT col1 FROM d) SELECT col1 FROM a WHERE IN_ID_SET(a.col1, " + correlated + ") = 1";
    assertRewrite(sql, sql, false, runner);
    assertEquals(runner._validated, List.of());
    assertEquals(runner._ran, List.of());

    // The ORDER BY of a WITH query parses outside the WITH, but the WITH items are in its scope too
    runner = new FakeRunner();
    sql = "WITH b AS (SELECT col1 FROM d) SELECT col1 FROM a ORDER BY CASE WHEN IN_ID_SET(col1, " + namesWithItem
        + ") = 1 THEN 0 ELSE 1 END LIMIT 10";
    assertRewrite(sql, sql, false, runner);
    assertEquals(runner._validated, List.of());

    // But not outside the query that has the WITH
    runner = new FakeRunner().returning(namesWithItem, "set1");
    assertRewrite("SELECT col1 FROM (WITH b AS (SELECT col1 FROM d) SELECT col1 FROM b) WHERE IN_ID_SET(col1, "
            + namesWithItem + ") = 1",
        "SELECT col1 FROM (WITH b AS (SELECT col1 FROM d) SELECT col1 FROM b) WHERE IN_ID_SET(col1, 'set1') = 1",
        false, runner);
  }

  @Test
  public void testRewritesIdSetSubqueriesInTheTestedExpression() {
    FakeRunner runner = new FakeRunner().returning("q1", "set1").returning("q2", "set2");
    assertRewrite(
        "SELECT col1 FROM a WHERE IN_SUBQUERY(CASE WHEN IN_SUBQUERY(col2, 'q2') = 1 THEN col1 END, 'q1') = 1",
        "SELECT col1 FROM a WHERE IN_ID_SET(CASE WHEN IN_ID_SET(col2, 'set2') = 1 THEN col1 END, 'set1') = 1",
        false, runner);
    assertEquals(runner._ran, List.of("q1", "q2"));
  }

  @Test
  public void testReadsSubqueriesSplitAcrossLiterals() {
    FakeRunner runner = new FakeRunner().returning("SELECT IDSET(col1) FROM b", "set1");
    assertRewrite("SELECT col1 FROM a WHERE IN_SUBQUERY(col1, 'SELECT IDSET(col1) '\n'FROM b') = 1",
        "SELECT col1 FROM a WHERE IN_ID_SET(col1, 'set1') = 1", false, runner);
  }

  /// Rewrites the query, checks that it becomes the expected query, and returns the replaced subqueries.
  private static List<String> assertRewrite(String sql, String expectedSql, boolean explain, FakeRunner runner) {
    SqlNode query = parse(sql);
    assertEquals(query.getKind() == SqlKind.EXPLAIN, explain);
    if (explain) {
      query = ((SqlExplain) query).getExplicandum();
    }
    List<String> subqueries = IdSetSubqueryRewriter.rewrite(sql, query, explain, runner);
    assertTrue(query.equalsDeep(parse(expectedSql), Litmus.THROW));
    return subqueries;
  }

  private static SqlNode parse(String sql) {
    return CalciteSqlParser.compileToSqlNodeAndOptions(sql).getSqlNode();
  }

  private static class FakeRunner implements IdSetSubqueryRewriter.SubqueryRunner {
    final Map<String, String> _results = new HashMap<>();
    final Set<String> _correlated = new HashSet<>();
    final List<String> _validated = new ArrayList<>();
    final List<String> _ran = new ArrayList<>();

    FakeRunner returning(String subquery, @Nullable String idSet) {
      _results.put(subquery, idSet);
      return this;
    }

    FakeRunner correlated(String subquery) {
      _correlated.add(subquery);
      return this;
    }

    @Override
    public boolean validates(String subquery) {
      _validated.add(subquery);
      return !_correlated.contains(subquery);
    }

    @Nullable
    @Override
    public String run(String subquery) {
      _ran.add(subquery);
      assertTrue(_results.containsKey(subquery), "Unexpected subquery: " + subquery);
      return _results.get(subquery);
    }
  }
}
