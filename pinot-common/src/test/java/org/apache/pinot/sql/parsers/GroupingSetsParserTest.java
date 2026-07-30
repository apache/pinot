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
package org.apache.pinot.sql.parsers;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests that GROUP BY GROUPING SETS / ROLLUP / CUBE are expanded by {@link CalciteSqlParser} into the union
/// of grouping columns ({@link PinotQuery#getGroupByList()}) plus, per grouping set, the list of participating
/// union-column indexes ({@link PinotQuery#getGroupingSets()}).
public class GroupingSetsParserTest {

  /// Returns the grouping sets as a set of column-name sets, decoupled from union/set ordering.
  private static Set<Set<String>> groupingSetsByName(PinotQuery query) {
    List<Expression> union = query.getGroupByList();
    Set<Set<String>> result = new HashSet<>();
    for (List<Integer> set : query.getGroupingSets()) {
      Set<String> names = new HashSet<>();
      for (int columnIndex : set) {
        names.add(union.get(columnIndex).getIdentifier().getName());
      }
      result.add(names);
    }
    return result;
  }

  private static Set<String> names(String... names) {
    return new HashSet<>(List.of(names));
  }

  @Test
  public void testPlainGroupByLeavesGroupingSetsUnset() {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery("SELECT a, SUM(c) FROM t GROUP BY a, b");
    assertEquals(query.getGroupByList().size(), 2);
    /// Plain GROUP BY must be byte-for-byte unchanged: no grouping sets emitted.
    assertNull(query.getGroupingSets());
    assertTrue(query.getGroupByListSize() == 2);
  }

  @Test
  public void testRollup() {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery("SELECT a, b, SUM(c) FROM t GROUP BY ROLLUP(a, b)");
    assertEquals(query.getGroupByList().size(), 2);
    assertEquals(groupingSetsByName(query), Set.of(names("a", "b"), names("a"), names()));
  }

  @Test
  public void testCube() {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery("SELECT a, b, SUM(c) FROM t GROUP BY CUBE(a, b)");
    assertEquals(query.getGroupByList().size(), 2);
    assertEquals(groupingSetsByName(query), Set.of(names("a", "b"), names("a"), names("b"), names()));
  }

  @Test
  public void testGroupingSets() {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery(
        "SELECT a, b, SUM(c) FROM t GROUP BY GROUPING SETS ((a, b), (a), ())");
    assertEquals(query.getGroupByList().size(), 2);
    assertEquals(groupingSetsByName(query), Set.of(names("a", "b"), names("a"), names()));
  }

  @Test
  public void testMixedPlainAndRollup() {
    /// GROUP BY a, ROLLUP(b, c) == GROUPING SETS ((a, b, c), (a, b), (a))
    PinotQuery query = CalciteSqlParser.compileToPinotQuery(
        "SELECT a, b, c, SUM(d) FROM t GROUP BY a, ROLLUP(b, c)");
    assertEquals(query.getGroupByList().size(), 3);
    assertEquals(groupingSetsByName(query), Set.of(names("a", "b", "c"), names("a", "b"), names("a")));
  }

  @Test
  public void testCompositeRollupLevel() {
    /// ROLLUP((a, b)) treats the parenthesized pair as one level: GROUPING SETS ((a, b), ()).
    PinotQuery query = CalciteSqlParser.compileToPinotQuery("SELECT a, b, SUM(c) FROM t GROUP BY ROLLUP((a, b))");
    assertEquals(groupingSetsByName(query), Set.of(names("a", "b"), names()));
  }

  @Test
  public void testNestedRollupInsideGroupingSets() {
    /// Grouping constructs may nest: GROUPING SETS ((a), ROLLUP(b)) -> {a}, {b}, {}.
    PinotQuery query = CalciteSqlParser.compileToPinotQuery(
        "SELECT a, b, SUM(c) FROM t GROUP BY GROUPING SETS ((a), ROLLUP(b))");
    assertEquals(groupingSetsByName(query), Set.of(names("a"), names("b"), names()));
  }

  @Test
  public void testDuplicateGroupingSetsDeduplicated() {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery(
        "SELECT a, SUM(c) FROM t GROUP BY GROUPING SETS ((a), (a), ())");
    assertEquals(groupingSetsByName(query), Set.of(names("a"), names()));
    /// {a} and {} only -> 2 distinct grouping sets.
    assertEquals(query.getGroupingSets().size(), 2);
  }

  @Test
  public void testGroupingSetsWithEmptyOnlyIsGrandTotal() {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery("SELECT SUM(c) FROM t GROUP BY GROUPING SETS (())");
    assertEquals(groupingSetsByName(query), Set.of(names()));
  }

  @Test
  public void testAggregationFreeGroupingSetsRejected() {
    /// Without an aggregation the query must fail loudly: it must neither be rewritten to DISTINCT (which
    /// would drop the rollup rows) nor fall through to the selection path (which would ignore GROUP BY).
    for (String sql : List.of(
        "SELECT a FROM t GROUP BY ROLLUP(a)",
        "SELECT a, b FROM t GROUP BY CUBE(a, b)",
        "SELECT a, b FROM t GROUP BY GROUPING SETS ((a), (b))",
        "SELECT a, b FROM t GROUP BY GROUPING SETS ((a, b))",
        "SELECT a, GROUPING(a) FROM t GROUP BY ROLLUP(a)")) {
      SqlCompilationException e =
          expectThrows(SqlCompilationException.class, () -> CalciteSqlParser.compileToPinotQuery(sql));
      assertTrue(e.getMessage().contains("requires at least one aggregation function"), e.getMessage());
    }
  }

  @Test
  public void testGroupingSetsWithAggregationOnlyInHavingOrOrderBy() {
    /// Aggregations in HAVING / ORDER-BY alone make it an aggregation group-by query; the grouping sets must
    /// survive compilation (no DISTINCT rewrite, no rejection).
    for (String sql : List.of(
        "SELECT a FROM t GROUP BY ROLLUP(a) HAVING COUNT(*) > 1",
        "SELECT a FROM t GROUP BY ROLLUP(a) ORDER BY COUNT(*)")) {
      PinotQuery query = CalciteSqlParser.compileToPinotQuery(sql);
      assertEquals(query.getGroupByList().size(), 1);
      assertEquals(groupingSetsByName(query), Set.of(names("a"), names()));
    }
  }

  @Test
  public void testManyGroupingColumnsSupported() {
    /// The number of grouping columns is unlimited (each set is a column-index list and the discriminator is
    /// the set ordinal, mirroring Calcite's per-set column bitset): 40 distinct grouping columns parse fine,
    /// where the retired 32-bit bitmask encoding capped the union at 31.
    StringBuilder sets = new StringBuilder();
    for (int i = 0; i < 40; i++) {
      sets.append(i == 0 ? "(c" : ", (c").append(i).append(')');
    }
    PinotQuery query = CalciteSqlParser.compileToPinotQuery(
        "SELECT SUM(m) FROM t GROUP BY GROUPING SETS (" + sets + ")");
    assertEquals(query.getGroupByList().size(), 40);
    assertEquals(query.getGroupingSets().size(), 40);
    /// Each set holds exactly one distinct column index within range.
    Set<Integer> seenColumns = new HashSet<>();
    for (List<Integer> set : query.getGroupingSets()) {
      assertEquals(set.size(), 1);
      int columnIndex = set.get(0);
      assertTrue(columnIndex >= 0 && columnIndex < 40, "column index out of range: " + columnIndex);
      assertTrue(seenColumns.add(columnIndex), "duplicate column index: " + columnIndex);
    }
  }

  @Test
  public void testTooManyGroupingFunctionArgsRejected() {
    /// GROUPING()/GROUPING_ID() pack one bit per argument into an INT, so a single call accepts at most 31
    /// arguments — rejected at compile time (not mid-execution), in SELECT and in HAVING/ORDER BY alike.
    StringBuilder sets = new StringBuilder();
    StringBuilder args = new StringBuilder();
    for (int i = 0; i < 32; i++) {
      sets.append(i == 0 ? "(c" : ", (c").append(i).append(')');
      args.append(i == 0 ? "c" : ", c").append(i);
    }
    for (String sql : List.of(
        "SELECT GROUPING_ID(" + args + "), SUM(m) FROM t GROUP BY GROUPING SETS (" + sets + ")",
        "SELECT SUM(m) FROM t GROUP BY GROUPING SETS (" + sets + ") ORDER BY GROUPING_ID(" + args + ")")) {
      SqlCompilationException e =
          expectThrows(SqlCompilationException.class, () -> CalciteSqlParser.compileToPinotQuery(sql));
      assertTrue(e.getMessage().contains("at most 31 arguments"), e.getMessage());
    }
  }

  @Test(expectedExceptions = SqlCompilationException.class)
  public void testCubeTooManySetsRejected() {
    /// CUBE over 13 columns expands to 2^13 = 8192 grouping sets, exceeding the 4096 cap.
    StringBuilder columns = new StringBuilder();
    for (int i = 0; i < 13; i++) {
      columns.append(i == 0 ? "c" : ", c").append(i);
    }
    CalciteSqlParser.compileToPinotQuery("SELECT SUM(m) FROM t GROUP BY CUBE(" + columns + ")");
  }
}
