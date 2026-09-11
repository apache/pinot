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
package org.apache.pinot.query.validate;

import javax.annotation.Nullable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.QueryEnvironment.CompiledQuery;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.validate.MatchRecognizeValidator.UnsupportedMatchRecognizeException;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests for [MatchRecognizeValidator]: the SQL:2016 `AFTER MATCH` default fix, the rejection of constructs that are
/// not supported yet, and end to end planning of a v1 legal `MATCH_RECOGNIZE` query.
public class MatchRecognizeValidatorTest extends QueryEnvironmentTestBase {

  /// A fully featured v1 legal query. `%s` is where the `AFTER MATCH` clause goes (possibly empty).
  private static final String QUERY_TEMPLATE =
      "SELECT * FROM a MATCH_RECOGNIZE ("
          + "  PARTITION BY col1"
          + "  ORDER BY ts"
          + "  MEASURES MATCH_NUMBER() AS mno, CLASSIFIER() AS cls, FIRST(A.col3) AS startv, LAST(B.col3) AS endv"
          + "  ONE ROW PER MATCH"
          + "  %s"
          + "  PATTERN (A B+ C?)"
          + "  DEFINE A AS A.col3 > 0, B AS B.col3 > PREV(B.col3, 1), C AS C.col3 < NEXT(C.col3))";

  @Test
  public void testOmittedAfterMatchDefaultsToSkipPastLastRow() {
    // Calcite's SqlToRelConverter substitutes SKIP TO NEXT ROW for an omitted AFTER MATCH clause, which produces
    // overlapping matches. SQL:2016 (and Trino / Snowflake / Oracle) mandate SKIP PAST LAST ROW.
    SqlMatchRecognize match = validate(String.format(QUERY_TEMPLATE, ""));
    assertAfterOption(match, SqlMatchRecognize.AfterOption.SKIP_PAST_LAST_ROW);
  }

  @Test
  public void testExplicitSkipToNextRowIsPreserved() {
    SqlMatchRecognize match = validate(String.format(QUERY_TEMPLATE, "AFTER MATCH SKIP TO NEXT ROW"));
    assertAfterOption(match, SqlMatchRecognize.AfterOption.SKIP_TO_NEXT_ROW);
  }

  @Test
  public void testExplicitSkipPastLastRowIsPreserved() {
    SqlMatchRecognize match = validate(String.format(QUERY_TEMPLATE, "AFTER MATCH SKIP PAST LAST ROW"));
    assertAfterOption(match, SqlMatchRecognize.AfterOption.SKIP_PAST_LAST_ROW);
  }

  @Test(dataProvider = "skipToVariableQueries")
  public void testExplicitSkipToVariableIsPreserved(String afterClause, String expectedUnparsed) {
    SqlMatchRecognize match = validate(String.format(QUERY_TEMPLATE, afterClause));
    SqlNode after = match.getAfter();
    assertNotNull(after);
    assertEquals(after.toString().replace('\n', ' '), expectedUnparsed);
  }

  @DataProvider(name = "skipToVariableQueries")
  public Object[][] skipToVariableQueries() {
    return new Object[][]{
        new Object[]{"AFTER MATCH SKIP TO FIRST B", "SKIP TO FIRST `B`"},
        new Object[]{"AFTER MATCH SKIP TO LAST B", "SKIP TO LAST `B`"}
    };
  }

  @Test
  public void testNestedMatchRecognizeIsRewritten() {
    // The visitor must reach a MATCH_RECOGNIZE nested inside a sub-query, not just a top level FROM clause.
    SqlMatchRecognize match = validate("SELECT * FROM (SELECT * FROM a MATCH_RECOGNIZE (ORDER BY ts "
        + "MEASURES MATCH_NUMBER() AS mno PATTERN (A) DEFINE A AS A.col3 > 0))");
    assertAfterOption(match, SqlMatchRecognize.AfterOption.SKIP_PAST_LAST_ROW);
  }

  @Test(dataProvider = "deferredConstructQueries")
  public void testDeferredConstructIsRejected(String sql, String expectedMessageFragment) {
    UnsupportedMatchRecognizeException e = expectThrows(UnsupportedMatchRecognizeException.class, () -> validate(sql));
    assertTrue(e.getMessage().contains(expectedMessageFragment),
        "Expected message to contain '" + expectedMessageFragment + "' but was: " + e.getMessage());
  }

  @DataProvider(name = "deferredConstructQueries")
  public Object[][] deferredConstructQueries() {
    //@formatter:off
    return new Object[][]{
        new Object[]{
            "SELECT * FROM a MATCH_RECOGNIZE (ORDER BY ts ALL ROWS PER MATCH PATTERN (A) DEFINE A AS A.col3 > 0)",
            "ALL ROWS PER MATCH is not supported yet"
        },
        new Object[]{
            "SELECT * FROM a MATCH_RECOGNIZE (ORDER BY ts PATTERN (A B) SUBSET S = (A, B) "
                + "DEFINE A AS A.col3 > 0, B AS B.col3 > 0)",
            "SUBSET is not supported yet"
        },
        new Object[]{
            "SELECT * FROM a MATCH_RECOGNIZE (ORDER BY ts PATTERN (PERMUTE(A)) DEFINE A AS A.col3 > 0)",
            "PERMUTE is not supported yet"
        },
        new Object[]{
            "SELECT * FROM a MATCH_RECOGNIZE (ORDER BY ts PATTERN (A {- B -} C) "
                + "DEFINE A AS A.col3 > 0, B AS B.col3 > 0, C AS C.col3 > 0)",
            "Pattern exclusions '{- -}' are not supported yet"
        },
        new Object[]{
            "SELECT * FROM a MATCH_RECOGNIZE (ORDER BY ts PATTERN (A B) WITHIN INTERVAL '10' SECOND "
                + "DEFINE A AS A.col3 > 0, B AS B.col3 > 0)",
            "WITHIN is not supported yet"
        },
        new Object[]{
            "SELECT * FROM a MATCH_RECOGNIZE (ORDER BY ts PATTERN (A B+) "
                + "DEFINE A AS A.col3 > 0, B AS SUM(B.col3) < 100)",
            "Aggregate function 'SUM' is not supported yet in the MATCH_RECOGNIZE DEFINE clause"
        },
        new Object[]{
            // Pinot specific aggregates are still unresolved at this point and are matched by name.
            "SELECT * FROM a MATCH_RECOGNIZE (ORDER BY ts PATTERN (A B+) "
                + "DEFINE A AS A.col3 > 0, B AS DISTINCTCOUNT(B.col3) < 100)",
            "Aggregate function 'DISTINCTCOUNT' is not supported yet in the MATCH_RECOGNIZE DEFINE clause"
        },
        new Object[]{
            "SELECT * FROM a MATCH_RECOGNIZE (ORDER BY ts NULLS FIRST PATTERN (A) DEFINE A AS A.col3 > 0)",
            "NULLS FIRST / NULLS LAST is not supported yet"
        },
        new Object[]{
            "SELECT * FROM a MATCH_RECOGNIZE (ORDER BY ts NULLS LAST PATTERN (A) DEFINE A AS A.col3 > 0)",
            "NULLS FIRST / NULLS LAST is not supported yet"
        },
        new Object[]{
            "SELECT * FROM a MATCH_RECOGNIZE (ORDER BY ts + 1 PATTERN (A) DEFINE A AS A.col3 > 0)",
            "ORDER BY on expressions is not supported yet"
        },
        new Object[]{
            "SELECT * FROM a MATCH_RECOGNIZE (PARTITION BY col3 + 1 ORDER BY ts PATTERN (A) "
                + "DEFINE A AS A.col3 > 0)",
            "PARTITION BY on expressions is not supported yet"
        },
        new Object[]{
            "SELECT * FROM a MATCH_RECOGNIZE (PATTERN (A) DEFINE A AS A.col3 > 0)",
            "MATCH_RECOGNIZE requires an ORDER BY clause"
        },
        new Object[]{
            // Calcite does not confine the RUNNING / FINAL prefix operators to a MatchRecognizeScope, so registering
            // them would otherwise let them leak into an ordinary projection.
            "SELECT FINAL col3 FROM a",
            "FINAL can only be used in the MEASURES clause"
        },
        new Object[]{
            "SELECT RUNNING col3 FROM a",
            "RUNNING can only be used in the MEASURES or DEFINE clause"
        },
        new Object[]{
            "SELECT * FROM a MATCH_RECOGNIZE (ORDER BY ts MEASURES LAST(A.col3) AS v PATTERN (A+) "
                + "DEFINE A AS FINAL LAST(A.col3) > 0)",
            "FINAL semantics is not supported in the MATCH_RECOGNIZE DEFINE clause"
        },
        new Object[]{
            // MEASURES is optional in SQL:2016, but Calcite then sets the MATCH_RECOGNIZE row type to the whole
            // *input* row type, which is not the ONE ROW PER MATCH shape. The query used to plan and then die on the
            // server with a message about an internal invariant.
            "SELECT * FROM a MATCH_RECOGNIZE (PARTITION BY col1 ORDER BY ts PATTERN (A) DEFINE A AS A.col3 > 0)",
            "MATCH_RECOGNIZE requires a MEASURES clause"
        },
        new Object[]{
            // Calcite adds a measure to the row type only if its alias is not taken yet, so this one vanishes from
            // the output altogether.
            "SELECT * FROM a MATCH_RECOGNIZE (PARTITION BY col1 ORDER BY ts MEASURES LAST(A.col3) AS col1 "
                + "PATTERN (A) DEFINE A AS A.col3 > 0)",
            "measure alias 'col1' collides with a PARTITION BY column or an earlier measure alias"
        },
        new Object[]{
            // Two measures with the same alias collapse through the same nameExists branch.
            "SELECT * FROM a MATCH_RECOGNIZE (ORDER BY ts MEASURES LAST(A.col3) AS v, FIRST(A.col3) AS v "
                + "PATTERN (A) DEFINE A AS A.col3 > 0)",
            "measure alias 'v' collides with a PARTITION BY column or an earlier measure alias"
        }
    };
    //@formatter:on
  }

  @Test(dataProvider = "acceptedQueries")
  public void testAcceptedQuery(String sql) {
    assertNotNull(validate(sql));
  }

  @DataProvider(name = "acceptedQueries")
  public Object[][] acceptedQueries() {
    //@formatter:off
    return new Object[][]{
        // Full pattern algebra: anchors, grouping, alternation, and all quantifier forms including reluctant ones.
        new Object[]{
            "SELECT * FROM a MATCH_RECOGNIZE (ORDER BY ts MEASURES MATCH_NUMBER() AS mno "
                + "PATTERN (^ A{2,5}? (B | C)* D+ E? F{3} G{2,} $) "
                + "DEFINE A AS A.col3 > 0, B AS B.col3 > 0, C AS C.col3 > 0, D AS D.col3 > 0, E AS E.col3 > 0, "
                + "F AS F.col3 > 0, G AS G.col3 > 0)"
        },
        // DESC is allowed on a plain column reference.
        new Object[]{
            "SELECT * FROM a MATCH_RECOGNIZE (ORDER BY ts DESC MEASURES MATCH_NUMBER() AS mno PATTERN (A) "
                + "DEFINE A AS A.col3 > 0)"
        },
        // A pattern variable used but not DEFINEd only warns; SQL:2016 says its condition defaults to TRUE.
        new Object[]{
            "SELECT * FROM a MATCH_RECOGNIZE (ORDER BY ts MEASURES MATCH_NUMBER() AS mno PATTERN (A B+ TYPO) "
                + "DEFINE A AS A.col3 > 0, B AS B.col3 > 0)"
        },
        // A pattern variable that happens to be named PERMUTE is legal as long as it is DEFINEd.
        new Object[]{
            "SELECT * FROM a MATCH_RECOGNIZE (ORDER BY ts MEASURES MATCH_NUMBER() AS mno PATTERN (PERMUTE A) "
                + "DEFINE PERMUTE AS PERMUTE.col3 > 0, A AS A.col3 > 0)"
        },
        // Cross-symbol references and PREV / NEXT with an explicit offset in DEFINE.
        new Object[]{
            "SELECT * FROM a MATCH_RECOGNIZE (ORDER BY ts MEASURES MATCH_NUMBER() AS mno PATTERN (A B+) "
                + "DEFINE A AS A.col3 > 0, B AS B.col3 > PREV(A.col3, 3) AND B.col3 < NEXT(A.col3, 2))"
        },
        // A measure alias that differs from a PARTITION BY column only in case is a distinct column, because
        // Calcite's RelDataTypeFactory.Builder#nameExists is case-sensitive. Rejecting it would break a query that
        // works today.
        new Object[]{
            "SELECT * FROM a MATCH_RECOGNIZE (PARTITION BY col1 ORDER BY ts MEASURES LAST(A.col3) AS COL1 "
                + "PATTERN (A) DEFINE A AS A.col3 > 0)"
        },
        // Pattern variables and row-source aliases occupy separate namespaces, even when their SQL names are equal.
        new Object[]{
            "SELECT * FROM a AS A MATCH_RECOGNIZE (PARTITION BY A.col1 ORDER BY A.ts "
                + "MEASURES LAST(col3) AS universal_s, LAST(a.col3) AS pattern_s "
                + "PATTERN (a B+) DEFINE a AS a.col3 > 0, B AS B.col3 > 0)"
        },
        // The same namespace rule applies when Calcite derives the row-source alias from the bare table name.
        new Object[]{
            "SELECT * FROM a MATCH_RECOGNIZE (ORDER BY ts "
                + "MEASURES LAST(col3) AS universal_s, LAST(\"a\".col3) AS pattern_s "
                + "PATTERN (\"a\" B+) DEFINE \"a\" AS \"a\".col3 > 0, B AS B.col3 > 0)"
        }
    };
    //@formatter:on
  }

  @Test
  public void testLegalQueryPlansToLogicalMatch() {
    String explain =
        _queryEnvironment.explainQuery("EXPLAIN PLAN FOR " + String.format(QUERY_TEMPLATE, ""),
            RANDOM_REQUEST_ID_GEN.nextLong());
    assertTrue(explain.contains("LogicalMatch"), "Expected a LogicalMatch in the plan but got:\n" + explain);
  }

  @Test
  public void testProcessingModesPlanThroughCalcite() {
    // Calcite 1.42 rejects RUNNING in DEFINE even though it is standard and semantically the default. The raw Pinot
    // validator removes that redundant wrapper before Calcite sees it, while preserving both modifiers in MEASURES.
    try (CompiledQuery query = _queryEnvironment.compile(
        "SELECT * FROM a MATCH_RECOGNIZE (PARTITION BY col1 ORDER BY ts "
            + "MEASURES RUNNING LAST(A.col3) AS running_v, FINAL LAST(A.col3) AS final_v "
            + "PATTERN (A+) DEFINE A AS RUNNING LAST(A.col3) > 0)")) {
      assertNotNull(query);
    }
  }

  @Test
  public void testDeferredConstructIsRejectedByQueryEnvironment() {
    // The validator must be wired into the planning pipeline, so the error has to surface from compile() too.
    RuntimeException e = expectThrows(RuntimeException.class, () -> _queryEnvironment.compile(
        "SELECT * FROM a MATCH_RECOGNIZE (ORDER BY ts ALL ROWS PER MATCH PATTERN (A) DEFINE A AS A.col3 > 0)"));
    assertTrue(e.getMessage().contains("ALL ROWS PER MATCH is not supported yet"), e.getMessage());
  }

  @Test(dataProvider = "physicalOptimizerOptions")
  public void testPhysicalOptimizerIsRejectedWithAnActionableMessage(String options, String expectedMode) {
    RuntimeException e = expectThrows(RuntimeException.class,
        () -> _queryEnvironment.compile(options + String.format(QUERY_TEMPLATE, "")));
    assertTrue(e.getMessage().contains("MATCH_RECOGNIZE is not supported by the multi-stage physical optimizer"
        + expectedMode), e.getMessage());
    assertTrue(e.getMessage().contains("usePhysicalOptimizer=false"), e.getMessage());
  }

  @DataProvider(name = "physicalOptimizerOptions")
  public Object[][] physicalOptimizerOptions() {
    return new Object[][]{
        new Object[]{"SET usePhysicalOptimizer=true; ", ""},
        new Object[]{"SET usePhysicalOptimizer=true; SET useLiteMode=true; ", " in lite mode"}
    };
  }

  @Test
  public void testPhysicalOptimizerQueryOptionCanOptBackIntoTheSupportedPlanner() {
    try (CompiledQuery ignored = _queryEnvironment.compile(
        "SET usePhysicalOptimizer=false; SET useLiteMode=true; " + String.format(QUERY_TEMPLATE, ""))) {
      assertNotNull(ignored);
    }
  }

  @Test
  public void testPhysicalOptimizerBrokerDefaultAndQueryOverride() {
    QueryEnvironment physicalOptimizerByDefault = getQueryEnvironment(3, 1, 2, TABLE_SCHEMAS,
        SERVER1_SEGMENTS, SERVER2_SEGMENTS, PARTITIONED_SEGMENTS_MAP, true);
    RuntimeException e = expectThrows(RuntimeException.class,
        () -> physicalOptimizerByDefault.compile(String.format(QUERY_TEMPLATE, "")));
    assertTrue(e.getMessage().contains("MATCH_RECOGNIZE is not supported by the multi-stage physical optimizer"),
        e.getMessage());

    try (CompiledQuery ignored = physicalOptimizerByDefault.compile(
        "SET usePhysicalOptimizer=false; " + String.format(QUERY_TEMPLATE, ""))) {
      assertNotNull(ignored);
    }
  }

  private static void assertAfterOption(SqlMatchRecognize match, SqlMatchRecognize.AfterOption expected) {
    SqlNode after = match.getAfter();
    assertNotNull(after, "AFTER MATCH operand should have been populated by MatchRecognizeValidator");
    assertTrue(after instanceof SqlLiteral, "Expected a symbol literal but got: " + after);
    assertEquals(((SqlLiteral) after).getValue(), expected);
  }

  /// Parses {@code sql}, runs [MatchRecognizeValidator] over it, and returns the first `MATCH_RECOGNIZE` node found.
  private static SqlMatchRecognize validate(String sql) {
    SqlNode sqlNode = CalciteSqlParser.compileToSqlNodeAndOptions(sql).getSqlNode();
    sqlNode.accept(new MatchRecognizeValidator());
    SqlMatchRecognize match = findMatchRecognize(sqlNode);
    assertNotNull(match, "Query does not contain a MATCH_RECOGNIZE clause: " + sql);
    return match;
  }

  @Nullable
  private static SqlMatchRecognize findMatchRecognize(SqlNode root) {
    SqlMatchRecognize[] found = new SqlMatchRecognize[1];
    root.accept(new SqlBasicVisitor<Void>() {
      @Override
      public Void visit(SqlCall call) {
        if (call instanceof SqlMatchRecognize && found[0] == null) {
          found[0] = (SqlMatchRecognize) call;
        }
        return super.visit(call);
      }
    });
    return found[0];
  }
}
