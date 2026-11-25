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
package org.apache.pinot.query;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.pinot.common.config.provider.StaticTableCache;
import org.apache.pinot.query.validate.GroupByDiagnostics;
import org.apache.pinot.query.validate.GroupByDiagnosticsCollector;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Focused real-compilation checks of bounded observations and compilation noninterference; no data execution.
public class GroupByDiagnosticsTest {
  private static final String POSITIVE =
      "SELECT 'UNUSED' AS payment_method, SUM(amount) FROM events GROUP BY payment_method";

  private QueryEnvironment environment(boolean caseSensitive) {
    List<TableConfig> tables = new ArrayList<>();
    List<Schema> schemas = new ArrayList<>();
    for (String table : List.of("events", "right_events")) {
      tables.add(new TableConfigBuilder(TableType.OFFLINE).setTableName(table).build());
      schemas.add(new Schema.SchemaBuilder().setSchemaName(table)
          .addSingleValueDimension("payment_method", DataType.STRING)
          .addSingleValueDimension("country", DataType.STRING)
          .addSingleValueDimension("name", DataType.STRING)
          .addMetric("amount", DataType.LONG).build());
    }
    return new QueryEnvironment(QueryEnvironment.configBuilder().requestId(-1L).database("default")
        .tableCache(new StaticTableCache(tables, schemas, null, !caseSensitive))
        .isCaseSensitive(caseSensitive).build());
  }

  @Test
  public void testOriginalAliasAndObservedExpansion() {
    GroupByDiagnostics result = analyze(POSITIVE);
    GroupByDiagnostics.Overlap finding = onlyFinding(result);
    assertEquals(finding.selectExpressions(), List.of("'UNUSED'"));
    assertEquals(finding.expandedGroupBy(), "events.payment_method");
    assertEquals(finding.position().startLine(), 1);
    assertEquals(finding.position().startColumn(), POSITIVE.lastIndexOf("payment_method") + 1);
    assertEquals(finding.position().endColumn(), POSITIVE.length());
    expectThrows(UnsupportedOperationException.class, () -> result.overlaps().clear());
    expectThrows(UnsupportedOperationException.class, () -> finding.selectExpressions().clear());
  }

  @Test
  public void testSameColumnAndGroupingEntryCorrespondence() {
    GroupByDiagnostics result = analyze("SELECT e.country AS country, 'x' AS payment_method, SUM(e.amount) "
        + "FROM events e GROUP BY name, country, payment_method");
    assertEquals(result.overlaps().stream().map(GroupByDiagnostics.Overlap::identifier).toList(),
        List.of("country", "payment_method"));
    assertEquals(result.overlaps().stream().map(GroupByDiagnostics.Overlap::expandedGroupBy).toList(),
        List.of("e.country", "e.payment_method"));
  }

  @Test
  public void testExplicitAliasBoundary() {
    for (String sql : List.of(
        "SELECT country AS normalized_country, SUM(amount) FROM events GROUP BY normalized_country",
        "SELECT country, SUM(amount) FROM events GROUP BY country",
        "SELECT 'UNUSED' AS payment_method, SUM(e.amount) FROM events e GROUP BY e.payment_method",
        "SELECT 'UNUSED' AS payment_method, SUM(amount) FROM events GROUP BY 1",
        "SELECT 1")) {
      GroupByDiagnostics result = analyze(sql);
      assertTrue(result.overlaps().isEmpty(), sql);
    }
    GroupByDiagnostics unsupported = analyze("SELECT 'x' AS country, COUNT(*) FROM events "
        + "GROUP BY country, ROLLUP(country), LOWER(country)");
    assertEquals(onlyFinding(unsupported).identifier(), "country");
  }

  @Test
  public void testNestedExposedNamespacesAndAliasIsolation() {
    String nestedSql = "WITH c(input_country, amount) AS (SELECT country, amount FROM events) "
        + "SELECT SUM(amount) FROM (SELECT 'x' AS input_country, SUM(amount) AS amount FROM c "
        + "GROUP BY input_country) d";
    GroupByDiagnostics nested = analyze(nestedSql);
    assertEquals(onlyFinding(nested).position().startColumn(), nestedSql.lastIndexOf("input_country") + 1);
    assertEquals(onlyFinding(nested).expandedGroupBy(), "c.input_country");
    GroupByDiagnostics renamed = analyze("WITH c AS (SELECT country AS input_country, amount FROM events) "
        + "SELECT LOWER(input_country) AS country, SUM(amount) FROM c GROUP BY country");
    assertTrue(renamed.overlaps().isEmpty());
    GroupByDiagnostics star = analyze("SELECT 'x' AS country, COUNT(*) FROM (SELECT * FROM events) d GROUP BY country");
    assertEquals(onlyFinding(star).expandedGroupBy(), "d.country");
    assertEquals(onlyFinding(analyze("SELECT *, 'x' AS country FROM (SELECT country FROM events) d "
        + "GROUP BY country")).selectExpressions(), List.of("'x'"));
  }

  @Test
  public void testReusedCteDoesNotDuplicateObservations() {
    String sql = "WITH grouped AS (" + POSITIVE + ") SELECT a.payment_method FROM grouped a "
        + "JOIN grouped b ON a.payment_method = b.payment_method";
    assertEquals(onlyFinding(analyze(sql)).expandedGroupBy(), "events.payment_method");
  }

  @Test
  public void testDuplicateAliasesAndJoinedInputsAreNeutralObservations() {
    GroupByDiagnostics duplicates = analyze("SELECT 'x' AS country, 'y' AS country, COUNT(*) "
        + "FROM events GROUP BY country");
    assertEquals(onlyFinding(duplicates).selectExpressions(), List.of("'x'", "'y'"));
    GroupByDiagnostics joined = analyze("SELECT l.country AS country, COUNT(*) FROM events l "
        + "LEFT JOIN right_events r ON l.name = r.name GROUP BY country");
    assertEquals(onlyFinding(joined).selectExpressions(), List.of("l.country"));
    for (String join : List.of("JOIN right_events USING (country)", "NATURAL JOIN right_events")) {
      GroupByDiagnostics merged = analyze("SELECT 'x' AS country, COUNT(*) FROM events " + join + " GROUP BY country");
      assertEquals(onlyFinding(merged).identifier(), "country");
    }
  }

  @Test
  public void testMatcherAndCorrelatedLookup() {
    String quoted = "SELECT 'x' AS \"COUNTRY\", SUM(amount) FROM events GROUP BY \"country\"";
    for (boolean sensitive : List.of(true, false)) {
      GroupByDiagnosticsCollector collector = new GroupByDiagnosticsCollector();
      assertTrue(compile(quoted, collector, environment(sensitive)).startsWith("PLAN:"));
      assertEquals(collector.getResult().overlaps().size(), sensitive ? 0 : 1);
    }
    GroupByDiagnosticsCollector collector = new GroupByDiagnosticsCollector();
    String correlated = "SELECT (SELECT 'x' AS country FROM (SELECT 1 AS n) i GROUP BY country) FROM events o";
    String enabled = compile(correlated, collector, environment(true));
    assertTrue(enabled.startsWith("PLAN:"), enabled);
    assertEquals(enabled, compile(correlated, null, environment(true)));
    assertEquals(onlyFinding(collector.getResult()).position().startColumn(), correlated.lastIndexOf("country")
        + 1);
    // A local input prevents parent fallback; an outer SELECT alias never leaks into an inner block.
    assertTrue(analyze("SELECT 'x' AS country FROM events o WHERE EXISTS "
        + "(SELECT country FROM right_events i GROUP BY country)").overlaps().isEmpty());
    assertEquals(onlyFinding(analyze("SELECT country FROM events o WHERE EXISTS "
        + "(SELECT i.country AS country FROM right_events i WHERE i.name = o.name GROUP BY country)"))
        .expandedGroupBy(), "i.country");
  }

  @Test
  public void testCompilationErrorsAreUnchanged() {
    for (String sql : List.of("SELECT FROM WHERE", "SELECT 'x' AS country, name FROM events GROUP BY country")) {
      String enabled = compile(sql, new GroupByDiagnosticsCollector(), environment(true));
      assertTrue(enabled.startsWith("ERROR:"), enabled);
      assertEquals(enabled, compile(sql, null, environment(true)));
    }
  }

  @Test
  public void testLaterFailureDiscardsEarlierObservationsAndStaysUnavailable() {
    String sql = "SELECT * FROM (" + POSITIVE + ") a CROSS JOIN "
        + "(SELECT 'x' AS country, COUNT(*) FROM events GROUP BY country) b CROSS JOIN "
        + "(SELECT 'x' AS name, COUNT(*) FROM events GROUP BY name) c";
    String baseline = compile(sql, null, environment(true));
    assertTrue(baseline.startsWith("PLAN:"), baseline);
    GroupByDiagnosticsCollector collector = new GroupByDiagnosticsCollector();
    try (MockedStatic<SqlNode> cloning = Mockito.mockStatic(SqlNode.class, Mockito.CALLS_REAL_METHODS)) {
      cloning.when(() -> SqlNode.clone(Mockito.any(SqlIdentifier.class))).thenAnswer(invocation -> {
        SqlIdentifier identifier = invocation.getArgument(0);
        if (identifier.isSimple() && identifier.getSimple().equals("country")) {
          // Fail only once a previous block has produced output, rather than counting Calcite's clone calls.
          List<GroupByDiagnostics.Overlap> observed = collector.getResult().overlaps();
          if (observed != null && !observed.isEmpty()) {
            throw new IllegalStateException("Injected failure after earlier observation");
          }
        }
        return invocation.callRealMethod();
      });
      assertEquals(compile(sql, collector, environment(true)), baseline);
    }
    assertNull(collector.getResult().overlaps());
  }

  private GroupByDiagnostics.Overlap onlyFinding(GroupByDiagnostics result) {
    assertEquals(result.overlaps().size(), 1);
    return result.overlaps().get(0);
  }

  private GroupByDiagnostics analyze(String sql) {
    GroupByDiagnosticsCollector collector = new GroupByDiagnosticsCollector();
    String enabled = compile(sql, collector, environment(true));
    assertTrue(enabled.startsWith("PLAN:"), enabled);
    assertEquals(enabled, compile(sql, null, environment(true)),
        "Diagnostics must preserve compilation and plan");
    return collector.getResult();
  }

  private String compile(String sql, GroupByDiagnosticsCollector collector, QueryEnvironment environment) {
    SqlNodeAndOptions parsed;
    try {
      parsed = CalciteSqlParser.compileToSqlNodeAndOptions(sql);
      parsed.getOptions().put("useMultistageEngine", "true");
    } catch (QueryException e) {
      return "ERROR:" + e.getErrorCode() + ":" + e.getMessage();
    }
    try (QueryEnvironment.CompiledQuery compiled = collector == null
        ? environment.compile(sql, parsed) : environment.compileForValidation(sql, parsed, collector)) {
      return "PLAN:" + RelOptUtil.toString(compiled.getRelRoot().rel);
    } catch (QueryException e) {
      return "ERROR:" + e.getErrorCode() + ":" + e.getMessage();
    }
  }
}
