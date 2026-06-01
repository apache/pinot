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
package org.apache.pinot.materializedview.scheduler;

import java.util.Collections;
import java.util.Optional;
import org.apache.helix.AccessOption;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.materializedview.analysis.MaterializedViewAnalyzer;
import org.apache.pinot.materializedview.context.MaterializedViewTaskGeneratorContext;
import org.apache.pinot.materializedview.metadata.MaterializedViewRuntimeMetadata;
import org.apache.pinot.spi.utils.CommonConstants.MaterializedViewTask;
import org.apache.zookeeper.data.Stat;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class MaterializedViewTaskSchedulerTest {

  @Test
  public void testAppendTimeRangeNoWhereClause() {
    String sql = "SELECT col1, SUM(col2) FROM myTable GROUP BY col1";
    String result = MaterializedViewTaskScheduler.appendTimeRange(sql, "ts", "100", "200");
    assertEquals(result, "SELECT col1, SUM(col2) FROM myTable WHERE ts >= 100 AND ts < 200 GROUP BY col1");
  }

  @Test
  public void testAppendTimeRangeWithExistingWhere() {
    String sql = "SELECT col1 FROM myTable WHERE col2 = 'foo' GROUP BY col1";
    String result = MaterializedViewTaskScheduler.appendTimeRange(sql, "ts", "100", "200");
    assertEquals(result, "SELECT col1 FROM myTable WHERE col2 = 'foo' AND ts >= 100 AND ts < 200 GROUP BY col1");
  }

  @Test
  public void testAppendTimeRangeWithLimit() {
    String sql = "SELECT col1 FROM myTable GROUP BY col1 LIMIT 50";
    String result = MaterializedViewTaskScheduler.appendTimeRange(sql, "ts", "100", "200");
    assertEquals(result, "SELECT col1 FROM myTable WHERE ts >= 100 AND ts < 200 GROUP BY col1 LIMIT 50");
  }

  @Test
  public void testAppendTimeRangeStripsTrailingSemicolon() {
    String sql = "SELECT col1 FROM myTable;";
    String result = MaterializedViewTaskScheduler.appendTimeRange(sql, "ts", "100", "200");
    assertEquals(result, "SELECT col1 FROM myTable WHERE ts >= 100 AND ts < 200");
  }

  // ---------------------------------------------------------------------------
  //  appendTimeRange quote-mask: keyword scans must skip text inside string
  //  literals and quoted identifiers so user-controlled values cannot fool the
  //  splitter into corrupting the SQL.
  // ---------------------------------------------------------------------------

  @Test
  public void testAppendTimeRangeIgnoresKeywordsInsideStringLiterals() {
    String sql = "SELECT col1 FROM myTable WHERE name = 'Acme WHERE Co' AND tag <> 'GROUP BY hack'";
    String result = MaterializedViewTaskScheduler.appendTimeRange(sql, "ts", "100", "200");
    // The fake WHERE/GROUP BY inside string literals must remain untouched, and the time
    // filter must AND-append at the real end of the WHERE conditions.
    assertTrue(result.contains("'Acme WHERE Co'"), "literal text was modified: " + result);
    assertTrue(result.contains("'GROUP BY hack'"), "literal text was modified: " + result);
    assertTrue(result.endsWith("AND ts >= 100 AND ts < 200"), "filter not appended at end: " + result);
  }

  @Test
  public void testAppendTimeRangeHandlesAnsiDoubledSingleQuoteEscape() {
    // 'It''s' is the SQL-standard escape for a single quote inside a literal.
    String sql = "SELECT col1 FROM myTable WHERE comment = 'It''s a WHERE test'";
    String result = MaterializedViewTaskScheduler.appendTimeRange(sql, "ts", "100", "200");
    assertTrue(result.contains("'It''s a WHERE test'"), "doubled-quote literal modified: " + result);
    assertTrue(result.endsWith("AND ts >= 100 AND ts < 200"), "filter not appended at end: " + result);
  }

  @Test
  public void testAppendTimeRangeKeywordInsideDoubleQuotedIdentifier() {
    // Double-quoted identifier — treated like a literal by the quote mask.
    String sql = "SELECT \"WHERE\" FROM myTable";
    String result = MaterializedViewTaskScheduler.appendTimeRange(sql, "ts", "100", "200");
    assertTrue(result.startsWith("SELECT \"WHERE\" FROM myTable"),
        "quoted identifier was modified: " + result);
    assertTrue(result.contains("WHERE ts >= 100"), "WHERE clause should be inserted: " + result);
  }

  // Line-comment handling intentionally not exhaustively tested:
  // appending text after a SQL that ends with `-- ...` would land inside the
  // comment unless a newline is also inserted. The block-comment case below is
  // safe because /* ... */ has a bounded end. Operators authoring definedSQL
  // with trailing line comments will see a downstream parse failure (the
  // verify-re-parse in buildTaskConfig catches it) rather than a silent
  // injection. Block comments embedded in the middle of the SQL are safe.

  @Test
  public void testAppendTimeRangeKeywordAsSubstringOfColumnName() {
    // Column names that contain a SQL keyword as a substring (e.g. "WHERETO",
    // "GROUPING_SET", "VARCHARLIMIT") must NOT be treated as the keyword. The
    // boundary-aware scanner requires whitespace/punctuation on both sides.
    String sql = "SELECT WHERETO, GROUPING_SET FROM myTable";
    String result = MaterializedViewTaskScheduler.appendTimeRange(sql, "ts", "100", "200");
    assertEquals(result, "SELECT WHERETO, GROUPING_SET FROM myTable WHERE ts >= 100 AND ts < 200");
  }

  @Test
  public void testAppendTimeRangeIgnoresKeywordsInsideBlockComment() {
    String sql = "SELECT col FROM myTable /* WHERE x AND GROUP BY y */ GROUP BY col";
    String result = MaterializedViewTaskScheduler.appendTimeRange(sql, "ts", "100", "200");
    assertTrue(result.contains("/* WHERE x AND GROUP BY y */"), "block comment modified: " + result);
    assertTrue(result.contains("WHERE ts >= 100 AND ts < 200 GROUP BY col"),
        "filter not inserted before real GROUP BY: " + result);
  }

  // ---------------------------------------------------------------------------
  //  LIMIT-injection contract (driven via tryExtractDeclaredLimit + the constant)
  // ---------------------------------------------------------------------------

  @Test
  public void testNoLimitFallsBackToDefaultMaterializedViewQueryLimit() {
    String sql = "SELECT col1 FROM myTable GROUP BY col1";
    Optional<Integer> declared = MaterializedViewAnalyzer.tryExtractDeclaredLimit(sql);
    assertFalse(declared.isPresent(), "definedSQL has no LIMIT; should be empty");
    int effectiveLimit = declared.orElse(MaterializedViewTask.DEFAULT_MATERIALIZED_VIEW_QUERY_LIMIT);
    assertEquals(effectiveLimit, 1_000_000, "fallback must equal DEFAULT_MATERIALIZED_VIEW_QUERY_LIMIT (1M)");
  }

  @Test
  public void testUserDeclaredLimitIsHonored() {
    String sql = "SELECT col1 FROM myTable GROUP BY col1 LIMIT 5000";
    Optional<Integer> declared = MaterializedViewAnalyzer.tryExtractDeclaredLimit(sql);
    assertTrue(declared.isPresent());
    assertEquals(declared.get().intValue(), 5000);
  }

  // ---------------------------------------------------------------------------
  //  appendTimeRange + LIMIT-injection composition (load-bearing safety: the broker
  //  must observe the appended LIMIT, otherwise it applies its small default of 10)
  // ---------------------------------------------------------------------------

  /// Helper that mirrors what generator.buildTaskConfig does for the no-LIMIT path.
  private static String appendTimeRangeAndLimit(String definedSql, int limit) {
    String withTimeRange = MaterializedViewTaskScheduler.appendTimeRange(definedSql, "ts", "100", "200");
    String trimmed = withTimeRange.trim();
    if (trimmed.endsWith(";")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1).trim();
    }
    return trimmed + " LIMIT " + limit;
  }

  private static void assertLimitObserved(String definedSql, int expectedLimit) {
    String composed = appendTimeRangeAndLimit(definedSql, expectedLimit);
    Optional<Integer> observed = MaterializedViewAnalyzer.tryExtractDeclaredLimit(composed);
    assertTrue(observed.isPresent(),
        "Composed SQL had no parseable LIMIT — broker would silently truncate. Composed: " + composed);
    assertEquals(observed.get().intValue(), expectedLimit,
        "Composed SQL LIMIT mismatch. Composed: " + composed);
  }

  @Test
  public void testLimitInjectionGroupBy() {
    assertLimitObserved("SELECT col1, count(*) FROM t GROUP BY col1", 1_000_000);
  }

  @Test
  public void testLimitInjectionWithExistingWhere() {
    assertLimitObserved("SELECT col1 FROM t WHERE col2 = 'foo' GROUP BY col1", 1_000_000);
  }

  @Test
  public void testLimitInjectionWithOrderBy() {
    assertLimitObserved("SELECT col1, count(*) FROM t GROUP BY col1 ORDER BY col1", 1_000_000);
  }

  @Test
  public void testLimitInjectionWithHaving() {
    assertLimitObserved(
        "SELECT col1, count(*) FROM t GROUP BY col1 HAVING count(*) > 0", 1_000_000);
  }

  @Test
  public void testLimitInjectionWithTrailingSemicolon() {
    assertLimitObserved("SELECT col1 FROM t GROUP BY col1;", 1_000_000);
  }

  @Test
  public void testExistingRuntimeWithZeroWatermarkIsReturned()
      throws Exception {
    HelixPropertyStore<ZNRecord> propertyStore = mockPropertyStore();
    MaterializedViewTaskGeneratorContext context = mock(MaterializedViewTaskGeneratorContext.class);
    when(context.getPropertyStore()).thenReturn(propertyStore);

    MaterializedViewRuntimeMetadata runtime = new MaterializedViewRuntimeMetadata(
        "mv_OFFLINE", 0L, Collections.emptyMap());
    when(propertyStore.get(
        eq(ZKMetadataProvider.constructPropertyStorePathForMaterializedViewRuntime("mv_OFFLINE")),
        any(Stat.class),
        eq(AccessOption.PERSISTENT))).thenReturn(runtime.toZNRecord());

    MaterializedViewTaskScheduler scheduler = new MaterializedViewTaskScheduler(context);
    long watermarkMs = scheduler.getWatermarkMs("mv", "orders", 86_400_000L,
        "SELECT city, COUNT(*) FROM orders GROUP BY city", java.util.Map.of());

    assertEquals(watermarkMs, 0L);
    verify(context, never()).getSegmentsZKMetadata(anyString());
    verify(propertyStore, never()).set(anyString(), any(ZNRecord.class), anyInt(), eq(AccessOption.PERSISTENT));
  }

  @SuppressWarnings("unchecked")
  private static HelixPropertyStore<ZNRecord> mockPropertyStore() {
    return mock(HelixPropertyStore.class);
  }
}
