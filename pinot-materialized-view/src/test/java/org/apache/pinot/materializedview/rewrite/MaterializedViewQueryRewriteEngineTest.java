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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.materializedview.metadata.MaterializedViewDefinitionMetadata;
import org.apache.pinot.materializedview.metadata.MaterializedViewDefinitionMetadata.MaterializedViewSplitSpec;
import org.apache.pinot.materializedview.metadata.PartitionFingerprint;
import org.apache.pinot.materializedview.metadata.PartitionInfo;
import org.apache.pinot.materializedview.metadata.PartitionState;
import org.apache.pinot.materializedview.rewrite.MaterializedViewMetadataCache.MaterializedViewCacheEntry;
import org.apache.pinot.materializedview.rewrite.strategy.AggregationSubsumptionStrategy;
import org.apache.pinot.materializedview.rewrite.strategy.ExactSubsumptionStrategy;
import org.apache.pinot.materializedview.rewrite.strategy.MaterializedViewMatchStrategy;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class MaterializedViewQueryRewriteEngineTest {

  /// Eligibility encoding for the rewrite engine's isEligible check under Design C:
  ///   ELIGIBLE   — watermarkMs=1, partitions={0L: VALID}  → engine accepts the candidate
  ///   INELIGIBLE — watermarkMs=0, partitions={}            → engine skips the candidate
  private enum Eligibility { ELIGIBLE, INELIGIBLE }

  private MaterializedViewCacheEntry createEntry(String viewTableName, String baseTable, String definedSql) {
    return createEntry(viewTableName, baseTable, definedSql, Eligibility.ELIGIBLE);
  }

  private MaterializedViewCacheEntry createEntry(String viewTableName, String baseTable, String definedSql,
      Eligibility eligibility) {
    return createEntry(viewTableName, baseTable, definedSql, eligibility, null, 0L);
  }

  /// Variant that attaches a [MaterializedViewSplitSpec] and a non-zero `watermarkMs`,
  /// so the rewrite engine goes down the SPLIT_REWRITE branch.
  private MaterializedViewCacheEntry createEntry(String viewTableName, String baseTable, String definedSql,
      Eligibility eligibility, @Nullable MaterializedViewSplitSpec splitSpec, long watermarkMs) {
    MaterializedViewDefinitionMetadata definition = new MaterializedViewDefinitionMetadata(
        viewTableName,
        Collections.singletonList(baseTable),
        definedSql,
        new HashMap<>(),
        splitSpec);
    PinotQuery compiledQuery = CalciteSqlParser.compileToPinotQuery(definedSql);
    long wm = watermarkMs;
    Map<Long, PartitionInfo> parts = Map.of();
    if (eligibility == Eligibility.ELIGIBLE) {
      if (wm == 0L) {
        wm = 1L;
      }
      parts = Map.of(0L, new PartitionInfo(PartitionState.VALID, new PartitionFingerprint(1, 1L), 0L));
    }
    return new MaterializedViewCacheEntry(definition, compiledQuery, wm, parts);
  }

  private MaterializedViewQueryRewriteEngine exactEngine(MaterializedViewMetadataCache cache) {
    return new MaterializedViewQueryRewriteEngine(cache, List.of(new ExactSubsumptionStrategy()));
  }

  private MaterializedViewQueryRewriteEngine aggregationEngine(MaterializedViewMetadataCache cache) {
    return new MaterializedViewQueryRewriteEngine(cache, List.of(new AggregationSubsumptionStrategy()));
  }

  @Test
  public void testNoRewriteWhenNoCandidates() {
    MaterializedViewMetadataCache cache = mock(MaterializedViewMetadataCache.class);
    when(cache.getMaterializedViewEntriesForBaseTable("orders")).thenReturn(null);

    MaterializedViewQueryRewriteEngine engine = exactEngine(cache);
    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city");

    MaterializedViewRewriteResult result = engine.tryRewrite(userQuery, "orders");
    assertNull(result);
  }

  @Test
  public void testNoRewriteWhenEmptyCandidates() {
    MaterializedViewMetadataCache cache = mock(MaterializedViewMetadataCache.class);
    when(cache.getMaterializedViewEntriesForBaseTable("orders")).thenReturn(new ArrayList<>());

    MaterializedViewQueryRewriteEngine engine = exactEngine(cache);
    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city");

    MaterializedViewRewriteResult result = engine.tryRewrite(userQuery, "orders");
    assertNull(result);
  }

  @Test
  public void testRewriteSelectsBestMatch() {
    String definedSql1 = "SELECT city, SUM(revenue) AS sum_revenue FROM orders GROUP BY city";
    String definedSql2 =
        "SELECT city, SUM(revenue) AS sum_revenue FROM orders WHERE region = 'US' GROUP BY city";
    String userSql = "SELECT city, SUM(revenue) FROM orders GROUP BY city";

    MaterializedViewCacheEntry entry1 = createEntry("mv_all_OFFLINE", "orders", definedSql1);
    MaterializedViewCacheEntry entry2 = createEntry("mv_us_OFFLINE", "orders", definedSql2);

    MaterializedViewMetadataCache cache = mock(MaterializedViewMetadataCache.class);
    when(cache.getMaterializedViewEntriesForBaseTable("orders")).thenReturn(List.of(entry1, entry2));

    MaterializedViewQueryRewriteEngine engine = exactEngine(cache);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(userSql);
    MaterializedViewRewriteResult result = engine.tryRewrite(userQuery, "orders");

    assertNotNull(result);
    assertTrue(result.isHit());
    assertEquals(result.getMaterializedViewQueriedName(), "mv_all_OFFLINE");
    assertEquals(result.getPlan().getCost(), 0.0);
    assertEquals(result.getCandidateNames(), List.of("mv_all_OFFLINE", "mv_us_OFFLINE"));
  }

  @Test
  public void testRewriteNoMatchForDifferentQuery() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_revenue FROM orders GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    MaterializedViewMetadataCache cache = mock(MaterializedViewMetadataCache.class);
    when(cache.getMaterializedViewEntriesForBaseTable("orders")).thenReturn(List.of(entry));

    MaterializedViewQueryRewriteEngine engine = exactEngine(cache);
    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT state, MAX(revenue) FROM orders GROUP BY state");

    MaterializedViewRewriteResult result = engine.tryRewrite(userQuery, "orders");
    assertNotNull(result);
    assertFalse(result.isHit());
    assertNull(result.getMaterializedViewQueriedName());
    assertEquals(result.getCandidateNames(), List.of("mv_orders_OFFLINE"));
  }

  @Test
  public void testStrategyExceptionPropagatesToCaller() {
    // The engine deliberately does NOT swallow strategy exceptions — a thrown exception is a
    // strategy bug or contract violation and is surfaced so the caller
    // (BaseSingleStageBrokerRequestHandler.compileRequest wraps `_materializedViewHandler.compile`
    // in try/catch) can fall back to the base query AND record QUERY_REWRITE_EXCEPTIONS.
    // Swallowing here would mask the regression behind a less-precise strategy match.
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders",
        "SELECT city, SUM(revenue) AS sum_revenue FROM orders GROUP BY city");

    MaterializedViewMatchStrategy throwingStrategy = mock(MaterializedViewMatchStrategy.class);
    when(throwingStrategy.match(any(), any())).thenThrow(new RuntimeException("test error"));

    MaterializedViewMetadataCache cache = mock(MaterializedViewMetadataCache.class);
    when(cache.getMaterializedViewEntriesForBaseTable("orders")).thenReturn(List.of(entry));

    MaterializedViewQueryRewriteEngine engine =
        new MaterializedViewQueryRewriteEngine(cache, List.of(throwingStrategy));
    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city");

    RuntimeException ex = expectThrows(RuntimeException.class, () -> engine.tryRewrite(userQuery, "orders"));
    assertEquals(ex.getMessage(), "test error");
  }

  @Test
  public void testMultipleStrategiesFirstMatchWins() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_revenue FROM orders GROUP BY city";
    String userSql = "SELECT city, SUM(revenue) FROM orders GROUP BY city";
    MaterializedViewCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery expectedRewritten = CalciteSqlParser.compileToPinotQuery(userSql);
    expectedRewritten.getDataSource().setTableName("mv_orders_OFFLINE");

    MaterializedViewMatchStrategy noOpStrategy = mock(MaterializedViewMatchStrategy.class);
    when(noOpStrategy.match(any(), any())).thenReturn(null);

    MaterializedViewMetadataCache cache = mock(MaterializedViewMetadataCache.class);
    when(cache.getMaterializedViewEntriesForBaseTable("orders")).thenReturn(List.of(entry));

    MaterializedViewQueryRewriteEngine engine = new MaterializedViewQueryRewriteEngine(cache,
        List.of(noOpStrategy, new ExactSubsumptionStrategy()));

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(userSql);
    MaterializedViewRewriteResult result = engine.tryRewrite(userQuery, "orders");

    assertNotNull(result);
    assertTrue(result.isHit());
    assertEquals(result.getMaterializedViewQueriedName(), "mv_orders_OFFLINE");
  }

  @Test
  public void testSkipStaleMaterializedView() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_revenue FROM orders GROUP BY city";
    String userSql = "SELECT city, SUM(revenue) FROM orders GROUP BY city";

    MaterializedViewCacheEntry staleEntry =
        createEntry("mv_stale_OFFLINE", "orders", definedSql, Eligibility.INELIGIBLE);

    MaterializedViewMetadataCache cache = mock(MaterializedViewMetadataCache.class);
    when(cache.getMaterializedViewEntriesForBaseTable("orders")).thenReturn(List.of(staleEntry));

    MaterializedViewQueryRewriteEngine engine = exactEngine(cache);
    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(userSql);

    MaterializedViewRewriteResult result = engine.tryRewrite(userQuery, "orders");
    assertNotNull(result);
    assertFalse(result.isHit());
    assertEquals(result.getCandidateNames(), List.of("mv_stale_OFFLINE"));
  }

  @Test
  public void testSkipDegradedMaterializedView() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_revenue FROM orders GROUP BY city";
    String userSql = "SELECT city, SUM(revenue) FROM orders GROUP BY city";

    MaterializedViewCacheEntry degradedEntry = createEntry("mv_degraded_OFFLINE", "orders", definedSql,
        Eligibility.INELIGIBLE);

    MaterializedViewMetadataCache cache = mock(MaterializedViewMetadataCache.class);
    when(cache.getMaterializedViewEntriesForBaseTable("orders")).thenReturn(List.of(degradedEntry));

    MaterializedViewQueryRewriteEngine engine = exactEngine(cache);
    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(userSql);

    MaterializedViewRewriteResult result = engine.tryRewrite(userQuery, "orders");
    assertNotNull(result);
    assertFalse(result.isHit());
    assertEquals(result.getCandidateNames(), List.of("mv_degraded_OFFLINE"));
  }

  @Test
  public void testSkipStaleButUseFreshMaterializedView() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_revenue FROM orders GROUP BY city";
    String userSql = "SELECT city, SUM(revenue) FROM orders GROUP BY city";

    MaterializedViewCacheEntry staleEntry =
        createEntry("mv_stale_OFFLINE", "orders", definedSql, Eligibility.INELIGIBLE);
    MaterializedViewCacheEntry freshEntry = createEntry("mv_fresh_OFFLINE", "orders", definedSql);

    MaterializedViewMetadataCache cache = mock(MaterializedViewMetadataCache.class);
    when(cache.getMaterializedViewEntriesForBaseTable("orders")).thenReturn(List.of(staleEntry, freshEntry));

    MaterializedViewQueryRewriteEngine engine = exactEngine(cache);
    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(userSql);

    MaterializedViewRewriteResult result = engine.tryRewrite(userQuery, "orders");
    assertNotNull(result);
    assertTrue(result.isHit());
    assertEquals(result.getMaterializedViewQueriedName(), "mv_fresh_OFFLINE");
    assertEquals(result.getCandidateNames(), List.of("mv_stale_OFFLINE", "mv_fresh_OFFLINE"));
  }

  // ---------------------------------------------------------------------------
  // SPLIT_REWRITE guard: malformed MaterializedViewSplitSpec must not produce a split plan.
  //
  // Without the MV-side time column + format, the broker cannot attach the
  // complementary `materializedViewTime < watermarkMs` filter on the MV branch. Running
  // a split query in that state risks double-counting rows materialized during
  // the endSegmentReplace -> watermarkMs publish window. The engine must
  // reject such candidates entirely (not silently fall back to an unguarded
  // split).
  // ---------------------------------------------------------------------------

  @Test
  public void testSkipSplitWhenMaterializedViewTimeColumnMissing() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_revenue FROM orders GROUP BY city";
    String userSql = "SELECT city, SUM(revenue) FROM orders GROUP BY city";

    MaterializedViewSplitSpec malformed =
        new MaterializedViewSplitSpec("ts", "1:MILLISECONDS:EPOCH", null, null, 86_400_000L);
    MaterializedViewCacheEntry entry = createEntry(
        "mv_split_OFFLINE", "orders", definedSql, Eligibility.ELIGIBLE, malformed, 86_400_000L);

    MaterializedViewMetadataCache cache = mock(MaterializedViewMetadataCache.class);
    when(cache.getMaterializedViewEntriesForBaseTable("orders")).thenReturn(List.of(entry));

    MaterializedViewQueryRewriteEngine engine = exactEngine(cache);
    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(userSql);

    MaterializedViewRewriteResult result = engine.tryRewrite(userQuery, "orders");
    assertNotNull(result);
    assertFalse(result.isHit(),
        "SPLIT_REWRITE must be skipped when viewTimeColumn is null; otherwise the MV branch "
            + "would run without an upper-bound filter and double-count with the base branch.");
    assertEquals(result.getCandidateNames(), List.of("mv_split_OFFLINE"));
  }

  @Test
  public void testSkipPartialMaterializedViewWhenAggregationPlanIsNotSplitSafe() {
    String definedSql = "SELECT city, state, COUNT(revenue) AS cnt FROM orders GROUP BY city, state";
    String userSql = "SELECT city, COUNT(revenue) FROM orders GROUP BY city";

    MaterializedViewSplitSpec splitSpec = new MaterializedViewSplitSpec(
        "ts", "1:MILLISECONDS:EPOCH", "materializedViewDay", "1:DAYS:EPOCH", 86_400_000L);
    MaterializedViewCacheEntry entry = createEntry(
        "mv_split_OFFLINE", "orders", definedSql, Eligibility.ELIGIBLE, splitSpec, 86_400_000L);

    MaterializedViewMetadataCache cache = mock(MaterializedViewMetadataCache.class);
    when(cache.getMaterializedViewEntriesForBaseTable("orders")).thenReturn(List.of(entry));

    MaterializedViewQueryRewriteEngine engine = aggregationEngine(cache);
    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(userSql);

    MaterializedViewRewriteResult result = engine.tryRewrite(userQuery, "orders");
    assertNotNull(result);
    assertFalse(result.isHit(),
        "COUNT->SUM re-aggregation is not split-safe. The engine must skip a partially covered MV "
            + "instead of falling back to FULL_REWRITE and dropping newer base-table rows.");
    assertEquals(result.getCandidateNames(), List.of("mv_split_OFFLINE"));
  }

  @Test
  public void testFullRewriteWhenNonSplitSafeQueryIsFullyCovered() {
    String definedSql = "SELECT eventDay, city, COUNT(*) AS cnt FROM orders GROUP BY eventDay, city";
    String userSql = "SELECT city, COUNT(*) FROM orders WHERE eventDay < 10 GROUP BY city";

    MaterializedViewSplitSpec splitSpec = new MaterializedViewSplitSpec(
        "eventDay", "1:DAYS:EPOCH", "eventDay", "1:DAYS:EPOCH", 86_400_000L);
    MaterializedViewCacheEntry entry = createEntry(
        "mv_split_OFFLINE", "orders", definedSql, Eligibility.ELIGIBLE, splitSpec,
        TimeUnit.DAYS.toMillis(10));

    MaterializedViewMetadataCache cache = mock(MaterializedViewMetadataCache.class);
    when(cache.getMaterializedViewEntriesForBaseTable("orders")).thenReturn(List.of(entry));

    MaterializedViewQueryRewriteEngine engine = aggregationEngine(cache);
    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(userSql);

    MaterializedViewRewriteResult result = engine.tryRewrite(userQuery, "orders");
    assertNotNull(result);
    assertTrue(result.isHit(), "A source-time upper bound before watermarkMs is safe for FULL_REWRITE");
    assertEquals(result.getPlan().getExecMode(), ExecutionMode.FULL_REWRITE);
    assertEquals(result.getMaterializedViewQueriedName(), "mv_split_OFFLINE");
  }

  @Test
  public void testSkipNonSplitSafeQueryWhenUpperBoundIncludesBoundary() {
    String definedSql = "SELECT eventDay, city, COUNT(*) AS cnt FROM orders GROUP BY eventDay, city";
    // Under the TIMESTAMP-only contract the watermark literal is raw millis; pick a user upper
    // bound equal to the watermark so the inclusivity check fires.
    long watermarkMs = TimeUnit.DAYS.toMillis(10);
    String userSql = "SELECT city, COUNT(*) FROM orders WHERE eventDay <= " + watermarkMs + " GROUP BY city";

    MaterializedViewSplitSpec splitSpec = new MaterializedViewSplitSpec(
        "eventDay", "1:DAYS:EPOCH", "eventDay", "1:DAYS:EPOCH", 86_400_000L);
    MaterializedViewCacheEntry entry = createEntry(
        "mv_split_OFFLINE", "orders", definedSql, Eligibility.ELIGIBLE, splitSpec, watermarkMs);

    MaterializedViewMetadataCache cache = mock(MaterializedViewMetadataCache.class);
    when(cache.getMaterializedViewEntriesForBaseTable("orders")).thenReturn(List.of(entry));

    MaterializedViewQueryRewriteEngine engine = aggregationEngine(cache);
    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(userSql);

    MaterializedViewRewriteResult result = engine.tryRewrite(userQuery, "orders");
    assertNotNull(result);
    assertFalse(result.isHit(),
        "eventDay <= boundary includes rows at the split boundary, so FULL_REWRITE would drop base rows");
  }

  @Test
  public void testExactMatchRejectedInSplitModeForAggregationMvWithoutGroupBy() {
    // The MV definition aggregates without a GROUP BY. The user query also has no GROUP BY
    // and EXACT-matches the projection. In split mode the base side returns aggregation
    // intermediates while the EXACT-rewritten MV side returns the materialized column
    // (a plain physical type). The reducer cannot merge those — historically this was
    // only guarded for GROUP BY queries; widen the guard to any aggregation MV.
    String definedSql = "SELECT SUM(revenue) AS sum_revenue FROM orders";
    String userSql = "SELECT SUM(revenue) FROM orders";

    MaterializedViewSplitSpec splitSpec = new MaterializedViewSplitSpec(
        "ts", "1:MILLISECONDS:EPOCH", "materializedViewDay", "1:DAYS:EPOCH", 86_400_000L);
    MaterializedViewCacheEntry entry = createEntry(
        "mv_split_no_groupby_OFFLINE", "orders", definedSql, Eligibility.ELIGIBLE, splitSpec,
        TimeUnit.DAYS.toMillis(10));

    MaterializedViewMetadataCache cache = mock(MaterializedViewMetadataCache.class);
    when(cache.getMaterializedViewEntriesForBaseTable("orders")).thenReturn(List.of(entry));

    MaterializedViewQueryRewriteEngine engine = exactEngine(cache);
    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(userSql);

    MaterializedViewRewriteResult result = engine.tryRewrite(userQuery, "orders");
    assertNotNull(result);
    assertFalse(result.isHit(),
        "EXACT match for aggregation MV in split mode must be rejected, even without GROUP BY — "
            + "the base/MV merge would otherwise see incompatible schemas (aggregation intermediate "
            + "vs plain materialized column).");
  }

  @Test
  public void testExactMatchRejectedInSplitModeForAggregationMvWithGroupBy() {
    // Same guard as testExactMatchRejectedInSplitModeForAggregationMvWithoutGroupBy but with a
    // GROUP BY on both sides.  Engine must fall through from EXACT to AGG_REAGG so the broker
    // reduce sees aggregation intermediates on both sides and merges them correctly instead of
    // mixing intermediates against materialized scalars.
    String definedSql = "SELECT city, SUM(revenue) AS sum_revenue FROM orders GROUP BY city";
    String userSql = "SELECT city, SUM(revenue) FROM orders GROUP BY city";

    MaterializedViewSplitSpec splitSpec = new MaterializedViewSplitSpec(
        "ts", "1:MILLISECONDS:EPOCH", "ts", "1:MILLISECONDS:EPOCH", 86_400_000L);
    MaterializedViewCacheEntry entry = createEntry(
        "mv_split_groupby_OFFLINE", "orders", definedSql, Eligibility.ELIGIBLE, splitSpec,
        TimeUnit.DAYS.toMillis(10));

    MaterializedViewMetadataCache cache = mock(MaterializedViewMetadataCache.class);
    when(cache.getMaterializedViewEntriesForBaseTable("orders")).thenReturn(List.of(entry));

    // exactEngine runs ExactSubsumptionStrategy alone — verifies the guard fires here without
    // a downstream strategy masking the rejection.
    MaterializedViewQueryRewriteEngine exactOnly = exactEngine(cache);
    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(userSql);
    MaterializedViewRewriteResult exactResult = exactOnly.tryRewrite(userQuery, "orders");
    assertNotNull(exactResult);
    assertFalse(exactResult.isHit(),
        "EXACT match for aggregation MV in split mode with GROUP BY must be rejected.");

    // With AggregationSubsumptionStrategy available the engine should fall through to AGG_REAGG
    // and produce a split-mode plan.
    MaterializedViewQueryRewriteEngine fullEngine = new MaterializedViewQueryRewriteEngine(cache,
        List.of(new ExactSubsumptionStrategy(), new AggregationSubsumptionStrategy()));
    PinotQuery userQuery2 = CalciteSqlParser.compileToPinotQuery(userSql);
    MaterializedViewRewriteResult fallthroughResult = fullEngine.tryRewrite(userQuery2, "orders");
    assertNotNull(fallthroughResult);
    assertTrue(fallthroughResult.isHit(),
        "Engine should fall through from EXACT to AGG_REAGG in split mode for aggregation MV.");
    assertEquals(fallthroughResult.getPlan().getMatchType(), MatchType.AGG_REAGG);
    assertEquals(fallthroughResult.getPlan().getExecMode(), ExecutionMode.SPLIT_REWRITE);
  }

  @Test
  public void testSplitRewriteForSketchAggregation() {
    String definedSql = "SELECT eventDay, city, DISTINCTCOUNTRAWHLL(user_id) AS raw_hll "
        + "FROM orders GROUP BY eventDay, city";
    String userSql = "SELECT city, DISTINCTCOUNTHLL(user_id) FROM orders GROUP BY city";

    MaterializedViewSplitSpec splitSpec = new MaterializedViewSplitSpec(
        "eventDay", "1:DAYS:EPOCH", "eventDay", "1:DAYS:EPOCH", 86_400_000L);
    MaterializedViewCacheEntry entry = createEntry(
        "mv_split_OFFLINE", "orders", definedSql, Eligibility.ELIGIBLE, splitSpec,
        TimeUnit.DAYS.toMillis(10));

    MaterializedViewMetadataCache cache = mock(MaterializedViewMetadataCache.class);
    when(cache.getMaterializedViewEntriesForBaseTable("orders")).thenReturn(List.of(entry));

    MaterializedViewQueryRewriteEngine engine = aggregationEngine(cache);
    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(userSql);

    MaterializedViewRewriteResult result = engine.tryRewrite(userQuery, "orders");
    assertNotNull(result);
    assertTrue(result.isHit(), "Sketch re-aggregation produces the same intermediate type on both split branches");
    assertEquals(result.getPlan().getExecMode(), ExecutionMode.SPLIT_REWRITE);
    assertEquals(result.getMaterializedViewQueriedName(), "mv_split_OFFLINE");
  }
}
