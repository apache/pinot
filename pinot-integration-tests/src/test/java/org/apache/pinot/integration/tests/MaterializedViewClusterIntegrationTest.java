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
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.materializedview.metadata.MaterializedViewDefinitionMetadata;
import org.apache.pinot.materializedview.metadata.MaterializedViewDefinitionMetadata.MaterializedViewSplitSpec;
import org.apache.pinot.materializedview.metadata.MaterializedViewDefinitionMetadataUtils;
import org.apache.pinot.materializedview.metadata.MaterializedViewRuntimeMetadata;
import org.apache.pinot.materializedview.metadata.MaterializedViewRuntimeMetadataUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * End-to-end integration tests for the Materialized View (MV) query rewrite pipeline.
 *
 * <p>These tests bypass the MV task generator/executor and directly set up the MV
 * infrastructure (MV table, segments, ZK metadata) to validate the broker's query
 * rewrite engine in a real cluster environment.
 *
 * <p>Phase 1 covers core rewrite scenarios:
 * <ol>
 *   <li>{@link #testFullRewriteWithExactMatch()} — MV covers full data range,
 *       {@code ExactSubsumptionStrategy} rewrites the query to the MV table.</li>
 *   <li>{@link #testSplitRewriteWithAggReagg()} — MV covers a historical range,
 *       {@code AggregationSubsumptionStrategy} merges MV + base table in split mode.</li>
 *   <li>{@link #testColdStartSkip()} — MV exists but {@code watermarkMs == 0},
 *       verifying the broker skips the MV during cold-start.</li>
 * </ol>
 *
 * <p>Phase 2 covers incremental updates and consistency:
 * <ol>
 *   <li>{@link #testIncrementalAppend()} — advance split MV coverage via new segments
 *       and ZK metadata update; verify the expanded coverage is respected.</li>
 *   <li>{@link #testFreshnessGating()} — tighten the per-MV {@code stalenessThresholdMs} SLO
 *       on the definition → broker skips the MV; relax it → broker hits it again.</li>
 *   <li>{@link #testMaterializedViewDefinitionDeletion()} — delete MV definition from ZK → broker
 *       no longer considers the MV as a candidate.</li>
 * </ol>
 *
 * <p>Phase 3 covers edge cases and advanced matching:
 * <ol>
 *   <li>{@link #testScanSubsumptionWithResidualFilter()} — SCAN-shaped MV with a
 *       user WHERE clause that becomes a residual filter on the MV table.</li>
 *   <li>{@link #testQueryWithLimitOffsetOrderBy()} — split mode query with
 *       ORDER BY + LIMIT + OFFSET; verify correct pagination and ordering.</li>
 *   <li>{@link #testMultipleMaterializedViewCostSelection()} — two MVs match the same query at
 *       different costs; verify the lower-cost MV is selected.</li>
 * </ol>
 *
 * <p><b>Why this extends {@link BaseClusterIntegrationTest} directly instead of
 * {@link org.apache.pinot.integration.tests.custom.CustomDataQueryClusterIntegrationTest}:</b>
 * {@code CustomDataQueryClusterIntegrationTest} runs all sub-tests against a single
 * suite-level shared cluster that is started once in {@code @BeforeSuite}. That shared
 * cluster is created before any sub-test has a chance to call {@link #overrideBrokerConf},
 * so {@code CONFIG_OF_BROKER_QUERY_ENABLE_MATERIALIZED_VIEW_REWRITE} cannot be applied to
 * it. MV rewrite is disabled by default; without a broker started with that flag the entire
 * test suite would be a no-op. A dedicated cluster started in {@code @BeforeClass} is
 * therefore required until the shared cluster supports per-test broker-config overrides.
 */
public class MaterializedViewClusterIntegrationTest extends BaseClusterIntegrationTest {

  private static final String SOURCE_TABLE_NAME = "materializedViewSourceTable";
  private static final String MATERIALIZED_VIEW_FULL_TABLE_NAME = "materializedViewFullTable";
  private static final String MATERIALIZED_VIEW_SPLIT_TABLE_NAME = "materializedViewSplitTable";
  private static final String MATERIALIZED_VIEW_COLD_TABLE_NAME = "materializedViewColdTable";
  private static final String MATERIALIZED_VIEW_SCAN_TABLE_NAME = "materializedViewScanTable";
  private static final String MATERIALIZED_VIEW_COST_TABLE_NAME = "materializedViewCostTable";

  private static final String MATERIALIZED_VIEW_FULL_TABLE_OFFLINE = MATERIALIZED_VIEW_FULL_TABLE_NAME + "_OFFLINE";
  private static final String MATERIALIZED_VIEW_SPLIT_TABLE_OFFLINE = MATERIALIZED_VIEW_SPLIT_TABLE_NAME + "_OFFLINE";
  private static final String MATERIALIZED_VIEW_COLD_TABLE_OFFLINE = MATERIALIZED_VIEW_COLD_TABLE_NAME + "_OFFLINE";
  private static final String MATERIALIZED_VIEW_SCAN_TABLE_OFFLINE = MATERIALIZED_VIEW_SCAN_TABLE_NAME + "_OFFLINE";
  private static final String MATERIALIZED_VIEW_COST_TABLE_OFFLINE = MATERIALIZED_VIEW_COST_TABLE_NAME + "_OFFLINE";

  private static final String TIME_COLUMN = "DaysSinceEpoch";

  // Airline dataset spans DaysSinceEpoch ~16071–16101 (days since epoch)
  // 16071 * 86400000 = 1_388_534_400_000 (Jan 1, 2014)
  // 16102 * 86400000 = 1_391_212_800_000 (Feb 1, 2014)
  private static final long DATA_MIN_TIME_MS = 16071L * 86_400_000L;
  private static final long DATA_MAX_TIME_MS = 16102L * 86_400_000L;

  // Split boundary: MV covers first ~15 days, base table covers the rest
  private static final long SPLIT_BOUNDARY_MS = 16086L * 86_400_000L;

  private static final String[] CARRIERS = {"AA", "DL", "UA", "WN", "US", "B6", "OO", "EV", "MQ", "NK"};

  private PinotHelixResourceManager _helixResourceManager;
  private HelixPropertyStore<ZNRecord> _propertyStore;

  @Override
  protected String getTableName() {
    return SOURCE_TABLE_NAME;
  }

  @Override
  @Nullable
  protected String getSortedColumn() {
    return null;
  }

  @Override
  @Nullable
  protected List<String> getInvertedIndexColumns() {
    return null;
  }

  @Override
  @Nullable
  protected List<String> getNoDictionaryColumns() {
    return null;
  }

  @Override
  @Nullable
  protected List<String> getRangeIndexColumns() {
    return null;
  }

  @Override
  @Nullable
  protected List<String> getBloomFilterColumns() {
    return null;
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    brokerConf.setProperty(Broker.CONFIG_OF_BROKER_QUERY_ENABLE_MATERIALIZED_VIEW_REWRITE, true);
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    startZk();
    startController();
    startBroker();
    startServer();

    _helixResourceManager = _controllerStarter.getHelixResourceManager();
    _propertyStore = _helixResourceManager.getPropertyStore();

    // --- Source table: load airline data ---
    Schema sourceSchema = createSchema();
    sourceSchema.setSchemaName(SOURCE_TABLE_NAME);
    addSchema(sourceSchema);

    TableConfig sourceTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(SOURCE_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setNumReplicas(1)
        .build();
    addTableConfig(sourceTableConfig);

    List<File> avroFiles = unpackAvroData(_tempDir);
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, sourceTableConfig, sourceSchema,
        0, _segmentDir, _tarDir);
    uploadSegments(SOURCE_TABLE_NAME, _tarDir);
    waitForAllDocsLoaded(600_000L);

    // --- MV tables: build with synthetic pre-aggregated data ---
    setupFullRewriteMv();
    setupSplitRewriteMv();
    setupColdStartMv();
    setupScanMv();
    setupCostCompetitorMv();

    // Wait for the broker's MaterializedViewMetadataCache to register every newly-published MV
    // by polling a sentinel query that should be served by the full-rewrite MV.  Polling on a
    // real query (rather than `Thread.sleep`) lets the test progress as soon as the ZK watchers
    // catch up — fast machines no longer pay 5s of wall clock and slow CI machines no longer
    // race the timeout.
    waitForMaterializedViewRegistered(MATERIALIZED_VIEW_FULL_TABLE_OFFLINE,
        "SELECT Carrier, SUM(ArrDelayMinutes) FROM " + SOURCE_TABLE_NAME + " GROUP BY Carrier");
  }

  /// Polls a query that is known to be rewritable to the given MV until the broker's metadata
  /// cache has caught up. Used in place of `Thread.sleep` after publishing MV znodes from a
  /// test setup step.
  private void waitForMaterializedViewRegistered(String expectedMaterializedViewOfflineTable, String sentinelQuery) {
    TestUtils.waitForCondition(() -> {
      try {
        JsonNode response = postQuery(sentinelQuery);
        String materializedViewQueried = getMaterializedViewQueried(response);
        return expectedMaterializedViewOfflineTable.equals(materializedViewQueried);
      } catch (Exception e) {
        return false;
      }
    }, 100L, 30_000L,
        "MV " + expectedMaterializedViewOfflineTable + " not registered with the broker cache",
        Duration.ofSeconds(6));
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    // Drop MV metadata FIRST so the controller's MV-dependency check on the base table allows
    // the source-table drop below.  The controller refuses to delete a base table while MV
    // definition znodes still reference it.
    cleanupMaterializedViewMetadata(MATERIALIZED_VIEW_FULL_TABLE_OFFLINE);
    cleanupMaterializedViewMetadata(MATERIALIZED_VIEW_SPLIT_TABLE_OFFLINE);
    cleanupMaterializedViewMetadata(MATERIALIZED_VIEW_COLD_TABLE_OFFLINE);
    cleanupMaterializedViewMetadata(MATERIALIZED_VIEW_SCAN_TABLE_OFFLINE);
    cleanupMaterializedViewMetadata(MATERIALIZED_VIEW_COST_TABLE_OFFLINE);

    // Drop MV tables before the base table so the controller's referential-integrity check
    // (introduced in pinot-controller to prevent orphaned MVs) does not block the base drop.
    dropOfflineTable(MATERIALIZED_VIEW_FULL_TABLE_NAME);
    dropOfflineTable(MATERIALIZED_VIEW_SPLIT_TABLE_NAME);
    dropOfflineTable(MATERIALIZED_VIEW_COLD_TABLE_NAME);
    dropOfflineTable(MATERIALIZED_VIEW_SCAN_TABLE_NAME);
    dropOfflineTable(MATERIALIZED_VIEW_COST_TABLE_NAME);
    dropOfflineTable(SOURCE_TABLE_NAME);

    stopServer();
    stopBroker();
    stopController();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  // -----------------------------------------------------------------------
  //  Phase 1, Test 1: Full rewrite with ExactSubsumptionStrategy
  // -----------------------------------------------------------------------

  /**
   * Verifies that when an MV fully covers the source data and the query matches
   * the MV definition exactly, the broker rewrites the query to hit the MV table
   * via {@code FULL_REWRITE} execution mode.
   */
  @Test
  public void testFullRewriteWithExactMatch()
      throws Exception {
    String query = ""
        + "SELECT Carrier, SUM(ArrDelayMinutes) AS sum_ArrDelayMinutes "
        + "FROM " + SOURCE_TABLE_NAME + " "
        + "GROUP BY Carrier";
    JsonNode response = postQuery(query);
    assertNoExceptions(response);

    String materializedViewQueried = getMaterializedViewQueried(response);
    assertNotNull(materializedViewQueried, "Expected MV to be hit for exact-match query. Response: " + response);
    assertEquals(materializedViewQueried, MATERIALIZED_VIEW_FULL_TABLE_OFFLINE,
        "Expected materializedViewQueried to be the full-rewrite MV table");

    JsonNode resultTable = response.get("resultTable");
    assertNotNull(resultTable, "resultTable should not be null");
    assertTrue(resultTable.get("rows").size() > 0, "Result should have rows");
  }

  // -----------------------------------------------------------------------
  //  Phase 1, Test 2: Split rewrite with AggregationSubsumptionStrategy
  // -----------------------------------------------------------------------

  /**
   * Verifies split-mode execution with re-aggregation. The MV covers only
   * historical data (up to {@code SPLIT_BOUNDARY_MS}). The broker merges
   * MV results with recent base table data.
   *
   * <p>Uses a query with {@code Origin} in GROUP BY, which matches the split
   * MV definition but not the full MV definition, ensuring this test targets
   * the split MV specifically.
   */
  @Test
  public void testSplitRewriteWithAggReagg()
      throws Exception {
    // This query matches the split MV (which groups by DaysSinceEpoch, Carrier, Origin)
    // via AggregationSubsumptionStrategy, but does NOT match the full MV (which only
    // groups by Carrier). The user query groups by (Carrier, Origin) which is a subset
    // of the split MV's (DaysSinceEpoch, Carrier, Origin), enabling re-aggregation.
    String query = ""
        + "SELECT Carrier, Origin, SUM(ArrDelayMinutes) AS sum_ArrDelayMinutes "
        + "FROM " + SOURCE_TABLE_NAME + " "
        + "GROUP BY Carrier, Origin";
    JsonNode materializedViewResponse = postQuery(query);
    assertNoExceptions(materializedViewResponse);

    String materializedViewQueried = getMaterializedViewQueried(materializedViewResponse);
    assertNotNull(materializedViewQueried,
        "Expected materialized view to be hit for split-mode query. Response: " + materializedViewResponse);
    assertEquals(materializedViewQueried, MATERIALIZED_VIEW_SPLIT_TABLE_OFFLINE,
        "Expected materializedViewQueried to be the split-rewrite MV table");

    JsonNode resultTable = materializedViewResponse.get("resultTable");
    assertNotNull(resultTable, "resultTable should not be null");
    assertTrue(resultTable.get("rows").size() > 0, "Result should have rows");

    // Verify result count matches direct query
    String directQuery = "SELECT Carrier, Origin, SUM(ArrDelayMinutes) AS sum_ArrDelayMinutes "
        + "FROM " + SOURCE_TABLE_NAME + " GROUP BY Carrier, Origin";
    JsonNode directResponse = postQuery(directQuery);
    assertNoExceptions(directResponse);

    int directRowCount = directResponse.get("resultTable").get("rows").size();
    int materializedViewRowCount = resultTable.get("rows").size();
    assertEquals(materializedViewRowCount, directRowCount,
        "Split MV query should return same number of groups as direct query");
  }

  // -----------------------------------------------------------------------
  //  Phase 1, Test 3: Cold-start skip
  // -----------------------------------------------------------------------

  /// Verifies that the broker skips an MV with `watermarkMs == 0`,
  /// simulating a cold-start where the metadata exists but no APPEND has completed.
  ///
  /// The cold-start MV has the same definition as the split MV (grouping by
  /// DaysSinceEpoch, Carrier, Origin). We use a query with `Carrier, Origin`
  /// in GROUP BY, which would match both the split MV and the cold-start MV via
  /// [AggregationSubsumptionStrategy]. The cold-start MV should be skipped
  /// because its `watermarkMs == 0`, and the split MV should be hit instead.
  @Test
  public void testColdStartSkip()
      throws Exception {
    /// This query matches both the split MV and the cold-start MV structurally.
    /// Only the split MV should be hit; the cold-start MV should be skipped.
    String query = ""
        + "SELECT Carrier, Origin, SUM(ArrDelayMinutes) AS sum_ArrDelayMinutes "
        + "FROM " + SOURCE_TABLE_NAME + " "
        + "GROUP BY Carrier, Origin";
    JsonNode response = postQuery(query);
    assertNoExceptions(response);

    String materializedViewQueried = getMaterializedViewQueried(response);
    assertNotNull(materializedViewQueried, "Expected an MV to be hit. Response: " + response);
    assertNotEquals(materializedViewQueried, MATERIALIZED_VIEW_COLD_TABLE_OFFLINE,
        "Cold-start MV should NOT be hit (watermarkMs == 0)");
    assertEquals(materializedViewQueried, MATERIALIZED_VIEW_SPLIT_TABLE_OFFLINE,
        "Expected the split MV to be hit instead of the cold-start MV");

    JsonNode resultTable = response.get("resultTable");
    assertNotNull(resultTable, "resultTable should not be null");
    assertTrue(resultTable.get("rows").size() > 0,
        "Query should still return results via the split MV");
  }

  // -----------------------------------------------------------------------
  //  Phase 2, Test 4: Incremental append — coverage boundary advances
  // -----------------------------------------------------------------------

  /// Simulates an incremental APPEND by uploading additional MV segments that cover
  /// days 16086–16095 (previously only 16071–16085 were materialized), then advancing
  /// `watermarkMs` in ZK. Verifies that the broker respects the expanded
  /// coverage boundary and still produces correct split-mode results.
  ///
  /// This test depends on the split MV state established by [#setUp()].
  @Test(dependsOnMethods = "testSplitRewriteWithAggReagg")
  public void testIncrementalAppend()
      throws Exception {
    long extendedBoundaryMs = 16096L * 86_400_000L;

    /// Upload additional MV segments for the newly materialized time window
    Schema materializedViewSchema = new Schema.SchemaBuilder()
        .setSchemaName(MATERIALIZED_VIEW_SPLIT_TABLE_NAME)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.INT, "1:DAYS:EPOCH", "1:DAYS")
        .addSingleValueDimension("Carrier", FieldSpec.DataType.STRING)
        .addSingleValueDimension("Origin", FieldSpec.DataType.STRING)
        .addMetric("sum_ArrDelayMinutes", FieldSpec.DataType.DOUBLE)
        .build();

    TableConfig materializedViewTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(MATERIALIZED_VIEW_SPLIT_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setNumReplicas(1)
        .build();

    String[] origins = {"SFO", "LAX", "JFK", "ORD", "ATL"};
    List<GenericRow> rows = new ArrayList<>();
    for (int day = 16086; day < 16096; day++) {
      for (String carrier : CARRIERS) {
        for (String origin : origins) {
          GenericRow row = new GenericRow();
          row.putValue(TIME_COLUMN, day);
          row.putValue("Carrier", carrier);
          row.putValue("Origin", origin);
          row.putValue("sum_ArrDelayMinutes",
              100.0 + (day + carrier.hashCode() + origin.hashCode()) % 200);
          rows.add(row);
        }
      }
    }
    buildAndUploadSegment(materializedViewTableConfig, materializedViewSchema, rows,
        MATERIALIZED_VIEW_SPLIT_TABLE_NAME, "materializedViewSplitSeg2");

    long previousCount = getCurrentCountStarResult(MATERIALIZED_VIEW_SPLIT_TABLE_NAME);
    TestUtils.waitForCondition(
        () -> getCurrentCountStarResult(MATERIALIZED_VIEW_SPLIT_TABLE_NAME) > previousCount,
        100L, 60_000L, "New MV split segments not loaded", Duration.ofSeconds(6));

    /// Advance watermarkMs in ZK to reflect the newly materialized window
    MaterializedViewRuntimeMetadata updatedRuntime = new MaterializedViewRuntimeMetadata(

        MATERIALIZED_VIEW_SPLIT_TABLE_OFFLINE, extendedBoundaryMs, new HashMap<>());
    MaterializedViewRuntimeMetadataUtils.persist(_propertyStore, updatedRuntime, -1);

    /// Wait for ZK watcher to propagate the watermark update to the broker cache.  Poll a
    /// sentinel query that should be served by the split MV; the watermark advance is
    /// observable indirectly because the rewrite path keeps choosing the split MV (which
    /// depends on the updated watermark for the split-boundary filter).
    waitForMaterializedViewRegistered(MATERIALIZED_VIEW_SPLIT_TABLE_OFFLINE,
        "SELECT Carrier, Origin, SUM(ArrDelayMinutes) FROM " + SOURCE_TABLE_NAME + " GROUP BY Carrier, Origin");

    /// Query and verify: the split MV should still be hit with the advanced boundary
    String query = ""
        + "SELECT Carrier, Origin, SUM(ArrDelayMinutes) AS sum_ArrDelayMinutes "
        + "FROM " + SOURCE_TABLE_NAME + " "
        + "GROUP BY Carrier, Origin";
    JsonNode response = postQuery(query);
    assertNoExceptions(response);

    String materializedViewQueried = getMaterializedViewQueried(response);
    assertNotNull(materializedViewQueried, "MV should be hit after incremental append. Response: " + response);
    assertEquals(materializedViewQueried, MATERIALIZED_VIEW_SPLIT_TABLE_OFFLINE);

    JsonNode resultTable = response.get("resultTable");
    assertNotNull(resultTable);
    assertTrue(resultTable.get("rows").size() > 0);

    /// Compare with direct query to verify correctness
    String directQuery = "SELECT Carrier, Origin, SUM(ArrDelayMinutes) AS sum_ArrDelayMinutes "
        + "FROM " + SOURCE_TABLE_NAME + " GROUP BY Carrier, Origin";
    JsonNode directResponse = postQuery(directQuery);
    assertNoExceptions(directResponse);
    assertEquals(
        resultTable.get("rows").size(),
        directResponse.get("resultTable").get("rows").size(),
        "Row count should match direct query after incremental append");
  }

  // -----------------------------------------------------------------------
  //  Correctness: non-atomic endSegmentReplace <-> watermarkMs publish window
  // -----------------------------------------------------------------------

  /// Reproduces the non-atomic publish window between the minion executor's
  /// `endSegmentReplace` (new MV segments become queryable on servers) and
  /// `postProcess` (ZK `watermarkMs` advances). During that window
  /// the broker observes a stale `watermarkMs = W_old` while the MV
  /// table physically contains already-materialized rows in `[W_old, W_new)`.
  ///
  /// Before the `materializedViewTime < watermarkMs` filter was attached on the MV branch:
  ///
  ///   - MV branch: reads rows `[0, W_new)` (no upper bound) — includes the new,
  ///       not-yet-published range.
  ///   - Base branch: reads rows `[W_old, +inf)` — also includes the new range.
  ///
  /// The overlap `[W_old, W_new)` is double-counted in the merged result.
  ///
  /// This test simulates that exact window by uploading MV segments covering
  /// a range beyond the current `watermarkMs` without advancing it,
  /// and asserts that the MV rewrite still produces the same sums as a baseline
  /// query taken before the overlap segments existed — i.e., the MV branch
  /// correctly excludes the not-yet-published range.
  ///
  /// Depends on [#testIncrementalAppend()] which leaves the split MV
  /// covering days 16071–16095 and `watermarkMs = 16096 * day`.
  @Test(dependsOnMethods = "testIncrementalAppend")
  public void testSplitCorrectnessDuringNonAtomicPublishWindow()
      throws Exception {
    /// NOTE: explicit ORDER BY on grouping keys + large LIMIT is required here.
    /// Without them we inherit the default broker LIMIT (10) with no ORDER BY, so the
    /// "top 10" groups are picked by server-side iteration order. Adding new MV segments
    /// perturbs segment iteration / group-merge order even when the materializedViewTime < watermarkMs
    /// filter correctly excludes every row in those segments, which would make this test
    /// flaky (keySet mismatch) for reasons unrelated to the CRITICAL #1 invariant we are
    /// actually verifying. Sorting by the grouping keys and asking for more rows than the
    /// real cardinality makes the result deterministic regardless of segment layout.
    String query = ""
        + "SELECT Carrier, Origin, SUM(ArrDelayMinutes) AS sum_ArrDelayMinutes "
        + "FROM " + SOURCE_TABLE_NAME + " "
        + "GROUP BY Carrier, Origin "
        + "ORDER BY Carrier, Origin "
        + "LIMIT 10000";

    /// Step 1: baseline — split query before the simulated overlap.
    /// MV has days 16071..16095, watermarkMs = 16096 * day.
    JsonNode baseline = postQuery(query);
    assertNoExceptions(baseline);
    /// Verify the split MV is actually being hit — if testIncrementalAppend did not run (e.g.
    /// test was invoked in isolation), this will produce a clear skip-style failure here rather
    /// than a misleading assertion failure deeper in the test.
    assertEquals(getMaterializedViewQueried(baseline), MATERIALIZED_VIEW_SPLIT_TABLE_OFFLINE,
        "Precondition failed: split MV must be hit. "
            + "Ensure testIncrementalAppend ran first (dependsOnMethods contract).");
    Map<String, Double> baselineSums = collectSumByCarrierOrigin(baseline);
    assertFalse(baselineSums.isEmpty(), "Baseline must contain grouped rows");

    /// Step 2: simulate endSegmentReplace without postProcess — upload MV segments
    /// for days 16096..16100 (ALREADY physically queryable on servers), but leave
    /// watermarkMs at 16096 in ZK (postProcess has not yet run).
    Schema materializedViewSchema = new Schema.SchemaBuilder()
        .setSchemaName(MATERIALIZED_VIEW_SPLIT_TABLE_NAME)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.INT, "1:DAYS:EPOCH", "1:DAYS")
        .addSingleValueDimension("Carrier", FieldSpec.DataType.STRING)
        .addSingleValueDimension("Origin", FieldSpec.DataType.STRING)
        .addMetric("sum_ArrDelayMinutes", FieldSpec.DataType.DOUBLE)
        .build();
    TableConfig materializedViewTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(MATERIALIZED_VIEW_SPLIT_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setNumReplicas(1)
        .build();

    String[] origins = {"SFO", "LAX", "JFK", "ORD", "ATL"};
    List<GenericRow> rows = new ArrayList<>();
    for (int day = 16096; day <= 16100; day++) {
      for (String carrier : CARRIERS) {
        for (String origin : origins) {
          GenericRow row = new GenericRow();
          row.putValue(TIME_COLUMN, day);
          row.putValue("Carrier", carrier);
          row.putValue("Origin", origin);
          /// Use large, distinctive sums so any leak into the merged result is
          /// trivially detectable — not a rounding-level perturbation.
          row.putValue("sum_ArrDelayMinutes", 1_000_000.0);
          rows.add(row);
        }
      }
    }
    long beforeCount = getCurrentCountStarResult(MATERIALIZED_VIEW_SPLIT_TABLE_NAME);
    buildAndUploadSegment(materializedViewTableConfig, materializedViewSchema, rows,
        MATERIALIZED_VIEW_SPLIT_TABLE_NAME, "materializedViewSplitOverlap");
    TestUtils.waitForCondition(
        () -> getCurrentCountStarResult(MATERIALIZED_VIEW_SPLIT_TABLE_NAME) > beforeCount,
        100L, 60_000L, "Overlap MV segments not visible on servers", Duration.ofSeconds(6));

    /// Intentionally NO MaterializedViewRuntimeMetadataUtils.persist here — this is the whole
    /// point of the test: segments are live, ZK watermark is stale.

    /// Step 3: rerun the split query. With the materializedViewTime < watermarkMs filter
    /// on the MV branch, the newly uploaded "leak" segments are excluded and
    /// sums must equal the baseline. Without the fix, each group's sum would be
    /// inflated by 1_000_000 * 5 days = 5_000_000 relative to the baseline.
    JsonNode after = postQuery(query);
    assertNoExceptions(after);
    assertEquals(getMaterializedViewQueried(after), MATERIALIZED_VIEW_SPLIT_TABLE_OFFLINE,
        "Split MV must still be hit after overlap segments are published");

    Map<String, Double> afterSums = collectSumByCarrierOrigin(after);
    assertEquals(afterSums.keySet(), baselineSums.keySet(),
        "Group keys must not change when new MV segments are published beyond coverage");
    for (Map.Entry<String, Double> e : baselineSums.entrySet()) {
      double expected = e.getValue();
      double actual = afterSums.get(e.getKey());
      /// Allow tiny floating-point drift; any leak is at least 1_000_000 per affected group.
      assertEquals(actual, expected, 1e-6,
          "MV branch must exclude segments beyond watermarkMs (non-atomic publish window). "
              + "Group=" + e.getKey() + ", baseline=" + expected + ", after=" + actual
              + ". A large delta here means the MV branch read rows in [watermarkMs, newMax), "
              + "double-counting with the base branch.");
    }
  }

  /// Extracts (Carrier|Origin -> sum_ArrDelayMinutes) from a grouped response.
  private static Map<String, Double> collectSumByCarrierOrigin(JsonNode response) {
    Map<String, Double> result = new HashMap<>();
    JsonNode rowsNode = response.get("resultTable").get("rows");
    for (int i = 0; i < rowsNode.size(); i++) {
      JsonNode row = rowsNode.get(i);
      String key = row.get(0).asText() + "|" + row.get(1).asText();
      result.put(key, row.get(2).asDouble());
    }
    return result;
  }

  // -----------------------------------------------------------------------
  //  Phase 2, Test 5: Staleness SLO gating — broker skips MV when SLO trips
  // -----------------------------------------------------------------------

  /// Verifies that the broker skips an MV when its staleness SLO is violated
  /// (`now - watermarkMs > stalenessThresholdMs`), and resumes using it once
  /// the SLO is disabled or relaxed.
  ///
  /// Under V2, freshness is not a persistent enum on the runtime ZNode — it is derived
  /// on read from `stalenessThresholdMs` on the MV definition. This test exercises
  /// the SLO branch of [MaterializedViewQueryRewriteEngine#isEligible].  The full
  /// MV's watermark is fixed at `DATA_MAX_TIME_MS` (an ancient timestamp), so any
  /// positive SLO of reasonable size will be exceeded.
  @Test(dependsOnMethods = "testFullRewriteWithExactMatch")
  public void testFreshnessGating()
      throws Exception {
    String query = ""
        + "SELECT Carrier, SUM(ArrDelayMinutes) AS sum_ArrDelayMinutes "
        + "FROM " + SOURCE_TABLE_NAME + " GROUP BY Carrier";
    String definedSql = "SELECT Carrier, SUM(ArrDelayMinutes) AS sum_ArrDelayMinutes "
        + "FROM " + SOURCE_TABLE_NAME + " GROUP BY Carrier";

    /// Step 1: Verify MV is currently hit (precondition; SLO disabled).
    JsonNode preResponse = postQuery(query);
    assertNoExceptions(preResponse);
    assertEquals(getMaterializedViewQueried(preResponse), MATERIALIZED_VIEW_FULL_TABLE_OFFLINE,
        "Precondition: full MV should be hit before SLO is tightened");

    /// Step 2: Tighten the SLO on the MV definition.  watermarkMs = DATA_MAX_TIME_MS is a
    /// 2014-era epoch, so any small positive threshold trips `(now - watermarkMs) > threshold`.
    MaterializedViewDefinitionMetadata staleDefinition = new MaterializedViewDefinitionMetadata(
        MATERIALIZED_VIEW_FULL_TABLE_OFFLINE,
        Collections.singletonList(SOURCE_TABLE_NAME),
        definedSql,
        Collections.emptyMap(),
        null,
        1L,
        true);
    MaterializedViewDefinitionMetadataUtils.persist(_propertyStore, staleDefinition, -1);

    /// Wait for the broker cache to observe the tightened SLO by polling until the MV stops
    /// being chosen for the query.
    TestUtils.waitForCondition(() -> {
      try {
        String mv = getMaterializedViewQueried(postQuery(query));
        return mv == null || !MATERIALIZED_VIEW_FULL_TABLE_OFFLINE.equals(mv);
      } catch (Exception e) {
        return false;
      }
    }, 100L, 30_000L, "Broker did not pick up the tightened staleness SLO", Duration.ofSeconds(6));

    /// Step 3: Query again — the MV should NOT be hit (SLO trips eligibility gate).
    JsonNode staleResponse = postQuery(query);
    assertNoExceptions(staleResponse);
    String staleMaterializedViewQueried = getMaterializedViewQueried(staleResponse);
    if (staleMaterializedViewQueried != null) {
      assertNotEquals(staleMaterializedViewQueried, MATERIALIZED_VIEW_FULL_TABLE_OFFLINE,
          "MV with violated staleness SLO should NOT be hit");
    }

    /// Query should still return results (broker falls back to base table).
    JsonNode staleResultTable = staleResponse.get("resultTable");
    assertNotNull(staleResultTable);
    assertTrue(staleResultTable.get("rows").size() > 0,
        "Query should still return results when MV is excluded by SLO");

    /// Step 4: Disable the SLO again by setting stalenessThresholdMs back to 0.
    MaterializedViewDefinitionMetadata freshDefinition = new MaterializedViewDefinitionMetadata(
        MATERIALIZED_VIEW_FULL_TABLE_OFFLINE,
        Collections.singletonList(SOURCE_TABLE_NAME),
        definedSql,
        Collections.emptyMap(),
        null,
        0L,
        true);
    MaterializedViewDefinitionMetadataUtils.persist(_propertyStore, freshDefinition, -1);

    /// Wait for the broker cache to observe the relaxed SLO; poll until the MV is chosen
    /// again rather than fixed-sleeping.
    waitForMaterializedViewRegistered(MATERIALIZED_VIEW_FULL_TABLE_OFFLINE, query);

    /// Step 5: Query again — the MV should be hit again.
    JsonNode freshResponse = postQuery(query);
    assertNoExceptions(freshResponse);
    assertEquals(getMaterializedViewQueried(freshResponse), MATERIALIZED_VIEW_FULL_TABLE_OFFLINE,
        "MV should be hit again after SLO is disabled");
  }

  // -----------------------------------------------------------------------
  //  Phase 2, Test 6: MV definition deletion — broker stops rewriting
  // -----------------------------------------------------------------------

  /**
   * Verifies that when an MV's definition is deleted from ZooKeeper, the broker
   * removes it from the cache and other MVs continue to serve queries.
   */
  @Test(dependsOnMethods = "testColdStartSkip")
  public void testMaterializedViewDefinitionDeletion()
      throws Exception {
    String query = ""
        + "SELECT Carrier, Origin, SUM(ArrDelayMinutes) AS sum_ArrDelayMinutes "
        + "FROM " + SOURCE_TABLE_NAME + " GROUP BY Carrier, Origin";

    // Precondition: split MV is currently the chosen MV for this query.
    JsonNode preResponse = postQuery(query);
    assertNoExceptions(preResponse);
    assertEquals(getMaterializedViewQueried(preResponse), MATERIALIZED_VIEW_SPLIT_TABLE_OFFLINE,
        "Precondition: split MV should be queried for this aggregation");

    // Delete the cold MV's definition + runtime from ZK.
    MaterializedViewDefinitionMetadataUtils.delete(_propertyStore, MATERIALIZED_VIEW_COLD_TABLE_OFFLINE);
    MaterializedViewRuntimeMetadataUtils.delete(_propertyStore, MATERIALIZED_VIEW_COLD_TABLE_OFFLINE);

    // Wait for the broker cache to observe the deletion by polling until the cold MV is no
    // longer chosen for a query that uniquely matches it (no-op here because split MV is the
    // only candidate for the test query, but the poll exits as soon as the listener fires and
    // any subsequent query against COLD's distinctive shape returns null).
    waitForMaterializedViewRegistered(MATERIALIZED_VIEW_SPLIT_TABLE_OFFLINE, query);

    // After deletion the split MV should still serve the query unaffected.
    JsonNode postResponse = postQuery(query);
    assertNoExceptions(postResponse);
    String materializedViewQueried = getMaterializedViewQueried(postResponse);
    assertNotNull(materializedViewQueried, "Split MV should still be available. Response: " + postResponse);
    assertEquals(materializedViewQueried, MATERIALIZED_VIEW_SPLIT_TABLE_OFFLINE);
  }

  // -----------------------------------------------------------------------
  //  Phase 3, Test 7: Scan subsumption with residual WHERE filter
  // -----------------------------------------------------------------------

  /**
   * Verifies that a SCAN-shaped MV (no GROUP BY, no aggregation) is matched by
   * {@code ScanSubsumptionStrategy} when the user query selects a subset of the
   * MV's columns and adds a WHERE filter that references only MV columns.
   *
   * <p>The scan MV stores {@code Carrier, Origin, Dest, ArrDelayMinutes} from the
   * source table. The user query selects {@code Carrier, ArrDelayMinutes} with
   * {@code WHERE Origin = 'SFO'} — the WHERE clause becomes a residual filter
   * on the MV table.
   */
  @Test
  public void testScanSubsumptionWithResidualFilter()
      throws Exception {
    // Query with WHERE filter — should hit the scan MV via ScanSubsumptionStrategy
    String query = ""
        + "SELECT Carrier, ArrDelayMinutes "
        + "FROM " + SOURCE_TABLE_NAME + " "
        + "WHERE Origin = 'SFO'";
    JsonNode materializedViewResponse = postQuery(query);
    assertNoExceptions(materializedViewResponse);

    String materializedViewQueried = getMaterializedViewQueried(materializedViewResponse);
    assertNotNull(materializedViewQueried, "Expected scan MV to be hit. Response: " + materializedViewResponse);
    assertEquals(materializedViewQueried, MATERIALIZED_VIEW_SCAN_TABLE_OFFLINE,
        "Expected materializedViewQueried to be the scan MV table");

    JsonNode resultTable = materializedViewResponse.get("resultTable");
    assertNotNull(resultTable, "resultTable should not be null");
    assertTrue(resultTable.get("rows").size() > 0, "Result should have rows");

    // Verify result count matches direct query (without MV rewrite)
    String directQuery = "SELECT Carrier, ArrDelayMinutes "
        + "FROM " + SOURCE_TABLE_NAME + " WHERE Origin = 'SFO'";
    JsonNode directResponse = postQuery(directQuery);
    assertNoExceptions(directResponse);

    assertEquals(
        resultTable.get("rows").size(),
        directResponse.get("resultTable").get("rows").size(),
        "Scan MV query should return same row count as direct query");
  }

  // -----------------------------------------------------------------------
  //  Phase 3, Test 8: Split mode with ORDER BY + LIMIT + OFFSET
  // -----------------------------------------------------------------------

  /**
   * Verifies that a split-mode query with ORDER BY, LIMIT, and OFFSET produces
   * correct results. The broker must collect enough rows from both MV and base
   * table sides to satisfy the OFFSET + LIMIT requirement, then apply final
   * ordering and pagination during the merge phase.
   */
  @Test
  public void testQueryWithLimitOffsetOrderBy()
      throws Exception {
    String materializedViewQuery = ""
        + "SELECT Carrier, Origin, SUM(ArrDelayMinutes) AS sum_ArrDelayMinutes "
        + "FROM " + SOURCE_TABLE_NAME + " "
        + "GROUP BY Carrier, Origin "
        + "ORDER BY sum_ArrDelayMinutes DESC "
        + "LIMIT 5 OFFSET 3";
    JsonNode materializedViewResponse = postQuery(materializedViewQuery);
    assertNoExceptions(materializedViewResponse);

    String materializedViewQueried = getMaterializedViewQueried(materializedViewResponse);
    assertNotNull(materializedViewQueried,
        "Expected materialized view to be hit for LIMIT/OFFSET query. Response: " + materializedViewResponse);
    assertEquals(materializedViewQueried, MATERIALIZED_VIEW_SPLIT_TABLE_OFFLINE);

    JsonNode materializedViewRows = materializedViewResponse.get("resultTable").get("rows");
    assertNotNull(materializedViewRows);
    assertEquals(materializedViewRows.size(), 5, "LIMIT 5 should return exactly 5 rows");

    for (int i = 1; i < materializedViewRows.size(); i++) {
      double prev = materializedViewRows.get(i - 1).get(2).asDouble();
      double curr = materializedViewRows.get(i).get(2).asDouble();
      assertTrue(prev >= curr,
          "Results should be ordered DESC by sum_ArrDelayMinutes: row " + (i - 1)
              + " (" + prev + ") < row " + i + " (" + curr + ")");
    }
  }

  // -----------------------------------------------------------------------
  //  Phase 3, Test 9: Multiple MVs — cost-based selection
  // -----------------------------------------------------------------------

  /**
   * Verifies that when two MVs match the same user query, the one with the lower
   * rewrite cost is selected.
   *
   * <p>The full MV matches {@code GROUP BY Carrier} via {@code ExactSubsumptionStrategy}
   * (cost 0.0). The cost-competitor MV (GROUP BY {@code DaysSinceEpoch, Carrier},
   * full coverage, no split spec) matches the same query via
   * {@code AggregationSubsumptionStrategy} (cost 6.0). The full MV should win.
   */
  @Test
  public void testMultipleMaterializedViewCostSelection()
      throws Exception {
    String query = ""
        + "SELECT Carrier, SUM(ArrDelayMinutes) AS sum_ArrDelayMinutes "
        + "FROM " + SOURCE_TABLE_NAME + " GROUP BY Carrier";
    JsonNode response = postQuery(query);
    assertNoExceptions(response);

    // The full MV (EXACT, cost 0.0) should win over the cost competitor (AGG_REAGG, cost 6.0).
    // V2 only surfaces the chosen MV via materializedViewQueried; candidate set is not exposed.
    String materializedViewQueried = getMaterializedViewQueried(response);
    assertNotNull(materializedViewQueried, "Expected an MV to be hit. Response: " + response);
    assertEquals(materializedViewQueried, MATERIALIZED_VIEW_FULL_TABLE_OFFLINE,
        "Full MV (EXACT, cost 0.0) should be selected over cost competitor (AGG_REAGG, cost 6.0)");
  }

  // -----------------------------------------------------------------------
  //  MV table setup
  // -----------------------------------------------------------------------

  /**
   * Full-rewrite MV: no split spec, covers all data.
   * Definition: {@code SELECT Carrier, SUM(ArrDelayMinutes) AS sum_ArrDelayMinutes
   * FROM materializedViewSourceTable GROUP BY Carrier}
   */
  private void setupFullRewriteMv()
      throws Exception {
    Schema materializedViewSchema = new Schema.SchemaBuilder()
        .setSchemaName(MATERIALIZED_VIEW_FULL_TABLE_NAME)
        .addSingleValueDimension("Carrier", FieldSpec.DataType.STRING)
        .addMetric("sum_ArrDelayMinutes", FieldSpec.DataType.DOUBLE)
        .build();
    addSchema(materializedViewSchema);

    TableConfig materializedViewTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(MATERIALIZED_VIEW_FULL_TABLE_NAME)
        .setNumReplicas(1)
        .build();
    addTableConfig(materializedViewTableConfig);

    // Build synthetic pre-aggregated rows: one row per carrier
    List<GenericRow> rows = new ArrayList<>();
    for (String carrier : CARRIERS) {
      GenericRow row = new GenericRow();
      row.putValue("Carrier", carrier);
      row.putValue("sum_ArrDelayMinutes", 1000.0 + carrier.hashCode() % 500);
      rows.add(row);
    }
    buildAndUploadSegment(materializedViewTableConfig, materializedViewSchema, rows,
        MATERIALIZED_VIEW_FULL_TABLE_NAME, "materializedViewFullSeg");

    waitForAnyDocLoaded(MATERIALIZED_VIEW_FULL_TABLE_NAME, 60_000L);

    String definedSql = "SELECT Carrier, SUM(ArrDelayMinutes) AS sum_ArrDelayMinutes "
        + "FROM " + SOURCE_TABLE_NAME + " GROUP BY Carrier";
    MaterializedViewDefinitionMetadata definition = new MaterializedViewDefinitionMetadata(
        MATERIALIZED_VIEW_FULL_TABLE_OFFLINE,
        Collections.singletonList(SOURCE_TABLE_NAME),
        definedSql,
        Collections.emptyMap(),
        null);
    MaterializedViewDefinitionMetadataUtils.persist(_propertyStore, definition, -1);

    MaterializedViewRuntimeMetadata runtime = new MaterializedViewRuntimeMetadata(
        MATERIALIZED_VIEW_FULL_TABLE_OFFLINE, DATA_MAX_TIME_MS, new HashMap<>());
    MaterializedViewRuntimeMetadataUtils.persist(_propertyStore, runtime, -1);
  }

  /**
   * Split-rewrite MV: has split spec, covers historical data up to SPLIT_BOUNDARY_MS.
   * Definition: {@code SELECT DaysSinceEpoch, Carrier, Origin,
   * SUM(ArrDelayMinutes) AS sum_ArrDelayMinutes
   * FROM materializedViewSourceTable GROUP BY DaysSinceEpoch, Carrier, Origin}
   *
   * <p>Includes {@code Origin} to differentiate from the full MV, ensuring test 2's
   * query (with {@code GROUP BY Carrier, Origin}) hits this MV via
   * {@code AggregationSubsumptionStrategy} and not the full MV.
   */
  private void setupSplitRewriteMv()
      throws Exception {
    Schema materializedViewSchema = new Schema.SchemaBuilder()
        .setSchemaName(MATERIALIZED_VIEW_SPLIT_TABLE_NAME)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.INT, "1:DAYS:EPOCH", "1:DAYS")
        .addSingleValueDimension("Carrier", FieldSpec.DataType.STRING)
        .addSingleValueDimension("Origin", FieldSpec.DataType.STRING)
        .addMetric("sum_ArrDelayMinutes", FieldSpec.DataType.DOUBLE)
        .build();
    addSchema(materializedViewSchema);

    TableConfig materializedViewTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(MATERIALIZED_VIEW_SPLIT_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setNumReplicas(1)
        .build();
    addTableConfig(materializedViewTableConfig);

    // Build synthetic rows: one per (day, carrier, origin) combination
    String[] origins = {"SFO", "LAX", "JFK", "ORD", "ATL"};
    List<GenericRow> rows = new ArrayList<>();
    for (int day = 16071; day < 16086; day++) {
      for (String carrier : CARRIERS) {
        for (String origin : origins) {
          GenericRow row = new GenericRow();
          row.putValue(TIME_COLUMN, day);
          row.putValue("Carrier", carrier);
          row.putValue("Origin", origin);
          row.putValue("sum_ArrDelayMinutes",
              100.0 + (day + carrier.hashCode() + origin.hashCode()) % 200);
          rows.add(row);
        }
      }
    }
    buildAndUploadSegment(materializedViewTableConfig, materializedViewSchema, rows,
        MATERIALIZED_VIEW_SPLIT_TABLE_NAME, "materializedViewSplitSeg");

    waitForAnyDocLoaded(MATERIALIZED_VIEW_SPLIT_TABLE_NAME, 60_000L);

    String definedSql = "SELECT " + TIME_COLUMN + ", Carrier, Origin, "
        + "SUM(ArrDelayMinutes) AS sum_ArrDelayMinutes "
        + "FROM " + SOURCE_TABLE_NAME + " "
        + "GROUP BY " + TIME_COLUMN + ", Carrier, Origin";
    MaterializedViewSplitSpec splitSpec =
        new MaterializedViewSplitSpec(TIME_COLUMN, "1:DAYS:EPOCH", TIME_COLUMN, "1:DAYS:EPOCH", 86_400_000L);
    MaterializedViewDefinitionMetadata definition = new MaterializedViewDefinitionMetadata(
        MATERIALIZED_VIEW_SPLIT_TABLE_OFFLINE,
        Collections.singletonList(SOURCE_TABLE_NAME),
        definedSql,
        Collections.emptyMap(),
        splitSpec);
    MaterializedViewDefinitionMetadataUtils.persist(_propertyStore, definition, -1);

    MaterializedViewRuntimeMetadata runtime = new MaterializedViewRuntimeMetadata(
        MATERIALIZED_VIEW_SPLIT_TABLE_OFFLINE, SPLIT_BOUNDARY_MS, new HashMap<>());
    MaterializedViewRuntimeMetadataUtils.persist(_propertyStore, runtime, -1);
  }

  /// Cold-start MV: definition with split spec exists, but no APPEND has completed yet
  /// (watermarkMs is the initial cold-start placeholder and the partitions map is empty).
  /// The cold-start check in `resolvePlan` only triggers for incremental MVs (with
  /// a split spec), so a split spec is required here to exercise the cold-start skip path.
  private void setupColdStartMv()
      throws Exception {
    Schema materializedViewSchema = new Schema.SchemaBuilder()
        .setSchemaName(MATERIALIZED_VIEW_COLD_TABLE_NAME)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.INT, "1:DAYS:EPOCH", "1:DAYS")
        .addSingleValueDimension("Carrier", FieldSpec.DataType.STRING)
        .addSingleValueDimension("Origin", FieldSpec.DataType.STRING)
        .addMetric("sum_ArrDelayMinutes", FieldSpec.DataType.DOUBLE)
        .build();
    addSchema(materializedViewSchema);

    TableConfig materializedViewTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(MATERIALIZED_VIEW_COLD_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setNumReplicas(1)
        .build();
    addTableConfig(materializedViewTableConfig);
    /// No segments uploaded — empty MV table (pre-APPEND state)

    /// Same definition as the split MV; cold-start state is conveyed via a 0/initial watermark
    /// and empty partitions map on the runtime znode below.
    String definedSql = "SELECT " + TIME_COLUMN + ", Carrier, Origin, "
        + "SUM(ArrDelayMinutes) AS sum_ArrDelayMinutes "
        + "FROM " + SOURCE_TABLE_NAME + " "
        + "GROUP BY " + TIME_COLUMN + ", Carrier, Origin";
    MaterializedViewSplitSpec splitSpec =
        new MaterializedViewSplitSpec(TIME_COLUMN, "1:DAYS:EPOCH", TIME_COLUMN, "1:DAYS:EPOCH", 86_400_000L);
    MaterializedViewDefinitionMetadata definition = new MaterializedViewDefinitionMetadata(
        MATERIALIZED_VIEW_COLD_TABLE_OFFLINE,
        Collections.singletonList(SOURCE_TABLE_NAME),
        definedSql,
        Collections.emptyMap(),
        splitSpec);
    MaterializedViewDefinitionMetadataUtils.persist(_propertyStore, definition, -1);

    /// Cold-start placeholder: watermark seeded at the source-table minimum, partitions map empty,
    /// so the broker has no confirmed coverage to query against.
    MaterializedViewRuntimeMetadata runtime = new MaterializedViewRuntimeMetadata(

        MATERIALIZED_VIEW_COLD_TABLE_OFFLINE, DATA_MIN_TIME_MS, new HashMap<>());
    MaterializedViewRuntimeMetadataUtils.persist(_propertyStore, runtime, -1);
  }

  /**
   * Scan MV: no GROUP BY, no aggregation. Stores a projection of the source table
   * columns for scan-subsumption matching.
   * Definition: {@code SELECT Carrier, Origin, Dest, ArrDelayMinutes FROM materializedViewSourceTable}
   */
  private void setupScanMv()
      throws Exception {
    Schema materializedViewSchema = new Schema.SchemaBuilder()
        .setSchemaName(MATERIALIZED_VIEW_SCAN_TABLE_NAME)
        .addSingleValueDimension("Carrier", FieldSpec.DataType.STRING)
        .addSingleValueDimension("Origin", FieldSpec.DataType.STRING)
        .addSingleValueDimension("Dest", FieldSpec.DataType.STRING)
        .addMetric("ArrDelayMinutes", FieldSpec.DataType.DOUBLE)
        .build();
    addSchema(materializedViewSchema);

    TableConfig materializedViewTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(MATERIALIZED_VIEW_SCAN_TABLE_NAME)
        .setNumReplicas(1)
        .build();
    addTableConfig(materializedViewTableConfig);

    // Build synthetic scan rows with representative data
    String[] origins = {"SFO", "LAX", "JFK", "ORD", "ATL"};
    String[] dests = {"LAX", "SFO", "ORD", "JFK", "DFW"};
    List<GenericRow> rows = new ArrayList<>();
    for (String carrier : CARRIERS) {
      for (int i = 0; i < origins.length; i++) {
        GenericRow row = new GenericRow();
        row.putValue("Carrier", carrier);
        row.putValue("Origin", origins[i]);
        row.putValue("Dest", dests[i]);
        row.putValue("ArrDelayMinutes", 10.0 + (carrier.hashCode() + i) % 50);
        rows.add(row);
      }
    }
    buildAndUploadSegment(materializedViewTableConfig, materializedViewSchema, rows,
        MATERIALIZED_VIEW_SCAN_TABLE_NAME, "materializedViewScanSeg");

    waitForAnyDocLoaded(MATERIALIZED_VIEW_SCAN_TABLE_NAME, 60_000L);

    String definedSql = "SELECT Carrier, Origin, Dest, ArrDelayMinutes "
        + "FROM " + SOURCE_TABLE_NAME;
    MaterializedViewDefinitionMetadata definition = new MaterializedViewDefinitionMetadata(
        MATERIALIZED_VIEW_SCAN_TABLE_OFFLINE,
        Collections.singletonList(SOURCE_TABLE_NAME),
        definedSql,
        Collections.emptyMap(),
        null);
    MaterializedViewDefinitionMetadataUtils.persist(_propertyStore, definition, -1);

    MaterializedViewRuntimeMetadata runtime = new MaterializedViewRuntimeMetadata(
        MATERIALIZED_VIEW_SCAN_TABLE_OFFLINE, DATA_MAX_TIME_MS, new HashMap<>());
    MaterializedViewRuntimeMetadataUtils.persist(_propertyStore, runtime, -1);
  }

  /**
   * Cost-competitor MV: groups by {@code DaysSinceEpoch, Carrier} with full coverage
   * and no split spec. This MV matches {@code GROUP BY Carrier} queries via
   * {@code AggregationSubsumptionStrategy} (cost 6.0), competing with the full MV
   * which matches via {@code ExactSubsumptionStrategy} (cost 0.0).
   */
  private void setupCostCompetitorMv()
      throws Exception {
    Schema materializedViewSchema = new Schema.SchemaBuilder()
        .setSchemaName(MATERIALIZED_VIEW_COST_TABLE_NAME)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.INT, "1:DAYS:EPOCH", "1:DAYS")
        .addSingleValueDimension("Carrier", FieldSpec.DataType.STRING)
        .addMetric("sum_ArrDelayMinutes", FieldSpec.DataType.DOUBLE)
        .build();
    addSchema(materializedViewSchema);

    TableConfig materializedViewTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(MATERIALIZED_VIEW_COST_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setNumReplicas(1)
        .build();
    addTableConfig(materializedViewTableConfig);

    // Build synthetic rows: one per (day, carrier) combination
    List<GenericRow> rows = new ArrayList<>();
    for (int day = 16071; day <= 16101; day++) {
      for (String carrier : CARRIERS) {
        GenericRow row = new GenericRow();
        row.putValue(TIME_COLUMN, day);
        row.putValue("Carrier", carrier);
        row.putValue("sum_ArrDelayMinutes", 50.0 + (day + carrier.hashCode()) % 100);
        rows.add(row);
      }
    }
    buildAndUploadSegment(materializedViewTableConfig, materializedViewSchema, rows,
        MATERIALIZED_VIEW_COST_TABLE_NAME, "materializedViewCostSeg");

    waitForAnyDocLoaded(MATERIALIZED_VIEW_COST_TABLE_NAME, 60_000L);

    String definedSql = "SELECT " + TIME_COLUMN + ", Carrier, "
        + "SUM(ArrDelayMinutes) AS sum_ArrDelayMinutes "
        + "FROM " + SOURCE_TABLE_NAME + " "
        + "GROUP BY " + TIME_COLUMN + ", Carrier";
    MaterializedViewDefinitionMetadata definition = new MaterializedViewDefinitionMetadata(
        MATERIALIZED_VIEW_COST_TABLE_OFFLINE,
        Collections.singletonList(SOURCE_TABLE_NAME),
        definedSql,
        Collections.emptyMap(),
        null);
    MaterializedViewDefinitionMetadataUtils.persist(_propertyStore, definition, -1);

    MaterializedViewRuntimeMetadata runtime = new MaterializedViewRuntimeMetadata(
        MATERIALIZED_VIEW_COST_TABLE_OFFLINE, DATA_MAX_TIME_MS, new HashMap<>());
    MaterializedViewRuntimeMetadataUtils.persist(_propertyStore, runtime, -1);
  }

  // -----------------------------------------------------------------------
  //  Segment building helpers
  // -----------------------------------------------------------------------

  private void buildAndUploadSegment(TableConfig tableConfig, Schema schema,
      List<GenericRow> rows, String tableName, String segmentName)
      throws Exception {
    File segOutputDir = new File(_tempDir, segmentName + "_output");
    File tarDir = new File(_tempDir, segmentName + "_tar");
    TestUtils.ensureDirectoriesExistAndEmpty(segOutputDir, tarDir);

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setTableName(tableConfig.getTableName());
    config.setOutDir(segOutputDir.getAbsolutePath());
    config.setSegmentName(segmentName);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();

    File segmentDir = new File(segOutputDir, segmentName);
    File tarFile = new File(tarDir, segmentName + TarCompressionUtils.TAR_GZ_FILE_EXTENSION);
    TarCompressionUtils.createCompressedTarFile(segmentDir, tarFile);

    uploadSegments(tableName, tarDir);
  }

  // -----------------------------------------------------------------------
  //  Utility helpers
  // -----------------------------------------------------------------------

  @Nullable
  private static String getMaterializedViewQueried(JsonNode response) {
    return response.has("materializedViewQueried") && !response.get("materializedViewQueried").isNull()
        ? response.get("materializedViewQueried").asText() : null;
  }

  private static void assertNoExceptions(JsonNode response) {
    JsonNode exceptions = response.get("exceptions");
    assertTrue(exceptions == null || exceptions.isEmpty(),
        "Query returned exceptions: " + exceptions);
  }

  private void cleanupMaterializedViewMetadata(String materializedViewTableNameWithType) {
    try {
      MaterializedViewDefinitionMetadataUtils.delete(_propertyStore, materializedViewTableNameWithType);
    } catch (Exception e) {
      // Ignore cleanup failures
    }
    try {
      MaterializedViewRuntimeMetadataUtils.delete(_propertyStore, materializedViewTableNameWithType);
    } catch (Exception e) {
      // Ignore cleanup failures
    }
  }
}
