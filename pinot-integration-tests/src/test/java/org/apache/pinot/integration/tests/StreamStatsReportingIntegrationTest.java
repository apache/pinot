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
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
import org.intellij.lang.annotations.Language;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Integration tests for the {@code SubmitWithStream} stats-reporting path. Verifies that:
 * <ol>
 *   <li>Queries run to completion with correct results when stream stats are enabled per-query via the
 *   {@code streamStats} query option.</li>
 *   <li>The broker response contains a non-null {@code streamStatsCoverage} array indexed by stage id with
 *   {@code responded} &gt; 0 and {@code missing} = {@code mergeFailed} = 0 on the success path.</li>
 *   <li>An N-ary set operation (three-way UNION) produces a correct tree-shaped stats payload — regression coverage
 *   for the known loss in the legacy flat-binary / inorder format when a set op has more than two inputs.</li>
 *   <li>The cluster-level config ({@code pinot.broker.mse.stream.stats}) activates stream mode
 *   for all queries without a per-query option.</li>
 * </ol>
 *
 * <p><b>Why this class spins up its own cluster instead of using the shared suite cluster:</b>
 * {@link #testClusterLevelConfigActivatesStreamMode} requires the broker to start with
 * {@link org.apache.pinot.spi.utils.CommonConstants.Broker#CONFIG_OF_STREAM_STATS} set to {@code true} (a
 * non-default value). That configuration is applied at broker startup via {@link #overrideBrokerConf} and cannot
 * be changed without restarting the broker. Using a shared cluster would therefore affect every other test class
 * that shares it.
 */
public class StreamStatsReportingIntegrationTest extends BaseClusterIntegrationTestSet {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamStatsReportingIntegrationTest.class);
  private static final String STREAM_OPTION = "streamStats=true";

  // Dimension table used by testSemiJoinPipelineBreaker to force a dynamic-broadcast (pipeline-breaker) semi-join.
  private static final String DIM_TABLE = "daysOfWeek";
  private static final String DIM_TABLE_DATA_PATH = "dimDayOfWeek_data.csv";
  private static final String DIM_TABLE_SCHEMA_PATH = "dimDayOfWeek_schema.json";
  private static final String DIM_TABLE_TABLE_CONFIG_PATH = "dimDayOfWeek_config.json";
  private static final int DIM_NUMBER_OF_RECORDS = 7;

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    startZk();
    startController();
    startBroker();
    startServer();

    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    List<File> avroFiles = unpackAvroData(_tempDir);
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), _tarDir);

    // Wait for the main table to finish loading BEFORE uploading the dimension table — otherwise the dimension
    // table's own (shorter) load wait competes with the main table's bulk load and can time out.
    waitForAllDocsLoaded(600_000L);

    setupDimensionTable();
  }

  /**
   * Sets up the {@value #DIM_TABLE} dimension table, broadcast as the build side of the semi-join in
   * {@link #testSemiJoinPipelineBreaker}, which makes the planner emit a {@code PIPELINE_BREAKER} exchange.
   */
  private void setupDimensionTable()
      throws Exception {
    Schema dimSchema = createSchema(DIM_TABLE_SCHEMA_PATH);
    addSchema(dimSchema);
    TableConfig dimTableConfig = createTableConfig(DIM_TABLE_TABLE_CONFIG_PATH);
    dimTableConfig.setTenantConfig(new TenantConfig(getBrokerTenant(), getServerTenant(), null));
    addTableConfig(dimTableConfig);
    createAndUploadSegmentFromClasspath(dimTableConfig, dimSchema, DIM_TABLE_DATA_PATH, FileFormat.CSV,
        DIM_NUMBER_OF_RECORDS, 60_000);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    // Drop the dimension table best-effort: if setUp failed before creating it, this must not abort cluster
    // shutdown (otherwise the embedded ZK/controller leaks and the next run hits "table already exists").
    try {
      dropOfflineTable(DIM_TABLE);
    } catch (Exception e) {
      LOGGER.warn("Failed to drop dimension table {} during teardown (continuing shutdown)", DIM_TABLE, e);
    }
    stopServer();
    stopBroker();
    stopController();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  @Override
  public boolean useMultiStageQueryEngine() {
    return true;
  }

  // ─── helpers ──────────────────────────────────────────────────────────────────

  /**
   * Posts a multi-stage query with the {@code streamStats=true} option.
   */
  private JsonNode postWithStreamStats(@Language("sql") String sql)
      throws Exception {
    return postQueryWithOptions(sql, STREAM_OPTION);
  }

  /**
   * Asserts that the {@code streamStatsCoverage} array in the response is well-formed: every non-null entry must have
   * responded &gt; 0, mergeFailed = 0, and missing = 0.
   */
  private static void assertFullCoverage(JsonNode response) {
    JsonNode coverage = response.get("streamStatsCoverage");
    assertNotNull(coverage, "streamStatsCoverage should be present in stream-mode response");
    assertTrue(coverage.isArray(), "streamStatsCoverage must be an array");
    boolean anyNonNull = false;
    for (JsonNode entry : coverage) {
      if (entry == null || entry.isNull()) {
        continue;
      }
      anyNonNull = true;
      int responded = entry.get("responded").asInt();
      int mergeFailed = entry.get("mergeFailed").asInt();
      int missing = entry.get("missing").asInt();
      assertTrue(responded > 0, "responded must be > 0 for a tracked stage, got: " + responded);
      assertEquals(mergeFailed, 0, "mergeFailed must be 0 on success path");
      assertEquals(missing, 0, "missing must be 0 on success path");
    }
    assertTrue(anyNonNull, "at least one non-null entry expected in streamStatsCoverage");
  }

  // ─── tests ────────────────────────────────────────────────────────────────────

  /**
   * Simple aggregation — exercises the one-stage path and verifies results + coverage.
   */
  @Test
  public void testSimpleAggregation()
      throws Exception {
    @Language("sql")
    String sql = "SELECT COUNT(*) FROM mytable LIMIT 1";
    JsonNode response = postWithStreamStats(sql);

    assertFalse(hasExceptions(response), "Query should succeed without exceptions");
    JsonNode resultTable = response.get("resultTable");
    assertNotNull(resultTable);
    assertTrue(resultTable.get("rows").get(0).get(0).asLong() > 0, "count(*) should be > 0");

    assertFullCoverage(response);
  }

  /**
   * Join query — creates at least two non-root stages and verifies that stats arrive for all of them.
   */
  @Test
  public void testJoinQuery()
      throws Exception {
    @Language("sql")
    String sql = "SELECT a.AirlineID, COUNT(*) "
        + "FROM mytable a JOIN mytable b ON a.AirlineID = b.AirlineID "
        + "WHERE a.DaysSinceEpoch = 16101 AND b.DaysSinceEpoch = 16101 "
        + "GROUP BY a.AirlineID "
        + "LIMIT 5";
    JsonNode response = postWithStreamStats(sql);

    assertFalse(hasExceptions(response), "Join query should succeed without exceptions");
    assertNotNull(response.get("resultTable"));

    assertFullCoverage(response);
    // A join plan has at least 3 stages: root (0), join (1), leaf (2). Coverage array must be at least length 3.
    JsonNode coverage = response.get("streamStatsCoverage");
    assertTrue(coverage.size() >= 3,
        "Join should produce at least 3 stages, got coverage of size: " + coverage.size());
  }

  /**
   * Three-way UNION — exercises the N-ary set-op path that the legacy flat-binary format reconstructed incorrectly.
   * Verifies that results are correct and that coverage is full (no missing stats from any branch).
   */
  @Test
  public void testThreeWayUnion()
      throws Exception {
    @Language("sql")
    String sql =
        "SELECT AirlineID FROM mytable WHERE DaysSinceEpoch = 16101 "
            + "UNION "
            + "SELECT AirlineID FROM mytable WHERE DaysSinceEpoch = 16102 "
            + "UNION "
            + "SELECT AirlineID FROM mytable WHERE DaysSinceEpoch = 16103";
    JsonNode response = postWithStreamStats(sql);

    assertFalse(hasExceptions(response), "Three-way UNION should succeed without exceptions");
    assertNotNull(response.get("resultTable"), "Result table must be present");

    assertFullCoverage(response);
    // A three-way UNION plan has leaf stages for each branch plus the union stage and root — at least 4 stages.
    JsonNode coverage = response.get("streamStatsCoverage");
    assertTrue(coverage.size() >= 4,
        "Three-way UNION should produce at least 4 stages, coverage size: " + coverage.size());
  }

  /**
   * Dynamic-broadcast semi-join — exercises the {@code PIPELINE_BREAKER} path, which is the case that double-counted
   * opchain completions before the fix. A pipeline-breaker opchain is built from the same
   * {@link org.apache.pinot.query.runtime.plan.OpChainExecutionContext} as its stage's main (leaf) opchain, so it
   * carries an identical {@code OpChainId} and fires the server-side completion listener too. If it were reported as
   * a separate stage it would satisfy the expected-opchain count one early, prematurely emit {@code ServerDone}, and
   * the real leaf opchain's stats would be dropped.
   *
   * <p>This guards both observable variants of that bug:
   * <ul>
   *   <li>{@link #assertFullCoverage} catches the missing / shape-mismatch variant (responded/missing/mergeFailed
   *   would not reconcile);</li>
   *   <li>the per-stage tree assertion catches the silent-replacement variant — where the pipeline-breaker's own
   *   tree is recorded as the stage with {@code missing=0} — by requiring the leaf stage to still carry the folded
   *   pipeline-breaker subtree ({@code LEAF → MAILBOX_RECEIVE → MAILBOX_SEND → LEAF}), exactly as the legacy
   *   mailbox path reports it.</li>
   * </ul>
   */
  @Test
  public void testSemiJoinPipelineBreaker()
      throws Exception {
    // Broadcasting the small dimension table as the build side of the IN semi-join makes the planner emit a
    // PIPELINE_BREAKER exchange on the mytable leaf stage (same query shape the legacy testStageStatsPipelineBreaker
    // relies on).
    @Language("sql")
    String sql = "SELECT * FROM mytable WHERE DayOfWeek IN (SELECT dayId FROM daysOfWeek)";
    JsonNode response = postWithStreamStats(sql);

    assertFalse(hasExceptions(response), "Semi-join should succeed without exceptions");
    assertNotNull(response.get("resultTable"), "Result table must be present");

    // Counters must reconcile. Catches the missing-stats / shape-mismatch variants of premature ServerDone.
    assertFullCoverage(response);

    // The leaf stage's tree must still carry the folded pipeline-breaker subtree: a LEAF node whose children
    // include a MAILBOX_RECEIVE (the dynamic-broadcast build side that ran as a pipeline breaker). This is the
    // shape the legacy mailbox path reports. With the bug, the pipeline-breaker opchain double-counted against
    // the expected total, firing ServerDone before the real leaf opchain reported — so the leaf stage's tree was
    // either dropped (missing) or replaced by the pipeline-breaker's own MAILBOX_RECEIVE-rooted tree, and this
    // pattern would be absent. assertFullCoverage above cannot catch the silent-replacement case (the broker sees
    // the pipeline-breaker's event as the stage's single response, so missing=0), which is why we assert the tree.
    JsonNode stageStats = response.get("stageStats");
    assertNotNull(stageStats, "stageStats tree must be present");
    assertTrue(hasLeafWithPipelineBreaker(stageStats),
        "leaf stage must carry the folded pipeline-breaker subtree (LEAF with a nested MAILBOX_RECEIVE child); "
            + "its absence means the real leaf opchain's stats were dropped or replaced by the pipeline breaker's. "
            + "Actual tree: " + stageStats.toPrettyString());
  }

  /**
   * Recursively searches the stats tree for a {@code LEAF} node that has at least one {@code MAILBOX_RECEIVE} child —
   * the signature of a dynamic-broadcast build side that executed as a pipeline breaker and had its stats folded into
   * the leaf opchain's tree.
   */
  private static boolean hasLeafWithPipelineBreaker(JsonNode node) {
    if (node == null || node.isNull()) {
      return false;
    }
    JsonNode children = node.get("children");
    if ("LEAF".equals(node.path("type").asText())) {
      if (children != null) {
        for (JsonNode child : children) {
          if ("MAILBOX_RECEIVE".equals(child.path("type").asText())) {
            return true;
          }
        }
      }
    }
    if (children != null) {
      for (JsonNode child : children) {
        if (hasLeafWithPipelineBreaker(child)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Verifies that the cluster-level config {@code pinot.broker.mse.stream.stats=true}
   * activates stream mode for queries that do not carry the per-query option. The broker is restarted with the config
   * set for this test class via {@link #overrideBrokerConf}.
   */
  @Test
  public void testClusterLevelConfigActivatesStreamMode()
      throws Exception {
    // Post WITHOUT the per-query option — stream mode should still be active because the broker was started
    // with CONFIG_OF_STREAM_STATS=true in overrideBrokerConf.
    @Language("sql")
    String sql = "SELECT COUNT(*) FROM mytable LIMIT 1";
    JsonNode response = postQuery(sql);

    assertFalse(hasExceptions(response), "Cluster-default stream mode query should succeed");
    assertNotNull(response.get("resultTable"));
    assertFullCoverage(response);
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    // Enable stream-mode cluster-wide so testClusterLevelConfigActivatesStreamMode can run without a per-query option.
    brokerConf.setProperty(CommonConstants.Broker.CONFIG_OF_STREAM_STATS, true);
    // Generous stats-drain window so these coverage assertions don't race the default 50ms cutoff under CI load: the
    // broker still returns as soon as all opchains report (early completion), so this only raises the upper bound on
    // how long it will wait for a slow opchain's stats, making full coverage deterministic.
    brokerConf.setProperty(CommonConstants.Broker.CONFIG_OF_STREAM_STATS_DRAIN_MS, 30_000L);
    super.overrideBrokerConf(brokerConf);
  }

  // ─── private helpers ──────────────────────────────────────────────────────────

  private static boolean hasExceptions(JsonNode response) {
    JsonNode exceptions = response.get("exceptions");
    return exceptions != null && exceptions.isArray() && !exceptions.isEmpty();
  }
}
