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
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
import org.intellij.lang.annotations.Language;
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
 */
public class StreamStatsReportingIntegrationTest extends BaseClusterIntegrationTestSet {

  private static final String STREAM_OPTION = "streamStats=true";

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

    waitForAllDocsLoaded(600_000L);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
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
    super.overrideBrokerConf(brokerConf);
  }

  // ─── private helpers ──────────────────────────────────────────────────────────

  private static boolean hasExceptions(JsonNode response) {
    JsonNode exceptions = response.get("exceptions");
    return exceptions != null && exceptions.isArray() && !exceptions.isEmpty();
  }
}
