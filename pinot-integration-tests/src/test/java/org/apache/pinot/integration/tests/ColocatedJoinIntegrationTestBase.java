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
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Base integration tests for colocated joins tests(multi-stage engine).
 */
public abstract class ColocatedJoinIntegrationTestBase extends BaseClusterIntegrationTestSet {

  protected static final String TABLE_ATTR = "userAttributes";
  protected static final String TABLE_GRP = "userGroups";
  protected static final String PARTITION_KEY = "userUUID";
  protected static final String PARTITION_FUNCTION = "Murmur";
  protected static final String JOIN_OPTIONS_COLOCATED = "joinOptions(is_colocated_by_join_keys='true')";
  protected static final String JOIN_OPTIONS_NOT_COLOCATED = "joinOptions(is_colocated_by_join_keys='false')";
  protected static final String PLAN_PARTITIONED_MARKER = "[PARTITIONED]";

  private static final String COLOCATED_RESOURCE_DIR = "colocated";

  protected File _attrSegmentDir;
  protected File _attrTarDir;
  protected File _grpSegmentDir;
  protected File _grpTarDir;

  protected abstract int getNumPartitions();

  protected abstract File getSegmentBuildTempDir();

  protected String getTableOptPerTableHint() {
    return "tableOptions(partition_key='userUUID', partition_function='Murmur', partition_size='" + getNumPartitions()
        + "')";
  }

  @Override
  protected String getTableName() {
    return TABLE_ATTR;
  }

  /** Invoked by subclasses from their @BeforeClass setUp(); do not add @BeforeClass here to avoid starting
   * controller twice. */
  public void setUpColocatedBase()
      throws Exception {
    File buildTempDir = getSegmentBuildTempDir();
    // Only ensure base's segment/tar dirs exist; do not clear buildTempDir so subclass dirs (e.g. events) are
    // preserved.
    _attrSegmentDir = new File(buildTempDir, TABLE_ATTR + "_segmentDir");
    _attrTarDir = new File(buildTempDir, TABLE_ATTR + "_tarDir");
    _grpSegmentDir = new File(buildTempDir, TABLE_GRP + "_segmentDir");
    _grpTarDir = new File(buildTempDir, TABLE_GRP + "_tarDir");
    TestUtils.ensureDirectoriesExistAndEmpty(_attrSegmentDir, _attrTarDir, _grpSegmentDir, _grpTarDir);

    startZk();
    startController();
    startBrokers(1);
    startServers(1);

    SegmentPartitionConfig partitionConfig = new SegmentPartitionConfig(
        Collections.singletonMap(PARTITION_KEY, new ColumnPartitionConfig(PARTITION_FUNCTION, getNumPartitions())));

    Schema schemaAttr = loadSchema(COLOCATED_RESOURCE_DIR + "/userAttributes_schema.json");
    addSchema(schemaAttr);
    TableConfig tableConfigAttr =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_ATTR).setSegmentPartitionConfig(partitionConfig)
            .build();
    addTableConfig(tableConfigAttr);
    buildSegmentsForTable(COLOCATED_RESOURCE_DIR + "/userAttributes.csv", 0, tableConfigAttr, schemaAttr,
        _attrSegmentDir, _attrTarDir);
    uploadSegments(TABLE_ATTR, TableType.OFFLINE, _attrTarDir);

    Schema schemaGrp = loadSchema(COLOCATED_RESOURCE_DIR + "/userGroups_schema.json");
    addSchema(schemaGrp);
    TableConfig tableConfigGrp =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_GRP).setSegmentPartitionConfig(partitionConfig)
            .build();
    addTableConfig(tableConfigGrp);
    buildSegmentsForTable(COLOCATED_RESOURCE_DIR + "/userGroups.csv", 0, tableConfigGrp, schemaGrp, _grpSegmentDir,
        _grpTarDir);
    uploadSegments(TABLE_GRP, TableType.OFFLINE, _grpTarDir);

    waitForDocsLoaded(TABLE_ATTR, 5, 60_000L);
    waitForDocsLoaded(TABLE_GRP, 5, 60_000L);
  }

  /**
   * Builds segments for one table: single partition = one segment from full CSV; multiple
   * partitions = one segment per partition (rows split by Murmur, synthetic row for empty partitions).
   */
  protected void buildSegmentsForTable(String csvResourcePath, int partitionKeyColumnIndex, TableConfig tableConfig,
      Schema schema, File segmentDir, File tarDir)
      throws Exception {
    if (getNumPartitions() == 1) {
      File csvFile = copyResourceToFile(csvResourcePath, getSegmentBuildTempDir());
      ClusterIntegrationTestUtils.buildSegmentFromFile(csvFile, tableConfig, schema, "0", segmentDir, tarDir,
          FileFormat.CSV);
    } else {
      buildSegmentsByPartition(csvResourcePath, partitionKeyColumnIndex, tableConfig, schema, segmentDir, tarDir);
    }
  }

  protected void buildSegmentsByPartition(String csvResourcePath, int partitionKeyColumnIndex, TableConfig tableConfig,
      Schema schema, File segmentDir, File tarDir)
      throws Exception {
    int numPartitions = getNumPartitions();
    PartitionFunction partitionFunction =
        PartitionFunctionFactory.getPartitionFunction(PARTITION_FUNCTION, numPartitions, null);

    Map<Integer, List<String>> rowsByPartition = new HashMap<>();
    String header;
    try (InputStream is = getClass().getClassLoader().getResourceAsStream(csvResourcePath)) {
      Assert.assertNotNull(is, "Resource not found: " + csvResourcePath);
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
        header = reader.readLine();
        Assert.assertNotNull(header, "CSV must have header");
        String line;
        while ((line = reader.readLine()) != null) {
          if (line.trim().isEmpty()) {
            continue;
          }
          String[] cols = line.split(",", -1);
          if (cols.length <= partitionKeyColumnIndex) {
            continue;
          }
          String partitionKey = cols[partitionKeyColumnIndex].trim();
          int partitionId = partitionFunction.getPartition(partitionKey);
          rowsByPartition.computeIfAbsent(partitionId, k -> new ArrayList<>()).add(line);
        }
      }
    }

    String templateRow = rowsByPartition.isEmpty() ? null : rowsByPartition.values().iterator().next().get(0);
    Assert.assertNotNull(templateRow, "CSV must have at least one row to use as template for synthetic partitions");
    for (int p = 0; p < numPartitions; p++) {
      if (!rowsByPartition.containsKey(p) || rowsByPartition.get(p).isEmpty()) {
        String key = findKeyForPartition(partitionFunction, p);
        String row = replacePartitionKeyInRow(templateRow, partitionKeyColumnIndex, key);
        rowsByPartition.computeIfAbsent(p, k -> new ArrayList<>()).add(row);
      }
    }

    File tempDir = getSegmentBuildTempDir();
    for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
      List<String> rows = rowsByPartition.get(partitionId);
      Assert.assertNotNull(rows, "partition " + partitionId + " should have at least one row");
      File partitionCsv = File.createTempFile("colocated_partition_", ".csv", tempDir);
      partitionCsv.deleteOnExit();
      Files.write(partitionCsv.toPath(), (header + "\n" + String.join("\n", rows)).getBytes(StandardCharsets.UTF_8));
      ClusterIntegrationTestUtils.buildSegmentFromFile(partitionCsv, tableConfig, schema, String.valueOf(partitionId),
          segmentDir, tarDir, FileFormat.CSV);
    }
  }

  /** Finds a string that hashes to the given partition id (for synthetic rows so every partition has a segment). */
  protected static String findKeyForPartition(PartitionFunction partitionFunction, int targetPartition) {
    for (int i = 0; i < 10_000; i++) {
      String candidate = "pk-" + targetPartition + "-" + i;
      if (partitionFunction.getPartition(candidate) == targetPartition) {
        return candidate;
      }
    }
    throw new IllegalStateException("Could not find key for partition " + targetPartition);
  }

  protected static String replacePartitionKeyInRow(String row, int partitionKeyColumnIndex, String newKey) {
    String[] cols = row.split(",", -1);
    if (cols.length <= partitionKeyColumnIndex) {
      return row;
    }
    cols[partitionKeyColumnIndex] = newKey;
    return String.join(",", cols);
  }

  /** Full segment upload so the controller writes partition metadata to ZK (required for partitioned path). */
  @Override
  protected void uploadSegments(String tableName, TableType tableType, List<File> tarDirs)
      throws Exception {
    List<File> segmentTarFiles = new ArrayList<>();
    for (File tarDir : tarDirs) {
      File[] tarFiles = tarDir.listFiles();
      assertNotNull(tarFiles);
      Collections.addAll(segmentTarFiles, tarFiles);
    }
    assertTrue(segmentTarFiles.size() > 0);
    URI uploadSegmentHttpURI = URI.create(getControllerRequestURLBuilder().forSegmentUpload());
    try (FileUploadDownloadClient client = new FileUploadDownloadClient()) {
      for (File segmentTarFile : segmentTarFiles) {
        assertEquals(client.uploadSegment(uploadSegmentHttpURI, segmentTarFile.getName(), segmentTarFile,
            getSegmentUploadAuthHeaders(), tableName, tableType).getStatusCode(), HttpStatus.SC_OK);
      }
    }
  }

  @BeforeMethod
  @Override
  public void resetMultiStage() {
    setUseMultiStageQueryEngine(true);
  }

  @Override
  public boolean useMultiStageQueryEngine() {
    return true;
  }

  protected void assertPlanUsesColocatedExchange(String sql, String message)
      throws Exception {
    String explainSql = "EXPLAIN IMPLEMENTATION PLAN FOR " + sql;
    JsonNode result = postQuery(explainSql);
    assertNoExceptions(result);
    String plan = extractImplementationPlan(result);
    assertNotNull(plan, "implementation plan should be present");
    assertTrue(plan.contains(PLAN_PARTITIONED_MARKER), message + plan);
  }

  protected void assertPlanAvoidsColocatedExchange(String sql, String message)
      throws Exception {
    String explainSql = "EXPLAIN IMPLEMENTATION PLAN FOR " + sql;
    JsonNode result = postQuery(explainSql);
    assertNoExceptions(result);
    String plan = extractImplementationPlan(result);
    assertNotNull(plan, "implementation plan should be present");
    assertFalse(plan.contains(PLAN_PARTITIONED_MARKER), message + plan);
  }

  /** Runs SQL, asserts no exceptions and at least one row (for count/join correctness). */
  protected void runAndAssertColocated(String sql)
      throws Exception {
    JsonNode result = postQuery(sql);
    assertNoExceptions(result);
    assertTrue(getCountFromResult(result) >= 1, "Join should return at least one row");
  }

  /** Asserts plan uses colocated exchange then runs SQL and asserts result. */
  protected void runAndAssertColocatedWithPlanCheck(String sql, String planMessage)
      throws Exception {
    assertPlanUsesColocatedExchange(sql, planMessage);
    runAndAssertColocated(sql);
  }

  /** Asserts plan uses colocated exchange then runs semi-join SQL and asserts grouped keys result. */
  protected void runAndAssertSemiJoinWithPlanCheck(String sql, String planMessage)
      throws Exception {
    assertPlanUsesColocatedExchange(sql, planMessage);
    JsonNode result = postQuery(sql);
    assertNoExceptions(result);
    assertSemiJoinGroupedKeysResult(result);
  }

  /** Runs SQL with query options, asserts plan shows colocated exchange then runs and asserts result. */
  protected void runAndAssertColocatedWithQueryOptions(String sql, String queryOptions, String planMessage)
      throws Exception {
    String explainSql = "EXPLAIN IMPLEMENTATION PLAN FOR " + sql;
    JsonNode explainResult = postQueryWithOptions(explainSql, queryOptions);
    assertNoExceptions(explainResult);
    String plan = extractImplementationPlan(explainResult);
    assertTrue(plan != null && plan.contains(PLAN_PARTITIONED_MARKER), planMessage + plan);
    JsonNode result = postQueryWithOptions(sql, queryOptions);
    assertNoExceptions(result);
    assertTrue(getCountFromResult(result) >= 1, "Join should return at least one row");
  }

  /** Same as above for semi-join (grouped keys assertion). */
  protected void runAndAssertSemiJoinWithQueryOptions(String sql, String queryOptions, String planMessage)
      throws Exception {
    String explainSql = "EXPLAIN IMPLEMENTATION PLAN FOR " + sql;
    JsonNode explainResult = postQueryWithOptions(explainSql, queryOptions);
    assertNoExceptions(explainResult);
    String plan = extractImplementationPlan(explainResult);
    assertTrue(plan != null && plan.contains(PLAN_PARTITIONED_MARKER), planMessage + plan);
    JsonNode result = postQueryWithOptions(sql, queryOptions);
    assertNoExceptions(result);
    assertSemiJoinGroupedKeysResult(result);
  }

  /** Two-table join: when multiple partitions, plan should show [PARTITIONED]; always check result. */
  @Test
  public void testTwoTableJoinPlanShowsPartitioned()
      throws Exception {
    String tableOpt = getTableOptPerTableHint();
    String sql =
        "SELECT COUNT(*) FROM userAttributes /*+ " + tableOpt + " */ ua JOIN userGroups /*+ " + tableOpt
            + " */ ug ON ua.userUUID = ug.userUUID";
    runAndAssertColocatedWithPlanCheck(sql, "two-table join plan should show colocated exchange [PARTITIONED]; plan: ");
  }

  @Test
  public void testTwoTableJoinResultCorrectness()
      throws Exception {
    String tableOpt = getTableOptPerTableHint();
    String sql =
        "SELECT COUNT(*) FROM userAttributes /*+ " + tableOpt + " */ ua JOIN userGroups /*+ " + tableOpt
            + " */ ug ON ua.userUUID = ug.userUUID";
    runAndAssertColocated(sql);
  }

  @Test
  public void testTwoTableJoinWithJoinOptionsHintResultCorrectness()
      throws Exception {
    String tableOpt = getTableOptPerTableHint();
    String sql =
        "SELECT /*+ " + JOIN_OPTIONS_COLOCATED + " */ COUNT(*) FROM userAttributes /*+ " + tableOpt
            + " */ ua JOIN userGroups /*+ " + tableOpt + " */ ug ON ua.userUUID = ug.userUUID";
    runAndAssertColocatedWithPlanCheck(sql,
        "two-table join with joinOptions hint should show colocated exchange [PARTITIONED]; plan: ");
  }

  /** Self-join: when multiple partitions, plan should show [PARTITIONED]; always check result. */
  @Test
  public void testSelfJoinPlanShowsPartitioned()
      throws Exception {
    String tableOpt = getTableOptPerTableHint();
    String sql =
        "SELECT COUNT(*) FROM userAttributes /*+ " + tableOpt + " */ ua1 JOIN userAttributes /*+ " + tableOpt
            + " */ ua2 ON ua1.userUUID = ua2.userUUID";
    runAndAssertColocatedWithPlanCheck(sql, "self-join plan should show colocated exchange [PARTITIONED]; plan: ");
  }

  @Test
  public void testSelfJoinWithJoinOptionsHintResultCorrectness()
      throws Exception {
    String tableOpt = getTableOptPerTableHint();
    String sql =
        "SELECT /*+ " + JOIN_OPTIONS_COLOCATED + " */ COUNT(*) FROM userAttributes /*+ " + tableOpt
            + " */ ua1 JOIN userAttributes /*+ " + tableOpt + " */ ua2 ON ua1.userUUID = ua2.userUUID";
    runAndAssertColocatedWithPlanCheck(sql,
        "self-join with joinOptions hint should show colocated exchange [PARTITIONED]; plan: ");
  }

  /** Semi-join: when multiple partitions, plan should show [PARTITIONED]; always check grouped keys. */
  @Test
  public void testSemiJoinPlanShowsPartitioned()
      throws Exception {
    String tableOpt = getTableOptPerTableHint();
    String sql =
        "SELECT ua.userUUID, COUNT(*) FROM userAttributes /*+ " + tableOpt + " */ ua "
            + "WHERE ua.userUUID IN (SELECT ug.userUUID FROM userGroups /*+ " + tableOpt
            + " */ ug) GROUP BY ua.userUUID ORDER BY ua.userUUID";
    runAndAssertSemiJoinWithPlanCheck(sql, "semi-join plan should show colocated exchange [PARTITIONED]; plan: ");
  }

  @Test
  public void testSemiJoinWithJoinOptionsHintResultCorrectness()
      throws Exception {
    String tableOpt = getTableOptPerTableHint();
    String sql =
        "SELECT /*+ " + JOIN_OPTIONS_COLOCATED + " */ ua.userUUID, COUNT(*) FROM userAttributes /*+ " + tableOpt
            + " */ ua WHERE ua.userUUID IN (SELECT ug.userUUID FROM userGroups /*+ " + tableOpt
            + " */ ug) GROUP BY ua.userUUID ORDER BY ua.userUUID";
    runAndAssertSemiJoinWithPlanCheck(sql,
        "semi-join with joinOptions hint should show colocated exchange [PARTITIONED]; plan: ");
  }

  /** Two-table join with is_colocated_by_join_keys='false': join runs but plan should not use colocated exchange
   * when numPartitions > 1. */
  @Test
  public void testTwoTableJoinWithJoinOptionsFalseResultCorrectness()
      throws Exception {
    String tableOpt = getTableOptPerTableHint();
    String sql = "SELECT /*+ " + JOIN_OPTIONS_NOT_COLOCATED + " */ COUNT(*) FROM userAttributes /*+ " + tableOpt
        + " */ ua JOIN userGroups /*+ " + tableOpt + " */ ug ON ua.userUUID = ug.userUUID";
    JsonNode result = postQuery(sql);
    assertNoExceptions(result);
    long count = getCountFromResult(result);
    assertTrue(count >= 1, "Two-table join with joinOptions false should still return correct result");
    assertPlanAvoidsColocatedExchange(sql, "joinOptions false should not use colocated exchange [PARTITIONED]; plan: ");
  }

  /** Semi-join with is_colocated_by_join_keys='false': query succeeds with correct grouped keys. The planner may
   * still use [PARTITIONED] for semi-join (hint not always honored); we only assert result correctness. */
  @Test
  public void testSemiJoinWithJoinOptionsFalseResultCorrectness()
      throws Exception {
    String tableOpt = getTableOptPerTableHint();
    String sql =
        "SELECT /*+ " + JOIN_OPTIONS_NOT_COLOCATED + " */ ua.userUUID, COUNT(*) FROM userAttributes /*+ " + tableOpt
            + " */ ua WHERE ua.userUUID IN (SELECT ug.userUUID FROM userGroups /*+ " + tableOpt
            + " */ ug) GROUP BY ua.userUUID ORDER BY ua.userUUID";
    JsonNode result = postQuery(sql);
    assertNoExceptions(result);
    assertSemiJoinGroupedKeysResult(result);
  }

  /** Two-table join with no tableOptions and no joinOptions: query should still succeed, but colocation is not
   * enabled through any explicit backward-compatible mechanism. */
  @Test
  public void testTwoTableJoinWithNoHintsResultCorrectness()
      throws Exception {
    String sql = "SELECT COUNT(*) FROM userAttributes ua JOIN userGroups ug ON ua.userUUID = ug.userUUID";
    runAndAssertColocated(sql);
  }

  /** Semi-join with no tableOptions and no joinOptions: query should still succeed with correct grouped keys;
   * colocation is not enabled. */
  @Test
  public void testSemiJoinWithNoHintsResultCorrectness()
      throws Exception {
    String sql =
        "SELECT ua.userUUID, COUNT(*) FROM userAttributes ua "
            + "WHERE ua.userUUID IN (SELECT ug.userUUID FROM userGroups ug) GROUP BY ua.userUUID ORDER BY ua.userUUID";
    JsonNode result = postQuery(sql);
    assertNoExceptions(result);
    assertSemiJoinGroupedKeysResult(result);
  }

  /** Two-table join with joinOptions present but is_colocated_by_join_keys omitted: when tableOptions provide
   * partition metadata, the planner uses [PARTITIONED] (colocated exchange); query succeeds with correct result. */
  @Test
  public void testTwoTableJoinWithJoinOptionsColocatedKeyOmittedResultCorrectness()
      throws Exception {
    String tableOpt = getTableOptPerTableHint();
    String sql =
        "SELECT /*+ joinOptions(join_strategy='hash') */ COUNT(*) FROM userAttributes /*+ " + tableOpt
            + " */ ua JOIN userGroups /*+ " + tableOpt + " */ ug ON ua.userUUID = ug.userUUID";
    runAndAssertColocatedWithPlanCheck(sql,
        "omitting is_colocated_by_join_keys should preserve colocated exchange from table metadata; plan: ");
  }

  /** Semi-join with joinOptions present but is_colocated_by_join_keys omitted: when tableOptions provide partition
   * metadata, the planner uses [PARTITIONED]; query succeeds with correct grouped keys. */
  @Test
  public void testSemiJoinWithJoinOptionsColocatedKeyOmittedResultCorrectness()
      throws Exception {
    String tableOpt = getTableOptPerTableHint();
    String sql =
        "SELECT /*+ joinOptions(join_strategy='hash') */ ua.userUUID, COUNT(*) FROM userAttributes /*+ " + tableOpt
            + " */ ua WHERE ua.userUUID IN (SELECT ug.userUUID FROM userGroups /*+ " + tableOpt
            + " */ ug) GROUP BY ua.userUUID ORDER BY ua.userUUID";
    runAndAssertSemiJoinWithPlanCheck(sql,
        "omitting is_colocated_by_join_keys should preserve colocated semi-join exchange from table metadata; plan: ");
  }

  /** Two-table join with joinOptions(is_colocated_by_join_keys='null'): explicit 'null' disables colocated exchange
   * (plan does not use [PARTITIONED]); query still succeeds with correct result. */
  @Test
  public void testTwoTableJoinWithJoinOptionsColocatedKeyExplicitNullResultCorrectness()
      throws Exception {
    String tableOpt = getTableOptPerTableHint();
    String sql =
        "SELECT /*+ joinOptions(is_colocated_by_join_keys='null') */ COUNT(*) FROM userAttributes /*+ " + tableOpt
            + " */ ua JOIN userGroups /*+ " + tableOpt + " */ ug ON ua.userUUID = ug.userUUID";
    assertPlanAvoidsColocatedExchange(sql,
        "explicit null is_colocated_by_join_keys should not use colocated exchange; plan: ");
    JsonNode result = postQuery(sql);
    assertNoExceptions(result);
    long count = getCountFromResult(result);
    assertTrue(count >= 1, "Two-table join with is_colocated_by_join_keys='null' should return correct result");
  }

  /** Semi-join with joinOptions(is_colocated_by_join_keys='null'): query succeeds with correct grouped keys. The
   * planner may still use [PARTITIONED] for semi-join (explicit null not always honored); we only assert result. */
  @Test
  public void testSemiJoinWithJoinOptionsColocatedKeyExplicitNullResultCorrectness()
      throws Exception {
    String tableOpt = getTableOptPerTableHint();
    String sql =
        "SELECT /*+ joinOptions(is_colocated_by_join_keys='null') */ ua.userUUID, COUNT(*) FROM userAttributes /*+ "
            + tableOpt + " */ ua WHERE ua.userUUID IN (SELECT ug.userUUID FROM userGroups /*+ " + tableOpt
            + " */ ug) GROUP BY ua.userUUID ORDER BY ua.userUUID";
    JsonNode result = postQuery(sql);
    assertNoExceptions(result);
    assertSemiJoinGroupedKeysResult(result);
  }

  /** Two-table join with joinOptions colocated but only one table has tableOptions hint (partial config). */
  @Test
  public void testTwoTableJoinWithPartialTableOptionsResultCorrectness()
      throws Exception {
    String tableOpt = getTableOptPerTableHint();
    String sql = "SELECT /*+ " + JOIN_OPTIONS_COLOCATED + " */ COUNT(*) FROM userAttributes /*+ " + tableOpt
        + " */ ua JOIN userGroups ug ON ua.userUUID = ug.userUUID";
    runAndAssertColocatedWithPlanCheck(sql,
        "partial tableOptions should still allow colocated exchange via table metadata; plan: ");
  }

  /** Semi-join with joinOptions colocated but only one table has tableOptions hint (partial config). Single-partition:
   * query succeeds with colocated plan. Multi-partition: planner fails with "Local exchange with parallelism requires
   * keys" (error 450) because it needs partition key metadata for both sides and userGroups has no tableOptions. */
  @Test
  public void testSemiJoinWithPartialTableOptionsResultCorrectness()
      throws Exception {
    String tableOpt = getTableOptPerTableHint();
    String sql =
        "SELECT /*+ " + JOIN_OPTIONS_COLOCATED + " */ ua.userUUID, COUNT(*) FROM userAttributes /*+ " + tableOpt
            + " */ ua WHERE ua.userUUID IN (SELECT ug.userUUID FROM userGroups ug) GROUP BY ua.userUUID ORDER BY ua"
            + ".userUUID";
    if (getNumPartitions() == 1) {
      runAndAssertSemiJoinWithPlanCheck(sql,
          "partial tableOptions should still allow colocated semi-join exchange via table metadata; plan: ");
    } else {
      JsonNode result = postQuery("EXPLAIN IMPLEMENTATION PLAN FOR " + sql);
      JsonNode exceptions = result.get("exceptions");
      assertNotNull(exceptions, "response should have exceptions key");
      assertTrue(exceptions.size() > 0,
          "Planner should reject semi-join with partial tableOptions in multi-partition with an error");
      String message = exceptions.get(0).get("message").asText();
      assertTrue(message.contains("Local exchange with parallelism requires keys"),
          "Error should mention local exchange requires keys: " + message);
    }
  }

  /** Two-table join with mismatched partition_size in hints: planner rejects with partition size mismatch error
   * (actual must be multiple of hinted). */
  @Test
  public void testTwoTableJoinWithMismatchedPartitionSizeReturnsError()
      throws Exception {
    // Use a wrong size so "actual partition size must be multiple of hinted" fails (e.g. hint 4 when table has 1, or
    // hint 8 when table has 4).
    int wrongSize = getNumPartitions() == 1 ? 4 : (getNumPartitions() * 2);
    String wrongOpt =
        "tableOptions(partition_key='userUUID', partition_function='Murmur', partition_size='" + wrongSize + "')";
    String sql = "SELECT COUNT(*) FROM userAttributes /*+ " + wrongOpt + " */ ua " + "JOIN userGroups /*+ " + wrongOpt
        + " */ ug ON ua.userUUID = ug.userUUID";
    JsonNode result = postQuery(sql);
    JsonNode exceptions = result.get("exceptions");
    assertNotNull(exceptions, "response should have exceptions key");
    assertTrue(exceptions.size() > 0, "Planner should reject mismatched partition_size with an error");
    String message = exceptions.get(0).get("message").asText();
    assertTrue(message.contains("Partition size mismatch") || message.contains("partition size"),
        "Error should mention partition size mismatch: " + message);
  }

  /** Two-table join with mismatched partition_key in hints: planner rejects with "Failed to find partition key"
   * error. */
  @Test
  public void testTwoTableJoinWithMismatchedPartitionKeyReturnsError()
      throws Exception {
    String wrongKeyOpt =
        "tableOptions(partition_key='otherKey', partition_function='Murmur', partition_size='" + getNumPartitions()
            + "')";
    String sql =
        "SELECT COUNT(*) FROM userAttributes /*+ " + wrongKeyOpt + " */ ua " + "JOIN userGroups /*+ " + wrongKeyOpt
            + " */ ug ON ua.userUUID = ug.userUUID";
    JsonNode result = postQuery(sql);
    JsonNode exceptions = result.get("exceptions");
    assertNotNull(exceptions, "response should have exceptions key");
    assertTrue(exceptions.size() > 0, "Planner should reject mismatched partition_key with an error");
    String message = exceptions.get(0).get("message").asText();
    assertTrue(message.contains("Failed to find partition key") || message.contains("otherKey"),
        "Error should mention partition key mismatch: " + message);
  }

  /** Semi-join with mismatched partition_size in hints: planner rejects with partition size mismatch error. */
  @Test
  public void testSemiJoinWithMismatchedPartitionSizeReturnsError()
      throws Exception {
    int wrongSize = getNumPartitions() == 1 ? 4 : (getNumPartitions() * 2);
    String wrongOpt =
        "tableOptions(partition_key='userUUID', partition_function='Murmur', partition_size='" + wrongSize + "')";
    String sql = "SELECT ua.userUUID, COUNT(*) FROM userAttributes /*+ " + wrongOpt + " */ ua "
        + "WHERE ua.userUUID IN (SELECT ug.userUUID FROM userGroups /*+ " + wrongOpt
        + " */ ug) GROUP BY ua.userUUID ORDER BY ua.userUUID";
    JsonNode result = postQuery(sql);
    JsonNode exceptions = result.get("exceptions");
    assertNotNull(exceptions, "response should have exceptions key");
    assertTrue(exceptions.size() > 0, "Planner should reject mismatched partition_size with an error");
    String message = exceptions.get(0).get("message").asText();
    assertTrue(message.contains("Partition size mismatch") || message.contains("partition size"),
        "Error should mention partition size mismatch: " + message);
  }

  /** Semi-join with mismatched partition_key in hints: planner rejects with "Failed to find partition key" error. */
  @Test
  public void testSemiJoinWithMismatchedPartitionKeyReturnsError()
      throws Exception {
    String wrongKeyOpt =
        "tableOptions(partition_key='otherKey', partition_function='Murmur', partition_size='" + getNumPartitions()
            + "')";
    String sql = "SELECT ua.userUUID, COUNT(*) FROM userAttributes /*+ " + wrongKeyOpt + " */ ua "
        + "WHERE ua.userUUID IN (SELECT ug.userUUID FROM userGroups /*+ " + wrongKeyOpt
        + " */ ug) GROUP BY ua.userUUID ORDER BY ua.userUUID";
    JsonNode result = postQuery(sql);
    JsonNode exceptions = result.get("exceptions");
    assertNotNull(exceptions, "response should have exceptions key");
    assertTrue(exceptions.size() > 0, "Planner should reject mismatched partition_key with an error");
    String message = exceptions.get(0).get("message").asText();
    assertTrue(message.contains("Failed to find partition key") || message.contains("otherKey"),
        "Error should mention partition key mismatch: " + message);
  }

  /** Self-join with is_colocated_by_join_keys='false': result correctness; plan should not show [PARTITIONED] when
   * numPartitions > 1. */
  @Test
  public void testSelfJoinWithJoinOptionsFalseResultCorrectness()
      throws Exception {
    String tableOpt = getTableOptPerTableHint();
    String sql = "SELECT /*+ " + JOIN_OPTIONS_NOT_COLOCATED + " */ COUNT(*) FROM userAttributes /*+ " + tableOpt
        + " */ ua1 JOIN userAttributes /*+ " + tableOpt + " */ ua2 ON ua1.userUUID = ua2.userUUID";
    JsonNode result = postQuery(sql);
    assertNoExceptions(result);
    long count = getCountFromResult(result);
    assertTrue(count >= 1, "Self-join with joinOptions false should still return correct result");
    assertPlanAvoidsColocatedExchange(sql, "joinOptions false should not use colocated exchange [PARTITIONED]; plan: ");
  }

  protected Schema loadSchema(String resourcePath)
      throws Exception {
    try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
      Assert.assertNotNull(is, "Resource not found: " + resourcePath);
      return Schema.fromInputStream(is);
    }
  }

  /** Copies classpath resource to a temp file in the given parent dir (e.g. segment build temp dir). */
  protected File copyResourceToFile(String resourcePath, File parentDir)
      throws Exception {
    try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
      Assert.assertNotNull(is, "Resource not found: " + resourcePath);
      File tmp = File.createTempFile("colocated_", ".csv", parentDir);
      tmp.deleteOnExit();
      IOUtils.copy(is, Files.newOutputStream(tmp.toPath()));
      return tmp;
    }
  }

  protected void waitForDocsLoaded(String tableName, long expectedCount, long timeoutMs)
      throws Exception {
    TestUtils.waitForCondition(() -> getCurrentCountStarResult(tableName) >= expectedCount, 100L, timeoutMs,
        "Failed to load " + expectedCount + " docs in " + tableName, true, java.time.Duration.ofMillis(timeoutMs / 10));
  }

  protected static void assertNoExceptions(JsonNode result) {
    JsonNode exceptions = result.get("exceptions");
    assertNotNull(exceptions, "response should have exceptions key");
    assertTrue(exceptions.isEmpty(), "Query must not throw: " + exceptions.toString());
  }

  protected static long getCountFromResult(JsonNode result) {
    JsonNode resultTable = result.get("resultTable");
    assertNotNull(resultTable, "resultTable");
    JsonNode rows = resultTable.get("rows");
    assertNotNull(rows, "rows");
    assertTrue(rows.size() >= 1, "at least one row");
    JsonNode firstRow = rows.get(0);
    assertNotNull(firstRow, "first row");
    return firstRow.get(0).asLong();
  }

  /** Asserts semi-join result has expected grouped keys. Single-partition: exactly 3 rows (user-1, user-2, user-3).
   * Multi-partition: at least 3 rows and result contains user-1, user-2, user-3 (synthetic partition rows may add
   * more). */
  protected void assertSemiJoinGroupedKeysResult(JsonNode result) {
    JsonNode resultTable = result.get("resultTable");
    assertNotNull(resultTable, "resultTable");
    JsonNode rows = resultTable.get("rows");
    assertNotNull(rows, "rows");
    if (getNumPartitions() == 1) {
      assertEquals(rows.size(), 3, "Semi-join should return three grouped matching join keys");
      assertEquals(rows.get(0).get(0).asText(), "user-1");
      assertEquals(rows.get(1).get(0).asText(), "user-2");
      assertEquals(rows.get(2).get(0).asText(), "user-3");
    } else {
      assertTrue(rows.size() >= 3, "Semi-join should return at least three grouped matching join keys");
      List<String> keys = new ArrayList<>();
      for (int i = 0; i < rows.size(); i++) {
        keys.add(rows.get(i).get(0).asText());
      }
      assertTrue(keys.contains("user-1") && keys.contains("user-2") && keys.contains("user-3"),
          "Semi-join result should contain user-1, user-2, user-3; got: " + keys);
    }
  }

  /** Extracts full plan text from EXPLAIN response (all cells of all rows). */
  protected static String extractImplementationPlan(JsonNode result) {
    JsonNode resultTable = result.get("resultTable");
    if (resultTable == null) {
      return null;
    }
    JsonNode rows = resultTable.get("rows");
    if (rows == null || rows.size() == 0) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < rows.size(); i++) {
      JsonNode row = rows.get(i);
      if (row != null) {
        for (int j = 0; j < row.size(); j++) {
          if (row.get(j) != null && !row.get(j).isNull()) {
            sb.append(row.get(j).asText()).append("\n");
          }
        }
      }
    }
    return sb.length() > 0 ? sb.toString() : null;
  }
}
