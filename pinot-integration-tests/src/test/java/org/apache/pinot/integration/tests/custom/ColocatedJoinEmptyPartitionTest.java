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
package org.apache.pinot.integration.tests.custom;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.integration.tests.ClusterIntegrationTestUtils;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// End-to-end coverage for a colocated join over a partitioned table whose declared partition count exceeds the
/// partitions that actually hold segments.
///
/// Two offline tables are partitioned identically (`Modulo` over 8 partitions) on the join key, but each populates only
/// 3 of the 8 partitions, and they populate *different* ones:
///
/// | table | populated partitions |
/// |---|---|
/// | left  | 0, 1, 2 |
/// | right | 1, 2, 3 |
///
/// The join keeps the union (classes 0..3) and drops the four classes neither side holds data in, so each side ends up
/// with one worker that has nothing to scan: the left table for class 3, the right table for class 0. What only an
/// end-to-end run can show is that a real server accepts and answers a leaf-stage request whose segment list is empty
/// for a genuinely partitioned table scan.
///
/// The same fixture covers the class reduction a filter buys. The tables configure the `partition` segment pruner, so
/// a restriction on the join key lets the broker eliminate segments before planning, and a class every member
/// eliminates leaves the group: `partitionKey IN (1, 2)` keeps only classes 1 and 2, halving the leaf worker count.
/// A restriction matching nothing anywhere falls back to the populated classes and returns an empty result.
///
/// The partition layout is supplied with explicit `tableOptions` hints rather than inferred, because hint inference is
/// off by default (`pinot.broker.multistage.infer.partition.hint`); the hints carry exactly what it would have
/// produced. The `is_colocated_by_join_keys` hint is spelled out for readability -- the exchange would be
/// pre-partitioned here without it, because the join key *is* the partition key.
///
/// What these tests do NOT prove: the cross-server fallback in `WorkerManager#assignPaddedWorker`, which only fires
/// when the server borrowed from the peer does not host the empty worker's table at all. Both tables are replicated on
/// both servers of the shared cluster, so such a worker always lands on its peer's server here.
@Test(suiteName = "CustomClusterIntegrationTest")
public class ColocatedJoinEmptyPartitionTest extends CustomDataQueryClusterIntegrationTest {
  private static final String LEFT_TABLE_NAME = "ColocatedJoinEmptyPartitionLeft";
  private static final String RIGHT_TABLE_NAME = "ColocatedJoinEmptyPartitionRight";

  private static final String PARTITION_KEY_COLUMN = "partitionKey";
  private static final String METRIC_COLUMN = "metricValue";
  private static final String PARTITION_FUNCTION = "Modulo";

  /// Deliberately larger than the number of partitions either table populates, which is what this test is about.
  private static final int NUM_DECLARED_PARTITIONS = 8;
  private static final List<Integer> LEFT_POPULATED_PARTITIONS = List.of(0, 1, 2);
  private static final List<Integer> RIGHT_POPULATED_PARTITIONS = List.of(1, 2, 3);
  /// The partition classes kept by a colocated join of the two tables, i.e. the union of the populated ones.
  private static final int NUM_KEPT_CLASSES_FOR_JOIN = 4;
  private static final int NUM_ROWS_PER_PARTITION = 2;

  /// The partition keys the pruning tests filter on. They land in partitions 1 and 2, the only ones both tables
  /// populate, so classes 0 (left only) and 3 (right only) are eliminated on every member of the colocated group.
  private static final List<Integer> FILTERED_KEYS = List.of(1, 2);
  /// A key in a partition neither table populates, so the filter matches nothing anywhere.
  private static final List<Integer> UNMATCHED_KEYS = List.of(5);

  private static final int LEFT_METRIC_MULTIPLIER = 10;
  private static final int RIGHT_METRIC_MULTIPLIER = 100;

  private static final String COLOCATED_JOIN_HINT = "/*+ joinOptions(is_colocated_by_join_keys='true') */";
  private static final String TABLE_HINT =
      String.format("/*+ tableOptions(partition_function='%s', partition_key='%s', partition_size='%d') */",
          PARTITION_FUNCTION, PARTITION_KEY_COLUMN, NUM_DECLARED_PARTITIONS);

  /// Matches one worker's pre-partitioned mailbox send line of an `EXPLAIN IMPLEMENTATION PLAN` tree, e.g.
  /// `[2]@localhost:1|[0] MAIL_SEND(HASH_DISTRIBUTED)[PARTITIONED]->{[1]@localhost:1|[0]}`. Group 1 is the sender
  /// worker id, group 2 the receiver list.
  private static final Pattern PRE_PARTITIONED_SEND_PATTERN =
      Pattern.compile("\\|\\[(\\d+)] MAIL_SEND\\([A-Z_]+\\)\\[PARTITIONED]->\\{([^}]*)}");

  @Override
  public String getTableName() {
    return LEFT_TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return createSchemaForTable(LEFT_TABLE_NAME);
  }

  @Override
  public List<File> createAvroFiles() {
    // Not used: setUpTable builds one Avro file per populated partition, for each of the two tables.
    return List.of();
  }

  @Override
  protected long getCountStarResult() {
    return (long) LEFT_POPULATED_PARTITIONS.size() * NUM_ROWS_PER_PARTITION;
  }

  @Override
  protected void setUpTable()
      throws Exception {
    setUpTable(LEFT_TABLE_NAME, LEFT_POPULATED_PARTITIONS, LEFT_METRIC_MULTIPLIER);
    setUpTable(RIGHT_TABLE_NAME, RIGHT_POPULATED_PARTITIONS, RIGHT_METRIC_MULTIPLIER);
    // Setting up the second table must not have removed the first one's tar files, see setUpTable(String, List, int).
    assertTarFilesRetained(LEFT_TABLE_NAME, LEFT_POPULATED_PARTITIONS.size());
    assertTarFilesRetained(RIGHT_TABLE_NAME, RIGHT_POPULATED_PARTITIONS.size());
  }

  /// Asserts that the segment tar files uploaded for the given table are still on disk. A metadata-only push, which
  /// `ClusterTest#uploadSegments` selects at random, makes the tar file the only deep store copy, so deleting one
  /// leaves its segment stuck in ERROR -- and only for a table whose segments no server had fetched yet, which is a
  /// race that fails rarely and far from its cause.
  private void assertTarFilesRetained(String tableName, int expectedNumSegments) {
    File[] tarFiles = new File(_tarDir, tableName).listFiles();
    assertNotNull(tarFiles, "Missing tar directory for table: " + tableName);
    assertEquals(tarFiles.length, expectedNumSegments,
        "Unexpected number of segment tar files for table: " + tableName);
  }

  @Override
  protected void waitForAllDocsLoaded(long timeoutMs) {
    long expectedNumDocs = getCountStarResult();
    for (String tableName : List.of(LEFT_TABLE_NAME, RIGHT_TABLE_NAME)) {
      TestUtils.waitForCondition(aVoid -> getCurrentCountStarResult(tableName) == expectedNumDocs, 100L, timeoutMs,
          "Failed to load " + expectedNumDocs + " documents into table: " + tableName);
    }
  }

  @Override
  @AfterClass
  public void tearDown()
      throws IOException {
    LOGGER.warn("Tearing down integration test class: {}", getClass().getSimpleName());
    dropOfflineTable(LEFT_TABLE_NAME);
    dropOfflineTable(RIGHT_TABLE_NAME);
    FileUtils.deleteDirectory(_tempDir);
    LOGGER.warn("Finished tearing down integration test class: {}", getClass().getSimpleName());
  }

  /// The case where a real server has to answer a leaf-stage request with an empty segment list: the two tables
  /// populate different subsets of the declared partitions, so each side ends up with a zero-segment worker.
  @Test
  public void testColocatedJoinWithEmptySegmentWorkers()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    String query = colocatedJoinQuery(LEFT_TABLE_NAME, RIGHT_TABLE_NAME);

    JsonNode response = queryBrokerHttpEndpoint(query);
    assertNoExceptions(response);

    // Rows only join on the keys of the partitions both tables populate, i.e. 1 and 2.
    List<List<Long>> expectedRows = new ArrayList<>();
    for (int partition : LEFT_POPULATED_PARTITIONS) {
      if (!RIGHT_POPULATED_PARTITIONS.contains(partition)) {
        continue;
      }
      for (int key : keysForPartition(partition)) {
        expectedRows.add(
            List.of((long) key, (long) key * LEFT_METRIC_MULTIPLIER, (long) key * RIGHT_METRIC_MULTIPLIER));
      }
    }
    assertRows(response, expectedRows);

    // Both leaves keep the union of the populated classes, so both have exactly one worker with nothing to scan.
    assertLeafStages(response, 2, NUM_KEPT_CLASSES_FOR_JOIN, LEFT_POPULATED_PARTITIONS.size());
    assertDirectExchanges(query, 2, NUM_KEPT_CLASSES_FOR_JOIN);
  }

  /// A self-join, where every kept class holds data on both sides: the plain worker-count reduction on its own, with
  /// the leaves running 3 workers for 8 declared partitions and nothing padded.
  @Test
  public void testColocatedSelfJoinWithoutEmptySegmentWorkers()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    String query = colocatedJoinQuery(LEFT_TABLE_NAME, LEFT_TABLE_NAME);

    JsonNode response = queryBrokerHttpEndpoint(query);
    assertNoExceptions(response);

    List<List<Long>> expectedRows = new ArrayList<>();
    for (int partition : LEFT_POPULATED_PARTITIONS) {
      for (int key : keysForPartition(partition)) {
        expectedRows.add(
            List.of((long) key, (long) key * LEFT_METRIC_MULTIPLIER, (long) key * LEFT_METRIC_MULTIPLIER));
      }
    }
    assertRows(response, expectedRows);

    int numKeptClasses = LEFT_POPULATED_PARTITIONS.size();
    assertLeafStages(response, 2, numKeptClasses, LEFT_POPULATED_PARTITIONS.size());
    assertDirectExchanges(query, 2, numKeptClasses);
  }

  /// Cross-checks the colocated result against the same join planned as a shuffle (no table hints), which rules out a
  /// colocated plan that pairs the wrong partition classes and drops or duplicates rows with no error. It also pins
  /// down that `fanOut` really tells the two plans apart, which the other tests rely on.
  @Test
  public void testColocatedJoinMatchesShuffledJoin()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    JsonNode colocatedResponse = queryBrokerHttpEndpoint(colocatedJoinQuery(LEFT_TABLE_NAME, RIGHT_TABLE_NAME));
    assertNoExceptions(colocatedResponse);
    JsonNode shuffledResponse = queryBrokerHttpEndpoint(shuffledJoinQuery(LEFT_TABLE_NAME, RIGHT_TABLE_NAME));
    assertNoExceptions(shuffledResponse);

    assertEquals(colocatedResponse.get("resultTable").get("rows"), shuffledResponse.get("resultTable").get("rows"),
        "Colocated and shuffled plans must return the same rows");
    assertShuffledLeafStages(shuffledResponse, 2);
  }

  /// The class reduction broker pruning buys: a filter that every member of the colocated group can prune with drops
  /// the classes all of them eliminate, so the leaves run fewer workers and scan fewer segments than the union of the
  /// populated classes. What the reduced width must not cost is the 1-to-1 wiring, which is what the two exchange
  /// assertions are for.
  @Test
  public void testColocatedJoinPrunesClassesEveryMemberFilters()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    String query = colocatedJoinQuery(LEFT_TABLE_NAME, RIGHT_TABLE_NAME, bothSidesFilter(FILTERED_KEYS));

    JsonNode response = queryBrokerHttpEndpoint(query);
    assertNoExceptions(response);
    assertRows(response, expectedFilteredRows(FILTERED_KEYS));

    // Classes 0 and 3 go: the left table's only class-0 segment is pruned by its own filter and the right table never
    // populated that class, and symmetrically for class 3. Both tables populate every surviving class with exactly one
    // segment, so both leaves scan one segment per kept class.
    List<Integer> keptClasses = keptClassesFor(FILTERED_KEYS);
    assertLeafStages(response, 2, keptClasses.size(), keptClasses.size());
    assertDirectExchanges(query, 2, keptClasses.size());

    // One segment per class that a table populated but the filter did not keep: the left table's class-0 segment and
    // the right table's class-3 one.
    long expectedNumPrunedSegments =
        Stream.concat(LEFT_POPULATED_PARTITIONS.stream(), RIGHT_POPULATED_PARTITIONS.stream())
            .filter(partition -> !keptClasses.contains(partition))
            .count();
    assertEquals(response.path("numSegmentsPrunedByBroker").asLong(-1), expectedNumPrunedSegments,
        "Unexpected number of broker-pruned segments in response: " + response);
  }

  /// The same differential check as [#testColocatedJoinMatchesShuffledJoin], at the reduced width. A plan that quietly
  /// fell back to a shuffle would return the right rows too, so this pairs with the `fanOut` and `[PARTITIONED]`
  /// assertions rather than replacing them; what it rules out is a reduction that pairs the wrong classes and drops or
  /// duplicates rows with no error. The shuffled plan reaches its answer by an independent route: with no table hints
  /// its leaves are assigned per server and pruned per segment, not per partition class.
  @Test
  public void testFilteredColocatedJoinMatchesFilteredShuffledJoin()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    String whereClause = bothSidesFilter(FILTERED_KEYS);
    JsonNode colocatedResponse = queryBrokerHttpEndpoint(
        colocatedJoinQuery(LEFT_TABLE_NAME, RIGHT_TABLE_NAME, whereClause));
    assertNoExceptions(colocatedResponse);
    // Pin the rows rather than only comparing the two plans: were the filter to stop matching anything, both sides
    // would return nothing and the comparison below would still pass while proving nothing at all.
    assertRows(colocatedResponse, expectedFilteredRows(FILTERED_KEYS));
    JsonNode shuffledResponse = queryBrokerHttpEndpoint(
        shuffledJoinQuery(LEFT_TABLE_NAME, RIGHT_TABLE_NAME, whereClause));
    assertNoExceptions(shuffledResponse);

    assertEquals(colocatedResponse.get("resultTable").get("rows"), shuffledResponse.get("resultTable").get("rows"),
        "Class-reduced colocated and shuffled plans must return the same rows");
    assertShuffledLeafStages(shuffledResponse, 2);
  }

  /// A filter every member prunes every segment with: the group is left with no surviving class at all. It must fall
  /// back to its populated classes and let the servers return the empty result, because a zero-worker leaf has no
  /// handling on a 1-to-1 exchange -- an empty answer, not an error.
  @Test
  public void testColocatedJoinWithFilterMatchingNothing()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    assertTrue(keptClassesFor(UNMATCHED_KEYS).isEmpty(),
        "The filter must match no partition either table populates, otherwise this is not the all-pruned fallback");
    String query = colocatedJoinQuery(LEFT_TABLE_NAME, RIGHT_TABLE_NAME, bothSidesFilter(UNMATCHED_KEYS));

    JsonNode response = queryBrokerHttpEndpoint(query);
    assertNoExceptions(response);
    assertRows(response, List.of());

    // Planned exactly as if there were no filter, and nothing is reported as pruned: the fallback dropped no class the
    // group would otherwise have kept.
    assertLeafStages(response, 2, NUM_KEPT_CLASSES_FOR_JOIN, LEFT_POPULATED_PARTITIONS.size());
    assertDirectExchanges(query, 2, NUM_KEPT_CLASSES_FOR_JOIN);
    assertEquals(response.path("numSegmentsPrunedByBroker").asLong(-1), 0L,
        "Unexpected number of broker-pruned segments in response: " + response);
  }

  private static String colocatedJoinQuery(String leftTableName, String rightTableName) {
    return colocatedJoinQuery(leftTableName, rightTableName, "");
  }

  private static String colocatedJoinQuery(String leftTableName, String rightTableName, String whereClause) {
    return String.format(
        "SELECT %s l.%s, l.%s, r.%s FROM %s %s AS l JOIN %s %s AS r ON l.%s = r.%s %s ORDER BY l.%s",
        COLOCATED_JOIN_HINT, PARTITION_KEY_COLUMN, METRIC_COLUMN, METRIC_COLUMN, leftTableName, TABLE_HINT,
        rightTableName, TABLE_HINT, PARTITION_KEY_COLUMN, PARTITION_KEY_COLUMN, whereClause, PARTITION_KEY_COLUMN);
  }

  private static String shuffledJoinQuery(String leftTableName, String rightTableName) {
    return shuffledJoinQuery(leftTableName, rightTableName, "");
  }

  private static String shuffledJoinQuery(String leftTableName, String rightTableName, String whereClause) {
    return String.format("SELECT l.%s, l.%s, r.%s FROM %s AS l JOIN %s AS r ON l.%s = r.%s %s ORDER BY l.%s",
        PARTITION_KEY_COLUMN, METRIC_COLUMN, METRIC_COLUMN, leftTableName, rightTableName, PARTITION_KEY_COLUMN,
        PARTITION_KEY_COLUMN, whereClause, PARTITION_KEY_COLUMN);
  }

  /// A partition-key restriction spelled out on both sides of the join rather than on one and left to Calcite's
  /// transitive inference, so that the leaf of each member carries it whatever the planner decides to push down.
  private static String bothSidesFilter(List<Integer> keys) {
    String keyList = keys.stream().map(String::valueOf).collect(Collectors.joining(", "));
    return String.format("WHERE l.%s IN (%s) AND r.%s IN (%s)", PARTITION_KEY_COLUMN, keyList, PARTITION_KEY_COLUMN,
        keyList);
  }

  /// The partition classes a colocated join of the two tables keeps under the given partition-key restriction. A class
  /// survives when at least one member still holds a segment its own filter leaves, i.e. when some restricted key
  /// hashes into a partition that member populates. One partition per class here, since the declared partition count
  /// is the hinted partition size.
  private static List<Integer> keptClassesFor(List<Integer> keys) {
    return keys.stream()
        .map(key -> key % NUM_DECLARED_PARTITIONS)
        .filter(partition -> LEFT_POPULATED_PARTITIONS.contains(partition)
            || RIGHT_POPULATED_PARTITIONS.contains(partition))
        .distinct()
        .sorted()
        .collect(Collectors.toList());
  }

  /// The rows the given partition-key restriction leaves in an inner join of the two tables: a key survives when both
  /// tables populate the partition it hashes into, and every populated partition holds every one of its keys.
  private static List<List<Long>> expectedFilteredRows(List<Integer> keys) {
    List<List<Long>> expectedRows = new ArrayList<>();
    for (int key : keys) {
      int partition = key % NUM_DECLARED_PARTITIONS;
      if (LEFT_POPULATED_PARTITIONS.contains(partition) && RIGHT_POPULATED_PARTITIONS.contains(partition)) {
        expectedRows.add(
            List.of((long) key, (long) key * LEFT_METRIC_MULTIPLIER, (long) key * RIGHT_METRIC_MULTIPLIER));
      }
    }
    return expectedRows;
  }

  private static void assertNoExceptions(JsonNode response) {
    JsonNode exceptions = response.get("exceptions");
    assertTrue(exceptions == null || exceptions.isEmpty(), "Query failed with exceptions: " + exceptions);
  }

  /// Compares the result table against the expected rows, sorted by their first column to match the queries' `ORDER
  /// BY`.
  private static void assertRows(JsonNode response, List<List<Long>> unsortedExpectedRows) {
    List<List<Long>> expectedRows = new ArrayList<>(unsortedExpectedRows);
    expectedRows.sort(Comparator.comparingLong(row -> row.get(0)));
    JsonNode resultTable = response.get("resultTable");
    assertNotNull(resultTable, "Missing result table in response: " + response);
    JsonNode rows = resultTable.get("rows");
    assertNotNull(rows, "Missing rows in response: " + response);
    assertEquals(rows.size(), expectedRows.size(), "Unexpected number of rows: " + rows);
    for (int i = 0; i < expectedRows.size(); i++) {
      List<Long> expectedRow = expectedRows.get(i);
      JsonNode row = rows.get(i);
      assertEquals(row.size(), expectedRow.size(), "Unexpected number of columns in row: " + row);
      for (int j = 0; j < expectedRow.size(); j++) {
        assertEquals(row.get(j).asLong(), (long) expectedRow.get(j),
            "Unexpected value at row " + i + " column " + j + " in rows: " + rows);
      }
    }
  }

  /// Asserts on every leaf stage of the executed plan, i.e. on every `MAILBOX_SEND` node of the `stageStats` tree whose
  /// only child is a `LEAF` node. `expectedNumWorkers` is the number of partition classes the colocated group kept,
  /// read from the send's summed `parallelism`; `expectedNumSegments` is lower than it exactly because some workers had
  /// nothing to scan.
  private static void assertLeafStages(JsonNode response, int expectedNumLeafStages, int expectedNumWorkers,
      int expectedNumSegments) {
    JsonNode stageStats = response.get("stageStats");
    assertNotNull(stageStats, "Missing stage stats in response: " + response);
    List<JsonNode> leafStageSends = new ArrayList<>();
    collectLeafStageSends(stageStats, leafStageSends);
    assertEquals(leafStageSends.size(), expectedNumLeafStages,
        "Unexpected number of leaf stages in stage stats: " + stageStats.toPrettyString());
    for (JsonNode leafStageSend : leafStageSends) {
      assertEquals(leafStageSend.path("parallelism").asInt(-1), expectedNumWorkers,
          "Unexpected leaf stage worker count, so the colocated group did not keep the expected partition classes. "
              + "Stage stats: " + stageStats.toPrettyString());
      // A pre-partitioned send is wired 1-to-1, so each sender writes exactly one receive mailbox. A shuffle would make
      // each sender write one mailbox per receiver worker.
      assertEquals(leafStageSend.path("fanOut").asInt(-1), 1,
          "Leaf stage send is not 1-to-1, so the plan fell back to a shuffle. Stage stats: "
              + stageStats.toPrettyString());
      JsonNode leaf = leafStageSend.get("children").get(0);
      assertEquals(leaf.path("numSegmentsQueried").asInt(-1), expectedNumSegments,
          "Unexpected number of segments queried by the leaf stage. Stage stats: " + stageStats.toPrettyString());
    }
  }

  /// Asserts that every leaf stage of a shuffled plan writes more than one receive mailbox. This is the control that
  /// gives the `fanOut` of 1 asserted for a colocated plan its meaning.
  private static void assertShuffledLeafStages(JsonNode response, int expectedNumLeafStages) {
    JsonNode stageStats = response.get("stageStats");
    assertNotNull(stageStats, "Missing stage stats in shuffled response: " + response);
    List<JsonNode> leafStageSends = new ArrayList<>();
    collectLeafStageSends(stageStats, leafStageSends);
    assertEquals(leafStageSends.size(), expectedNumLeafStages,
        "Unexpected number of leaf stages in stage stats: " + stageStats.toPrettyString());
    for (JsonNode leafStageSend : leafStageSends) {
      assertTrue(leafStageSend.path("fanOut").asInt(-1) > 1,
          "A shuffled leaf send must write more than one receive mailbox, otherwise the fanOut of 1 asserted for the "
              + "colocated plan proves nothing. Stage stats: " + stageStats.toPrettyString());
    }
  }

  private static void collectLeafStageSends(JsonNode node, List<JsonNode> leafStageSends) {
    JsonNode children = node.get("children");
    if ("MAILBOX_SEND".equals(node.path("type").asText()) && children != null && children.size() == 1 && "LEAF".equals(
        children.get(0).path("type").asText())) {
      leafStageSends.add(node);
      return;
    }
    if (children != null) {
      for (JsonNode child : children) {
        collectLeafStageSends(child, leafStageSends);
      }
    }
  }

  /// Asserts that the planner wired the leaf stages into direct (1-to-1) exchanges rather than shuffles, by reading the
  /// physical plan: `MailboxSendNode#explain` marks a pre-partitioned send with `[PARTITIONED]`, and the physical
  /// explain prints one such line per leaf worker together with the receiver mailboxes it targets.
  private void assertDirectExchanges(String query, int expectedNumLeafStages, int expectedNumWorkers)
      throws Exception {
    JsonNode response = queryBrokerHttpEndpoint("EXPLAIN IMPLEMENTATION PLAN FOR " + query);
    assertNoExceptions(response);
    JsonNode rows = response.get("resultTable").get("rows");
    assertNotNull(rows, "Missing rows in explain response: " + response);
    StringBuilder planBuilder = new StringBuilder();
    for (JsonNode row : rows) {
      for (JsonNode cell : row) {
        planBuilder.append(cell.asText()).append('\n');
      }
    }
    String plan = planBuilder.toString();
    assertFalse(plan.isEmpty(), "Empty implementation plan for query: " + query);

    int numPrePartitionedSends = 0;
    Matcher matcher = PRE_PARTITIONED_SEND_PATTERN.matcher(plan);
    while (matcher.find()) {
      numPrePartitionedSends++;
      String senderWorkerId = matcher.group(1);
      String receivers = matcher.group(2);
      // One receiver mailbox, and it is the receiver worker with the same id: that is the direct exchange. A shuffle
      // would list every receiver worker here.
      assertFalse(receivers.contains(","),
          "Pre-partitioned send targets more than one receiver mailbox, so the exchange is not 1-to-1. Plan:\n" + plan);
      assertTrue(receivers.endsWith("|[" + senderWorkerId + "]"),
          "Pre-partitioned send from worker " + senderWorkerId + " targets receiver " + receivers
              + " instead of the receiver worker with the same id. Plan:\n" + plan);
    }
    assertEquals(numPrePartitionedSends, expectedNumLeafStages * expectedNumWorkers,
        "Unexpected number of pre-partitioned mailbox sends (one per leaf worker is expected) in plan:\n" + plan);
  }

  private void setUpTable(String tableName, List<Integer> populatedPartitions, int metricMultiplier)
      throws Exception {
    Schema schema = createSchemaForTable(tableName);
    addSchema(schema);
    TableConfig tableConfig = createTableConfigForTable(tableName);
    addTableConfig(tableConfig);

    // Give each table its own directories, and never empty a directory another table already uploaded from. A
    // metadata-only push records a file:// download URI that points at the tar file, and the servers read it after the
    // upload call has returned, so deleting that file makes the segment unloadable. Separate directories also keep
    // uploadSegments, which pushes every tar it finds, from picking up the other table's segments.
    File segmentDir = new File(_segmentDir, tableName);
    File tarDir = new File(_tarDir, tableName);
    TestUtils.ensureDirectoriesExistAndEmpty(segmentDir, tarDir);
    int segmentIndex = 0;
    for (int partition : populatedPartitions) {
      // One segment per partition, so that every segment holds exactly one partition id (a segment spanning several has
      // no usable partition metadata) and every partition has a fully replicated server.
      File avroFile = createAvroFile(tableName, partition, metricMultiplier);
      ClusterIntegrationTestUtils.buildSegmentFromAvro(avroFile, tableConfig, schema, segmentIndex++, segmentDir,
          tarDir);
    }
    uploadSegments(tableName, tarDir);
  }

  private static Schema createSchemaForTable(String tableName) {
    return new Schema.SchemaBuilder().setSchemaName(tableName)
        .addSingleValueDimension(PARTITION_KEY_COLUMN, FieldSpec.DataType.INT)
        .addMetric(METRIC_COLUMN, FieldSpec.DataType.INT)
        .addDateTime(TIMESTAMP_FIELD_NAME, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
  }

  private static TableConfig createTableConfigForTable(String tableName) {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName)
        .setTimeColumnName(TIMESTAMP_FIELD_NAME)
        // Replicate every segment on both servers of the shared cluster, so that each partition has both of them as
        // fully replicated servers and a zero-segment worker deterministically lands on the server its peer picked.
        .setNumReplicas(2)
        .setSegmentPartitionConfig(new SegmentPartitionConfig(
            Map.of(PARTITION_KEY_COLUMN, new ColumnPartitionConfig(PARTITION_FUNCTION, NUM_DECLARED_PARTITIONS))))
        // Without this the broker builds no partition pruner at all (SegmentPrunerFactory only reads the routing
        // config), a filter on the partition key would prune nothing, and the filtered tests below would assert the
        // unfiltered worker count. The unfiltered tests are unaffected: with no filter there is nothing to prune.
        .setRoutingConfig(new RoutingConfig(null, List.of(RoutingConfig.PARTITION_SEGMENT_PRUNER_TYPE), null, null))
        .build();
  }

  private File createAvroFile(String tableName, int partition, int metricMultiplier)
      throws IOException {
    var avroSchema = SchemaBuilder.record("record")
        .fields()
        .name(PARTITION_KEY_COLUMN).type().intType().noDefault()
        .name(METRIC_COLUMN).type().intType().noDefault()
        .name(TIMESTAMP_FIELD_NAME).type().longType().noDefault()
        .endRecord();
    File avroFile = new File(_tempDir, tableName + "_partition_" + partition + ".avro");
    try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, avroFile);
      for (int key : keysForPartition(partition)) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(PARTITION_KEY_COLUMN, key);
        record.put(METRIC_COLUMN, key * metricMultiplier);
        record.put(TIMESTAMP_FIELD_NAME, 1_600_000_000_000L + key);
        fileWriter.append(record);
      }
    }
    return avroFile;
  }

  /// Returns the join keys that land in the given partition: `Modulo` maps a key to `key % numPartitions`.
  private static int[] keysForPartition(int partition) {
    int[] keys = new int[NUM_ROWS_PER_PARTITION];
    for (int i = 0; i < NUM_ROWS_PER_PARTITION; i++) {
      keys[i] = partition + i * NUM_DECLARED_PARTITIONS;
    }
    return keys;
  }
}
