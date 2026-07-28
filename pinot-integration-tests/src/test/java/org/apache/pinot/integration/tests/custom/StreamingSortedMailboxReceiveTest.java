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
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// Integration parity test for the streaming k-way merge in `SortedMailboxReceiveOperator`.
///
/// Verifies that a `SELECT ... ORDER BY <key> LIMIT <n>` query returns identical row sets AND identical order
/// with the k-way merge on and off. The merge needs both `sortedSelectionMergeEnabled` (planner marks the receive
/// node `sortedOnSender`) and `streamingSortedMailboxReceive` (runtime activates the merge given that
/// marking), so each parity test compares an all-off arm against an all-on arm; see [#mergeOptions].
///
/// Also asserts that the streaming sorted leaf — the precondition behind the `sortedOnSender` marking — is
/// opt-in, appearing in `EXPLAIN PLAN FOR` output only when the option is set.
///
/// The base cluster starts two servers and the data is split across two segments, so the receive node merges multiple
/// pre-sorted sender streams.
@Test(suiteName = "CustomClusterIntegrationTest")
public class StreamingSortedMailboxReceiveTest extends CustomDataQueryClusterIntegrationTest {
  private static final String DEFAULT_TABLE_NAME = "StreamingSortedMailboxReceiveTest";
  private static final int NUM_TOTAL_DOCS = 1000;
  private static final String KEY_INT = "keyInt";
  private static final String KEY_STR = "keyStr";
  private static final String PAYLOAD = "payload";

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(KEY_INT, FieldSpec.DataType.INT)
        .addSingleValueDimension(KEY_STR, FieldSpec.DataType.STRING)
        .addSingleValueDimension(PAYLOAD, FieldSpec.DataType.LONG)
        .build();
  }

  @Override
  protected long getCountStarResult() {
    return NUM_TOTAL_DOCS;
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(List.of(
        new org.apache.avro.Schema.Field(KEY_INT, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT),
            null, null),
        new org.apache.avro.Schema.Field(KEY_STR, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
            null, null),
        new org.apache.avro.Schema.Field(PAYLOAD, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG),
            null, null)));

    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      List<DataFileWriter<GenericData.Record>> writers = avroFilesAndWriters.getWriters();
      for (int i = 0; i < NUM_TOTAL_DOCS; i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        // Unique keys so ORDER BY produces a single deterministic ordering (no ties to differ across paths).
        record.put(KEY_INT, i);
        record.put(KEY_STR, String.format("key-%05d", i));
        record.put(PAYLOAD, (long) i * 7);
        // Round-robin across the avro files (segments) so each segment holds an interleaved key range.
        writers.get(i % getNumAvroFiles()).append(record);
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  @Test
  public void testOrderByLimitParityWithAndWithoutStreamingMerge()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    // LIMIT below the total doc count so a leaf ORDER BY LIMIT Sort is pushed to the senders.
    String baseQuery = String.format("SELECT %s, %s, %s FROM %s ORDER BY %s LIMIT 50", KEY_INT, KEY_STR, PAYLOAD,
        getTableName(), KEY_INT);

    JsonNode baselineResponse = runAndGetResponse(mergeOptions(false) + baseQuery);
    JsonNode mergeResponse = runAndGetResponse(mergeOptions(true) + baseQuery);
    JsonNode baselineRows = baselineResponse.get("resultTable").get("rows");
    JsonNode mergeRows = mergeResponse.get("resultTable").get("rows");
    assertRowsIdenticalInOrder(baselineRows, mergeRows);
    // The parity assertion above holds trivially if both arms took the accumulate path, so pin the paths apart.
    assertKWayMergeUsed(baselineResponse, false);
    assertKWayMergeUsed(mergeResponse, true);
    assertEquals(mergeRows.size(), 50, "LIMIT must be honored");
    // Sanity: results are actually globally sorted ascending by keyInt.
    for (int i = 1; i < mergeRows.size(); i++) {
      assertTrue(mergeRows.get(i).get(0).asInt() >= mergeRows.get(i - 1).get(0).asInt(),
          "merge output must be globally sorted ascending by " + KEY_INT);
    }
  }

  @Test
  public void testOrderByLimitParityWithFilter()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    // A WHERE predicate on the leaf compiles to a FilterNode between the Sort/Project and the TableScan. This must
    // still let PlanFragmenter mark the receive sortedOnSender (a filter only removes rows; it neither reorders the
    // survivors nor changes the collation) and activate the k-way merge, same as the unfiltered case above.
    String baseQuery = String.format("SELECT %s, %s, %s FROM %s WHERE %s >= 500 ORDER BY %s LIMIT 50", KEY_INT,
        KEY_STR, PAYLOAD, getTableName(), KEY_INT, KEY_INT);

    JsonNode baselineResponse = runAndGetResponse(mergeOptions(false) + baseQuery);
    JsonNode mergeResponse = runAndGetResponse(mergeOptions(true) + baseQuery);
    JsonNode baselineRows = baselineResponse.get("resultTable").get("rows");
    JsonNode mergeRows = mergeResponse.get("resultTable").get("rows");
    assertRowsIdenticalInOrder(baselineRows, mergeRows);
    assertKWayMergeUsed(baselineResponse, false);
    assertKWayMergeUsed(mergeResponse, true);
    assertEquals(mergeRows.size(), 50, "LIMIT must be honored");
    for (int i = 0; i < mergeRows.size(); i++) {
      int keyInt = mergeRows.get(i).get(0).asInt();
      assertTrue(keyInt >= 500, "WHERE predicate must be honored, got " + keyInt);
      if (i > 0) {
        assertTrue(keyInt >= mergeRows.get(i - 1).get(0).asInt(),
            "merge output must be globally sorted ascending by " + KEY_INT);
      }
    }
  }

  @Test
  public void testOrderByLimitParityDescending()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    String baseQuery = String.format("SELECT %s, %s FROM %s ORDER BY %s DESC LIMIT 25", KEY_INT, PAYLOAD,
        getTableName(), KEY_INT);

    JsonNode baselineRows = runAndGetRows(mergeOptions(false) + baseQuery);
    JsonNode mergeResponse = runAndGetResponse(mergeOptions(true) + baseQuery);
    JsonNode mergeRows = mergeResponse.get("resultTable").get("rows");
    assertRowsIdenticalInOrder(baselineRows, mergeRows);
    assertKWayMergeUsed(mergeResponse, true);
    assertEquals(mergeRows.size(), 25, "LIMIT must be honored");
    // Sanity: results are actually globally sorted descending by keyInt (DESC is where an inverted comparator or wrong
    // null direction in the merge would surface, since both paths could otherwise share the same bug undetected).
    for (int i = 1; i < mergeRows.size(); i++) {
      assertTrue(mergeRows.get(i).get(0).asInt() <= mergeRows.get(i - 1).get(0).asInt(),
          "merge output must be globally sorted descending by " + KEY_INT);
    }
  }

  @Test
  public void testPlannerAutoActivationParity()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    String baseQuery = String.format("SELECT %s, %s, %s FROM %s ORDER BY %s LIMIT 50", KEY_INT, KEY_STR, PAYLOAD,
        getTableName(), KEY_INT);

    // Baseline: both options off, so the receive accumulates and sorts.
    JsonNode baselineRows = runAndGetRows(mergeOptions(false) + baseQuery);
    // Merge arm: sortedSelectionMergeEnabled marks the receive node sortedOnSender AND streamingSortedMailboxReceive
    // activates the k-way merge given that marking.
    JsonNode autoResponse = runAndGetResponse(mergeOptions(true) + baseQuery);
    assertRowsIdenticalInOrder(baselineRows, autoResponse.get("resultTable").get("rows"));
    // Anchors: the merge arm must actually have run the merge, not silently fallen back. The runtime evidence is the
    // kWayMergeUsed stat; the planner-side precondition is the leaf running the streaming sorted selection combine.
    assertKWayMergeUsed(autoResponse, true);
    assertStreamingSortedLeafPlanned(baseQuery);
  }

  @Test
  public void testExplainShowsStreamingSortedLeafWithStep1Hint()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    // The planner sets MailboxReceiveNode.sortedOnSender during fragmentation when the step-1 hint is on and the
    // sender fragment is a leaf selection ORDER BY. That internal flag is NOT surfaced as text by any EXPLAIN mode
    // today (the asking-servers explain renders only the leaf stage via PlanNodeToRelConverter; the intermediate
    // exchange stage stays a logical PinotLogicalExchange), so we cannot assert the flag string directly here.
    // Its runtime effect is proven by testPlannerAutoActivationParity. What the asking-servers explain DOES show
    // stably is the precondition the flag encodes: with the step-1 hint on, the leaf runs the streaming sorted
    // selection combine (SelectOrderbyStreaming) under the exchange, i.e. each sender stream is globally sorted.
    String query = String.format(
        "SET %s=true; SET %s=true; EXPLAIN PLAN FOR SELECT %s, %s FROM %s ORDER BY %s LIMIT 50",
        CommonConstants.Broker.Request.QueryOptionKey.SORTED_SELECTION_MERGE_ENABLED,
        CommonConstants.Broker.Request.QueryOptionKey.EXPLAIN_ASKING_SERVERS, KEY_INT, PAYLOAD, getTableName(),
        KEY_INT);
    JsonNode plan = postQuery(query);
    assertEquals(plan.get("exceptions").size(), 0, "EXPLAIN produced exceptions: " + plan.get("exceptions"));
    String planText = plan.toString();
    assertTrue(planText.contains("SelectOrderbyStreaming"),
        "Step-1 hint should activate the streaming sorted leaf selection. Plan: " + plan);
    assertTrue(planText.contains("PinotLogicalExchange"),
        "Plan should retain the exchange feeding the sorted receiver. Plan: " + plan);
    // Negative control: without the option the leaf must NOT plan the streaming sorted selection, so the assertion
    // above cannot pass vacuously on a plan that always contains that string.
    JsonNode plainPlan = postQuery(String.format(
        "SET %s=true; EXPLAIN PLAN FOR SELECT %s, %s FROM %s ORDER BY %s LIMIT 50",
        CommonConstants.Broker.Request.QueryOptionKey.EXPLAIN_ASKING_SERVERS, KEY_INT, PAYLOAD, getTableName(),
        KEY_INT));
    assertEquals(plainPlan.get("exceptions").size(), 0,
        "EXPLAIN produced exceptions: " + plainPlan.get("exceptions"));
    assertFalse(plainPlan.toString().contains("SelectOrderbyStreaming"),
        "Streaming sorted leaf must be opt-in. Plan: " + plainPlan);
  }

  /// Query-option prefix for one arm of a parity test. For the plain leaf-selection shape these tests use, the k-way
  /// merge needs BOTH options: `sortedSelectionMergeEnabled` is what lets the planner mark the receive node
  /// `sortedOnSender` (there is no rel-level sort exchange here to declare it), and `streamingSortedMailboxReceive` is
  /// what turns the merge on given that marking. Setting only the latter is a silent no-op, so the "merge" arm must set
  /// both or the test compares the accumulate path against itself.
  private static String mergeOptions(boolean enabled) {
    return "SET " + CommonConstants.Broker.Request.QueryOptionKey.SORTED_SELECTION_MERGE_ENABLED + "=" + enabled + "; "
        + "SET " + CommonConstants.Broker.Request.QueryOptionKey.STREAMING_SORTED_MAILBOX_RECEIVE + "=" + enabled
        + "; ";
  }

  private JsonNode runAndGetRows(String query)
      throws Exception {
    return runAndGetResponse(query).get("resultTable").get("rows");
  }

  private JsonNode runAndGetResponse(String query)
      throws Exception {
    JsonNode response = postQuery(query);
    assertEquals(response.get("exceptions").size(), 0, "Query produced exceptions: " + response.get("exceptions"));
    return response;
  }

  /// Asserts whether any MAILBOX_RECEIVE node in the response `stageStats` tree reports `kWayMergeUsed`.
  ///
  /// This is the runtime half of the anchor (see [#assertStreamingSortedLeafPlanned] for the planner half),
  /// and the only direct evidence that the k-way merge actually ran rather than silently falling back to
  /// accumulate-then-sort. Reporting is presence-based: the stat is rendered only when the merge was used, so its
  /// absence across every receive node means every receive took the accumulate path.
  private void assertKWayMergeUsed(JsonNode response, boolean expected) {
    JsonNode stageStats = response.get("stageStats");
    assertNotNull(stageStats, "Response should carry stageStats: " + response);
    assertEquals(findKWayMergeUsed(stageStats), expected,
        "Unexpected kWayMergeUsed in stageStats: " + stageStats);
  }

  private static boolean findKWayMergeUsed(JsonNode node) {
    if (node == null || !node.isObject()) {
      return false;
    }
    JsonNode used = node.get("kWayMergeUsed");
    if (used != null && used.asBoolean()) {
      return true;
    }
    JsonNode children = node.get("children");
    if (children != null) {
      for (JsonNode child : children) {
        if (findKWayMergeUsed(child)) {
          return true;
        }
      }
    }
    return false;
  }

  /// Asserts that, under the merge options, the leaf stage really does run the streaming sorted selection combine.
  ///
  /// That is the precondition the planner requires before it marks the receive node `sortedOnSender`, which in
  /// turn is what activates the k-way merge. The `sortedOnSender` flag itself is not rendered by any EXPLAIN mode
  /// (the asking-servers explain renders only the leaf stage; the intermediate exchange stage stays a logical
  /// `PinotLogicalExchange`), so this covers the planner half; [#assertKWayMergeUsed] covers the runtime
  /// half by reading the `kWayMergeUsed` stat out of the response `stageStats`. Without both anchors a
  /// parity test passes just as happily when both arms silently take the accumulate path.
  private void assertStreamingSortedLeafPlanned(String baseQuery)
      throws Exception {
    JsonNode plan = postQuery(mergeOptions(true) + "SET "
        + CommonConstants.Broker.Request.QueryOptionKey.EXPLAIN_ASKING_SERVERS + "=true; EXPLAIN PLAN FOR "
        + baseQuery);
    assertEquals(plan.get("exceptions").size(), 0, "EXPLAIN produced exceptions: " + plan.get("exceptions"));
    String planText = plan.toString();
    assertTrue(planText.contains("SelectOrderbyStreaming"),
        "Merge arm did not plan the streaming sorted leaf, so the k-way merge could not have activated. Plan: " + plan);
    assertTrue(planText.contains("PinotLogicalExchange"),
        "Plan should retain the exchange feeding the sorted receiver. Plan: " + plan);
  }

  private static void assertRowsIdenticalInOrder(JsonNode expected, JsonNode actual) {
    assertEquals(actual.size(), expected.size(), "row count mismatch");
    List<String> expectedRows = new ArrayList<>();
    List<String> actualRows = new ArrayList<>();
    for (int i = 0; i < expected.size(); i++) {
      expectedRows.add(expected.get(i).toString());
      actualRows.add(actual.get(i).toString());
    }
    // Order-sensitive comparison: row i must match across both result sets.
    assertEquals(actualRows, expectedRows, "row sets / order differ between streaming-merge and baseline");
  }
}
