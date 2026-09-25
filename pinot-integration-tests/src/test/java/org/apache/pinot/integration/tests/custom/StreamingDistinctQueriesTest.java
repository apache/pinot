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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// End-to-end tests for the streaming DISTINCT leaf stage's cross-segment early exit.
///
/// The exit is a per-server decision -- a leaf stops once it has emitted at least LIMIT distinct values, on the
/// grounds that the FINAL stage de-duplicates across servers and applies LIMIT. Nothing below the broker can observe
/// whether that composition holds, which is why these live here rather than in `pinot-core`.
///
/// ## Two signals, because neither is sufficient alone
///
/// - **`numDocsScanned < totalDocs`** shows the exit *fired*. The row count cannot: a response holds LIMIT rows
///   whether the leaf stopped early or read everything, so without this a regression back to a full scan would pass
///   silently. Verified by mutation -- with the exit condition broken these assertions fail, and with the tracker
///   made to throw, the exact tracker is seen for [#testExactModeStopsScanningEarly], the estimating one for
///   [#testEstimatedModeStopsScanningEarly] and neither for [#testEstimatedModeOffByDefaultScansEverything].
/// - **`rows == LIMIT`** shows the exit was not *premature*, which is the failure that returns a silently short
///   result with no exception and no partial-result flag.
///
/// ## What still escapes them
///
/// The row-count half cannot catch a leaf that stops marginally short. The exit is per-server but the assertion is
/// on the broker's union, so with two servers a leaf emitting `E` values yields `min(2E, LIMIT)` rows and nothing
/// shows until `E` falls below half of LIMIT. That masking is a property of multi-server deployments, not of this
/// test: in production too, a leaf stopping slightly short is often invisible in the response. The per-server
/// property these rest on is pinned exactly, per value type, in `DistinctCardinalityTrackerTest`.
///
@Test(suiteName = "CustomClusterIntegrationTest")
public class StreamingDistinctQueriesTest extends CustomDataQueryClusterIntegrationTest {
  private static final String TABLE_NAME = "StreamingDistinctQueriesCustomTest";
  private static final String INT_COL = "intCol";

  /// Split across the two servers `CustomDataQueryClusterIntegrationTest` starts, each segment holding few enough
  /// distinct values that the accumulator must merge several before it reaches the flush threshold. That is what
  /// puts the early exit partway through the scan, where `numDocsScanned` can see it -- with one big segment per
  /// server the first flush already carries more than LIMIT and nothing is ever skipped.
  private static final int NUM_SEGMENTS = 40;
  private static final int DISTINCT_PER_SEGMENT = 200;
  private static final int COPIES_PER_VALUE = 5;
  private static final int NUM_ROWS_PER_SEGMENT = DISTINCT_PER_SEGMENT * COPIES_PER_VALUE;
  private static final long TOTAL_DOCS = (long) NUM_ROWS_PER_SEGMENT * NUM_SEGMENTS;
  /// Segments hold disjoint ranges, so a server's cumulative cardinality grows as it merges them.
  private static final int NUM_DISTINCT_VALUES = NUM_SEGMENTS * DISTINCT_PER_SEGMENT;

  /// Above a single segment's cardinality, so several segments merge into each flush window.
  private static final int FLUSH_THRESHOLD = 500;
  /// Above the flush threshold (required for streaming) and well below what one server holds, so the exit can fire.
  private static final int LIMIT = 1_000;

  /// Stops a worker thread running ahead of the consumer and scanning every segment before satisfaction is noticed.
  /// Without it the hand-off queue holds 100 blocks and the scan completes regardless of the exit.
  private static final String BOUNDED_QUEUE = ";maxStreamingPendingBlocks=1";

  @Override
  protected long getCountStarResult() {
    return TOTAL_DOCS;
  }

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(INT_COL, FieldSpec.DataType.INT)
        .build();
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema =
        SchemaBuilder.record("StreamingDistinctRecord").fields().requiredInt(INT_COL).endRecord();
    List<File> files = new ArrayList<>(NUM_SEGMENTS);
    for (int segment = 0; segment < NUM_SEGMENTS; segment++) {
      File file = new File(_tempDir, "streaming-distinct-data-" + segment + ".avro");
      try (DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
        writer.create(avroSchema, file);
        for (int copy = 0; copy < COPIES_PER_VALUE; copy++) {
          for (int i = 0; i < DISTINCT_PER_SEGMENT; i++) {
            GenericData.Record record = new GenericData.Record(avroSchema);
            record.put(INT_COL, segment * DISTINCT_PER_SEGMENT + i);
            writer.append(record);
          }
        }
      }
      files.add(file);
    }
    return files;
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    // Raw column, so DistinctPlanNode picks the scanning DistinctOperator rather than reading the dictionary whole.
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName())
        .setNoDictionaryColumns(List.of(INT_COL)).build();
  }

  @Override
  protected String getSortedColumn() {
    return null;
  }

  /// Exact mode, the one that ships on by default: cumulative cardinality is an exact set of value hashes, so
  /// reaching LIMIT is a counted fact.
  @Test
  public void testExactModeStopsScanningEarly()
      throws Exception {
    assertEarlyExit("streamingDistinctFlushThreshold=" + FLUSH_THRESHOLD + BOUNDED_QUEUE);
  }

  /// Estimated mode, which is opt-in: above `streamingDistinctMaxTrackedCardinality` the leaf measures cardinality
  /// with a theta sketch and exits on its lower confidence bound.
  ///
  /// The bound is lowered to 512 so a modest LIMIT lands in this mode without a huge fixture. That still leaves a
  /// wide margin against a flaky short result: the last flush boundary below LIMIT sits well under it, while the
  /// sketch's relative standard error at 512 nominal entries is ~4%, so crossing early would take an excursion of
  /// several sigma on top of the 3-sigma reduction the lower bound already applies.
  @Test
  public void testEstimatedModeStopsScanningEarly()
      throws Exception {
    assertEarlyExit("streamingDistinctFlushThreshold=" + FLUSH_THRESHOLD
        + ";streamingDistinctMaxTrackedCardinality=512;streamingDistinctEstimatedExitStdDev=3" + BOUNDED_QUEUE);
  }

  /// The default above the bound: the estimated exit is unsound, so it is off unless asked for, and the leaf reads
  /// every segment. This is the assertion that pins the default -- if the estimated exit ever became opt-out, this
  /// test would start seeing a partial scan.
  ///
  /// The full scan here is exact, not incidental. Without a tracker the only satisfaction check left is the merger's,
  /// which reads the accumulated table -- and flushing empties that every `streamingDistinctFlushThreshold` values,
  /// which is below LIMIT, so it can never fire. That is precisely the regression this change repairs.
  @Test
  public void testEstimatedModeOffByDefaultScansEverything()
      throws Exception {
    JsonNode response = runDistinct("streamingDistinctFlushThreshold=" + FLUSH_THRESHOLD
        + ";streamingDistinctMaxTrackedCardinality=512" + BOUNDED_QUEUE);
    assertEquals(response.path("numDocsScanned").asLong(), TOTAL_DOCS,
        "Without an explicit opt-in the estimated exit must not fire, so every document is read");
    assertFullResult(response);
  }

  /// Parity with the blocking path. Note this asserts only the result, not the scan: the blocking
  /// `DistinctCombineOperator` has an early exit of its own -- `DistinctTable#isSatisfied()` fires once the
  /// accumulated table reaches LIMIT -- and it reads a varying fraction of the table depending on how the worker
  /// threads interleave. Restoring that exit for the streaming path is what this change is for.
  @Test
  public void testStreamingDisabledReturnsSameResult()
      throws Exception {
    assertFullResult(runDistinct("streamingDistinctFlushThreshold=0" + BOUNDED_QUEUE));
  }

  /// Asserts both halves of the contract, which need different evidence:
  ///
  /// - the exit actually fired — visible only in `numDocsScanned`, since the row count cannot show it;
  /// - the exit was not premature — the response still holds LIMIT genuine rows.
  private void assertEarlyExit(String streamingOptions)
      throws Exception {
    JsonNode response = runDistinct(streamingOptions);
    long scanned = response.path("numDocsScanned").asLong();
    assertTrue(scanned < TOTAL_DOCS,
        "Expected the early exit to leave documents unread, but scanned " + scanned + " of " + TOTAL_DOCS
            + " (segments " + response.path("numSegmentsProcessed").asInt() + " of "
            + response.path("numSegmentsQueried").asInt() + ")");
    assertFullResult(response);
  }

  /// The cross-role invariant: a leaf that stops early must still have emitted enough distinct values for the FINAL
  /// stage to fill LIMIT.
  private void assertFullResult(JsonNode response) {
    Set<Integer> values = distinctValues(response);
    assertEquals(values.size(), LIMIT,
        "The leaf exited before emitting LIMIT distinct values, so the result is silently short");
    for (int value : values) {
      assertTrue(value >= 0 && value < NUM_DISTINCT_VALUES, "Value outside the ingested range: " + value);
    }
  }

  private JsonNode runDistinct(String queryOptions)
      throws Exception {
    setUseMultiStageQueryEngine(true);
    String sql = String.format("SELECT DISTINCT %s FROM %s LIMIT %d", INT_COL, getTableName(), LIMIT);
    JsonNode response = postQueryWithOptions(sql, queryOptions);
    assertTrue(response.path("exceptions").isEmpty(), "Expected no exceptions: " + response.path("exceptions"));
    // Pin the topology the scan assertions assume: fewer segments per server and a worker could finish before the
    // consumer notices satisfaction, turning the early-exit assertions into silent no-ops.
    assertEquals(response.path("numSegmentsQueried").asInt(), NUM_SEGMENTS);
    return response;
  }

  private static Set<Integer> distinctValues(JsonNode response) {
    JsonNode rows = response.path("resultTable").path("rows");
    Set<Integer> values = new HashSet<>(rows.size());
    for (JsonNode row : rows) {
      values.add(row.get(0).asInt());
    }
    return values;
  }
}
