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
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedByteChunkSVForwardIndexReaderV7;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.server.starter.helix.BaseServerStarter;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Realtime coverage for raw forward-index encodings, limited to what is realtime-specific: the
/// configured codec is ignored while rows are consuming and is applied when a consuming segment is
/// committed and converted to an immutable segment.
///
/// This deliberately does NOT extend [CodecPipelineIntegrationTest] and deliberately does not repeat
/// its matrix. Once a segment exists on disk, the codec matrix, both query engines, dictionary
/// coexistence and the chunk-boundary read paths are all table-type agnostic, so inheriting them
/// re-ran the whole offline matrix against a realtime table for no added coverage. Three columns are
/// enough here, because the commit path is what is under test rather than the codecs:
///
/// - `longSpecDeltaLz4` (`DELTA,LZ4`) and `intSpecT64Zstd` (`T64,ZSTD(3)`) — one LONG and one INT
///   `codecSpec` column, so both the long and the int side of the V7 writer run through a commit.
/// - `longLegacyPassThrough` — legacy `compressionCodec: PASS_THROUGH`, the uncompressed baseline and
///   the negative control for the routing branch in `ForwardIndexCreatorFactory`: only `codecSpec`
///   selects the V7 writer, so a regression that routed every raw column to V7 would still pass every
///   other assertion here.
///
/// `ForwardIndexType.createMutableIndex` never looks at `codecSpec` — a no-dictionary INT/LONG column
/// always gets `FixedByteSVMutableForwardIndex` — so consuming rows are uncompressed by design. The
/// inherited `setUp` waits for every row to be queryable before this class force-commits, so that
/// consuming path is already exercised by setup. The flush threshold is below the row count, so
/// segments seal both by threshold and by `forceCommit`.
@Test(suiteName = "CustomClusterIntegrationTest")
public class CodecPipelineRealtimeIntegrationTest extends CustomDataQueryClusterIntegrationTest {

  private static final String TABLE_NAME = "CodecPipelineRealtimeIntegrationTest";
  private static final int NUM_DOCS = 600;
  private static final int SEGMENT_FLUSH_SIZE = 250;
  private static final int V7_TARGET_DOCS_PER_CHUNK = 64;
  private static final long LONG_VALUE_SCALE = 1_000_000_000L;
  private static final long FORCE_COMMIT_TIMEOUT_MS = 120_000L;
  private static final long SEGMENT_LOAD_TIMEOUT_MS = 120_000L;
  /// Segments sealed by the flush threshold, plus the one force-committed remainder.
  private static final int MIN_COMMITTED_SEGMENTS = NUM_DOCS / SEGMENT_FLUSH_SIZE + 1;

  private static final String TIME_COL = "ts";

  /// The columns under test: two `codecSpec` shapes and the uncompressed legacy control. Everything
  /// below — schema, Avro fields, `noDictionaryColumns`, `FieldConfig`s, the format assertions and the
  /// queries — is derived from this list.
  private static final List<RealtimeColumn> COLUMNS = List.of(
      new RealtimeColumn("longSpecDeltaLz4", DataType.LONG, "DELTA,LZ4", "DELTA,LZ4", null),
      new RealtimeColumn("intSpecT64Zstd", DataType.INT, "T64,ZSTD", "T64,ZSTD(3)", null),
      new RealtimeColumn("longLegacyPassThrough", DataType.LONG, null, null, CompressionCodec.PASS_THROUGH));

  /// Doc ids at and around a V7 chunk boundary (63/64), a segment boundary (249/250), and the last row
  /// of the final force-committed segment.
  private static final int[] POINT_LOOKUP_IDS = {0, 63, 64, 249, 250, 599};
  private static final String POINT_LOOKUP_ID_LIST = "0, 63, 64, 249, 250, 599";

  /// One column under test. Exactly one of the `codecSpec` pair and [#_compressionCodec] is set,
  /// because `ForwardIndexConfig` rejects both at once.
  private static final class RealtimeColumn {
    final String _column;
    final DataType _dataType;
    @Nullable
    final String _codecSpec;
    /// The canonical spec the codec runtime freezes into the V7 header; `T64,ZSTD` materializes ZSTD's
    /// default level, so it is not always the configured string.
    @Nullable
    final String _canonicalCodecSpec;
    @Nullable
    final CompressionCodec _compressionCodec;

    private RealtimeColumn(String column, DataType dataType, @Nullable String codecSpec,
        @Nullable String canonicalCodecSpec, @Nullable CompressionCodec compressionCodec) {
      _column = column;
      _dataType = dataType;
      _codecSpec = codecSpec;
      _canonicalCodecSpec = canonicalCodecSpec;
      _compressionCodec = compressionCodec;
    }

    boolean isCodecPipeline() {
      return _codecSpec != null;
    }

    @Override
    public String toString() {
      return _column + "[" + _dataType + ", " + (_codecSpec != null ? "codecSpec=" + _codecSpec
          : "compressionCodec=" + _compressionCodec) + "]";
    }
  }

  /// INT columns hold the doc id; LONG columns scale it past the INT range so a LONG column cannot
  /// accidentally pass with 32-bit decoding. Both are a pure function of the doc id.
  private static long valueFor(DataType dataType, long docId) {
    return dataType == DataType.INT ? docId : docId * LONG_VALUE_SCALE;
  }

  /// No sorted column: the forward index is the only thing under test, and the base class would
  /// otherwise make the time column sorted, which reorders rows at commit.
  @Override
  protected String getSortedColumn() {
    return null;
  }

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  public boolean isRealtimeTable() {
    return true;
  }

  @Override
  protected int getNumKafkaPartitions() {
    return 1;
  }

  @Override
  public int getNumAvroFiles() {
    // One file and one partition, so rows reach the single consumer in `ts` order. That makes the
    // segment and chunk boundaries the point lookups below aim at deterministic.
    return 1;
  }

  @Override
  protected int getRealtimeSegmentFlushSize() {
    // < NUM_DOCS so a segment seals during consumption; forceCommit seals the remainder.
    return SEGMENT_FLUSH_SIZE;
  }

  @Override
  public String getTimeColumnName() {
    return TIME_COL;
  }

  @Override
  protected long getCountStarResult() {
    return NUM_DOCS;
  }

  @Override
  public Schema createSchema() {
    Schema.SchemaBuilder builder = new Schema.SchemaBuilder().setSchemaName(getTableName());
    for (RealtimeColumn column : COLUMNS) {
      if (column.isCodecPipeline()) {
        builder.addMetric(column._column, column._dataType);
      } else {
        // Deliberately a dimension, not a metric: metric columns already default to PASS_THROUGH, so
        // asserting PASS_THROUGH on a metric would pass even if the explicit compressionCodec were
        // dropped. Dimensions default to LZ4, which makes the negative control meaningful.
        builder.addSingleValueDimension(column._column, column._dataType);
      }
    }
    builder.addDateTimeField(TIME_COL, DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS");
    return builder.build();
  }

  @Override
  public List<File> createAvroFiles()
      throws IOException {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("codecRealtimeRecord", null, null, false);
    List<Field> fields = new ArrayList<>();
    for (RealtimeColumn column : COLUMNS) {
      Type avroType = column._dataType == DataType.INT ? Type.INT : Type.LONG;
      fields.add(new Field(column._column, org.apache.avro.Schema.create(avroType), null, null));
    }
    fields.add(new Field(TIME_COL, org.apache.avro.Schema.create(Type.LONG), null, null));
    avroSchema.setFields(fields);

    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      List<DataFileWriter<GenericData.Record>> writers = avroFilesAndWriters.getWriters();
      for (int docId = 0; docId < NUM_DOCS; docId++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        for (RealtimeColumn column : COLUMNS) {
          if (column._dataType == DataType.INT) {
            record.put(column._column, docId);
          } else {
            record.put(column._column, valueFor(DataType.LONG, docId));
          }
        }
        record.put(TIME_COL, (long) docId);
        writers.get(docId % getNumAvroFiles()).append(record);
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  @Override
  protected List<String> getNoDictionaryColumns() {
    List<String> noDictionaryColumns = new ArrayList<>(COLUMNS.size());
    for (RealtimeColumn column : COLUMNS) {
      noDictionaryColumns.add(column._column);
    }
    return noDictionaryColumns;
  }

  @Override
  protected List<FieldConfig> getFieldConfigs() {
    List<FieldConfig> fieldConfigs = new ArrayList<>(COLUMNS.size());
    for (RealtimeColumn column : COLUMNS) {
      ObjectNode forward = JsonUtils.newObjectNode();
      // Keep chunks well below the flush size so committed segments hold several chunks.
      forward.put("targetDocsPerChunk", V7_TARGET_DOCS_PER_CHUNK);
      if (column.isCodecPipeline()) {
        forward.put("codecSpec", column._codecSpec);
      }
      ObjectNode indexes = JsonUtils.newObjectNode();
      indexes.set("forward", forward);
      FieldConfig.Builder builder = new FieldConfig.Builder(column._column)
          .withEncodingType(FieldConfig.EncodingType.RAW)
          .withIndexes(indexes);
      if (!column.isCodecPipeline()) {
        builder.withCompressionCodec(column._compressionCodec);
      }
      fieldConfigs.add(builder.build());
    }
    return fieldConfigs;
  }

  @Override
  @BeforeClass
  public void setUp()
      throws Exception {
    // Loads the schema and realtime table config, pushes all rows into Kafka, and waits until every
    // row is queryable — which happens while the last segment is still consuming.
    super.setUp();
    forceCommitAndWait();
    // The commit job completing means the segments are committed in ZooKeeper; the servers still
    // have to load them before their forward-index format can be inspected or queried.
    TestUtils.waitForCondition(aVoid -> countCommittedSegments() >= MIN_COMMITTED_SEGMENTS, 1_000L,
        SEGMENT_LOAD_TIMEOUT_MS, "Timed out waiting for " + MIN_COMMITTED_SEGMENTS
            + " committed realtime segments to load");
  }

  /// The realtime-specific assertion: the configured forward-index format is applied when a consuming
  /// segment is committed and converted to an immutable segment. A `codecSpec` column becomes a
  /// self-describing V7 index carrying the canonical spec and no legacy ChunkCompressionType, while
  /// the legacy column in the same segment stays on the legacy writer and reports `PASS_THROUGH`.
  @Test
  public void testCommittedSegmentsUseConfiguredForwardIndexFormats() {
    // A lower bound rather than an equality: every committed segment found is asserted, so an extra
    // seal is not a failure, but finding fewer than the guaranteed ones is.
    int inspected = forEachCommittedSegment(CodecPipelineRealtimeIntegrationTest::assertForwardIndexFormats);
    assertTrue(inspected >= MIN_COMMITTED_SEGMENTS,
        "Expected at least " + MIN_COMMITTED_SEGMENTS + " committed realtime segments, inspected " + inspected);
  }

  private int countCommittedSegments() {
    return forEachCommittedSegment(segment -> {
    });
  }

  /// Visits every committed (immutable) segment of this table loaded on the shared servers and
  /// returns how many there were. Consuming segments are skipped: while consuming, the rows live in
  /// a mutable forward index that ignores `codecSpec` by design.
  private int forEachCommittedSegment(Consumer<ImmutableSegmentImpl> visitor) {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    int visited = 0;
    for (BaseServerStarter serverStarter : getSharedServerStarters()) {
      TableDataManager tableDataManager = serverStarter.getServerInstance().getInstanceDataManager()
          .getTableDataManager(realtimeTableName);
      if (tableDataManager == null) {
        continue;
      }
      List<SegmentDataManager> segmentDataManagers = tableDataManager.acquireAllSegments();
      try {
        for (SegmentDataManager segmentDataManager : segmentDataManagers) {
          IndexSegment segment = segmentDataManager.getSegment();
          if (segment instanceof ImmutableSegmentImpl) {
            visitor.accept((ImmutableSegmentImpl) segment);
            visited++;
          }
        }
      } finally {
        for (SegmentDataManager segmentDataManager : segmentDataManagers) {
          tableDataManager.releaseSegment(segmentDataManager);
        }
      }
    }
    return visited;
  }

  /// End-to-end read back of the committed segments through both query engines: an aggregate over
  /// every row, the whole-table cross-codec equality check, and point lookups at V7 chunk and segment
  /// boundaries — all derived from [#COLUMNS] so they cannot drift from it.
  @Test(dataProvider = "useBothQueryEngines")
  public void testQueryCommittedSegments(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    List<String> sums = new ArrayList<>(COLUMNS.size());
    List<String> selectList = new ArrayList<>(COLUMNS.size() + 1);
    List<String> predicates = new ArrayList<>(COLUMNS.size());
    selectList.add(TIME_COL);
    for (RealtimeColumn column : COLUMNS) {
      sums.add("SUM(" + column._column + ")");
      selectList.add(column._column);
      predicates.add(column._dataType == DataType.INT ? column._column + " = " + TIME_COL
          : column._column + " = " + TIME_COL + " * " + LONG_VALUE_SCALE);
    }

    JsonNode sumResult = postQuery("SELECT " + String.join(", ", sums) + " FROM " + getTableName());
    JsonNode sumRow = sumResult.get("resultTable").get("rows").get(0);
    for (int i = 0; i < COLUMNS.size(); i++) {
      RealtimeColumn column = COLUMNS.get(i);
      assertEquals(sumRow.get(i).asLong(), valueFor(column._dataType, (long) NUM_DOCS * (NUM_DOCS - 1) / 2),
          "Unexpected SUM for " + column + " after commit");
    }

    // Every row of every column agrees with `ts`, so a per-row decoding error that happens to
    // preserve the SUM cannot slip through.
    JsonNode agreement = postQuery(
        "SELECT COUNT(*) FROM " + getTableName() + " WHERE " + String.join(" AND ", predicates));
    assertEquals(agreement.get("resultTable").get("rows").get(0).get(0).asLong(), NUM_DOCS,
        "Not every row agrees across the committed realtime columns: " + String.join(" AND ", predicates));

    JsonNode result = postQuery("SELECT " + String.join(", ", selectList) + " FROM " + getTableName() + " WHERE "
        + TIME_COL + " IN (" + POINT_LOOKUP_ID_LIST + ") ORDER BY " + TIME_COL);
    JsonNode rows = result.get("resultTable").get("rows");
    assertEquals(rows.size(), POINT_LOOKUP_IDS.length, "Unexpected point-lookup row count after commit");
    for (int rowId = 0; rowId < POINT_LOOKUP_IDS.length; rowId++) {
      int docId = POINT_LOOKUP_IDS[rowId];
      JsonNode row = rows.get(rowId);
      assertEquals(row.get(0).asInt(), docId, "Unexpected point-lookup order after commit");
      for (int i = 0; i < COLUMNS.size(); i++) {
        RealtimeColumn column = COLUMNS.get(i);
        assertEquals(row.get(i + 1).asLong(), valueFor(column._dataType, docId),
            "Wrong " + column + " for ts=" + docId);
      }
    }
  }

  private static void assertForwardIndexFormats(ImmutableSegmentImpl segment) {
    try (SegmentDirectory directory = new SegmentLocalFSDirectory(segment.getSegmentMetadata().getIndexDir(),
        ReadMode.mmap); SegmentDirectory.Reader segmentReader = directory.createReader()) {
      for (RealtimeColumn column : COLUMNS) {
        ForwardIndexReader<?> reader = segment.getDataSource(column._column).getForwardIndex();
        if (!column.isCodecPipeline()) {
          assertFalse(reader instanceof FixedByteChunkSVForwardIndexReaderV7,
              column + " has no codecSpec and must not be routed to V7");
          assertEquals(reader.getCompressionType(), ChunkCompressionType.PASS_THROUGH,
              "Unexpected ChunkCompressionType for " + column);
          continue;
        }
        assertTrue(reader instanceof FixedByteChunkSVForwardIndexReaderV7,
            column + " was routed to " + reader.getClass().getSimpleName());
        PinotDataBuffer forwardIndexBuffer = segmentReader.getIndexFor(column._column, StandardIndexes.forward());
        assertEquals(FixedByteChunkSVForwardIndexReaderV7.readCodecSpec(forwardIndexBuffer),
            column._canonicalCodecSpec, "Unexpected canonical spec in the V7 header of " + column);
        assertNull(reader.getCompressionType(), column + " must not report a legacy ChunkCompressionType");
      }
    } catch (Exception e) {
      throw new AssertionError("Failed to inspect the forward-index formats of " + segment.getSegmentName(), e);
    }
  }

  private void forceCommitAndWait()
      throws Exception {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    String response = getOrCreateAdminClient().getTableClient().forceCommit(realtimeTableName);
    String jobId = JsonUtils.stringToJsonNode(response).get("forceCommitJobId").asText();
    TestUtils.waitForCondition(aVoid -> isForceCommitComplete(jobId), 1_000L, FORCE_COMMIT_TIMEOUT_MS,
        "Timed out waiting for forceCommit job: " + jobId);
  }

  private boolean isForceCommitComplete(String jobId) {
    try {
      String response = getOrCreateAdminClient().getTableClient().getForceCommitJobStatus(jobId);
      JsonNode status = JsonUtils.stringToJsonNode(response);
      return status.get(CommonConstants.ControllerJob.NUM_CONSUMING_SEGMENTS_YET_TO_BE_COMMITTED).asInt(-1) == 0;
    } catch (Exception e) {
      return false;
    }
  }
}
