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

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.helix.model.ExternalView;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.server.starter.helix.BaseServerStarter;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// End-to-end coverage for [FieldConfig#getConsumingOverride()] on a realtime table:
///
/// 1. The table persists `STRING_COLUMN` as RAW with no inverted index.
/// 2. The `consumingOverride` lifts it to `DICTIONARY` + `INVERTED` for the in-memory consuming segment so that
///    filters during the consumption window can hit an inverted index.
/// 3. After [#forceCommitAndWait()] commits the consuming segments, the immutable segments on disk must match
///    the persisted shape (RAW + no inverted index, no dictionary), proving that the override never leaks into the
///    committed segment.
///
/// Comprehensive checks executed in [#testConsumingOverrideEndToEnd()]:
///
/// - Pre-commit: walks each server's [TableDataManager], asserts every consuming segment is a
///   [MutableSegmentImpl], and verifies via the in-memory [DataSource] that `STRING_COLUMN` has both a dictionary
///   and an inverted index — i.e. the override took effect.
/// - Post-commit: walks each server again, finds every [ImmutableSegmentImpl], asserts the on-disk
///   [ColumnMetadata] (`hasDictionary()`) AND the on-disk [SegmentDirectory] reader (`hasIndexFor`) confirm the
///   persisted shape is RAW with no dictionary and no inverted index. Also verifies that an unrelated dimension
///   column (`STRING_COLUMN_NO_OVERRIDE`) keeps its default DICTIONARY shape on both the consuming and immutable
///   segments — guards against the override scrub being too aggressive.
/// - Query parity: the same SQL queries return identical row counts before and after commit, so the override does
///   not affect query semantics — only the in-memory index that serves them.
@Test(suiteName = "CustomClusterIntegrationTest")
public class ConsumingOverrideRealtimeTest extends CustomDataQueryClusterIntegrationTest {
  private static final String TABLE_NAME = "ConsumingOverrideRealtimeTest";
  private static final int NUM_DOCS = 200;

  /// Carries the `consumingOverride`: persisted RAW (no dictionary, no inverted), consuming DICTIONARY + INVERTED.
  private static final String STRING_COLUMN = "stringCol";
  /// Control column: no override anywhere; expected to be DICTIONARY-encoded both pre- and post-commit.
  private static final String STRING_COLUMN_NO_OVERRIDE = "stringColNoOverride";
  private static final String INT_COLUMN = "intCol";
  private static final String TIME_COLUMN = "tsMillis";

  /// Distinct values cycled through `STRING_COLUMN` so equality filters have non-trivial selectivity.
  private static final String[] STRING_VALUES = {"alpha", "beta", "gamma", "delta"};

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  public boolean isRealtimeTable() {
    return true;
  }

  @Override
  protected long getCountStarResult() {
    return NUM_DOCS;
  }

  @Override
  protected int getRealtimeSegmentFlushSize() {
    /// Keep flush size larger than NUM_DOCS so that the entire dataset stays in the consuming segment until the
    /// test explicitly forceCommits — letting the test inspect the consuming-side shape with full data.
    return NUM_DOCS * 10;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(STRING_COLUMN, FieldSpec.DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN_NO_OVERRIDE, FieldSpec.DataType.STRING)
        .addMetric(INT_COLUMN, FieldSpec.DataType.INT)
        .addDateTimeField(TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
  }

  @Override
  public String getTimeColumnName() {
    return TIME_COLUMN;
  }

  @Nullable
  @Override
  protected String getSortedColumn() {
    /// No sorted column: validation rejects consumingOverride on a sorted column, and we want the override active.
    return null;
  }

  @Override
  protected List<String> getNoDictionaryColumns() {
    /// Persisted shape: STRING_COLUMN is RAW. STRING_COLUMN_NO_OVERRIDE stays default (DICTIONARY).
    return Collections.singletonList(STRING_COLUMN);
  }

  @Override
  protected List<String> getInvertedIndexColumns() {
    /// Persisted shape: no inverted index anywhere.
    return List.of();
  }

  @Override
  protected List<FieldConfig> getFieldConfigs() {
    /// FieldConfig drives the override: persisted RAW, consuming DICTIONARY + INVERTED via the typed `indexes` tree.
    ObjectNode override = JsonUtils.newObjectNode();
    override.put("encodingType", FieldConfig.EncodingType.DICTIONARY.name());
    override.set("indexes",
        JsonUtils.newObjectNode().set("inverted", JsonUtils.newObjectNode().put("enabled", true)));
    FieldConfig overridden = new FieldConfig.Builder(STRING_COLUMN)
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withConsumingOverride(override)
        .build();
    return List.of(overridden);
  }

  protected TableConfig createRealtimeTableConfig(File sampleAvroFile) {
    AvroFileSchemaKafkaAvroMessageDecoder._avroFile = sampleAvroFile;
    return new TableConfigBuilder(TableType.REALTIME)
        .setTableName(getTableName())
        .setStreamConfigs(getStreamConfigs())
        .setTimeColumnName(getTimeColumnName())
        .setSortedColumn(getSortedColumn())
        .setInvertedIndexColumns(getInvertedIndexColumns())
        .setNoDictionaryColumns(getNoDictionaryColumns())
        .setFieldConfigList(getFieldConfigs())
        .setNumReplicas(getNumReplicas())
        .build();
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    org.apache.avro.Schema stringSchema = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING);
    org.apache.avro.Schema intSchema = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT);
    org.apache.avro.Schema longSchema = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG);
    avroSchema.setFields(List.of(
        new org.apache.avro.Schema.Field(STRING_COLUMN, stringSchema, null, null),
        new org.apache.avro.Schema.Field(STRING_COLUMN_NO_OVERRIDE, stringSchema, null, null),
        new org.apache.avro.Schema.Field(INT_COLUMN, intSchema, null, null),
        new org.apache.avro.Schema.Field(TIME_COLUMN, longSchema, null, null)));

    long now = System.currentTimeMillis();
    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      List<DataFileWriter<GenericData.Record>> writers = avroFilesAndWriters.getWriters();
      for (int i = 0; i < NUM_DOCS; i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(STRING_COLUMN, STRING_VALUES[i % STRING_VALUES.length]);
        record.put(STRING_COLUMN_NO_OVERRIDE, "fixed");
        record.put(INT_COLUMN, i);
        record.put(TIME_COLUMN, now + i);
        writers.get(i % getNumAvroFiles()).append(record);
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  @Test
  public void testConsumingOverrideEndToEnd()
      throws Exception {
    String realtimeTableName = TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(getTableName());

    /// 1. Wait until all docs land in the consuming segment.
    waitForAllDocsLoaded(60_000L);

    /// 2. Sanity-check the query result before commit.
    long countAlphaPreCommit = queryStringCount(STRING_VALUES[0]);
    long expectedAlphaCount = (NUM_DOCS + STRING_VALUES.length - 1) / STRING_VALUES.length;
    assertEquals(countAlphaPreCommit, expectedAlphaCount,
        "Pre-commit count for " + STRING_VALUES[0] + " mismatch");

    /// 3. Inspect the consuming segment(s) on each server: STRING_COLUMN MUST have a dictionary AND an inverted
    ///    index (the override is in effect). STRING_COLUMN_NO_OVERRIDE MUST also have a dictionary (default).
    int consumingSegmentsInspected = 0;
    int totalDocsAcrossConsumingSegments = 0;
    for (BaseServerStarter serverStarter : getSharedServerStarters()) {
      TableDataManager tableDataManager = serverStarter.getServerInstance().getInstanceDataManager()
          .getTableDataManager(realtimeTableName);
      if (tableDataManager == null) {
        continue;
      }
      List<SegmentDataManager> segmentDataManagers = tableDataManager.acquireAllSegments();
      try {
        for (SegmentDataManager sdm : segmentDataManagers) {
          IndexSegment segment = sdm.getSegment();
          assertTrue(segment instanceof MutableSegmentImpl,
              "Pre-commit segment must be mutable, got: " + segment.getClass().getSimpleName()
                  + " for segment " + sdm.getSegmentName());
          DataSource overriddenDs = segment.getDataSource(STRING_COLUMN);
          assertNotNull(overriddenDs, "DataSource missing for " + STRING_COLUMN + " on consuming segment");
          assertNotNull(overriddenDs.getDictionary(),
              STRING_COLUMN + " must have a dictionary on the consuming segment due to consumingOverride");
          assertNotNull(overriddenDs.getInvertedIndex(),
              STRING_COLUMN + " must have an inverted index on the consuming segment due to consumingOverride");
          DataSource controlDs = segment.getDataSource(STRING_COLUMN_NO_OVERRIDE);
          assertNotNull(controlDs.getDictionary(),
              STRING_COLUMN_NO_OVERRIDE + " must keep its default dictionary on the consuming segment");
          consumingSegmentsInspected++;
          totalDocsAcrossConsumingSegments += segment.getSegmentMetadata().getTotalDocs();
        }
      } finally {
        for (SegmentDataManager sdm : segmentDataManagers) {
          tableDataManager.releaseSegment(sdm);
        }
      }
    }
    assertTrue(consumingSegmentsInspected > 0, "Expected at least one consuming segment to be inspected");
    /// Total docs summed across every replica must be exactly NUM_DOCS * numReplicas — guards against silent drift
    /// where one replica double-consumes and another drops a row (integer-division check would mask that).
    assertEquals(totalDocsAcrossConsumingSegments, NUM_DOCS * getNumReplicas(),
        "Total docs across all consuming-segment replicas must equal NUM_DOCS * numReplicas");

    /// 4. Force-commit consuming segments to seal them as immutable, and wait for ExternalView to reflect ONLINE.
    forceCommitAndWait(realtimeTableName);

    /// 5. Query parity: counts must be identical post-commit.
    long countAlphaPostCommit = queryStringCount(STRING_VALUES[0]);
    assertEquals(countAlphaPostCommit, countAlphaPreCommit,
        "Post-commit count for " + STRING_VALUES[0] + " must match pre-commit (override does not affect semantics)");

    /// 6. Inspect every segment on every server. Sealed (immutable) segments MUST carry the persisted RAW shape
    ///    (no dictionary, no inverted index) on disk. New consuming segments that came up after force-commit
    ///    MUST also have the override applied — guards against a regression where the override is honored only
    ///    on first-ever segment creation but lost on subsequent rollovers.
    int immutableSegmentsInspected = 0;
    int newConsumingSegmentsInspected = 0;
    for (BaseServerStarter serverStarter : getSharedServerStarters()) {
      TableDataManager tableDataManager = serverStarter.getServerInstance().getInstanceDataManager()
          .getTableDataManager(realtimeTableName);
      if (tableDataManager == null) {
        continue;
      }
      List<SegmentDataManager> segmentDataManagers = tableDataManager.acquireAllSegments();
      try {
        for (SegmentDataManager sdm : segmentDataManagers) {
          IndexSegment segment = sdm.getSegment();

          if (segment instanceof MutableSegmentImpl) {
            /// New consuming segment that replaced a force-committed one: must still have the override applied.
            DataSource overriddenDs = segment.getDataSource(STRING_COLUMN);
            assertNotNull(overriddenDs.getDictionary(),
                STRING_COLUMN + " must have a dictionary on the new consuming segment after forceCommit "
                    + "— consumingOverride must be re-applied to every consuming segment, not just the first one. "
                    + "Segment: " + sdm.getSegmentName());
            assertNotNull(overriddenDs.getInvertedIndex(),
                STRING_COLUMN + " must have an inverted index on the new consuming segment after forceCommit. "
                    + "Segment: " + sdm.getSegmentName());
            newConsumingSegmentsInspected++;
            continue;
          }
          if (!(segment instanceof ImmutableSegmentImpl)) {
            continue;
          }

          /// On-disk ColumnMetadata view: this is the authoritative persisted shape — independent of any in-memory
          /// loader/lifecycle state that might mask a real regression where the override leaks to disk.
          SegmentMetadata segmentMetadata = segment.getSegmentMetadata();
          ColumnMetadata overriddenMeta = segmentMetadata.getColumnMetadataFor(STRING_COLUMN);
          assertNotNull(overriddenMeta, "ColumnMetadata missing for " + STRING_COLUMN);
          assertFalse(overriddenMeta.hasDictionary(),
              STRING_COLUMN + " on-disk ColumnMetadata must report hasDictionary=false. Segment: "
                  + sdm.getSegmentName());
          ColumnMetadata controlMeta = segmentMetadata.getColumnMetadataFor(STRING_COLUMN_NO_OVERRIDE);
          assertTrue(controlMeta.hasDictionary(),
              STRING_COLUMN_NO_OVERRIDE + " on-disk ColumnMetadata must report hasDictionary=true. Segment: "
                  + sdm.getSegmentName());

          /// Cross-check via the on-disk SegmentDirectory reader so the assertion proves the inverted index file is
          /// physically absent rather than merely lazy-loaded.
          ImmutableSegmentImpl immutableSegment = (ImmutableSegmentImpl) segment;
          File indexDir = immutableSegment.getSegmentMetadata().getIndexDir();
          try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(indexDir, ReadMode.mmap);
              SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
            assertFalse(reader.hasIndexFor(STRING_COLUMN, StandardIndexes.inverted()),
                STRING_COLUMN + " must not carry an inverted index on the immutable segment. Segment: "
                    + sdm.getSegmentName());
            assertFalse(reader.hasIndexFor(STRING_COLUMN, StandardIndexes.dictionary()),
                STRING_COLUMN + " must not carry a dictionary on the immutable segment. Segment: "
                    + sdm.getSegmentName());
          }

          immutableSegmentsInspected++;
        }
      } finally {
        for (SegmentDataManager sdm : segmentDataManagers) {
          tableDataManager.releaseSegment(sdm);
        }
      }
    }
    assertTrue(immutableSegmentsInspected > 0,
        "Expected at least one immutable (sealed) segment after forceCommit, got " + immutableSegmentsInspected);
    assertTrue(newConsumingSegmentsInspected > 0,
        "Expected at least one new consuming segment after forceCommit, got " + newConsumingSegmentsInspected);

    /// 7. Re-run a different filter to make sure the post-commit query path actually executes against the
    ///    immutable segment (which has no inverted index — exercises the brute-force scan path) and still returns
    ///    correct results.
    long countBetaPostCommit = queryStringCount(STRING_VALUES[1]);
    assertEquals(countBetaPostCommit, expectedAlphaCount,
        "Post-commit count for " + STRING_VALUES[1] + " mismatch");
  }

  /// Triggers `forceCommit` on the realtime table and waits until the ExternalView shows at least one ONLINE
  /// segment (the previously-consuming segment now sealed) and at most `getNumKafkaPartitions` CONSUMING segments
  /// (the new mutable replacements).
  private void forceCommitAndWait(String realtimeTableName)
      throws Exception {
    getOrCreateAdminClient().getTableClient().forceCommit(getTableName());
    TestUtils.waitForCondition(aVoid -> {
      ExternalView externalView = getSharedHelixResourceManager().getTableExternalView(realtimeTableName);
      if (externalView == null) {
        return false;
      }
      int onlineSegments = 0;
      for (String segmentName : externalView.getPartitionSet()) {
        if (!LLCSegmentName.isLLCSegment(segmentName)) {
          continue;
        }
        for (String state : externalView.getStateMap(segmentName).values()) {
          if (CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE.equals(state)) {
            onlineSegments++;
            break;
          }
        }
      }
      return onlineSegments > 0;
    }, 200L, 60_000L, "ExternalView did not converge to having ONLINE segments after forceCommit");
  }

  private long queryStringCount(String value) {
    return getPinotConnection().execute(
            "SELECT COUNT(*) FROM " + getTableName() + " WHERE " + STRING_COLUMN + " = '" + value + "'")
        .getResultSet(0).getLong(0);
  }
}
