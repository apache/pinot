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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
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
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// End-to-end coverage for the synthetic `consuming` tier override on a realtime table.
///
/// The table persists `profiledString` as RAW with no inverted index. `tierOverwrites.consuming` upgrades that column
/// to DICTIONARY + INVERTED only while the segment is mutable and consuming. After force-commit, the immutable segment
/// must retain the persisted RAW/no-inverted shape on disk.
@Test(suiteName = "CustomClusterIntegrationTest")
public class ConsumingSegmentTierOverrideRealtimeTest extends CustomDataQueryClusterIntegrationTest {
  private static final String TABLE_NAME = "ConsumingSegmentTierOverrideRealtimeTest";
  private static final int NUM_DOCS = 200;
  private static final String PROFILED_STRING_COLUMN = "profiledString";
  private static final String CONTROL_STRING_COLUMN = "controlString";
  private static final String INT_COLUMN = "intCol";
  private static final String TIME_COLUMN = "tsMillis";
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
    return NUM_DOCS * 10;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(PROFILED_STRING_COLUMN, FieldSpec.DataType.STRING)
        .addSingleValueDimension(CONTROL_STRING_COLUMN, FieldSpec.DataType.STRING)
        .addMetric(INT_COLUMN, FieldSpec.DataType.INT)
        .addDateTimeField(TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
  }

  @Override
  public String getTimeColumnName() {
    return TIME_COLUMN;
  }

  @Override
  protected String getSortedColumn() {
    return null;
  }

  @Override
  protected List<String> getNoDictionaryColumns() {
    return List.of(PROFILED_STRING_COLUMN);
  }

  @Override
  protected List<String> getInvertedIndexColumns() {
    return List.of();
  }

  @Override
  protected List<FieldConfig> getFieldConfigs() {
    return List.of(new FieldConfig.Builder(PROFILED_STRING_COLUMN)
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withTierOverwrites(jsonNode("{\"consuming\":{\"encodingType\":\"DICTIONARY\","
            + "\"indexes\":{\"inverted\":{\"enabled\":true}}}}"))
        .build());
  }

  @Override
  protected TableConfig createRealtimeTableConfig(File sampleAvroFile) {
    AvroFileSchemaKafkaAvroMessageDecoder._avroFile = sampleAvroFile;
    return getTableConfigBuilder(TableType.REALTIME)
        .setTierOverwrites(jsonNode("{\"consuming\":{\"noDictionaryColumns\":[]}}"))
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
        new org.apache.avro.Schema.Field(PROFILED_STRING_COLUMN, stringSchema, null, null),
        new org.apache.avro.Schema.Field(CONTROL_STRING_COLUMN, stringSchema, null, null),
        new org.apache.avro.Schema.Field(INT_COLUMN, intSchema, null, null),
        new org.apache.avro.Schema.Field(TIME_COLUMN, longSchema, null, null)));

    long now = System.currentTimeMillis();
    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      List<DataFileWriter<GenericData.Record>> writers = avroFilesAndWriters.getWriters();
      for (int i = 0; i < NUM_DOCS; i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(PROFILED_STRING_COLUMN, STRING_VALUES[i % STRING_VALUES.length]);
        record.put(CONTROL_STRING_COLUMN, "fixed");
        record.put(INT_COLUMN, i);
        record.put(TIME_COLUMN, now + i);
        writers.get(i % getNumAvroFiles()).append(record);
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  @Test
  public void testConsumingSegmentTierOverrideEndToEnd()
      throws Exception {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    waitForAllDocsLoaded(60_000L);

    long alphaCountBeforeCommit = queryStringCount();
    assertEquals(alphaCountBeforeCommit, (NUM_DOCS + STRING_VALUES.length - 1) / STRING_VALUES.length);

    int consumingSegmentsInspected = inspectConsumingSegments(realtimeTableName);
    assertTrue(consumingSegmentsInspected > 0,
        "Expected at least one consuming segment before forceCommit, got " + consumingSegmentsInspected);

    forceCommitAndWait(realtimeTableName);
    long alphaCountAfterCommit = queryStringCount();
    assertEquals(alphaCountAfterCommit, alphaCountBeforeCommit,
        "Consuming tier override must not change query semantics after commit");

    int immutableSegmentsInspected = inspectImmutableSegments(realtimeTableName);
    assertTrue(immutableSegmentsInspected > 0,
        "Expected at least one immutable segment after forceCommit, got " + immutableSegmentsInspected);
  }

  private int inspectConsumingSegments(String realtimeTableName) {
    int inspected = 0;
    for (BaseServerStarter serverStarter : getSharedServerStarters()) {
      TableDataManager tableDataManager = serverStarter.getServerInstance().getInstanceDataManager()
          .getTableDataManager(realtimeTableName);
      assertNotNull(tableDataManager, "Missing table data manager on server");
      List<SegmentDataManager> segmentDataManagers = tableDataManager.acquireAllSegments();
      try {
        for (SegmentDataManager segmentDataManager : segmentDataManagers) {
          IndexSegment segment = segmentDataManager.getSegment();
          if (!(segment instanceof MutableSegmentImpl)) {
            continue;
          }
          DataSource profiled = segment.getDataSource(PROFILED_STRING_COLUMN);
          assertNotNull(profiled.getDictionary(),
              PROFILED_STRING_COLUMN + " must have a consuming-segment dictionary");
          assertNotNull(profiled.getInvertedIndex(),
              PROFILED_STRING_COLUMN + " must have a consuming-segment inverted index");

          DataSource control = segment.getDataSource(CONTROL_STRING_COLUMN);
          assertNotNull(control.getDictionary(), CONTROL_STRING_COLUMN + " should keep default dictionary encoding");
          inspected++;
        }
      } finally {
        for (SegmentDataManager segmentDataManager : segmentDataManagers) {
          tableDataManager.releaseSegment(segmentDataManager);
        }
      }
    }
    return inspected;
  }

  private int inspectImmutableSegments(String realtimeTableName)
      throws Exception {
    int inspected = 0;
    for (BaseServerStarter serverStarter : getSharedServerStarters()) {
      TableDataManager tableDataManager = serverStarter.getServerInstance().getInstanceDataManager()
          .getTableDataManager(realtimeTableName);
      assertNotNull(tableDataManager, "Missing table data manager on server");
      List<SegmentDataManager> segmentDataManagers = tableDataManager.acquireAllSegments();
      try {
        for (SegmentDataManager segmentDataManager : segmentDataManagers) {
          IndexSegment segment = segmentDataManager.getSegment();
          if (!(segment instanceof ImmutableSegmentImpl)) {
            continue;
          }
          SegmentMetadata metadata = segment.getSegmentMetadata();
          ColumnMetadata profiledMetadata = metadata.getColumnMetadataFor(PROFILED_STRING_COLUMN);
          assertNotNull(profiledMetadata, "Missing metadata for " + PROFILED_STRING_COLUMN);
          assertFalse(profiledMetadata.hasDictionary(),
              PROFILED_STRING_COLUMN + " must remain RAW/no-dictionary after commit");
          assertEquals(profiledMetadata.getForwardIndexEncoding(), FieldConfig.EncodingType.RAW,
              PROFILED_STRING_COLUMN + " must persist with RAW forward-index encoding after commit");

          ColumnMetadata controlMetadata = metadata.getColumnMetadataFor(CONTROL_STRING_COLUMN);
          assertNotNull(controlMetadata, "Missing metadata for " + CONTROL_STRING_COLUMN);
          assertTrue(controlMetadata.hasDictionary(), CONTROL_STRING_COLUMN + " must keep default dictionary encoding");

          try (SegmentDirectory directory = new SegmentLocalFSDirectory(metadata.getIndexDir(), ReadMode.mmap);
              SegmentDirectory.Reader reader = directory.createReader()) {
            assertTrue(reader.hasIndexFor(PROFILED_STRING_COLUMN, StandardIndexes.forward()),
                PROFILED_STRING_COLUMN + " RAW forward index must be persisted");
            assertFalse(reader.hasIndexFor(PROFILED_STRING_COLUMN, StandardIndexes.dictionary()),
                PROFILED_STRING_COLUMN + " dictionary must not be persisted");
            assertFalse(reader.hasIndexFor(PROFILED_STRING_COLUMN, StandardIndexes.inverted()),
                PROFILED_STRING_COLUMN + " inverted index must not be persisted");
          }
          inspected++;
        }
      } finally {
        for (SegmentDataManager segmentDataManager : segmentDataManagers) {
          tableDataManager.releaseSegment(segmentDataManager);
        }
      }
    }
    return inspected;
  }

  private void forceCommitAndWait(String realtimeTableName)
      throws Exception {
    Set<String> consumingSegments = getConsumingSegments(realtimeTableName);
    assertFalse(consumingSegments.isEmpty(), "Expected consuming segments before forceCommit");
    String response = getOrCreateAdminClient().getTableClient().forceCommit(realtimeTableName);
    String jobId = JsonUtils.stringToJsonNode(response).get("forceCommitJobId").asText();

    TestUtils.waitForCondition(aVoid -> {
      try {
        String jobStatusResponse = getOrCreateAdminClient().getTableClient().getForceCommitJobStatus(jobId);
        JsonNode jobStatus = JsonUtils.stringToJsonNode(jobStatusResponse);
        return jobStatus.get(CommonConstants.ControllerJob.NUM_CONSUMING_SEGMENTS_YET_TO_BE_COMMITTED).asInt(-1) == 0;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 120_000L, "Timed out waiting for forceCommit job: " + jobId);

    TestUtils.waitForCondition(aVoid -> {
      for (String segmentName : consumingSegments) {
        SegmentZKMetadata metadata = getSharedHelixResourceManager().getSegmentZKMetadata(realtimeTableName,
            segmentName);
        if (metadata == null || metadata.getStatus() != CommonConstants.Segment.Realtime.Status.DONE) {
          return false;
        }
      }
      return true;
    }, 120_000L, "Timed out waiting for force-committed segments to become DONE");

    TestUtils.waitForCondition(aVoid -> areSegmentsImmutableOnServers(realtimeTableName, consumingSegments),
        120_000L, "Timed out waiting for force-committed segments to load as immutable on servers");
  }

  private boolean areSegmentsImmutableOnServers(String realtimeTableName, Set<String> segmentNames) {
    Set<String> immutableSegments = new HashSet<>();
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
          if (segment instanceof ImmutableSegmentImpl && segmentNames.contains(segment.getSegmentName())) {
            immutableSegments.add(segment.getSegmentName());
          }
        }
      } finally {
        for (SegmentDataManager segmentDataManager : segmentDataManagers) {
          tableDataManager.releaseSegment(segmentDataManager);
        }
      }
    }
    return immutableSegments.containsAll(segmentNames);
  }

  private Set<String> getConsumingSegments(String realtimeTableName) {
    IdealState idealState = getSharedHelixResourceManager().getTableIdealState(realtimeTableName);
    Set<String> consumingSegments = new HashSet<>();
    if (idealState == null) {
      return consumingSegments;
    }
    for (String segmentName : idealState.getPartitionSet()) {
      if (!LLCSegmentName.isLLCSegment(segmentName)) {
        continue;
      }
      Map<String, String> stateMap = idealState.getInstanceStateMap(segmentName);
      if (stateMap != null && stateMap.containsValue(CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING)) {
        consumingSegments.add(segmentName);
      }
    }
    return consumingSegments;
  }

  private long queryStringCount() {
    return Long.parseLong(getPinotConnection()
        .execute("SELECT COUNT(*) FROM " + getTableName() + " WHERE " + PROFILED_STRING_COLUMN + " = '"
            + STRING_VALUES[0] + "'")
        .getResultSet(0).getString(0));
  }

  private static JsonNode jsonNode(String json) {
    try {
      return JsonUtils.stringToJsonNode(json);
    } catch (IOException e) {
      throw new IllegalStateException("Invalid test JSON: " + json, e);
    }
  }
}
