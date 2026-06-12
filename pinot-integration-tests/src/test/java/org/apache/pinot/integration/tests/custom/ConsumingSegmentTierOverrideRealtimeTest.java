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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
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
import org.apache.pinot.spi.stream.StreamConfigProperties;
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
  private static final String TIME_COLUMN = "tsMillis";

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
    return NUM_DOCS * 1000;
  }

  @Override
  protected Map<String, String> getStreamConfigMap() {
    Map<String, String> streamConfigMap = new HashMap<>(super.getStreamConfigMap());
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_TIME, "365d");
    return streamConfigMap;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(PROFILED_STRING_COLUMN, FieldSpec.DataType.STRING)
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
  protected List<FieldConfig> getFieldConfigs() {
    return List.of(new FieldConfig.Builder(PROFILED_STRING_COLUMN)
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withTierOverwrites(jsonNode("{\"consuming\":{\"encodingType\":\"DICTIONARY\","
            + "\"indexes\":{\"inverted\":{\"disabled\":false}}}}"))
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
    org.apache.avro.Schema longSchema = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG);
    avroSchema.setFields(List.of(
        new org.apache.avro.Schema.Field(PROFILED_STRING_COLUMN, stringSchema, null, null),
        new org.apache.avro.Schema.Field(TIME_COLUMN, longSchema, null, null)));

    long now = System.currentTimeMillis();
    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      List<DataFileWriter<GenericData.Record>> writers = avroFilesAndWriters.getWriters();
      for (int i = 0; i < NUM_DOCS; i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(PROFILED_STRING_COLUMN, "value-" + i % 4);
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

    int consumingSegmentsInspected = inspectConsumingSegments(realtimeTableName);
    assertTrue(consumingSegmentsInspected > 0,
        "Expected at least one consuming segment before forceCommit, got " + consumingSegmentsInspected);

    forceCommitAndWait(realtimeTableName);
  }

  private int inspectConsumingSegments(String realtimeTableName)
      throws Exception {
    return inspectSegments(realtimeTableName, MutableSegmentImpl.class, segment -> {
      DataSource profiled = segment.getDataSource(PROFILED_STRING_COLUMN);
      assertNotNull(profiled.getDictionary(),
          PROFILED_STRING_COLUMN + " must have a consuming-segment dictionary");
      assertNotNull(profiled.getInvertedIndex(),
          PROFILED_STRING_COLUMN + " must have a consuming-segment inverted index");
    });
  }

  private int inspectImmutableSegments(String realtimeTableName)
      throws Exception {
    return inspectSegments(realtimeTableName, ImmutableSegmentImpl.class, segment -> {
      SegmentMetadata metadata = segment.getSegmentMetadata();
      ColumnMetadata profiledMetadata = metadata.getColumnMetadataFor(PROFILED_STRING_COLUMN);
      assertNotNull(profiledMetadata, "Missing metadata for " + PROFILED_STRING_COLUMN);
      assertFalse(profiledMetadata.hasDictionary(),
          PROFILED_STRING_COLUMN + " must remain RAW/no-dictionary after commit");
      assertEquals(profiledMetadata.getForwardIndexEncoding(), FieldConfig.EncodingType.RAW,
          PROFILED_STRING_COLUMN + " must persist with RAW forward-index encoding after commit");

      try (SegmentDirectory directory = new SegmentLocalFSDirectory(metadata.getIndexDir(), ReadMode.mmap);
          SegmentDirectory.Reader reader = directory.createReader()) {
        assertTrue(reader.hasIndexFor(PROFILED_STRING_COLUMN, StandardIndexes.forward()),
            PROFILED_STRING_COLUMN + " RAW forward index must be persisted");
        assertFalse(reader.hasIndexFor(PROFILED_STRING_COLUMN, StandardIndexes.dictionary()),
            PROFILED_STRING_COLUMN + " dictionary must not be persisted");
        assertFalse(reader.hasIndexFor(PROFILED_STRING_COLUMN, StandardIndexes.inverted()),
            PROFILED_STRING_COLUMN + " inverted index must not be persisted");
      }
    });
  }

  private int inspectSegments(String realtimeTableName, Class<?> segmentType, SegmentAssertion assertion)
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
          if (!segmentType.isInstance(segment)) {
            continue;
          }
          assertion.accept(segment);
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
    String response = getOrCreateAdminClient().getTableClient().forceCommit(realtimeTableName);
    String jobId = JsonUtils.stringToJsonNode(response).get("forceCommitJobId").asText();

    TestUtils.waitForCondition(aVoid -> isForceCommitComplete(jobId), 120_000L,
        "Timed out waiting for forceCommit job: " + jobId);
    TestUtils.waitForCondition(aVoid -> {
      try {
        return inspectImmutableSegments(realtimeTableName) > 0;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 120_000L, "Timed out waiting for force-committed segments to load as immutable");
  }

  private static JsonNode jsonNode(String json) {
    try {
      return JsonUtils.stringToJsonNode(json);
    } catch (IOException e) {
      throw new IllegalStateException("Invalid test JSON: " + json, e);
    }
  }

  private boolean isForceCommitComplete(String jobId) {
    try {
      String jobStatusResponse = getOrCreateAdminClient().getTableClient().getForceCommitJobStatus(jobId);
      JsonNode jobStatus = JsonUtils.stringToJsonNode(jobStatusResponse);
      return jobStatus.get(CommonConstants.ControllerJob.NUM_CONSUMING_SEGMENTS_YET_TO_BE_COMMITTED).asInt(-1) == 0;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private interface SegmentAssertion {
    void accept(IndexSegment segment)
        throws Exception;
  }
}
