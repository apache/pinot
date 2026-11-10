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
package org.apache.pinot.segment.local.indexsegment.mutable;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.TransformPipeline;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.TableUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.TableUpsertMetadataManagerFactory;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class MutableSegmentImplUpsertTest {
  private static final String SCHEMA_FILE_PATH = "data/test_upsert_schema.json";
  private static final String DATA_FILE_PATH = "data/test_upsert_data.json";
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String TIME_COLUMN = "secondsSinceEpoch";
  private static final String OTHER_COMPARISON_COLUMN = "otherComparisonColumn";
  private static final String PRIMARY_KEY_COLUMN = "event_id";
  private static final String DELETE_RECORD_COLUMN = "is_deleted";

  private MutableSegmentImpl _mutableSegmentImpl;
  private PartitionUpsertMetadataManager _partitionUpsertMetadataManager;

  private UpsertConfig createPartialUpsertConfig(HashFunction hashFunction) {
    UpsertConfig upsertConfigWithHash = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    upsertConfigWithHash.setHashFunction(hashFunction);
    upsertConfigWithHash.setComparisonColumns(List.of(TIME_COLUMN, OTHER_COMPARISON_COLUMN));
    return upsertConfigWithHash;
  }

  private UpsertConfig createFullUpsertConfig(HashFunction hashFunction) {
    UpsertConfig upsertConfigWithHash = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfigWithHash.setHashFunction(hashFunction);
    return upsertConfigWithHash;
  }

  private void setup(UpsertConfig upsertConfigWithHash)
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setUpsertConfig(upsertConfigWithHash)
        .setNullHandlingEnabled(true)
        .build();
    URL schemaResourceUrl = getClass().getClassLoader().getResource(SCHEMA_FILE_PATH);
    assertNotNull(schemaResourceUrl);
    Schema schema = Schema.fromFile(new File(schemaResourceUrl.getFile()));
    TransformPipeline transformPipeline = new TransformPipeline(tableConfig, schema);
    URL dataResourceUrl = getClass().getClassLoader().getResource(DATA_FILE_PATH);
    assertNotNull(dataResourceUrl);
    File jsonFile = new File(dataResourceUrl.getFile());
    TableUpsertMetadataManager tableUpsertMetadataManager =
        TableUpsertMetadataManagerFactory.create(new PinotConfiguration(), tableConfig, schema,
            mock(TableDataManager.class), null);
    _partitionUpsertMetadataManager = tableUpsertMetadataManager.getOrCreatePartitionManager(0);
    _mutableSegmentImpl =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, true, TIME_COLUMN, _partitionUpsertMetadataManager,
            null);

    GenericRow reuse = new GenericRow();
    try (RecordReader recordReader = RecordReaderFactory.getRecordReader(FileFormat.JSON, jsonFile,
        schema.getColumnNames(), null)) {
      while (recordReader.hasNext()) {
        recordReader.next(reuse);
        TransformPipeline.Result result = transformPipeline.processRow(reuse);
        for (GenericRow transformedRow : result.getTransformedRows()) {
          _mutableSegmentImpl.index(transformedRow, null);
        }
        reuse.clear();
      }
    }
  }

  private void tearDown()
      throws IOException {
    if (_mutableSegmentImpl != null) {
      _mutableSegmentImpl.destroy();
      _mutableSegmentImpl = null;
    }
    if (_partitionUpsertMetadataManager != null) {
      _partitionUpsertMetadataManager.stop();
      _partitionUpsertMetadataManager.close();
      _partitionUpsertMetadataManager = null;
    }
  }

  @Test
  public void testHashFunctions()
      throws Exception {
    testUpsertIngestion(createFullUpsertConfig(HashFunction.NONE));
    testUpsertIngestion(createFullUpsertConfig(HashFunction.MD5));
    testUpsertIngestion(createFullUpsertConfig(HashFunction.MURMUR3));
  }

  /// The upsert metadata manager captures the primary key columns once, when the server starts. A consuming
  /// segment created later picks up whatever schema is current, which may list a different set. Both must key
  /// records identically, otherwise the same row hashes under two different keys and neither invalidates the other.
  @Test
  public void testPrimaryKeyColumnsTakenFromUpsertContext()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setUpsertConfig(createFullUpsertConfig(HashFunction.MURMUR3))
        .setNullHandlingEnabled(true)
        .build();
    URL schemaResourceUrl = getClass().getClassLoader().getResource(SCHEMA_FILE_PATH);
    assertNotNull(schemaResourceUrl);
    // Schema as it was when the metadata manager was created: a single primary key column.
    Schema managerSchema = Schema.fromFile(new File(schemaResourceUrl.getFile()));
    // Schema the consuming segment was created with: a second column has since joined the primary key.
    Schema segmentSchema = Schema.fromFile(new File(schemaResourceUrl.getFile()));
    segmentSchema.setPrimaryKeyColumns(List.of(PRIMARY_KEY_COLUMN, OTHER_COMPARISON_COLUMN));

    TableUpsertMetadataManager tableUpsertMetadataManager =
        TableUpsertMetadataManagerFactory.create(new PinotConfiguration(), tableConfig, managerSchema,
            mock(TableDataManager.class), null);
    PartitionUpsertMetadataManager partitionUpsertMetadataManager =
        tableUpsertMetadataManager.getOrCreatePartitionManager(0);
    MutableSegmentImpl mutableSegment =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(segmentSchema, true, TIME_COLUMN,
            partitionUpsertMetadataManager, null);
    try {
      // Two records sharing event_id, differing only on the column that the segment schema treats as part of the
      // key but the metadata manager does not.
      mutableSegment.index(createRow("aa", 1L, 1567205396L), null);
      mutableSegment.index(createRow("aa", 2L, 1567205397L), null);

      // Keyed on the manager's columns these are the same record, so the older doc must have been invalidated.
      ImmutableRoaringBitmap bitmap = mutableSegment.getValidDocIds().getMutableRoaringBitmap();
      assertEquals(bitmap.getCardinality(), 1);
      assertFalse(bitmap.contains(0));
      assertTrue(bitmap.contains(1));
    } finally {
      mutableSegment.destroy();
      partitionUpsertMetadataManager.stop();
      partitionUpsertMetadataManager.close();
    }
  }

  private static GenericRow createRow(String eventId, long otherComparisonValue, long timeValue) {
    GenericRow row = new GenericRow();
    row.putValue(PRIMARY_KEY_COLUMN, eventId);
    row.putValue("description", "d");
    row.putValue(OTHER_COMPARISON_COLUMN, otherComparisonValue);
    row.putValue(TIME_COLUMN, timeValue);
    return row;
  }

  /// Verifies that loading an immutable segment with a newer comparison value while a mutable segment is actively
  /// consuming invalidates the late event's docId in the consuming segment's `_validDocIds` and `_queryableDocIds`,
  /// and that subsequent consumption with an even newer comparison value invalidates the immutable segment's docId.
  @Test
  public void testSegmentLoadDuringConsumptionUpdatesValidDocIds()
      throws Exception {
    UpsertConfig upsertConfig = createFullUpsertConfig(HashFunction.NONE);
    upsertConfig.setDeleteRecordColumn(DELETE_RECORD_COLUMN);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setUpsertConfig(upsertConfig)
        .setNullHandlingEnabled(true)
        .build();
    URL schemaResourceUrl = getClass().getClassLoader().getResource(SCHEMA_FILE_PATH);
    assertNotNull(schemaResourceUrl);
    Schema schema = Schema.fromFile(new File(schemaResourceUrl.getFile()));
    schema.addField(new DimensionFieldSpec(DELETE_RECORD_COLUMN, DataType.BOOLEAN, true));

    TableUpsertMetadataManager tableUpsertMetadataManager =
        TableUpsertMetadataManagerFactory.create(new PinotConfiguration(), tableConfig, schema,
            mock(TableDataManager.class), null);
    PartitionUpsertMetadataManager partitionUpsertMetadataManager =
        tableUpsertMetadataManager.getOrCreatePartitionManager(0);
    MutableSegmentImpl mutableSegment =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, true, TIME_COLUMN,
            partitionUpsertMetadataManager, null);
    File tempDir = Files.createTempDirectory("MutableSegmentImplUpsertTest").toFile();
    ImmutableSegmentImpl immutableSegment = null;
    try {
      // Consuming segment indexes late events for "aa" (docId 0, ts=1567205394) and "cc" (docId 2, ts=1567205394),
      // and a newer event for "bb" (docId 1, ts=1567205398).
      mutableSegment.index(createRowWithDelete("aa", 1L, 1567205394L, false), null);
      mutableSegment.index(createRowWithDelete("bb", 1L, 1567205398L, false), null);
      mutableSegment.index(createRowWithDelete("cc", 1L, 1567205394L, false), null);
      ImmutableRoaringBitmap mutableValidDocIds = mutableSegment.getValidDocIds().getMutableRoaringBitmap();
      ImmutableRoaringBitmap mutableQueryableDocIds = mutableSegment.getQueryableDocIds().getMutableRoaringBitmap();
      assertEquals(mutableValidDocIds.toArray(), new int[]{0, 1, 2});
      assertEquals(mutableQueryableDocIds.toArray(), new int[]{0, 1, 2});

      // Build and load an immutable segment where "aa" has a newer timestamp (1567205396), "bb" has an older
      // timestamp (1567205395), and "cc" is a newer delete record (1567205396, isDeleted=true).
      List<GenericRow> immutableRows = List.of(
          createRowWithDelete("aa", 1L, 1567205396L, false),
          createRowWithDelete("bb", 1L, 1567205395L, false),
          createRowWithDelete("cc", 1L, 1567205396L, true));
      String segmentName = "testTable__0__1__20260101T0000Z";
      SegmentGeneratorConfig generatorConfig = new SegmentGeneratorConfig(tableConfig, schema);
      generatorConfig.setOutDir(tempDir.getAbsolutePath());
      generatorConfig.setSegmentName(segmentName);
      generatorConfig.setTableName(RAW_TABLE_NAME);
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(generatorConfig, new GenericRowRecordReader(immutableRows));
      driver.build();
      immutableSegment =
          (ImmutableSegmentImpl) ImmutableSegmentLoader.load(new File(tempDir, segmentName), ReadMode.mmap);

      partitionUpsertMetadataManager.addSegment(immutableSegment);

      // "aa" (doc 0) and "cc" (doc 2) in the consuming segment must be invalidated by the newly loaded immutable
      // segment, while "bb" (doc 1) in the consuming segment remains valid.
      mutableValidDocIds = mutableSegment.getValidDocIds().getMutableRoaringBitmap();
      mutableQueryableDocIds = mutableSegment.getQueryableDocIds().getMutableRoaringBitmap();
      assertEquals(mutableValidDocIds.toArray(), new int[]{1});
      assertEquals(mutableQueryableDocIds.toArray(), new int[]{1});

      ImmutableRoaringBitmap immutableValidDocIds = immutableSegment.getValidDocIds().getMutableRoaringBitmap();
      ImmutableRoaringBitmap immutableQueryableDocIds = immutableSegment.getQueryableDocIds().getMutableRoaringBitmap();
      assertEquals(immutableValidDocIds.toArray(), new int[]{0, 2});
      assertEquals(immutableQueryableDocIds.toArray(), new int[]{0});

      // Subsequent out-of-order event in the consuming segment for "aa" (docId 3, ts=1567205395) stays invalid.
      mutableSegment.index(createRowWithDelete("aa", 1L, 1567205395L, false), null);
      assertEquals(mutableSegment.getValidDocIds().getMutableRoaringBitmap().toArray(), new int[]{1});
      assertEquals(mutableSegment.getQueryableDocIds().getMutableRoaringBitmap().toArray(), new int[]{1});
      assertEquals(immutableSegment.getValidDocIds().getMutableRoaringBitmap().toArray(), new int[]{0, 2});
      assertEquals(immutableSegment.getQueryableDocIds().getMutableRoaringBitmap().toArray(), new int[]{0});

      // Subsequent consumption of a newer event for "aa" (docId 4, ts=1567205399) invalidates doc 0 in the immutable
      // segment and makes doc 4 valid and queryable in the consuming segment.
      mutableSegment.index(createRowWithDelete("aa", 1L, 1567205399L, false), null);
      assertEquals(mutableSegment.getValidDocIds().getMutableRoaringBitmap().toArray(), new int[]{1, 4});
      assertEquals(mutableSegment.getQueryableDocIds().getMutableRoaringBitmap().toArray(), new int[]{1, 4});
      assertEquals(immutableSegment.getValidDocIds().getMutableRoaringBitmap().toArray(), new int[]{2});
      assertTrue(immutableSegment.getQueryableDocIds().getMutableRoaringBitmap().isEmpty());
    } finally {
      mutableSegment.destroy();
      if (immutableSegment != null) {
        partitionUpsertMetadataManager.removeSegment(immutableSegment);
        immutableSegment.destroy();
      }
      partitionUpsertMetadataManager.stop();
      partitionUpsertMetadataManager.close();
      FileUtils.deleteQuietly(tempDir);
    }
  }

  private static GenericRow createRowWithDelete(String eventId, long otherComparisonValue, long timeValue,
      boolean isDeleted) {
    GenericRow row = createRow(eventId, otherComparisonValue, timeValue);
    row.putValue(DELETE_RECORD_COLUMN, isDeleted ? 1 : 0);
    return row;
  }

  @Test
  public void testMultipleComparisonColumns()
      throws Exception {
    testUpsertIngestion(createPartialUpsertConfig(HashFunction.NONE));
    testUpsertIngestion(createPartialUpsertConfig(HashFunction.MD5));
    testUpsertIngestion(createPartialUpsertConfig(HashFunction.MURMUR3));
  }

  private void testUpsertIngestion(UpsertConfig upsertConfig)
      throws Exception {
    setup(upsertConfig);
    try {
      ImmutableRoaringBitmap bitmap = _mutableSegmentImpl.getValidDocIds().getMutableRoaringBitmap();
      if (upsertConfig.getComparisonColumns() == null) {
        // aa
        assertFalse(bitmap.contains(0));
        assertTrue(bitmap.contains(1));
        assertFalse(bitmap.contains(2));
        assertFalse(bitmap.contains(3));
        // bb
        assertFalse(bitmap.contains(4));
        assertTrue(bitmap.contains(5));
        assertFalse(bitmap.contains(6));
      } else {
        // aa
        assertFalse(bitmap.contains(0));
        assertFalse(bitmap.contains(1));
        assertTrue(bitmap.contains(2));
        assertFalse(bitmap.contains(3));
        // Confirm that both comparison column values have made it into the persisted upserted doc
        assertEquals(_mutableSegmentImpl.getValue(2, TIME_COLUMN), 1567205397L);
        assertEquals(_mutableSegmentImpl.getValue(2, OTHER_COMPARISON_COLUMN), 1567205395L);
        assertFalse(_mutableSegmentImpl.getDataSource(TIME_COLUMN).getNullValueVector().isNull(2));
        assertFalse(_mutableSegmentImpl.getDataSource(OTHER_COMPARISON_COLUMN).getNullValueVector().isNull(2));

        // bb
        assertFalse(bitmap.contains(4));
        assertTrue(bitmap.contains(5));
        assertFalse(bitmap.contains(6));
        // Confirm that comparison column values have made it into the persisted upserted doc
        assertEquals(_mutableSegmentImpl.getValue(5, TIME_COLUMN), 1567205396L);
        assertEquals(_mutableSegmentImpl.getValue(5, OTHER_COMPARISON_COLUMN), Long.MIN_VALUE);
        assertFalse(_mutableSegmentImpl.getDataSource(TIME_COLUMN).getNullValueVector().isNull(5));
        assertTrue(_mutableSegmentImpl.getDataSource(OTHER_COMPARISON_COLUMN).getNullValueVector().isNull(5));
      }
    } finally {
      tearDown();
    }
  }
}
