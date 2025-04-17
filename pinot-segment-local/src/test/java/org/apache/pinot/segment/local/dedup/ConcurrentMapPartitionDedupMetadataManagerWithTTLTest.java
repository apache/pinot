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
package org.apache.pinot.segment.local.dedup;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.local.segment.readers.PrimaryKeyReader;
import org.apache.pinot.segment.local.utils.HashUtils;
import org.apache.pinot.segment.local.utils.WatermarkUtils;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class ConcurrentMapPartitionDedupMetadataManagerWithTTLTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(),
      ConcurrentMapPartitionDedupMetadataManagerWithTTLTest.class.getSimpleName());
  private static final int METADATA_TTL = 10000;
  private static final String DEDUP_TIME_COLUMN_NAME = "dedupTimeColumn";
  private DedupContext.Builder _dedupContextBuilder;

  @BeforeMethod
  public void setUpContextBuilder()
      throws IOException {
    FileUtils.forceMkdir(TEMP_DIR);
    TableDataManager tableDataManager = mock(TableDataManager.class);
    when(tableDataManager.getTableDataDir()).thenReturn(TEMP_DIR);
    _dedupContextBuilder = new DedupContext.Builder()
        .setTableConfig(mock(TableConfig.class))
        .setSchema(mock(Schema.class))
        .setTableDataManager(tableDataManager)
        .setPrimaryKeyColumns(List.of("primaryKeyColumn"))
        .setMetadataTTL(METADATA_TTL)
        .setDedupTimeColumn(DEDUP_TIME_COLUMN_NAME);
  }

  @AfterMethod
  public void cleanup() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  @Test
  public void creatingMetadataManagerThrowsExceptions() {
    TableDataManager tableDataManager = mock(TableDataManager.class);
    when(tableDataManager.getTableDataDir()).thenReturn(TEMP_DIR);
    DedupContext dedupContext = new DedupContext.Builder()
        .setTableConfig(mock(TableConfig.class))
        .setSchema(mock(Schema.class))
        .setTableDataManager(tableDataManager)
        .setPrimaryKeyColumns(List.of("primaryKeyColumn"))
        .setHashFunction(HashFunction.NONE)
        .setMetadataTTL(1)
        .setDedupTimeColumn(null)
        .build();
    assertThrows(IllegalArgumentException.class,
        () -> new ConcurrentMapPartitionDedupMetadataManager(DedupTestUtils.REALTIME_TABLE_NAME, 0, dedupContext));
  }

  @Test
  public void testRemoveExpiredPrimaryKeys()
      throws IOException {
    verifyRemoveExpiredPrimaryKeys(HashFunction.NONE);
    verifyRemoveExpiredPrimaryKeys(HashFunction.MD5);
    verifyRemoveExpiredPrimaryKeys(HashFunction.MURMUR3);
  }

  private void verifyRemoveExpiredPrimaryKeys(HashFunction hashFunction)
      throws IOException {
    _dedupContextBuilder.setHashFunction(hashFunction);
    ConcurrentMapPartitionDedupMetadataManager metadataManager =
        new ConcurrentMapPartitionDedupMetadataManager(DedupTestUtils.REALTIME_TABLE_NAME, 0,
            _dedupContextBuilder.build());

    IndexSegment segment = Mockito.mock(IndexSegment.class);
    for (int i = 0; i < 20; i++) {
      double time = i * 1000;
      Object primaryKeyKey = HashUtils.hashPrimaryKey(DedupTestUtils.getPrimaryKey(i), hashFunction);
      metadataManager._primaryKeyToSegmentAndTimeMap.computeIfAbsent(primaryKeyKey, k -> Pair.of(segment, time));
    }
    metadataManager._largestSeenTime.set(19000);
    assertEquals(metadataManager._primaryKeyToSegmentAndTimeMap.size(), 20);
    verifyInMemoryState(metadataManager, 0, 20, segment, hashFunction);

    metadataManager.removeExpiredPrimaryKeys();
    assertEquals(metadataManager.getNumPrimaryKeys(), 11);
    assertEquals(metadataManager._primaryKeyToSegmentAndTimeMap.size(), 11);
    verifyInMemoryState(metadataManager, 9, 11, segment, hashFunction);

    metadataManager.stop();
    metadataManager.close();
  }

  @Test
  public void testAddRemoveTheSameSegment()
      throws IOException {
    verifyAddRemoveTheSameSegment(HashFunction.NONE);
    verifyAddRemoveTheSameSegment(HashFunction.MD5);
    verifyAddRemoveTheSameSegment(HashFunction.MURMUR3);
  }

  private void verifyAddRemoveTheSameSegment(HashFunction hashFunction)
      throws IOException {
    _dedupContextBuilder.setHashFunction(hashFunction);
    ConcurrentMapPartitionDedupMetadataManager metadataManager =
        new ConcurrentMapPartitionDedupMetadataManager(DedupTestUtils.REALTIME_TABLE_NAME, 0,
            _dedupContextBuilder.build());

    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader = generateDedupRecordInfoReader(10, 0);
    Iterator<DedupRecordInfo> dedupRecordInfoIterator =
        DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader, 10);
    IndexSegment segment = DedupTestUtils.mockSegment(1, 10);
    metadataManager.doAddOrReplaceSegment(null, segment, dedupRecordInfoIterator);
    verifyInitialSegmentAddition(metadataManager, segment, hashFunction);

    dedupRecordInfoIterator = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader, 10);
    metadataManager.doRemoveSegment(segment, dedupRecordInfoIterator);
    assertEquals(metadataManager._primaryKeyToSegmentAndTimeMap.size(), 0);

    metadataManager.stop();
    metadataManager.close();
  }

  @Test
  public void verifyAddingTwoSegmentWithSamePrimaryKeys()
      throws IOException {
    verifyAddingTwoSegmentWithSamePrimaryKeys(HashFunction.NONE);
    verifyAddingTwoSegmentWithSamePrimaryKeys(HashFunction.MD5);
    verifyAddingTwoSegmentWithSamePrimaryKeys(HashFunction.MURMUR3);
  }

  private void verifyAddingTwoSegmentWithSamePrimaryKeys(HashFunction hashFunction)
      throws IOException {
    _dedupContextBuilder.setHashFunction(hashFunction);
    ConcurrentMapPartitionDedupMetadataManager metadataManager =
        new ConcurrentMapPartitionDedupMetadataManager(DedupTestUtils.REALTIME_TABLE_NAME, 0,
            _dedupContextBuilder.build());

    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader = generateDedupRecordInfoReader(10, 0);
    IndexSegment segment = DedupTestUtils.mockSegment(1, 10);
    Iterator<DedupRecordInfo> dedupRecordInfoIterator =
        DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader, 10);
    metadataManager.doAddOrReplaceSegment(null, segment, dedupRecordInfoIterator);
    verifyInitialSegmentAddition(metadataManager, segment, hashFunction);

    IndexSegment segment2 = DedupTestUtils.mockSegment(2, 10);
    dedupRecordInfoIterator = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader, 10);
    metadataManager.doAddOrReplaceSegment(segment, segment2, dedupRecordInfoIterator);
    verifyInitialSegmentAddition(metadataManager, segment2, hashFunction);

    metadataManager.stop();
    metadataManager.close();
  }

  @Test
  public void testRemoveAnotherSegmentWithTheSamePrimaryKeys()
      throws IOException {
    verifyRemoveAnotherSegmentWithTheSamePrimaryKeys(HashFunction.NONE);
    verifyRemoveAnotherSegmentWithTheSamePrimaryKeys(HashFunction.MD5);
    verifyRemoveAnotherSegmentWithTheSamePrimaryKeys(HashFunction.MURMUR3);
  }

  private void verifyRemoveAnotherSegmentWithTheSamePrimaryKeys(HashFunction hashFunction)
      throws IOException {
    _dedupContextBuilder.setHashFunction(hashFunction);
    ConcurrentMapPartitionDedupMetadataManager metadataManager =
        new ConcurrentMapPartitionDedupMetadataManager(DedupTestUtils.REALTIME_TABLE_NAME, 0,
            _dedupContextBuilder.build());

    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader = generateDedupRecordInfoReader(10, 0);
    IndexSegment segment = DedupTestUtils.mockSegment(1, 10);
    Iterator<DedupRecordInfo> dedupRecordInfoIterator =
        DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader, 10);
    metadataManager.doAddOrReplaceSegment(null, segment, dedupRecordInfoIterator);
    verifyInitialSegmentAddition(metadataManager, segment, hashFunction);

    IndexSegment segment2 = DedupTestUtils.mockSegment(2, 10);
    dedupRecordInfoIterator = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader, 10);
    metadataManager.doRemoveSegment(segment2, dedupRecordInfoIterator);
    verifyInitialSegmentAddition(metadataManager, segment, hashFunction);

    metadataManager.stop();
    metadataManager.close();
  }

  private void verifyInitialSegmentAddition(ConcurrentMapPartitionDedupMetadataManager metadataManager,
      IndexSegment segment, HashFunction hashFunction) {
    assertEquals(metadataManager._primaryKeyToSegmentAndTimeMap.size(), 10);
    verifyInMemoryState(metadataManager, 0, 10, segment, hashFunction);
  }

  private void verifyInMemoryState(ConcurrentMapPartitionDedupMetadataManager metadataManager, int startPrimaryKeyId,
      int recordCount, IndexSegment segment, HashFunction hashFunction) {
    for (int primaryKeyId = startPrimaryKeyId; primaryKeyId < startPrimaryKeyId + recordCount; primaryKeyId++) {
      Object primaryKey = HashUtils.hashPrimaryKey(DedupTestUtils.getPrimaryKey(primaryKeyId), hashFunction);
      assertEquals(metadataManager._primaryKeyToSegmentAndTimeMap.get(primaryKey),
          Pair.of(segment, (double) primaryKeyId * 1000));
    }
  }

  @Test
  public void testAddTwoDifferentSegmentsRemoveEarlySegmentFirst()
      throws IOException {
    verifyAddTwoDifferentSegmentsRemoveEarlySegmentFirst(HashFunction.NONE);
    verifyAddTwoDifferentSegmentsRemoveEarlySegmentFirst(HashFunction.MD5);
    verifyAddTwoDifferentSegmentsRemoveEarlySegmentFirst(HashFunction.MURMUR3);
  }

  private void verifyAddTwoDifferentSegmentsRemoveEarlySegmentFirst(HashFunction hashFunction)
      throws IOException {
    _dedupContextBuilder.setHashFunction(hashFunction);
    ConcurrentMapPartitionDedupMetadataManager metadataManager =
        new ConcurrentMapPartitionDedupMetadataManager(DedupTestUtils.REALTIME_TABLE_NAME, 0,
            _dedupContextBuilder.build());

    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader1 = generateDedupRecordInfoReader(10, 0);
    IndexSegment segment1 = DedupTestUtils.mockSegment(1, 10);
    Iterator<DedupRecordInfo> dedupRecordInfoIterator1 =
        DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader1, 10);
    metadataManager.doAddOrReplaceSegment(null, segment1, dedupRecordInfoIterator1);
    verifyInitialSegmentAddition(metadataManager, segment1, hashFunction);

    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader2 = generateDedupRecordInfoReader(10, 10);
    IndexSegment segment2 = DedupTestUtils.mockSegment(2, 10);
    Iterator<DedupRecordInfo> dedupRecordInfoIterator2 =
        DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader2, 10);
    metadataManager.doAddOrReplaceSegment(null, segment2, dedupRecordInfoIterator2);

    metadataManager._largestSeenTime.set(19000);
    metadataManager.removeExpiredPrimaryKeys();
    assertEquals(metadataManager.getNumPrimaryKeys(), 11);
    assertEquals(metadataManager._primaryKeyToSegmentAndTimeMap.size(), 11);
    verifyInMemoryState(metadataManager, 9, 1, segment1, hashFunction);
    verifyInMemoryState(metadataManager, 10, 10, segment2, hashFunction);
    assertEquals(WatermarkUtils.loadWatermark(metadataManager.getWatermarkFile(), -1), 19000);

    dedupRecordInfoIterator1 = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader1, 10);
    metadataManager.doRemoveSegment(segment1, dedupRecordInfoIterator1);
    assertEquals(metadataManager._primaryKeyToSegmentAndTimeMap.size(), 10);
    verifyInMemoryState(metadataManager, 10, 10, segment2, hashFunction);

    dedupRecordInfoIterator2 = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader2, 10);
    metadataManager.doRemoveSegment(segment2, dedupRecordInfoIterator2);
    assertTrue(metadataManager._primaryKeyToSegmentAndTimeMap.isEmpty());

    metadataManager.stop();
    metadataManager.close();
  }

  @Test
  public void testAddTwoDifferentSegmentsRemoveRecentSegmentFirst()
      throws IOException {
    verifyAddTwoDifferentSegmentsRemoveRecentSegmentFirst(HashFunction.NONE);
    verifyAddTwoDifferentSegmentsRemoveRecentSegmentFirst(HashFunction.MD5);
    verifyAddTwoDifferentSegmentsRemoveRecentSegmentFirst(HashFunction.MURMUR3);
  }

  private void verifyAddTwoDifferentSegmentsRemoveRecentSegmentFirst(HashFunction hashFunction)
      throws IOException {
    _dedupContextBuilder.setHashFunction(hashFunction);
    ConcurrentMapPartitionDedupMetadataManager metadataManager =
        new ConcurrentMapPartitionDedupMetadataManager(DedupTestUtils.REALTIME_TABLE_NAME, 0,
            _dedupContextBuilder.build());

    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader1 = generateDedupRecordInfoReader(10, 0);
    IndexSegment segment1 = DedupTestUtils.mockSegment(1, 10);
    Iterator<DedupRecordInfo> dedupRecordInfoIterator1 =
        DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader1, 10);
    metadataManager.doAddOrReplaceSegment(null, segment1, dedupRecordInfoIterator1);
    verifyInitialSegmentAddition(metadataManager, segment1, hashFunction);

    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader2 = generateDedupRecordInfoReader(10, 10);
    IndexSegment segment2 = DedupTestUtils.mockSegment(2, 10);
    Iterator<DedupRecordInfo> dedupRecordInfoIterator2 =
        DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader2, 10);
    metadataManager.doAddOrReplaceSegment(null, segment2, dedupRecordInfoIterator2);
    metadataManager._largestSeenTime.set(19000);
    metadataManager.removeExpiredPrimaryKeys();
    assertEquals(metadataManager.getNumPrimaryKeys(), 11);
    assertEquals(metadataManager._primaryKeyToSegmentAndTimeMap.size(), 11);
    verifyInMemoryState(metadataManager, 10, 10, segment2, hashFunction);
    assertEquals(WatermarkUtils.loadWatermark(metadataManager.getWatermarkFile(), -1), 19000);

    dedupRecordInfoIterator2 = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader2, 10);
    metadataManager.doRemoveSegment(segment2, dedupRecordInfoIterator2);
    assertEquals(metadataManager._primaryKeyToSegmentAndTimeMap.size(), 1);
    verifyInMemoryState(metadataManager, 9, 1, segment1, hashFunction);

    dedupRecordInfoIterator1 = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader1, 10);
    metadataManager.doRemoveSegment(segment1, dedupRecordInfoIterator1);
    assertTrue(metadataManager._primaryKeyToSegmentAndTimeMap.isEmpty());

    metadataManager.stop();
    metadataManager.close();
  }

  @Test
  public void testAddingSegmentWithDuplicatedPrimaryKeys()
      throws IOException {
    verifyAddingSegmentWithDuplicatedPrimaryKeys(HashFunction.NONE);
    verifyAddingSegmentWithDuplicatedPrimaryKeys(HashFunction.MD5);
    verifyAddingSegmentWithDuplicatedPrimaryKeys(HashFunction.MURMUR3);
  }

  private void verifyAddingSegmentWithDuplicatedPrimaryKeys(HashFunction hashFunction)
      throws IOException {
    _dedupContextBuilder.setHashFunction(hashFunction);
    ConcurrentMapPartitionDedupMetadataManager metadataManager =
        new ConcurrentMapPartitionDedupMetadataManager(DedupTestUtils.REALTIME_TABLE_NAME, 0,
            _dedupContextBuilder.build());

    PrimaryKey primaryKey = DedupTestUtils.getPrimaryKey(0);
    PrimaryKeyReader primaryKeyReader = Mockito.mock(PrimaryKeyReader.class);
    for (int i = 0; i < 3; i++) {
      Mockito.when(primaryKeyReader.getPrimaryKey(i)).thenReturn(primaryKey);
    }
    PinotSegmentColumnReader dedupTimeColumnReader = Mockito.mock(PinotSegmentColumnReader.class);
    Mockito.when(dedupTimeColumnReader.getValue(0)).thenReturn(1000.0);
    Mockito.when(dedupTimeColumnReader.getValue(1)).thenReturn(15000.0);
    Mockito.when(dedupTimeColumnReader.getValue(2)).thenReturn(25000.0);
    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader =
        new DedupUtils.DedupRecordInfoReader(primaryKeyReader, dedupTimeColumnReader);
    metadataManager._largestSeenTime.set(20000);

    ImmutableSegmentImpl immutableSegment = DedupTestUtils.mockSegment(1, 3);
    Iterator<DedupRecordInfo> dedupRecordInfoIterator = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader, 3);
    metadataManager.doAddOrReplaceSegment(null, immutableSegment, dedupRecordInfoIterator);
    assertEquals(metadataManager._primaryKeyToSegmentAndTimeMap.size(), 1);
    Object primaryKeyHash = HashUtils.hashPrimaryKey(primaryKey, hashFunction);
    assertEquals(metadataManager._primaryKeyToSegmentAndTimeMap.size(), 1);
    assertEquals(metadataManager._primaryKeyToSegmentAndTimeMap.get(primaryKeyHash),
        Pair.of(immutableSegment, 25000.0));

    metadataManager.stop();
    metadataManager.close();
  }

  @Test
  public void testAddRow()
      throws IOException {
    verifyAddRow(HashFunction.NONE);
    verifyAddRow(HashFunction.MD5);
    verifyAddRow(HashFunction.MURMUR3);
  }

  private void verifyAddRow(HashFunction hashFunction)
      throws IOException {
    _dedupContextBuilder.setHashFunction(hashFunction);
    ConcurrentMapPartitionDedupMetadataManager metadataManager =
        new ConcurrentMapPartitionDedupMetadataManager(DedupTestUtils.REALTIME_TABLE_NAME, 0,
            _dedupContextBuilder.build());

    metadataManager._largestSeenTime.set(20000);

    PrimaryKey primaryKey = DedupTestUtils.getPrimaryKey(0);
    DedupRecordInfo dedupRecordInfo = new DedupRecordInfo(primaryKey, 1000);
    ImmutableSegmentImpl immutableSegment = DedupTestUtils.mockSegment(1, 1);
    assertFalse(metadataManager.checkRecordPresentOrUpdate(dedupRecordInfo, immutableSegment));
    assertFalse(metadataManager._primaryKeyToSegmentAndTimeMap.isEmpty());
    assertEquals(metadataManager._largestSeenTime.get(), 20000);

    Object primaryKeyHash = HashUtils.hashPrimaryKey(primaryKey, hashFunction);
    dedupRecordInfo = new DedupRecordInfo(primaryKey, 15000);
    assertFalse(metadataManager.checkRecordPresentOrUpdate(dedupRecordInfo, immutableSegment));
    assertEquals(metadataManager._primaryKeyToSegmentAndTimeMap.size(), 1);
    assertEquals(metadataManager._primaryKeyToSegmentAndTimeMap.get(primaryKeyHash),
        Pair.of(immutableSegment, 15000.0));
    assertEquals(metadataManager._largestSeenTime.get(), 20000);

    dedupRecordInfo = new DedupRecordInfo(primaryKey, 25000);
    assertTrue(metadataManager.checkRecordPresentOrUpdate(dedupRecordInfo, immutableSegment));
    assertEquals(metadataManager._primaryKeyToSegmentAndTimeMap.size(), 1);
    assertEquals(metadataManager._primaryKeyToSegmentAndTimeMap.get(primaryKeyHash),
        Pair.of(immutableSegment, 15000.0));
    assertEquals(metadataManager._largestSeenTime.get(), 25000);

    metadataManager.stop();
    metadataManager.close();
  }

  private static DedupUtils.DedupRecordInfoReader generateDedupRecordInfoReader(int numberOfDocs,
      int startPrimaryKeyValue) {
    PrimaryKeyReader primaryKeyReader = Mockito.mock(PrimaryKeyReader.class);
    PinotSegmentColumnReader dedupTimeColumnReader = Mockito.mock(PinotSegmentColumnReader.class);
    for (int i = 0; i < numberOfDocs; i++) {
      int primaryKeyValue = startPrimaryKeyValue + i;
      Mockito.when(primaryKeyReader.getPrimaryKey(i)).thenReturn(DedupTestUtils.getPrimaryKey(primaryKeyValue));
      double time = primaryKeyValue * 1000;
      Mockito.when(dedupTimeColumnReader.getValue(i)).thenReturn(time);
    }
    return new DedupUtils.DedupRecordInfoReader(primaryKeyReader, dedupTimeColumnReader);
  }

  @Test
  public void testAddSegmentAfterStop() {
    verifyAddSegmentAfterStop(HashFunction.NONE);
    verifyAddSegmentAfterStop(HashFunction.MD5);
    verifyAddSegmentAfterStop(HashFunction.MURMUR3);
  }

  private void verifyAddSegmentAfterStop(HashFunction hashFunction) {
    _dedupContextBuilder.setHashFunction(hashFunction);
    ConcurrentMapPartitionDedupMetadataManager metadataManager =
        new ConcurrentMapPartitionDedupMetadataManager(DedupTestUtils.REALTIME_TABLE_NAME, 0,
            _dedupContextBuilder.build());

    IndexSegment segment = DedupTestUtils.mockSegment(1, 10);
    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    ColumnMetadata columnMetadata = mock(ColumnMetadata.class);
    when(segmentMetadata.getColumnMetadataMap()).thenReturn(new TreeMap<>() {{
      this.put(DEDUP_TIME_COLUMN_NAME, columnMetadata);
    }});
    when(columnMetadata.getMaxValue()).thenReturn(System.currentTimeMillis());
    when(segment.getSegmentMetadata()).thenReturn(segmentMetadata);
    // throws when not stopped
    assertThrows(RuntimeException.class, () -> metadataManager.addSegment(segment));

    metadataManager.stop();
    // do not throw when stopped
    metadataManager.addSegment(segment);
  }
}
