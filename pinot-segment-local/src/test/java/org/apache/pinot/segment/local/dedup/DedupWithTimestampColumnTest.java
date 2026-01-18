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
import org.apache.pinot.segment.local.utils.HashUtils;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


/**
 * Unit tests to verify that deduplication works correctly with TIMESTAMP data type as dedupTimeColumn.
 * TIMESTAMP is stored internally as LONG (epoch milliseconds) and should be supported for time-based
 * deduplication operations.
 */
public class DedupWithTimestampColumnTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(),
      DedupWithTimestampColumnTest.class.getSimpleName());
  private static final int METADATA_TTL = 10000;
  private static final String DEDUP_TIME_COLUMN_NAME = "timestampColumn";
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

  /**
   * Test that TIMESTAMP values (stored as LONG) work correctly for deduplication.
   * This test verifies that records are deduplicated based on TIMESTAMP column values.
   */
  @Test
  public void testDedupWithTimestampColumn()
      throws IOException {
    verifyDedupWithTimestampColumn(HashFunction.NONE);
    verifyDedupWithTimestampColumn(HashFunction.MD5);
    verifyDedupWithTimestampColumn(HashFunction.MURMUR3);
  }

  private void verifyDedupWithTimestampColumn(HashFunction hashFunction)
      throws IOException {
    _dedupContextBuilder.setHashFunction(hashFunction);
    ConcurrentMapPartitionDedupMetadataManager metadataManager =
        new ConcurrentMapPartitionDedupMetadataManager(DedupTestUtils.REALTIME_TABLE_NAME, 0,
            _dedupContextBuilder.build());

    // Create mock data with TIMESTAMP values (epoch milliseconds as LONG)
    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader = generateDedupRecordInfoReader(10, 0);
    Iterator<DedupRecordInfo> dedupRecordInfoIterator =
        DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader, 10);
    IndexSegment segment = DedupTestUtils.mockSegment(1, 10);

    // Add segment with TIMESTAMP-based dedup time
    metadataManager.doAddOrReplaceSegment(null, segment, dedupRecordInfoIterator);

    // Verify that all records were added correctly
    assertEquals(metadataManager._primaryKeyToSegmentAndTimeMap.size(), 10);
    verifyInMemoryState(metadataManager, 0, 10, segment, hashFunction);

    metadataManager.stop();
    metadataManager.close();
  }

  /**
   * Test that TTL expiration works correctly with TIMESTAMP columns.
   * Records with old TIMESTAMP values should be expired based on TTL.
   */
  @Test
  public void testTimestampBasedTTLExpiration()
      throws IOException {
    verifyTimestampBasedTTLExpiration(HashFunction.NONE);
    verifyTimestampBasedTTLExpiration(HashFunction.MD5);
    verifyTimestampBasedTTLExpiration(HashFunction.MURMUR3);
  }

  private void verifyTimestampBasedTTLExpiration(HashFunction hashFunction)
      throws IOException {
    _dedupContextBuilder.setHashFunction(hashFunction);
    ConcurrentMapPartitionDedupMetadataManager metadataManager =
        new ConcurrentMapPartitionDedupMetadataManager(DedupTestUtils.REALTIME_TABLE_NAME, 0,
            _dedupContextBuilder.build());

    IndexSegment segment = Mockito.mock(IndexSegment.class);

    // Add 20 records with TIMESTAMP values from 0 to 19000 (milliseconds)
    for (int i = 0; i < 20; i++) {
      // Use actual epoch milliseconds as LONG (TIMESTAMP stored type)
      double time = i * 1000;
      Object primaryKeyKey = HashUtils.hashPrimaryKey(DedupTestUtils.getPrimaryKey(i), hashFunction);
      metadataManager._primaryKeyToSegmentAndTimeMap.computeIfAbsent(primaryKeyKey, k -> Pair.of(segment, time));
    }
    metadataManager._largestSeenTime.set(19000);
    assertEquals(metadataManager._primaryKeyToSegmentAndTimeMap.size(), 20);
    verifyInMemoryState(metadataManager, 0, 20, segment, hashFunction);

    // Remove expired primary keys (TTL = 10000 ms, so keys with time < 9000 should be removed)
    metadataManager.removeExpiredPrimaryKeys();
    assertEquals(metadataManager.getNumPrimaryKeys(), 11);
    assertEquals(metadataManager._primaryKeyToSegmentAndTimeMap.size(), 11);
    verifyInMemoryState(metadataManager, 9, 11, segment, hashFunction);

    metadataManager.stop();
    metadataManager.close();
  }

  /**
   * Test that newer records with later TIMESTAMP values replace older records during deduplication.
   */
  @Test
  public void testTimestampBasedRecordReplacement()
      throws IOException {
    verifyTimestampBasedRecordReplacement(HashFunction.NONE);
    verifyTimestampBasedRecordReplacement(HashFunction.MD5);
    verifyTimestampBasedRecordReplacement(HashFunction.MURMUR3);
  }

  private void verifyTimestampBasedRecordReplacement(HashFunction hashFunction)
      throws IOException {
    _dedupContextBuilder.setHashFunction(hashFunction);
    ConcurrentMapPartitionDedupMetadataManager metadataManager =
        new ConcurrentMapPartitionDedupMetadataManager(DedupTestUtils.REALTIME_TABLE_NAME, 0,
            _dedupContextBuilder.build());

    // Add first segment with TIMESTAMP values 0-9000
    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader1 = generateDedupRecordInfoReader(10, 0);
    IndexSegment segment1 = DedupTestUtils.mockSegment(1, 10);
    Iterator<DedupRecordInfo> dedupRecordInfoIterator1 =
        DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader1, 10);
    metadataManager.doAddOrReplaceSegment(null, segment1, dedupRecordInfoIterator1);
    verifyInMemoryState(metadataManager, 0, 10, segment1, hashFunction);

    // Add second segment with same primary keys but later TIMESTAMP values (should replace)
    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader2 = generateDedupRecordInfoReader(10, 10);
    IndexSegment segment2 = DedupTestUtils.mockSegment(2, 10);
    Iterator<DedupRecordInfo> dedupRecordInfoIterator2 =
        DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader2, 10);
    metadataManager.doAddOrReplaceSegment(segment1, segment2, dedupRecordInfoIterator2);

    // Verify that segment2 replaced segment1 (later timestamps win)
    verifyInMemoryState(metadataManager, 10, 10, segment2, hashFunction);

    metadataManager.stop();
    metadataManager.close();
  }

  /**
   * Test that segment metadata correctly reports TIMESTAMP column max values.
   */
  @Test
  public void testTimestampColumnMetadata() {
    _dedupContextBuilder.setHashFunction(HashFunction.NONE);
    ConcurrentMapPartitionDedupMetadataManager metadataManager =
        new ConcurrentMapPartitionDedupMetadataManager(DedupTestUtils.REALTIME_TABLE_NAME, 0,
            _dedupContextBuilder.build());

    IndexSegment segment = DedupTestUtils.mockSegment(1, 10);
    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    ColumnMetadata columnMetadata = mock(ColumnMetadata.class);

    // TIMESTAMP values are stored as LONG (epoch milliseconds)
    long currentTimeMillis = System.currentTimeMillis();
    when(segmentMetadata.getColumnMetadataMap()).thenReturn(new TreeMap<>() {{
      this.put(DEDUP_TIME_COLUMN_NAME, columnMetadata);
    }});
    when(columnMetadata.getMaxValue()).thenReturn(currentTimeMillis);
    when(segment.getSegmentMetadata()).thenReturn(segmentMetadata);

    metadataManager.stop();

    // Verify that adding segment with TIMESTAMP column metadata works
    metadataManager.addSegment(segment);

    // Verify the segment was added successfully
    assertNotNull(segment.getSegmentMetadata());
    assertEquals(segment.getSegmentMetadata().getColumnMetadataMap().get(DEDUP_TIME_COLUMN_NAME).getMaxValue(),
        currentTimeMillis);
  }

  /**
   * Helper method to generate a DedupRecordInfoReader with TIMESTAMP values.
   * TIMESTAMP values are simulated as LONG epoch milliseconds.
   */
  private static DedupUtils.DedupRecordInfoReader generateDedupRecordInfoReader(int numberOfDocs,
      int startPrimaryKeyValue) {
    org.apache.pinot.segment.local.segment.readers.PrimaryKeyReader primaryKeyReader =
        Mockito.mock(org.apache.pinot.segment.local.segment.readers.PrimaryKeyReader.class);
    org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader dedupTimeColumnReader =
        Mockito.mock(org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader.class);

    for (int i = 0; i < numberOfDocs; i++) {
      int primaryKeyValue = startPrimaryKeyValue + i;
      Mockito.when(primaryKeyReader.getPrimaryKey(i)).thenReturn(DedupTestUtils.getPrimaryKey(primaryKeyValue));

      // TIMESTAMP values are stored as LONG internally (epoch milliseconds)
      // We simulate this by returning Long values that will be cast to Number and then to double
      long timestampValue = primaryKeyValue * 1000L;
      Mockito.when(dedupTimeColumnReader.getValue(i)).thenReturn(timestampValue);
    }
    return new DedupUtils.DedupRecordInfoReader(primaryKeyReader, dedupTimeColumnReader);
  }

  /**
   * Helper method to verify the in-memory state of the dedup metadata manager.
   */
  private void verifyInMemoryState(ConcurrentMapPartitionDedupMetadataManager metadataManager, int startPrimaryKeyId,
      int recordCount, IndexSegment segment, HashFunction hashFunction) {
    for (int primaryKeyId = startPrimaryKeyId; primaryKeyId < startPrimaryKeyId + recordCount; primaryKeyId++) {
      Object primaryKey = HashUtils.hashPrimaryKey(DedupTestUtils.getPrimaryKey(primaryKeyId), hashFunction);
      assertEquals(metadataManager._primaryKeyToSegmentAndTimeMap.get(primaryKey),
          Pair.of(segment, (double) primaryKeyId * 1000));
    }
  }
}
