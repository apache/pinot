
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

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.local.segment.readers.PrimaryKeyReader;
import org.apache.pinot.segment.local.utils.HashUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;


public class RemoveOptimizedRetentionConcurrentMapPartitionDedupMetadataManagerTest {
  private static final int METADATA_TTL = 10000;
  private RemoveOptimizedRetentionConcurrentMapPartitionDedupMetadataManager _metadataManager;

  @BeforeMethod
  public void setUp() {
    _metadataManager =
        new RemoveOptimizedRetentionConcurrentMapPartitionDedupMetadataManager(DedupTestUtils.REALTIME_TABLE_NAME, null,
            0, mock(ServerMetrics.class), HashFunction.NONE, METADATA_TTL, "metadataTimeColumn");
  }

  @Test
  public void creatingMetadataManagerThrowsExceptions() {
    assertThrows(IllegalArgumentException.class,
        () -> new RemoveOptimizedRetentionConcurrentMapPartitionDedupMetadataManager(DedupTestUtils.REALTIME_TABLE_NAME,
            null, 0, null, HashFunction.NONE, 0, null));
    assertThrows(IllegalArgumentException.class,
        () -> new RemoveOptimizedRetentionConcurrentMapPartitionDedupMetadataManager(DedupTestUtils.REALTIME_TABLE_NAME,
            null, 0, null, HashFunction.NONE, 1, null));
  }

  @Test
  public void verifyRemoveExpiredPrimaryKeys() {
    IndexSegment segment = Mockito.mock(IndexSegment.class);
    for (int i = 0; i < 20; i++) {
      double time = i * 1000;
      long timeBucketId
          = _metadataManager.getTimeBucketId(time, METADATA_TTL);
      Object primaryKeyKey = HashUtils.hashPrimaryKey(DedupTestUtils.getPrimaryKey(i), HashFunction.NONE);
      _metadataManager._timeBucketToPrimaryKeyToSegmentMap.computeIfAbsent(timeBucketId, k -> new ConcurrentHashMap<>())
          .putIfAbsent(primaryKeyKey, segment);
    }
    _metadataManager._largestSeenTimeBucketId.set(4L);
    assertEquals(_metadataManager._timeBucketToPrimaryKeyToSegmentMap.size(), 4);
    verifyInMemoryState(0, 4, segment);

    _metadataManager.removeExpiredPrimaryKeys();
    assertEquals(_metadataManager._timeBucketToPrimaryKeyToSegmentMap.size(), 2);
    verifyInMemoryState(2, 2, segment);
  }

  @Test
  public void verifyAddRemoveTheSameSegment() {
    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader = DedupTestUtils.generateDedupRecordInfoReader(10, 0);
    Iterator<DedupRecordInfo> dedupRecordInfoIterator
        = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader, 10);
    IndexSegment segment = DedupTestUtils.mockSegment(1, 10);
    _metadataManager.addSegment(segment, dedupRecordInfoIterator);
    verifyInitialSegmentAddition(segment);

    dedupRecordInfoIterator = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader, 10);
    _metadataManager.removeSegment(segment, dedupRecordInfoIterator);
    assertEquals(_metadataManager._timeBucketToPrimaryKeyToSegmentMap.size(), 0);
    assertEquals(_metadataManager._largestSeenTimeBucketId.get(), 1);
  }

  @Test
  public void verifyAddingTwoSegmentWithSamePrimaryKeys() {
    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader = DedupTestUtils.generateDedupRecordInfoReader(10, 0);
    IndexSegment segment = DedupTestUtils.mockSegment(1, 10);
    Iterator<DedupRecordInfo> dedupRecordInfoIterator
        = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader, 10);
    _metadataManager.addSegment(segment, dedupRecordInfoIterator);
    verifyInitialSegmentAddition(segment);

    IndexSegment segment2 = DedupTestUtils.mockSegment(2, 10);
    dedupRecordInfoIterator = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader, 10);
    _metadataManager.addSegment(segment2, dedupRecordInfoIterator);
    verifyInitialSegmentAddition(segment);
  }

  @Test
  public void verifyRemoveAnotherSegmentWithTheSamePrimaryKeys() {
    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader = DedupTestUtils.generateDedupRecordInfoReader(10, 0);
    IndexSegment segment = DedupTestUtils.mockSegment(1, 10);
    Iterator<DedupRecordInfo> dedupRecordInfoIterator
        = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader, 10);
    _metadataManager.addSegment(segment, dedupRecordInfoIterator);
    verifyInitialSegmentAddition(segment);

    IndexSegment segment2 = DedupTestUtils.mockSegment(2, 10);
    dedupRecordInfoIterator = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader, 10);
    _metadataManager.removeSegment(segment2, dedupRecordInfoIterator);
    verifyInitialSegmentAddition(segment);
  }

  private void verifyInitialSegmentAddition(IndexSegment segment) {
    assertEquals(_metadataManager._largestSeenTimeBucketId.get(), 1);
    assertEquals(_metadataManager._timeBucketToPrimaryKeyToSegmentMap.size(), 2);
    verifyInMemoryState(0, 2, segment);
  }

  private void verifyInMemoryState(int startBucketId, int bucketCount, IndexSegment segment) {
    for (int bucketId = startBucketId; bucketId < startBucketId + bucketCount; bucketId++) {
      assertTrue(_metadataManager._timeBucketToPrimaryKeyToSegmentMap.containsKey((long) bucketId));
      ConcurrentHashMap<Object, IndexSegment> primaryKeyToSegmentMap =
          _metadataManager._timeBucketToPrimaryKeyToSegmentMap.get((long) bucketId);
      assertEquals(primaryKeyToSegmentMap.size(), 5);
      for (int i = 0; i < 5; i++) {
        Object primaryKeyKey
            = HashUtils.hashPrimaryKey(DedupTestUtils.getPrimaryKey(bucketId * 5 + i), HashFunction.NONE);
        assertEquals(primaryKeyToSegmentMap.get(primaryKeyKey), segment);
      }
    }
  }

  @Test
  public void verifyAddTwoDifferentSegmentsRemoveEarlySegmentFirst() {
    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader1 = DedupTestUtils.generateDedupRecordInfoReader(10, 0);
    IndexSegment segment1 = DedupTestUtils.mockSegment(1, 10);
    Iterator<DedupRecordInfo> dedupRecordInfoIterator1
        = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader1, 10);
    _metadataManager.addSegment(segment1, dedupRecordInfoIterator1);
    verifyInitialSegmentAddition(segment1);

    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader2 = DedupTestUtils.generateDedupRecordInfoReader(10, 10);
    IndexSegment segment2 = DedupTestUtils.mockSegment(2, 10);
    Iterator<DedupRecordInfo> dedupRecordInfoIterator2
        = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader2, 10);
    _metadataManager.addSegment(segment2, dedupRecordInfoIterator2);
    _metadataManager.removeExpiredPrimaryKeys();
    assertEquals(_metadataManager._timeBucketToPrimaryKeyToSegmentMap.size(), 3);
    verifyInMemoryState(1, 1, segment1);
    verifyInMemoryState(2, 2, segment2);
    assertEquals(_metadataManager._largestSeenTimeBucketId.get(), 3);

    dedupRecordInfoIterator1 = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader1, 10);
    _metadataManager.removeSegment(segment1, dedupRecordInfoIterator1);
    assertEquals(_metadataManager._timeBucketToPrimaryKeyToSegmentMap.size(), 2);
    verifyInMemoryState(2, 2, segment2);
    assertEquals(_metadataManager._largestSeenTimeBucketId.get(), 3);

    dedupRecordInfoIterator2 = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader2, 10);
    _metadataManager.removeSegment(segment2, dedupRecordInfoIterator2);
    assertTrue(_metadataManager._timeBucketToPrimaryKeyToSegmentMap.isEmpty());
    assertEquals(_metadataManager._largestSeenTimeBucketId.get(), 3);
  }

  @Test
  public void verifyAddTwoDifferentSegmentsRemoveRecentSegmentFirst() {
    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader1 = DedupTestUtils.generateDedupRecordInfoReader(10, 0);
    IndexSegment segment1 = DedupTestUtils.mockSegment(1, 10);
    Iterator<DedupRecordInfo> dedupRecordInfoIterator1
        = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader1, 10);
    _metadataManager.addSegment(segment1, dedupRecordInfoIterator1);
    verifyInitialSegmentAddition(segment1);

    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader2 = DedupTestUtils.generateDedupRecordInfoReader(10, 10);
    IndexSegment segment2 = DedupTestUtils.mockSegment(2, 10);
    Iterator<DedupRecordInfo> dedupRecordInfoIterator2
        = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader2, 10);
    _metadataManager.addSegment(segment2, dedupRecordInfoIterator2);
    _metadataManager.removeExpiredPrimaryKeys();
    assertEquals(_metadataManager._timeBucketToPrimaryKeyToSegmentMap.size(), 3);
    verifyInMemoryState(2, 2, segment2);
    assertEquals(_metadataManager._largestSeenTimeBucketId.get(), 3);

    dedupRecordInfoIterator2 = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader2, 10);
    _metadataManager.removeSegment(segment2, dedupRecordInfoIterator2);
    assertEquals(_metadataManager._timeBucketToPrimaryKeyToSegmentMap.size(), 1);
    verifyInMemoryState(1, 1, segment1);
    assertEquals(_metadataManager._largestSeenTimeBucketId.get(), 3);

    dedupRecordInfoIterator1 = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader1, 10);
    _metadataManager.removeSegment(segment1, dedupRecordInfoIterator1);
    assertTrue(_metadataManager._timeBucketToPrimaryKeyToSegmentMap.isEmpty());
    assertEquals(_metadataManager._largestSeenTimeBucketId.get(), 3);
  }

  @Test
  public void verifyAddingSegmentWithDuplicatedPrimaryKeys() {
    PrimaryKey primaryKey = DedupTestUtils.getPrimaryKey(0);
    PrimaryKeyReader primaryKeyReader = Mockito.mock(PrimaryKeyReader.class);
    for (int i = 0; i < 3; i++) {
      Mockito.when(primaryKeyReader.getPrimaryKey(i)).thenReturn(primaryKey);
    }
    PinotSegmentColumnReader dedupTimeColumnReader = Mockito.mock(PinotSegmentColumnReader.class);
    Mockito.when(dedupTimeColumnReader.getValue(0)).thenReturn(1000.0);
    Mockito.when(dedupTimeColumnReader.getValue(1)).thenReturn(15000.0);
    Mockito.when(dedupTimeColumnReader.getValue(2)).thenReturn(25000.0);
    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader
        = new DedupUtils.DedupRecordInfoReader(primaryKeyReader, dedupTimeColumnReader);
    _metadataManager._largestSeenTimeBucketId.set(4L);

    ImmutableSegmentImpl immutableSegment = DedupTestUtils.mockSegment(1, 3);
    Iterator<DedupRecordInfo> dedupRecordInfoIterator = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader, 3);
    _metadataManager.addSegment(immutableSegment, dedupRecordInfoIterator);
    assertEquals(_metadataManager._timeBucketToPrimaryKeyToSegmentMap.size(), 1);
    Object primaryKeyHash = HashUtils.hashPrimaryKey(primaryKey, HashFunction.NONE);
    assertEquals(_metadataManager._timeBucketToPrimaryKeyToSegmentMap.size(), 1);
    assertEquals(_metadataManager._timeBucketToPrimaryKeyToSegmentMap.get(3L).size(), 1);
    assertEquals(_metadataManager._timeBucketToPrimaryKeyToSegmentMap.get(3L).get(primaryKeyHash), immutableSegment);
    assertEquals(_metadataManager._largestSeenTimeBucketId.get(), 5);
  }

  @Test
  public void verifyAddRow() {
    _metadataManager._largestSeenTimeBucketId.set(4L);

    PrimaryKey primaryKey = DedupTestUtils.getPrimaryKey(0);
    DedupRecordInfo dedupRecordInfo = new DedupRecordInfo(primaryKey, 1000);
    ImmutableSegmentImpl immutableSegment = DedupTestUtils.mockSegment(1, 1);
    assertTrue(_metadataManager.dropOrAddRecord(dedupRecordInfo, immutableSegment));
    assertTrue(_metadataManager._timeBucketToPrimaryKeyToSegmentMap.isEmpty());
    assertEquals(_metadataManager._largestSeenTimeBucketId.get(), 4L);

    Object primaryKeyHash = HashUtils.hashPrimaryKey(primaryKey, HashFunction.NONE);
    dedupRecordInfo = new DedupRecordInfo(primaryKey, 15000);
    assertFalse(_metadataManager.dropOrAddRecord(dedupRecordInfo, immutableSegment));
    assertEquals(_metadataManager._timeBucketToPrimaryKeyToSegmentMap.size(), 1);
    assertEquals(_metadataManager._timeBucketToPrimaryKeyToSegmentMap.get(3L).size(), 1);
    assertEquals(_metadataManager._timeBucketToPrimaryKeyToSegmentMap.get(3L).get(primaryKeyHash), immutableSegment);
    assertEquals(_metadataManager._largestSeenTimeBucketId.get(), 4L);

    dedupRecordInfo = new DedupRecordInfo(primaryKey, 25000);
    assertTrue(_metadataManager.dropOrAddRecord(dedupRecordInfo, immutableSegment));
    assertEquals(_metadataManager._timeBucketToPrimaryKeyToSegmentMap.size(), 1);
    assertEquals(_metadataManager._timeBucketToPrimaryKeyToSegmentMap.get(3L).size(), 1);
    assertEquals(_metadataManager._timeBucketToPrimaryKeyToSegmentMap.get(3L).get(primaryKeyHash), immutableSegment);
    assertEquals(_metadataManager._largestSeenTimeBucketId.get(), 5L);
  }
}
