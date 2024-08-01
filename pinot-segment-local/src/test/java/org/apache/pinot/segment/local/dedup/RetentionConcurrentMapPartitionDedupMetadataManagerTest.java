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

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.commons.lang3.tuple.Pair;
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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class RetentionConcurrentMapPartitionDedupMetadataManagerTest {
  private static final int METADATA_TTL = 10000;
  private RetentionConcurrentMapPartitionDedupMetadataManager _metadataManager;

  @BeforeMethod
  public void setUp() {
    _metadataManager =
        new RetentionConcurrentMapPartitionDedupMetadataManager(DedupTestUtils.REALTIME_TABLE_NAME, null, 0,
            mock(ServerMetrics.class), HashFunction.NONE, METADATA_TTL, "metadataTimeColumn");
  }

  @Test
  public void creatingMetadataManagerThrowsExceptions() {
    assertThrows(IllegalArgumentException.class,
        () -> new RetentionConcurrentMapPartitionDedupMetadataManager(DedupTestUtils.REALTIME_TABLE_NAME, null, 0, null,
            HashFunction.NONE, 0, null));
    assertThrows(IllegalArgumentException.class,
        () -> new RetentionConcurrentMapPartitionDedupMetadataManager(DedupTestUtils.REALTIME_TABLE_NAME, null, 0, null,
            HashFunction.NONE, 1, null));
  }

  @Test
  public void verifyRemoveExpiredPrimaryKeysWithEachBuckHasOneRecord() {
    verifyRemoveExpiredPrimaryKeys(i -> (double) (i * 1000));
  }

  @Test
  public void verifyRemoveExpiredPrimaryKeysWithEachBuckHasMultipleRecords() {
    verifyRemoveExpiredPrimaryKeys(i -> {
        if (i < 10) {
          return 1000.0;
        } else {
          return 11000.0;
        }
    });
  }

  private void verifyRemoveExpiredPrimaryKeys(Function<Integer, Double> timeFunction) {
    IndexSegment segment = Mockito.mock(IndexSegment.class);
    for (int i = 0; i < 20; i++) {
      double time = timeFunction.apply(i);
      long timeBucketId = RetentionConcurrentMapPartitionDedupMetadataManager.getTimeBucketId(time, METADATA_TTL);
      Object primaryKeyKey = HashUtils.hashPrimaryKey(DedupTestUtils.getPrimaryKey(i), HashFunction.NONE);
      _metadataManager._primaryKeyToSegmentAndTimeMap.put(primaryKeyKey, Pair.of(segment, timeBucketId));
      _metadataManager._bucketIdToPrimaryKeySetMap
          .computeIfAbsent(timeBucketId, k -> ConcurrentHashMap.newKeySet()).add(primaryKeyKey);
    }
    _metadataManager._largestSeenTimeBucketId.set(200L);

    _metadataManager.removeExpiredPrimaryKeys();
    assertEquals(_metadataManager._primaryKeyToSegmentAndTimeMap.size(), 10);
    for (int i = 10; i < 20; i++) {
      double time = timeFunction.apply(i);
      long timeBucketId = RetentionConcurrentMapPartitionDedupMetadataManager.getTimeBucketId(time, METADATA_TTL);
      Object primaryKeyKey = HashUtils.hashPrimaryKey(DedupTestUtils.getPrimaryKey(i), HashFunction.NONE);
      assertEquals(_metadataManager._primaryKeyToSegmentAndTimeMap.get(primaryKeyKey), Pair.of(segment, timeBucketId));
      assertTrue(_metadataManager._bucketIdToPrimaryKeySetMap.get(timeBucketId).contains(primaryKeyKey));
    }
  }

  @Test
  public void verifyAddRemoveTheSameSegment() {
    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader = generateDedupRecordInfoReader(10, 0);
    IndexSegment segment = DedupTestUtils.mockSegment(1, 10);
    _metadataManager.addSegment(segment, dedupRecordInfoReader);
    verifyInitialSegmentAddition(segment);

    _metadataManager.removeSegment(segment, dedupRecordInfoReader);
    assertEquals(_metadataManager._primaryKeyToSegmentAndTimeMap.size(), 0);
    assertEquals(_metadataManager._bucketIdToPrimaryKeySetMap.size(), 0);
    assertEquals(_metadataManager._largestSeenTimeBucketId.get(), 90);
  }

  @Test
  public void verifyAddingTwoSegmentWithSamePrimaryKeys() {
    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader = generateDedupRecordInfoReader(10, 0);
    IndexSegment segment = DedupTestUtils.mockSegment(1, 10);
    _metadataManager.addSegment(segment, dedupRecordInfoReader);
    verifyInitialSegmentAddition(segment);

    IndexSegment segment2 = DedupTestUtils.mockSegment(2, 10);
    _metadataManager.addSegment(segment2, dedupRecordInfoReader);
    verifyInitialSegmentAddition(segment);
  }

  @Test
  public void verifyRemoveAnotherSegmentWithTheSamePrimaryKeys() {
    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader = generateDedupRecordInfoReader(10, 0);
    IndexSegment segment = DedupTestUtils.mockSegment(1, 10);
    _metadataManager.addSegment(segment, dedupRecordInfoReader);
    verifyInitialSegmentAddition(segment);

    IndexSegment segment2 = DedupTestUtils.mockSegment(2, 10);
    _metadataManager.removeSegment(segment2, dedupRecordInfoReader);
    verifyInitialSegmentAddition(segment);
  }

  private void verifyInitialSegmentAddition(IndexSegment segment) {
    assertEquals(_metadataManager._primaryKeyToSegmentAndTimeMap.size(), 10);
    assertEquals(_metadataManager._bucketIdToPrimaryKeySetMap.size(), 10);
    assertEquals(_metadataManager._largestSeenTimeBucketId.get(), 90);
    for (int i = 0; i < 10; i++) {
      Object primaryKeyKey = HashUtils.hashPrimaryKey(DedupTestUtils.getPrimaryKey(i), HashFunction.NONE);
      assertEquals(_metadataManager._primaryKeyToSegmentAndTimeMap.get(primaryKeyKey), Pair.of(segment, (long) i * 10));
      assertEquals(_metadataManager._bucketIdToPrimaryKeySetMap.get((long) i * 10).size(), 1);
      assertTrue(_metadataManager._bucketIdToPrimaryKeySetMap.get((long) i * 10).contains(primaryKeyKey));
    }
  }

  @Test
  public void verifyAddRemoveTwoDifferentSegmentsRemoveEarlySegmentFirst() {
    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader1 = generateDedupRecordInfoReader(10, 0);
    IndexSegment segment1 = DedupTestUtils.mockSegment(1, 10);
    _metadataManager.addSegment(segment1, dedupRecordInfoReader1);
    verifyInitialSegmentAddition(segment1);

    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader2 = generateDedupRecordInfoReader(10, 10);
    IndexSegment segment2 = DedupTestUtils.mockSegment(2, 10);
    _metadataManager.addSegment(segment2, dedupRecordInfoReader2);
    assertEquals(_metadataManager._primaryKeyToSegmentAndTimeMap.size(), 11);
    assertEquals(_metadataManager._bucketIdToPrimaryKeySetMap.size(), 11);
    assertEquals(_metadataManager._largestSeenTimeBucketId.get(), 190);

    _metadataManager.removeSegment(segment1, dedupRecordInfoReader1);
    assertEquals(_metadataManager._primaryKeyToSegmentAndTimeMap.size(), 10);
    assertEquals(_metadataManager._bucketIdToPrimaryKeySetMap.size(), 10);
    assertEquals(_metadataManager._largestSeenTimeBucketId.get(), 190);

    _metadataManager.removeSegment(segment2, dedupRecordInfoReader2);
    assertEquals(_metadataManager._primaryKeyToSegmentAndTimeMap.size(), 0);
    assertEquals(_metadataManager._bucketIdToPrimaryKeySetMap.size(), 0);
    assertEquals(_metadataManager._largestSeenTimeBucketId.get(), 190);
  }

  @Test
  public void verifyAddRemoveTwoDifferentSegmentsRemoveRecentSegmentFirst() {
    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader1 = generateDedupRecordInfoReader(10, 0);
    IndexSegment segment1 = DedupTestUtils.mockSegment(1, 10);
    _metadataManager.addSegment(segment1, dedupRecordInfoReader1);
    verifyInitialSegmentAddition(segment1);

    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader2 = generateDedupRecordInfoReader(10, 10);
    IndexSegment segment2 = DedupTestUtils.mockSegment(2, 10);
    _metadataManager.addSegment(segment2, dedupRecordInfoReader2);
    assertEquals(_metadataManager._primaryKeyToSegmentAndTimeMap.size(), 11);
    assertEquals(_metadataManager._bucketIdToPrimaryKeySetMap.size(), 11);
    assertEquals(_metadataManager._largestSeenTimeBucketId.get(), 190);

    _metadataManager.removeSegment(segment2, dedupRecordInfoReader2);
    assertEquals(_metadataManager._primaryKeyToSegmentAndTimeMap.size(), 1);
    assertEquals(_metadataManager._bucketIdToPrimaryKeySetMap.size(), 1);
    assertEquals(_metadataManager._largestSeenTimeBucketId.get(), 190);

    _metadataManager.removeSegment(segment1, dedupRecordInfoReader1);
    assertEquals(_metadataManager._primaryKeyToSegmentAndTimeMap.size(), 0);
    assertEquals(_metadataManager._bucketIdToPrimaryKeySetMap.size(), 0);
    assertEquals(_metadataManager._largestSeenTimeBucketId.get(), 190);
  }



  private DedupUtils.DedupRecordInfoReader generateDedupRecordInfoReader(int numberOfDocs, int startPrimaryKeyValue) {
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
    _metadataManager._largestSeenTimeBucketId.set(200L);

    ImmutableSegmentImpl immutableSegment = DedupTestUtils.mockSegment(1, 3);
    _metadataManager.addSegment(immutableSegment, dedupRecordInfoReader);
    assertEquals(_metadataManager._primaryKeyToSegmentAndTimeMap.size(), 1);
    Object primaryKeyHash = HashUtils.hashPrimaryKey(primaryKey, HashFunction.NONE);
    assertEquals(_metadataManager._primaryKeyToSegmentAndTimeMap.get(primaryKeyHash), Pair.of(immutableSegment, 150L));
    assertEquals(_metadataManager._bucketIdToPrimaryKeySetMap.size(), 1);
    assertEquals(_metadataManager._bucketIdToPrimaryKeySetMap.get(150L).size(), 1);
    assertTrue(_metadataManager._bucketIdToPrimaryKeySetMap.get(150L).contains(primaryKeyHash));
    assertEquals(_metadataManager._largestSeenTimeBucketId.get(), 250);
  }

  @Test
  public void verifyAddRow() {
    _metadataManager._largestSeenTimeBucketId.set(200L);

    PrimaryKey primaryKey = DedupTestUtils.getPrimaryKey(0);
    DedupRecordInfo dedupRecordInfo = new DedupRecordInfo(primaryKey, 1000);
    ImmutableSegmentImpl immutableSegment = DedupTestUtils.mockSegment(1, 1);
    assertTrue(_metadataManager.dropOrAddRecord(dedupRecordInfo, immutableSegment));
    assertEquals(_metadataManager._primaryKeyToSegmentAndTimeMap.size(), 0);
    assertEquals(_metadataManager._bucketIdToPrimaryKeySetMap.size(), 0);
    assertEquals(_metadataManager._largestSeenTimeBucketId.get(), 200);

    Object primaryKeyHash = HashUtils.hashPrimaryKey(primaryKey, HashFunction.NONE);
    dedupRecordInfo = new DedupRecordInfo(primaryKey, 15000);
    assertFalse(_metadataManager.dropOrAddRecord(dedupRecordInfo, immutableSegment));
    assertEquals(_metadataManager._primaryKeyToSegmentAndTimeMap.size(), 1);
    assertEquals(_metadataManager._primaryKeyToSegmentAndTimeMap.get(primaryKeyHash), Pair.of(immutableSegment, 150L));
    assertEquals(_metadataManager._bucketIdToPrimaryKeySetMap.size(), 1);
    assertEquals(_metadataManager._bucketIdToPrimaryKeySetMap.get(150L).size(), 1);
    assertTrue(_metadataManager._bucketIdToPrimaryKeySetMap.get(150L).contains(primaryKeyHash));
    assertEquals(_metadataManager._largestSeenTimeBucketId.get(), 200);

    dedupRecordInfo = new DedupRecordInfo(primaryKey, 25000);
    assertTrue(_metadataManager.dropOrAddRecord(dedupRecordInfo, immutableSegment));
    assertEquals(_metadataManager._primaryKeyToSegmentAndTimeMap.size(), 1);
    assertEquals(_metadataManager._primaryKeyToSegmentAndTimeMap.get(primaryKeyHash), Pair.of(immutableSegment, 150L));
    assertEquals(_metadataManager._bucketIdToPrimaryKeySetMap.size(), 1);
    assertEquals(_metadataManager._bucketIdToPrimaryKeySetMap.get(150L).size(), 1);
    assertTrue(_metadataManager._bucketIdToPrimaryKeySetMap.get(150L).contains(primaryKeyHash));
    assertEquals(_metadataManager._largestSeenTimeBucketId.get(), 250);
  }
}
