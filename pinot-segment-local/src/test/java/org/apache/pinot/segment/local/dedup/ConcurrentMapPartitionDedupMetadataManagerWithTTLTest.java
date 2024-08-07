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
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.local.segment.readers.PrimaryKeyReader;
import org.apache.pinot.segment.local.utils.HashUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;


public class ConcurrentMapPartitionDedupMetadataManagerWithTTLTest {
  private static final int METADATA_TTL = 10000;
  private ConcurrentMapPartitionDedupMetadataManager _metadataManager;

  @BeforeMethod
  public void setUp() {
    DedupContext.Builder dedupContextBuider = new DedupContext.Builder();
    dedupContextBuider.setTableConfig(mock(TableConfig.class)).setSchema(mock(Schema.class))
        .setPrimaryKeyColumns(List.of("primaryKeyColumn")).setHashFunction(HashFunction.NONE)
        .setMetadataTTL(METADATA_TTL).setMetadataTimeColumn("metadataTimeColumn")
        .setServerMetrics(mock(ServerMetrics.class));
    DedupContext dedupContext = dedupContextBuider.build();
    _metadataManager =
        new ConcurrentMapPartitionDedupMetadataManager(DedupTestUtils.REALTIME_TABLE_NAME, 0, dedupContext);
  }

  @Test
  public void creatingMetadataManagerThrowsExceptions() {
    DedupContext.Builder dedupContextBuider = new DedupContext.Builder();
    dedupContextBuider.setTableConfig(mock(TableConfig.class)).setSchema(mock(Schema.class))
        .setPrimaryKeyColumns(List.of("primaryKeyColumn")).setHashFunction(HashFunction.NONE).setMetadataTTL(1)
        .setMetadataTimeColumn(null).setServerMetrics(mock(ServerMetrics.class));
    DedupContext dedupContext = dedupContextBuider.build();
    assertThrows(IllegalArgumentException.class,
        () -> new ConcurrentMapPartitionDedupMetadataManager(DedupTestUtils.REALTIME_TABLE_NAME, 0,
            dedupContext));
  }

  @Test
  public void verifyRemoveExpiredPrimaryKeys() {
    IndexSegment segment = Mockito.mock(IndexSegment.class);
    for (int i = 0; i < 20; i++) {
      double time = i * 1000;
      Object primaryKeyKey = HashUtils.hashPrimaryKey(DedupTestUtils.getPrimaryKey(i), HashFunction.NONE);
      _metadataManager._primaryKeyToSegmentAndTimeMap.computeIfAbsent(primaryKeyKey, k -> Pair.of(segment, time));
    }
    _metadataManager._largestSeenTime.set(19000);
    assertEquals(_metadataManager._primaryKeyToSegmentAndTimeMap.size(), 20);
    verifyInMemoryState(0, 20, segment);

    assertEquals(_metadataManager.removeExpiredPrimaryKeys(), 11);
    assertEquals(_metadataManager._primaryKeyToSegmentAndTimeMap.size(), 11);
    verifyInMemoryState(9, 11, segment);
  }

  @Test
  public void verifyAddRemoveTheSameSegment() {
    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader = generateDedupRecordInfoReader(10, 0);
    Iterator<DedupRecordInfo> dedupRecordInfoIterator =
        DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader, 10);
    IndexSegment segment = DedupTestUtils.mockSegment(1, 10);
    _metadataManager.addSegment(segment, dedupRecordInfoIterator);
    verifyInitialSegmentAddition(segment);

    dedupRecordInfoIterator = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader, 10);
    _metadataManager.removeSegment(segment, dedupRecordInfoIterator);
    assertEquals(_metadataManager._primaryKeyToSegmentAndTimeMap.size(), 0);
    assertEquals(_metadataManager._largestSeenTime.get(), 9000);
  }

  @Test
  public void verifyAddingTwoSegmentWithSamePrimaryKeys() {
    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader = generateDedupRecordInfoReader(10, 0);
    IndexSegment segment = DedupTestUtils.mockSegment(1, 10);
    Iterator<DedupRecordInfo> dedupRecordInfoIterator =
        DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader, 10);
    _metadataManager.addSegment(segment, dedupRecordInfoIterator);
    verifyInitialSegmentAddition(segment);

    IndexSegment segment2 = DedupTestUtils.mockSegment(2, 10);
    dedupRecordInfoIterator = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader, 10);
    _metadataManager.addSegment(segment2, dedupRecordInfoIterator);
    verifyInitialSegmentAddition(segment);
  }

  @Test
  public void verifyRemoveAnotherSegmentWithTheSamePrimaryKeys() {
    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader = generateDedupRecordInfoReader(10, 0);
    IndexSegment segment = DedupTestUtils.mockSegment(1, 10);
    Iterator<DedupRecordInfo> dedupRecordInfoIterator =
        DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader, 10);
    _metadataManager.addSegment(segment, dedupRecordInfoIterator);
    verifyInitialSegmentAddition(segment);

    IndexSegment segment2 = DedupTestUtils.mockSegment(2, 10);
    dedupRecordInfoIterator = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader, 10);
    _metadataManager.removeSegment(segment2, dedupRecordInfoIterator);
    verifyInitialSegmentAddition(segment);
  }

  private void verifyInitialSegmentAddition(IndexSegment segment) {
    assertEquals(_metadataManager._largestSeenTime.get(), 9000);
    assertEquals(_metadataManager._primaryKeyToSegmentAndTimeMap.size(), 10);
    verifyInMemoryState(0, 10, segment);
  }

  private void verifyInMemoryState(int startPrimaryKeyId, int recordCount, IndexSegment segment) {
    for (int primaryKeyId = startPrimaryKeyId; primaryKeyId < startPrimaryKeyId + recordCount; primaryKeyId++) {
      PrimaryKey primaryKey = DedupTestUtils.getPrimaryKey(primaryKeyId);
      assertEquals(_metadataManager._primaryKeyToSegmentAndTimeMap.get(primaryKey),
          Pair.of(segment, (double) primaryKeyId * 1000));
    }
  }

  @Test
  public void verifyAddTwoDifferentSegmentsRemoveEarlySegmentFirst() {
    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader1 = generateDedupRecordInfoReader(10, 0);
    IndexSegment segment1 = DedupTestUtils.mockSegment(1, 10);
    Iterator<DedupRecordInfo> dedupRecordInfoIterator1 =
        DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader1, 10);
    _metadataManager.addSegment(segment1, dedupRecordInfoIterator1);
    verifyInitialSegmentAddition(segment1);

    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader2 = generateDedupRecordInfoReader(10, 10);
    IndexSegment segment2 = DedupTestUtils.mockSegment(2, 10);
    Iterator<DedupRecordInfo> dedupRecordInfoIterator2 =
        DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader2, 10);
    _metadataManager.addSegment(segment2, dedupRecordInfoIterator2);
    assertEquals(_metadataManager.removeExpiredPrimaryKeys(), 11);
    assertEquals(_metadataManager._primaryKeyToSegmentAndTimeMap.size(), 11);
    verifyInMemoryState(9, 1, segment1);
    verifyInMemoryState(10, 10, segment2);
    assertEquals(_metadataManager._largestSeenTime.get(), 19000);

    dedupRecordInfoIterator1 = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader1, 10);
    _metadataManager.removeSegment(segment1, dedupRecordInfoIterator1);
    assertEquals(_metadataManager._primaryKeyToSegmentAndTimeMap.size(), 10);
    verifyInMemoryState(10, 10, segment2);
    assertEquals(_metadataManager._largestSeenTime.get(), 19000);

    dedupRecordInfoIterator2 = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader2, 10);
    _metadataManager.removeSegment(segment2, dedupRecordInfoIterator2);
    assertTrue(_metadataManager._primaryKeyToSegmentAndTimeMap.isEmpty());
    assertEquals(_metadataManager._largestSeenTime.get(), 19000);
  }

  @Test
  public void verifyAddTwoDifferentSegmentsRemoveRecentSegmentFirst() {
    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader1 = generateDedupRecordInfoReader(10, 0);
    IndexSegment segment1 = DedupTestUtils.mockSegment(1, 10);
    Iterator<DedupRecordInfo> dedupRecordInfoIterator1 =
        DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader1, 10);
    _metadataManager.addSegment(segment1, dedupRecordInfoIterator1);
    verifyInitialSegmentAddition(segment1);

    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader2 = generateDedupRecordInfoReader(10, 10);
    IndexSegment segment2 = DedupTestUtils.mockSegment(2, 10);
    Iterator<DedupRecordInfo> dedupRecordInfoIterator2 =
        DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader2, 10);
    _metadataManager.addSegment(segment2, dedupRecordInfoIterator2);
    assertEquals(_metadataManager.removeExpiredPrimaryKeys(), 11);
    assertEquals(_metadataManager._primaryKeyToSegmentAndTimeMap.size(), 11);
    verifyInMemoryState(10, 10, segment2);
    assertEquals(_metadataManager._largestSeenTime.get(), 19000);

    dedupRecordInfoIterator2 = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader2, 10);
    _metadataManager.removeSegment(segment2, dedupRecordInfoIterator2);
    assertEquals(_metadataManager._primaryKeyToSegmentAndTimeMap.size(), 1);
    verifyInMemoryState(9, 1, segment1);
    assertEquals(_metadataManager._largestSeenTime.get(), 19000);

    dedupRecordInfoIterator1 = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader1, 10);
    _metadataManager.removeSegment(segment1, dedupRecordInfoIterator1);
    assertTrue(_metadataManager._primaryKeyToSegmentAndTimeMap.isEmpty());
    assertEquals(_metadataManager._largestSeenTime.get(), 19000);
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
    DedupUtils.DedupRecordInfoReader dedupRecordInfoReader =
        new DedupUtils.DedupRecordInfoReader(primaryKeyReader, dedupTimeColumnReader);
    _metadataManager._largestSeenTime.set(20000);

    ImmutableSegmentImpl immutableSegment = DedupTestUtils.mockSegment(1, 3);
    Iterator<DedupRecordInfo> dedupRecordInfoIterator = DedupUtils.getDedupRecordInfoIterator(dedupRecordInfoReader, 3);
    _metadataManager.addSegment(immutableSegment, dedupRecordInfoIterator);
    assertEquals(_metadataManager._primaryKeyToSegmentAndTimeMap.size(), 1);
    Object primaryKeyHash = HashUtils.hashPrimaryKey(primaryKey, HashFunction.NONE);
    assertEquals(_metadataManager._primaryKeyToSegmentAndTimeMap.size(), 1);
    assertEquals(_metadataManager._primaryKeyToSegmentAndTimeMap.get(primaryKeyHash),
        Pair.of(immutableSegment, 25000.0));
    assertEquals(_metadataManager._largestSeenTime.get(), 25000);
  }

  @Test
  public void verifyAddRow() {
    _metadataManager._largestSeenTime.set(20000);

    PrimaryKey primaryKey = DedupTestUtils.getPrimaryKey(0);
    DedupRecordInfo dedupRecordInfo = new DedupRecordInfo(primaryKey, 1000);
    ImmutableSegmentImpl immutableSegment = DedupTestUtils.mockSegment(1, 1);
    assertTrue(_metadataManager.dropOrAddRecord(dedupRecordInfo, immutableSegment));
    assertTrue(_metadataManager._primaryKeyToSegmentAndTimeMap.isEmpty());
    assertEquals(_metadataManager._largestSeenTime.get(), 20000);

    Object primaryKeyHash = HashUtils.hashPrimaryKey(primaryKey, HashFunction.NONE);
    dedupRecordInfo = new DedupRecordInfo(primaryKey, 15000);
    assertFalse(_metadataManager.dropOrAddRecord(dedupRecordInfo, immutableSegment));
    assertEquals(_metadataManager._primaryKeyToSegmentAndTimeMap.size(), 1);
    assertEquals(_metadataManager._primaryKeyToSegmentAndTimeMap.get(primaryKeyHash),
        Pair.of(immutableSegment, 15000.0));
    assertEquals(_metadataManager._largestSeenTime.get(), 20000);

    dedupRecordInfo = new DedupRecordInfo(primaryKey, 25000);
    assertTrue(_metadataManager.dropOrAddRecord(dedupRecordInfo, immutableSegment));
    assertEquals(_metadataManager._primaryKeyToSegmentAndTimeMap.size(), 1);
    assertEquals(_metadataManager._primaryKeyToSegmentAndTimeMap.get(primaryKeyHash),
        Pair.of(immutableSegment, 15000.0));
    assertEquals(_metadataManager._largestSeenTime.get(), 25000);
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
}
