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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.upsert.RecordInfo;
import org.apache.pinot.segment.local.utils.HashUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class PartitionDedupMetadataManagerTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME);

  @Test
  public void verifyAddRemoveSegment() throws Exception {
    HashFunction hashFunction = HashFunction.NONE;
    DedupConfig dedupConfig = new DedupConfig(true, hashFunction);
    TestMetadataManager metadataManager =
        new TestMetadataManager(REALTIME_TABLE_NAME, null, 0, mock(ServerMetrics.class), dedupConfig);

    // Add the first segment
    List<PrimaryKey> pkList1 = new ArrayList<>();
    pkList1.add(getPrimaryKey(0));
    pkList1.add(getPrimaryKey(1));
    pkList1.add(getPrimaryKey(2));
    pkList1.add(getPrimaryKey(0));
    pkList1.add(getPrimaryKey(1));
    pkList1.add(getPrimaryKey(0));
    metadataManager._primaryKeyIterator = pkList1.iterator();
    ImmutableSegmentImpl segment1 = mockSegment(1);
    metadataManager.addSegment(segment1);
    checkRecordLocation(metadataManager, 0, segment1, hashFunction);
    checkRecordLocation(metadataManager, 1, segment1, hashFunction);
    checkRecordLocation(metadataManager, 2, segment1, hashFunction);

    metadataManager._primaryKeyIterator = pkList1.iterator();
    metadataManager.removeSegment(segment1);
    metadataManager._keyValueStore.compact();
    Assert.assertEquals(metadataManager._keyValueStore.getKeyCount(), 0);
  }

  @Test
  public void verifyReloadSegment() throws Exception {
    HashFunction hashFunction = HashFunction.NONE;
    DedupConfig dedupConfig = new DedupConfig(true, hashFunction);
    TestMetadataManager metadataManager =
        new TestMetadataManager(REALTIME_TABLE_NAME, null, 1, mock(ServerMetrics.class), dedupConfig);

    // Add the first segment
    List<PrimaryKey> pkList1 = new ArrayList<>();
    pkList1.add(getPrimaryKey(0));
    pkList1.add(getPrimaryKey(1));
    pkList1.add(getPrimaryKey(2));
    pkList1.add(getPrimaryKey(0));
    pkList1.add(getPrimaryKey(1));
    pkList1.add(getPrimaryKey(0));
    metadataManager._primaryKeyIterator = pkList1.iterator();
    ImmutableSegmentImpl segment1 = mockSegment(1);
    metadataManager.addSegment(segment1);

    // Remove another segment with same PK rows
    metadataManager._primaryKeyIterator = pkList1.iterator();
    ImmutableSegmentImpl segment2 = mockSegment(2);
    metadataManager.removeSegment(segment2);
    metadataManager._keyValueStore.compact();
    Assert.assertEquals(metadataManager._keyValueStore.getKeyCount(), 3);

    // Keys should still exist
    checkRecordLocation(metadataManager, 0, segment1, hashFunction);
    checkRecordLocation(metadataManager, 1, segment1, hashFunction);
    checkRecordLocation(metadataManager, 2, segment1, hashFunction);
  }

  @Test
  public void verifyAddRow() throws Exception {
    HashFunction hashFunction = HashFunction.NONE;
    DedupConfig dedupConfig = new DedupConfig(true, hashFunction);
    TestMetadataManager metadataManager =
        new TestMetadataManager(REALTIME_TABLE_NAME, null, 2, mock(ServerMetrics.class), dedupConfig);

    // Add the first segment
    List<PrimaryKey> pkList1 = new ArrayList<>();
    pkList1.add(getPrimaryKey(0));
    pkList1.add(getPrimaryKey(1));
    pkList1.add(getPrimaryKey(2));
    pkList1.add(getPrimaryKey(0));
    pkList1.add(getPrimaryKey(1));
    pkList1.add(getPrimaryKey(0));
    metadataManager._primaryKeyIterator = pkList1.iterator();
    ImmutableSegmentImpl segment1 = mockSegment(1);
    metadataManager.addSegment(segment1);

    // Same PK exists
    RecordInfo recordInfo = mock(RecordInfo.class);
    when(recordInfo.getPrimaryKey()).thenReturn(getPrimaryKey(0));
    ImmutableSegmentImpl segment2 = mockSegment(2);
    Assert.assertTrue(metadataManager.checkRecordPresentOrUpdate(recordInfo.getPrimaryKey(), segment2));
    checkRecordLocation(metadataManager, 0, segment1, hashFunction);

    // New PK
    when(recordInfo.getPrimaryKey()).thenReturn(getPrimaryKey(3));
    Assert.assertFalse(metadataManager.checkRecordPresentOrUpdate(recordInfo.getPrimaryKey(), segment2));
    checkRecordLocation(metadataManager, 3, segment2, hashFunction);

    // Same PK as the one recently ingested
    when(recordInfo.getPrimaryKey()).thenReturn(getPrimaryKey(3));
    Assert.assertTrue(metadataManager.checkRecordPresentOrUpdate(recordInfo.getPrimaryKey(), segment2));
  }

  private static ImmutableSegmentImpl mockSegment(int sequenceNumber) {
    ImmutableSegmentImpl segment = mock(ImmutableSegmentImpl.class);
    String segmentName = getSegmentName(sequenceNumber);
    when(segment.getSegmentName()).thenReturn(segmentName);
    return segment;
  }

  private static String getSegmentName(int sequenceNumber) {
    return new LLCSegmentName(RAW_TABLE_NAME, 0, sequenceNumber, System.currentTimeMillis()).toString();
  }

  private static PrimaryKey getPrimaryKey(int value) {
    return new PrimaryKey(new Object[]{value});
  }

  private static void checkRecordLocation(TestMetadataManager metadataManager, int keyValue,
      IndexSegment segment, HashFunction hashFunction) throws Exception {
    Object pk = HashUtils.hashPrimaryKey(getPrimaryKey(keyValue), hashFunction);
    byte[] pkBytes = PartitionDedupMetadataManager.serializePrimaryKey(pk);
    byte[] indexSegmentBytes = metadataManager._keyValueStore.get(pkBytes);
    Assert.assertNotNull(indexSegmentBytes);
    byte[] segmentBytes = PartitionDedupMetadataManager.serializeSegment(segment);
    Assert.assertEquals(indexSegmentBytes, segmentBytes);
  }

  private static class TestMetadataManager extends PartitionDedupMetadataManager {
    Iterator<PrimaryKey> _primaryKeyIterator;

    TestMetadataManager(String tableNameWithType, List<String> primaryKeyColumns, int partitionId,
        ServerMetrics serverMetrics, DedupConfig dedupConfig) {
      super(tableNameWithType, primaryKeyColumns, partitionId, serverMetrics, dedupConfig);
    }

    @Override
    Iterator<PrimaryKey> getPrimaryKeyIterator(IndexSegment segment) {
      return _primaryKeyIterator;
    }
  }
}
