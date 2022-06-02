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
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.utils.HashUtils;
import org.apache.pinot.segment.local.utils.RecordInfo;
import org.apache.pinot.segment.local.utils.tablestate.TableStateUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.mockito.MockedStatic;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;


public class PartitionDedupMetadataManagerTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME);

  @BeforeClass
  public void init() {
    MockedStatic mocked = mockStatic(TableStateUtils.class);
    mocked.when(() -> TableStateUtils.isAllSegmentsLoaded(any(), any())).thenReturn(true);
  }

  @Test
  public void verifyAddRemoveSegment() {
    HashFunction hashFunction = HashFunction.NONE;
    PartitionDedupMetadataManager partitionDedupMetadataManager =
        new PartitionDedupMetadataManager(mock(HelixManager.class), REALTIME_TABLE_NAME, null, 0,
            mock(ServerMetrics.class), hashFunction);
    Map<Object, IndexSegment> recordLocationMap = partitionDedupMetadataManager._primaryKeyToSegmentMap;

    // Add the first segment
    List<PrimaryKey> pkList1 = new ArrayList<>();
    pkList1.add(getPrimaryKey(0));
    pkList1.add(getPrimaryKey(1));
    pkList1.add(getPrimaryKey(2));
    pkList1.add(getPrimaryKey(0));
    pkList1.add(getPrimaryKey(1));
    pkList1.add(getPrimaryKey(0));
    ImmutableSegmentImpl segment1 = mockSegment(1);
    MockedStatic mocked = mockStatic(PartitionDedupMetadataManager.class);
    mocked.when(() -> PartitionDedupMetadataManager.getPrimaryKeyIterator(any(), any()))
        .thenReturn(pkList1.iterator());

    partitionDedupMetadataManager.addSegment(segment1);
    checkRecordLocation(recordLocationMap, 0, segment1, hashFunction);
    checkRecordLocation(recordLocationMap, 1, segment1, hashFunction);
    checkRecordLocation(recordLocationMap, 2, segment1, hashFunction);

    pkList1 = new ArrayList<>();
    pkList1.add(getPrimaryKey(0));
    pkList1.add(getPrimaryKey(1));
    pkList1.add(getPrimaryKey(2));
    pkList1.add(getPrimaryKey(0));
    pkList1.add(getPrimaryKey(1));
    pkList1.add(getPrimaryKey(0));

    mocked.close();
    mocked = mockStatic(PartitionDedupMetadataManager.class);
    mocked.when(() -> PartitionDedupMetadataManager.getPrimaryKeyIterator(any(), any()))
        .thenReturn(pkList1.iterator());

    partitionDedupMetadataManager.removeSegment(segment1);
    Assert.assertEquals(recordLocationMap.size(), 0);
    mocked.close();
  }

  @Test
  public void verifyReloadSegment() {
    HashFunction hashFunction = HashFunction.NONE;
    PartitionDedupMetadataManager partitionDedupMetadataManager =
        new PartitionDedupMetadataManager(mock(HelixManager.class), REALTIME_TABLE_NAME, null, 0,
            mock(ServerMetrics.class), hashFunction);
    Map<Object, IndexSegment> recordLocationMap = partitionDedupMetadataManager._primaryKeyToSegmentMap;

    // Add the first segment
    List<PrimaryKey> pkList1 = new ArrayList<>();
    pkList1.add(getPrimaryKey(0));
    pkList1.add(getPrimaryKey(1));
    pkList1.add(getPrimaryKey(2));
    pkList1.add(getPrimaryKey(0));
    pkList1.add(getPrimaryKey(1));
    pkList1.add(getPrimaryKey(0));
    ImmutableSegmentImpl segment1 = mockSegment(1);
    MockedStatic mocked = mockStatic(PartitionDedupMetadataManager.class);
    mocked.when(() -> PartitionDedupMetadataManager.getPrimaryKeyIterator(any(), any()))
        .thenReturn(pkList1.iterator());

    partitionDedupMetadataManager.addSegment(segment1);

    // Remove another segment with same PK rows
    pkList1 = new ArrayList<>();
    pkList1.add(getPrimaryKey(0));
    pkList1.add(getPrimaryKey(1));
    pkList1.add(getPrimaryKey(2));
    pkList1.add(getPrimaryKey(0));
    pkList1.add(getPrimaryKey(1));
    pkList1.add(getPrimaryKey(0));
    ImmutableSegmentImpl segment2 = mockSegment(1);

    mocked.close();
    mocked = mockStatic(PartitionDedupMetadataManager.class);
    mocked.when(() -> PartitionDedupMetadataManager.getPrimaryKeyIterator(any(), any()))
        .thenReturn(pkList1.iterator());

    partitionDedupMetadataManager.removeSegment(segment2);
    Assert.assertEquals(recordLocationMap.size(), 3);

    // Keys should still exist
    checkRecordLocation(recordLocationMap, 0, segment1, hashFunction);
    checkRecordLocation(recordLocationMap, 1, segment1, hashFunction);
    checkRecordLocation(recordLocationMap, 2, segment1, hashFunction);
    mocked.close();
  }

  @Test
  public void verifyAddRow() {
    HashFunction hashFunction = HashFunction.NONE;
    PartitionDedupMetadataManager partitionDedupMetadataManager =
        new PartitionDedupMetadataManager(mock(HelixManager.class), REALTIME_TABLE_NAME, null, 0,
            mock(ServerMetrics.class), hashFunction);
    Map<Object, IndexSegment> recordLocationMap = partitionDedupMetadataManager._primaryKeyToSegmentMap;

    // Add the first segment
    List<PrimaryKey> pkList1 = new ArrayList<>();
    pkList1.add(getPrimaryKey(0));
    pkList1.add(getPrimaryKey(1));
    pkList1.add(getPrimaryKey(2));
    pkList1.add(getPrimaryKey(0));
    pkList1.add(getPrimaryKey(1));
    pkList1.add(getPrimaryKey(0));
    ImmutableSegmentImpl segment1 = mockSegment(1);
    MockedStatic mocked = mockStatic(PartitionDedupMetadataManager.class);
    mocked.when(() -> PartitionDedupMetadataManager.getPrimaryKeyIterator(any(), any()))
        .thenReturn(pkList1.iterator());
    partitionDedupMetadataManager.addSegment(segment1);
    mocked.close();

    // Same PK exists
    RecordInfo recordInfo = mock(RecordInfo.class);
    when(recordInfo.getPrimaryKey()).thenReturn(getPrimaryKey(0));
    ImmutableSegmentImpl segment2 = mockSegment(2);
    Assert.assertTrue(partitionDedupMetadataManager.checkRecordPresentOrUpdate(recordInfo.getPrimaryKey(), segment2));
    checkRecordLocation(recordLocationMap, 0, segment1, hashFunction);

    // New PK
    when(recordInfo.getPrimaryKey()).thenReturn(getPrimaryKey(3));
    Assert.assertFalse(partitionDedupMetadataManager.checkRecordPresentOrUpdate(recordInfo.getPrimaryKey(), segment2));
    checkRecordLocation(recordLocationMap, 3, segment2, hashFunction);

    // Same PK as the one recently ingested
    when(recordInfo.getPrimaryKey()).thenReturn(getPrimaryKey(3));
    Assert.assertTrue(partitionDedupMetadataManager.checkRecordPresentOrUpdate(recordInfo.getPrimaryKey(), segment2));
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

  private static void checkRecordLocation(Map<Object, IndexSegment> recordLocationMap, int keyValue,
      IndexSegment segment, HashFunction hashFunction) {
    IndexSegment indexSegment = recordLocationMap.get(HashUtils.hashPrimaryKey(getPrimaryKey(keyValue), hashFunction));
    assertNotNull(indexSegment);
    assertSame(indexSegment, segment);
  }
}
