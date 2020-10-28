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
package org.apache.pinot.core.upsert;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.realtime.impl.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.core.upsert.PartitionUpsertMetadataManager.RecordInfo;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;


public class PartitionUpsertMetadataManagerTest {
  private static final String SEGMENT_PREFIX = "testSegment";
  private static final String TEST_TABLE = "testTable";

  @Test
  public void testAddSegment() {
    PartitionUpsertMetadataManager upsertMetadataManager =
        new PartitionUpsertMetadataManager(TEST_TABLE, 0, Mockito.mock(ServerMetrics.class));
    Map<PrimaryKey, RecordLocation> recordLocationMap = upsertMetadataManager._primaryKeyToRecordLocationMap;

    // Add the first segment
    String segment1 = SEGMENT_PREFIX + 1;
    List<RecordInfo> recordInfoList1 = new ArrayList<>();
    recordInfoList1.add(new RecordInfo(getPrimaryKey(0), 0, 100));
    recordInfoList1.add(new RecordInfo(getPrimaryKey(1), 1, 100));
    recordInfoList1.add(new RecordInfo(getPrimaryKey(2), 2, 100));
    recordInfoList1.add(new RecordInfo(getPrimaryKey(0), 3, 80));
    recordInfoList1.add(new RecordInfo(getPrimaryKey(1), 4, 120));
    recordInfoList1.add(new RecordInfo(getPrimaryKey(0), 5, 100));
    ThreadSafeMutableRoaringBitmap validDocIds1 =
        upsertMetadataManager.addSegment(segment1, recordInfoList1.iterator());
    // segment1: 0 -> {0, 100}, 1 -> {4, 120}, 2 -> {2, 100}
    checkRecordLocation(recordLocationMap, 0, segment1, 0, 100);
    checkRecordLocation(recordLocationMap, 1, segment1, 4, 120);
    checkRecordLocation(recordLocationMap, 2, segment1, 2, 100);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{0, 2, 4});

    // Add the second segment
    String segment2 = SEGMENT_PREFIX + 2;
    List<RecordInfo> recordInfoList2 = new ArrayList<>();
    recordInfoList2.add(new RecordInfo(getPrimaryKey(0), 0, 100));
    recordInfoList2.add(new RecordInfo(getPrimaryKey(1), 1, 100));
    recordInfoList2.add(new RecordInfo(getPrimaryKey(2), 2, 120));
    recordInfoList2.add(new RecordInfo(getPrimaryKey(3), 3, 80));
    recordInfoList2.add(new RecordInfo(getPrimaryKey(0), 4, 80));
    ThreadSafeMutableRoaringBitmap validDocIds2 =
        upsertMetadataManager.addSegment(segment2, recordInfoList2.iterator());
    // segment1: 0 -> {0, 100}, 1 -> {4, 120}
    // segment2: 2 -> {2, 120}, 3 -> {3, 80}
    checkRecordLocation(recordLocationMap, 0, segment1, 0, 100);
    checkRecordLocation(recordLocationMap, 1, segment1, 4, 120);
    checkRecordLocation(recordLocationMap, 2, segment2, 2, 120);
    checkRecordLocation(recordLocationMap, 3, segment2, 3, 80);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{0, 4});
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{2, 3});

    // Replace (reload) the first segment
    ThreadSafeMutableRoaringBitmap newValidDocIds1 =
        upsertMetadataManager.addSegment(segment1, recordInfoList1.iterator());
    // original segment1: 0 -> {0, 100}, 1 -> {4, 120}
    // segment2: 2 -> {2, 120}, 3 -> {3, 80}
    // new segment1: 0 -> {0, 100}, 1 -> {4, 120}
    checkRecordLocation(recordLocationMap, 0, segment1, 0, 100);
    checkRecordLocation(recordLocationMap, 1, segment1, 4, 120);
    checkRecordLocation(recordLocationMap, 2, segment2, 2, 120);
    checkRecordLocation(recordLocationMap, 3, segment2, 3, 80);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{0, 4});
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{2, 3});
    assertEquals(newValidDocIds1.getMutableRoaringBitmap().toArray(), new int[]{0, 4});
    assertSame(recordLocationMap.get(getPrimaryKey(0)).getValidDocIds(), newValidDocIds1);
    assertSame(recordLocationMap.get(getPrimaryKey(1)).getValidDocIds(), newValidDocIds1);

    // Remove the original segment1
    upsertMetadataManager.removeSegment(segment1, validDocIds1);
    // segment2: 2 -> {2, 120}, 3 -> {3, 80}
    // new segment1: 0 -> {0, 100}, 1 -> {4, 120}
    checkRecordLocation(recordLocationMap, 0, segment1, 0, 100);
    checkRecordLocation(recordLocationMap, 1, segment1, 4, 120);
    checkRecordLocation(recordLocationMap, 2, segment2, 2, 120);
    checkRecordLocation(recordLocationMap, 3, segment2, 3, 80);
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{2, 3});
    assertEquals(newValidDocIds1.getMutableRoaringBitmap().toArray(), new int[]{0, 4});
    assertSame(recordLocationMap.get(getPrimaryKey(0)).getValidDocIds(), newValidDocIds1);
    assertSame(recordLocationMap.get(getPrimaryKey(1)).getValidDocIds(), newValidDocIds1);
  }

  private static PrimaryKey getPrimaryKey(int value) {
    return new PrimaryKey(new Object[]{value});
  }

  private static void checkRecordLocation(Map<PrimaryKey, RecordLocation> recordLocationMap, int keyValue,
      String segmentName, int docId, long timestamp) {
    RecordLocation recordLocation = recordLocationMap.get(getPrimaryKey(keyValue));
    assertNotNull(recordLocation);
    assertEquals(recordLocation.getSegmentName(), segmentName);
    assertEquals(recordLocation.getDocId(), docId);
    assertEquals(recordLocation.getTimestamp(), timestamp);
  }

  @Test
  public void testUpdateRecord() {
    PartitionUpsertMetadataManager upsertMetadataManager =
        new PartitionUpsertMetadataManager(TEST_TABLE, 0, Mockito.mock(ServerMetrics.class));
    Map<PrimaryKey, RecordLocation> recordLocationMap = upsertMetadataManager._primaryKeyToRecordLocationMap;

    // Add the first segment
    // segment1: 0 -> {0, 100}, 1 -> {1, 120}, 2 -> {2, 100}
    String segment1 = SEGMENT_PREFIX + 1;
    List<RecordInfo> recordInfoList1 = new ArrayList<>();
    recordInfoList1.add(new RecordInfo(getPrimaryKey(0), 0, 100));
    recordInfoList1.add(new RecordInfo(getPrimaryKey(1), 1, 120));
    recordInfoList1.add(new RecordInfo(getPrimaryKey(2), 2, 100));
    ThreadSafeMutableRoaringBitmap validDocIds1 =
        upsertMetadataManager.addSegment(segment1, recordInfoList1.iterator());

    // Update records from the second segment
    String segment2 = SEGMENT_PREFIX + 2;
    ThreadSafeMutableRoaringBitmap validDocIds2 = new ThreadSafeMutableRoaringBitmap();

    upsertMetadataManager.updateRecord(segment2, new RecordInfo(getPrimaryKey(3), 0, 100), validDocIds2);
    // segment1: 0 -> {0, 100}, 1 -> {1, 120}, 2 -> {2, 100}
    // segment2: 3 -> {0, 100}
    checkRecordLocation(recordLocationMap, 0, segment1, 0, 100);
    checkRecordLocation(recordLocationMap, 1, segment1, 1, 120);
    checkRecordLocation(recordLocationMap, 2, segment1, 2, 100);
    checkRecordLocation(recordLocationMap, 3, segment2, 0, 100);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{0, 1, 2});
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0});

    upsertMetadataManager.updateRecord(segment2, new RecordInfo(getPrimaryKey(2), 1, 120), validDocIds2);
    // segment1: 0 -> {0, 100}, 1 -> {1, 120}
    // segment2: 2 -> {1, 120}, 3 -> {0, 100}
    checkRecordLocation(recordLocationMap, 0, segment1, 0, 100);
    checkRecordLocation(recordLocationMap, 1, segment1, 1, 120);
    checkRecordLocation(recordLocationMap, 2, segment2, 1, 120);
    checkRecordLocation(recordLocationMap, 3, segment2, 0, 100);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{0, 1});
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 1});

    upsertMetadataManager.updateRecord(segment2, new RecordInfo(getPrimaryKey(1), 2, 100), validDocIds2);
    // segment1: 0 -> {0, 100}, 1 -> {1, 120}
    // segment2: 2 -> {1, 120}, 3 -> {0, 100}
    checkRecordLocation(recordLocationMap, 0, segment1, 0, 100);
    checkRecordLocation(recordLocationMap, 1, segment1, 1, 120);
    checkRecordLocation(recordLocationMap, 2, segment2, 1, 120);
    checkRecordLocation(recordLocationMap, 3, segment2, 0, 100);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{0, 1});
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 1});

    upsertMetadataManager.updateRecord(segment2, new RecordInfo(getPrimaryKey(0), 3, 100), validDocIds2);
    // segment1: 0 -> {0, 100}, 1 -> {1, 120}
    // segment2: 2 -> {1, 120}, 3 -> {0, 100}
    checkRecordLocation(recordLocationMap, 0, segment1, 0, 100);
    checkRecordLocation(recordLocationMap, 1, segment1, 1, 120);
    checkRecordLocation(recordLocationMap, 2, segment2, 1, 120);
    checkRecordLocation(recordLocationMap, 3, segment2, 0, 100);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{0, 1});
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 1});
  }

  @Test
  public void testRemoveSegment() {
    PartitionUpsertMetadataManager upsertMetadataManager =
        new PartitionUpsertMetadataManager(TEST_TABLE, 0, Mockito.mock(ServerMetrics.class));
    Map<PrimaryKey, RecordLocation> recordLocationMap = upsertMetadataManager._primaryKeyToRecordLocationMap;

    // Add 2 segments
    // segment1: 0 -> {0, 100}, 1 -> {1, 100}
    // segment2: 2 -> {0, 100}, 3 -> {0, 100}
    String segment1 = SEGMENT_PREFIX + 1;
    List<RecordInfo> recordInfoList1 = new ArrayList<>();
    recordInfoList1.add(new RecordInfo(getPrimaryKey(0), 0, 100));
    recordInfoList1.add(new RecordInfo(getPrimaryKey(1), 1, 100));
    ThreadSafeMutableRoaringBitmap validDocIds1 =
        upsertMetadataManager.addSegment(segment1, recordInfoList1.iterator());
    String segment2 = SEGMENT_PREFIX + 1;
    List<RecordInfo> recordInfoList2 = new ArrayList<>();
    recordInfoList2.add(new RecordInfo(getPrimaryKey(2), 0, 100));
    recordInfoList2.add(new RecordInfo(getPrimaryKey(3), 1, 100));
    ThreadSafeMutableRoaringBitmap validDocIds2 =
        upsertMetadataManager.addSegment(segment2, recordInfoList2.iterator());

    // Remove the first segment
    upsertMetadataManager.removeSegment(segment1, validDocIds1);
    // segment2: 2 -> {0, 100}, 3 -> {0, 100}
    assertNull(recordLocationMap.get(getPrimaryKey(0)));
    assertNull(recordLocationMap.get(getPrimaryKey(1)));
    checkRecordLocation(recordLocationMap, 2, segment1, 0, 100);
    checkRecordLocation(recordLocationMap, 3, segment1, 1, 100);
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 1});
  }
}
