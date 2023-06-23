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
package org.apache.pinot.segment.local.upsert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.segment.local.indexsegment.immutable.EmptyIndexSegment;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.upsert.ConcurrentMapPartitionUpsertMetadataManager.RecordLocation;
import org.apache.pinot.segment.local.utils.HashUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;


public class ConcurrentMapPartitionUpsertMetadataManagerTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME);

  @Test
  public void testAddReplaceRemoveSegment()
      throws IOException {
    verifyAddReplaceRemoveSegment(HashFunction.NONE, false);
    verifyAddReplaceRemoveSegment(HashFunction.MD5, false);
    verifyAddReplaceRemoveSegment(HashFunction.MURMUR3, false);
    verifyAddReplaceRemoveSegment(HashFunction.NONE, true);
    verifyAddReplaceRemoveSegment(HashFunction.MD5, true);
    verifyAddReplaceRemoveSegment(HashFunction.MURMUR3, true);
  }

  private void verifyAddReplaceRemoveSegment(HashFunction hashFunction, boolean enableSnapshot)
      throws IOException {
    String comparisonColumn = "timeCol";
    ConcurrentMapPartitionUpsertMetadataManager upsertMetadataManager =
        new ConcurrentMapPartitionUpsertMetadataManager(REALTIME_TABLE_NAME, 0, Collections.singletonList("pk"),
            Collections.singletonList(comparisonColumn), null, hashFunction, null, false, mock(ServerMetrics.class));
    Map<Object, RecordLocation> recordLocationMap = upsertMetadataManager._primaryKeyToRecordLocationMap;
    Set<IndexSegment> trackedSegments = upsertMetadataManager._trackedSegments;

    // Add the first segment
    int numRecords = 6;
    int[] primaryKeys = new int[]{0, 1, 2, 0, 1, 0};
    int[] timestamps = new int[]{100, 100, 100, 80, 120, 100};
    ThreadSafeMutableRoaringBitmap validDocIds1 = new ThreadSafeMutableRoaringBitmap();
    List<PrimaryKey> primaryKeys1 = getPrimaryKeyList(numRecords, primaryKeys);
    ImmutableSegmentImpl segment1 = mockImmutableSegment(1, validDocIds1, null, primaryKeys1);
    List<RecordInfo> recordInfoList1;
    if (enableSnapshot) {
      // get recordInfo from validDocIdSnapshot.
      // segment1 snapshot: 0 -> {5, 100}, 1 -> {4, 120}, 2 -> {2, 100}
      int[] docIds1 = new int[]{2, 4, 5};
      MutableRoaringBitmap validDocIdsSnapshot1 = new MutableRoaringBitmap();
      validDocIdsSnapshot1.add(docIds1);
      recordInfoList1 = getRecordInfoList(validDocIdsSnapshot1, primaryKeys, timestamps, null);
    } else {
      // get recordInfo by iterating all records.
      recordInfoList1 = getRecordInfoList(numRecords, primaryKeys, timestamps, null);
    }
    upsertMetadataManager.addSegment(segment1, validDocIds1, null, recordInfoList1.iterator());
    trackedSegments.add(segment1);
    // segment1: 0 -> {5, 100}, 1 -> {4, 120}, 2 -> {2, 100}
    assertEquals(recordLocationMap.size(), 3);
    checkRecordLocation(recordLocationMap, 0, segment1, 5, 100, hashFunction);
    checkRecordLocation(recordLocationMap, 1, segment1, 4, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 2, segment1, 2, 100, hashFunction);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{2, 4, 5});

    // Add the second segment
    numRecords = 5;
    primaryKeys = new int[]{0, 1, 2, 3, 0};
    timestamps = new int[]{100, 100, 120, 80, 80};
    ThreadSafeMutableRoaringBitmap validDocIds2 = new ThreadSafeMutableRoaringBitmap();
    ImmutableSegmentImpl segment2 =
        mockImmutableSegment(2, validDocIds2, null, getPrimaryKeyList(numRecords, primaryKeys));
    List<RecordInfo> recordInfoList2;
    if (enableSnapshot) {
      // get recordInfo from validDocIdSnapshot.
      // segment2 snapshot: 0 -> {0, 100}, 2 -> {2, 120}, 3 -> {3, 80}
      // segment1 snapshot: 1 -> {4, 120}
      MutableRoaringBitmap validDocIdsSnapshot2 = new MutableRoaringBitmap();
      validDocIdsSnapshot2.add(0, 2, 3);
      recordInfoList2 = getRecordInfoList(validDocIdsSnapshot2, primaryKeys, timestamps, null);
    } else {
      // get recordInfo by iterating all records.
      recordInfoList2 = getRecordInfoList(numRecords, primaryKeys, timestamps, null);
    }
    upsertMetadataManager.addSegment(segment2, validDocIds2, null, recordInfoList2.iterator());
    trackedSegments.add(segment2);

    // segment1: 1 -> {4, 120}
    // segment2: 0 -> {0, 100}, 2 -> {2, 120}, 3 -> {3, 80}
    assertEquals(recordLocationMap.size(), 4);
    checkRecordLocation(recordLocationMap, 0, segment2, 0, 100, hashFunction);
    checkRecordLocation(recordLocationMap, 1, segment1, 4, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 2, segment2, 2, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 3, segment2, 3, 80, hashFunction);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{4});
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 2, 3});

    // Add an empty segment
    EmptyIndexSegment emptySegment = mockEmptySegment(3);
    upsertMetadataManager.addSegment(emptySegment);
    // segment1: 1 -> {4, 120}
    // segment2: 0 -> {0, 100}, 2 -> {2, 120}, 3 -> {3, 80}
    assertEquals(recordLocationMap.size(), 4);
    checkRecordLocation(recordLocationMap, 0, segment2, 0, 100, hashFunction);
    checkRecordLocation(recordLocationMap, 1, segment1, 4, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 2, segment2, 2, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 3, segment2, 3, 80, hashFunction);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{4});
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 2, 3});

    // Replace (reload) the first segment
    ThreadSafeMutableRoaringBitmap newValidDocIds1 = new ThreadSafeMutableRoaringBitmap();
    ImmutableSegmentImpl newSegment1 = mockImmutableSegment(1, newValidDocIds1, null, primaryKeys1);
    upsertMetadataManager.replaceSegment(newSegment1, newValidDocIds1, null, recordInfoList1.iterator(), segment1);
    trackedSegments.add(newSegment1);
    trackedSegments.remove(segment1);
    // original segment1: 1 -> {4, 120} (not in the map)
    // segment2: 0 -> {0, 100}, 2 -> {2, 120}, 3 -> {3, 80}
    // new segment1: 1 -> {4, 120}
    assertEquals(recordLocationMap.size(), 4);
    checkRecordLocation(recordLocationMap, 0, segment2, 0, 100, hashFunction);
    checkRecordLocation(recordLocationMap, 1, newSegment1, 4, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 2, segment2, 2, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 3, segment2, 3, 80, hashFunction);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{4});
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 2, 3});
    assertEquals(newValidDocIds1.getMutableRoaringBitmap().toArray(), new int[]{4});

    // Remove the original segment1
    upsertMetadataManager.removeSegment(segment1);
    // segment2: 0 -> {0, 100}, 2 -> {2, 120}, 3 -> {3, 80}
    // new segment1: 1 -> {4, 120}
    assertEquals(recordLocationMap.size(), 4);
    checkRecordLocation(recordLocationMap, 0, segment2, 0, 100, hashFunction);
    checkRecordLocation(recordLocationMap, 1, newSegment1, 4, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 2, segment2, 2, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 3, segment2, 3, 80, hashFunction);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{4});
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 2, 3});
    assertEquals(newValidDocIds1.getMutableRoaringBitmap().toArray(), new int[]{4});

    // Remove the empty segment
    upsertMetadataManager.removeSegment(emptySegment);
    // segment2: 0 -> {0, 100}, 2 -> {2, 120}, 3 -> {3, 80}
    // new segment1: 1 -> {4, 120}
    assertEquals(recordLocationMap.size(), 4);
    checkRecordLocation(recordLocationMap, 0, segment2, 0, 100, hashFunction);
    checkRecordLocation(recordLocationMap, 1, newSegment1, 4, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 2, segment2, 2, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 3, segment2, 3, 80, hashFunction);
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 2, 3});
    assertEquals(newValidDocIds1.getMutableRoaringBitmap().toArray(), new int[]{4});

    // Remove segment2
    upsertMetadataManager.removeSegment(segment2);
    // segment2: 0 -> {0, 100}, 2 -> {2, 120}, 3 -> {3, 80} (not in the map)
    // new segment1: 1 -> {4, 120}
    assertEquals(recordLocationMap.size(), 1);
    checkRecordLocation(recordLocationMap, 1, newSegment1, 4, 120, hashFunction);
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 2, 3});
    assertEquals(newValidDocIds1.getMutableRoaringBitmap().toArray(), new int[]{4});
    assertEquals(trackedSegments, Collections.singleton(newSegment1));

    // Stop the metadata manager
    upsertMetadataManager.stop();

    // Remove new segment1, should be no-op
    upsertMetadataManager.removeSegment(newSegment1);
    // new segment1: 1 -> {4, 120}
    assertEquals(recordLocationMap.size(), 1);
    checkRecordLocation(recordLocationMap, 1, newSegment1, 4, 120, hashFunction);
    assertEquals(newValidDocIds1.getMutableRoaringBitmap().toArray(), new int[]{4});
    assertEquals(trackedSegments, Collections.singleton(newSegment1));

    // Close the metadata manager
    upsertMetadataManager.close();
  }

  @Test
  public void testAddReplaceRemoveSegmentWithRecordDelete()
      throws IOException {
    verifyAddReplaceRemoveSegmentWithRecordDelete(HashFunction.NONE, false);
    verifyAddReplaceRemoveSegmentWithRecordDelete(HashFunction.MD5, false);
    verifyAddReplaceRemoveSegmentWithRecordDelete(HashFunction.MURMUR3, false);
    verifyAddReplaceRemoveSegmentWithRecordDelete(HashFunction.NONE, true);
    verifyAddReplaceRemoveSegmentWithRecordDelete(HashFunction.MD5, true);
    verifyAddReplaceRemoveSegmentWithRecordDelete(HashFunction.MURMUR3, true);
  }

  private void verifyAddReplaceRemoveSegmentWithRecordDelete(HashFunction hashFunction, boolean enableSnapshot)
      throws IOException {
    String comparisonColumn = "timeCol";
    String deleteRecordColumn = "deleteCol";
    ConcurrentMapPartitionUpsertMetadataManager upsertMetadataManager =
        new ConcurrentMapPartitionUpsertMetadataManager(REALTIME_TABLE_NAME, 0, Collections.singletonList("pk"),
            Collections.singletonList(comparisonColumn), deleteRecordColumn, hashFunction, null, false,
            mock(ServerMetrics.class));
    Map<Object, RecordLocation> recordLocationMap = upsertMetadataManager._primaryKeyToRecordLocationMap;
    Set<IndexSegment> trackedSegments = upsertMetadataManager._trackedSegments;

    // Add the first segment
    int numRecords = 6;
    int[] primaryKeys = new int[]{0, 1, 2, 0, 1, 0};
    int[] timestamps = new int[]{100, 100, 100, 80, 120, 100};
    boolean[] deleteFlags = new boolean[]{false, false, false, true, true, false};
    ThreadSafeMutableRoaringBitmap validDocIds1 = new ThreadSafeMutableRoaringBitmap();
    ThreadSafeMutableRoaringBitmap queryableDocIds1 = new ThreadSafeMutableRoaringBitmap();
    List<PrimaryKey> primaryKeys1 = getPrimaryKeyList(numRecords, primaryKeys);
    ImmutableSegmentImpl segment1 = mockImmutableSegment(1, validDocIds1, queryableDocIds1, primaryKeys1);
    List<RecordInfo> recordInfoList1;
    if (enableSnapshot) {
      // get recordInfo from validDocIdSnapshot.
      // segment1 snapshot: 0 -> {5, 100}, 1 -> {4, 120}, 2 -> {2, 100}
      int[] docIds1 = new int[]{2, 4, 5};
      MutableRoaringBitmap validDocIdsSnapshot1 = new MutableRoaringBitmap();
      validDocIdsSnapshot1.add(docIds1);
      recordInfoList1 = getRecordInfoList(validDocIdsSnapshot1, primaryKeys, timestamps, deleteFlags);
    } else {
      // get recordInfo by iterating all records.
      recordInfoList1 = getRecordInfoList(numRecords, primaryKeys, timestamps, deleteFlags);
    }
    upsertMetadataManager.addSegment(segment1, validDocIds1, queryableDocIds1, recordInfoList1.iterator());
    trackedSegments.add(segment1);
    // segment1: 0 -> {5, 100}, 1 -> {4, 120}, 2 -> {2, 100}
    assertEquals(recordLocationMap.size(), 3);
    checkRecordLocation(recordLocationMap, 0, segment1, 5, 100, hashFunction);
    checkRecordLocation(recordLocationMap, 1, segment1, 4, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 2, segment1, 2, 100, hashFunction);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{2, 4, 5});
    assertEquals(queryableDocIds1.getMutableRoaringBitmap().toArray(), new int[]{2, 5});

    // Add the second segment
    numRecords = 5;
    primaryKeys = new int[]{0, 1, 2, 3, 0};
    timestamps = new int[]{100, 100, 120, 80, 80};
    deleteFlags = new boolean[]{false, true, true, false, false};
    ThreadSafeMutableRoaringBitmap validDocIds2 = new ThreadSafeMutableRoaringBitmap();
    ThreadSafeMutableRoaringBitmap queryableDocIds2 = new ThreadSafeMutableRoaringBitmap();
    ImmutableSegmentImpl segment2 =
        mockImmutableSegment(2, validDocIds2, queryableDocIds2, getPrimaryKeyList(numRecords, primaryKeys));
    List<RecordInfo> recordInfoList2;
    if (enableSnapshot) {
      // get recordInfo from validDocIdSnapshot.
      // segment2 snapshot: 0 -> {0, 100}, 2 -> {2, 120}, 3 -> {3, 80}
      // segment1 snapshot: 1 -> {4, 120}
      MutableRoaringBitmap validDocIdsSnapshot2 = new MutableRoaringBitmap();
      validDocIdsSnapshot2.add(0, 2, 3);
      recordInfoList2 = getRecordInfoList(validDocIdsSnapshot2, primaryKeys, timestamps, deleteFlags);
    } else {
      // get recordInfo by iterating all records.
      recordInfoList2 = getRecordInfoList(numRecords, primaryKeys, timestamps, deleteFlags);
    }
    upsertMetadataManager.addSegment(segment2, validDocIds2, queryableDocIds2, recordInfoList2.iterator());
    trackedSegments.add(segment2);

    // segment1: 1 -> {4, 120}
    // segment2: 0 -> {0, 100}, 2 -> {2, 120}, 3 -> {3, 80}
    assertEquals(recordLocationMap.size(), 4);
    checkRecordLocation(recordLocationMap, 0, segment2, 0, 100, hashFunction);
    checkRecordLocation(recordLocationMap, 1, segment1, 4, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 2, segment2, 2, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 3, segment2, 3, 80, hashFunction);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{4});
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 2, 3});
    Assert.assertTrue(queryableDocIds1.getMutableRoaringBitmap().isEmpty());
    assertEquals(queryableDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 3});

    // Add an empty segment
    EmptyIndexSegment emptySegment = mockEmptySegment(3);
    upsertMetadataManager.addSegment(emptySegment);
    // segment1: 1 -> {4, 120}
    // segment2: 0 -> {0, 100}, 2 -> {2, 120}, 3 -> {3, 80}
    assertEquals(recordLocationMap.size(), 4);
    checkRecordLocation(recordLocationMap, 0, segment2, 0, 100, hashFunction);
    checkRecordLocation(recordLocationMap, 1, segment1, 4, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 2, segment2, 2, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 3, segment2, 3, 80, hashFunction);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{4});
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 2, 3});
    Assert.assertTrue(queryableDocIds1.getMutableRoaringBitmap().isEmpty());
    assertEquals(queryableDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 3});

    // Replace (reload) the first segment
    ThreadSafeMutableRoaringBitmap newValidDocIds1 = new ThreadSafeMutableRoaringBitmap();
    ThreadSafeMutableRoaringBitmap newQueryableDocIds1 = new ThreadSafeMutableRoaringBitmap();
    ImmutableSegmentImpl newSegment1 = mockImmutableSegment(1, newValidDocIds1, newQueryableDocIds1, primaryKeys1);
    upsertMetadataManager.replaceSegment(newSegment1, newValidDocIds1, newQueryableDocIds1, recordInfoList1.iterator(),
        segment1);
    trackedSegments.add(newSegment1);
    trackedSegments.remove(segment1);

    // original segment1: 1 -> {4, 120} (not in the map)
    // segment2: 0 -> {0, 100}, 2 -> {2, 120}, 3 -> {3, 80}
    // new segment1: 1 -> {4, 120}
    assertEquals(recordLocationMap.size(), 4);
    checkRecordLocation(recordLocationMap, 0, segment2, 0, 100, hashFunction);
    checkRecordLocation(recordLocationMap, 1, newSegment1, 4, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 2, segment2, 2, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 3, segment2, 3, 80, hashFunction);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{4});
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 2, 3});
    assertEquals(newValidDocIds1.getMutableRoaringBitmap().toArray(), new int[]{4});
    Assert.assertTrue(queryableDocIds1.getMutableRoaringBitmap().isEmpty());
    assertEquals(queryableDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 3});
    Assert.assertTrue(newQueryableDocIds1.getMutableRoaringBitmap().isEmpty());

    // Remove the original segment1
    upsertMetadataManager.removeSegment(segment1);
    // segment2: 0 -> {0, 100}, 2 -> {2, 120}, 3 -> {3, 80}
    // new segment1: 1 -> {4, 120}
    assertEquals(recordLocationMap.size(), 4);
    checkRecordLocation(recordLocationMap, 0, segment2, 0, 100, hashFunction);
    checkRecordLocation(recordLocationMap, 1, newSegment1, 4, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 2, segment2, 2, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 3, segment2, 3, 80, hashFunction);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{4});
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 2, 3});
    assertEquals(newValidDocIds1.getMutableRoaringBitmap().toArray(), new int[]{4});
    Assert.assertTrue(queryableDocIds1.getMutableRoaringBitmap().isEmpty());
    assertEquals(queryableDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 3});
    Assert.assertTrue(newQueryableDocIds1.getMutableRoaringBitmap().isEmpty());

    // Remove the empty segment
    upsertMetadataManager.removeSegment(emptySegment);
    // segment2: 0 -> {0, 100}, 2 -> {2, 120}, 3 -> {3, 80}
    // new segment1: 1 -> {4, 120}
    assertEquals(recordLocationMap.size(), 4);
    checkRecordLocation(recordLocationMap, 0, segment2, 0, 100, hashFunction);
    checkRecordLocation(recordLocationMap, 1, newSegment1, 4, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 2, segment2, 2, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 3, segment2, 3, 80, hashFunction);
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 2, 3});
    assertEquals(newValidDocIds1.getMutableRoaringBitmap().toArray(), new int[]{4});
    assertEquals(queryableDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 3});
    Assert.assertTrue(newQueryableDocIds1.getMutableRoaringBitmap().isEmpty());

    // Remove segment2
    upsertMetadataManager.removeSegment(segment2);
    // segment2: 0 -> {0, 100}, 2 -> {2, 120}, 3 -> {3, 80} (not in the map)
    // new segment1: 1 -> {4, 120}
    assertEquals(recordLocationMap.size(), 1);
    checkRecordLocation(recordLocationMap, 1, newSegment1, 4, 120, hashFunction);
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 2, 3});
    assertEquals(newValidDocIds1.getMutableRoaringBitmap().toArray(), new int[]{4});
    assertEquals(trackedSegments, Collections.singleton(newSegment1));
    assertEquals(queryableDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 3});
    Assert.assertTrue(newQueryableDocIds1.getMutableRoaringBitmap().isEmpty());

    // Stop the metadata manager
    upsertMetadataManager.stop();

    // Remove new segment1, should be no-op
    upsertMetadataManager.removeSegment(newSegment1);
    // new segment1: 1 -> {4, 120}
    assertEquals(recordLocationMap.size(), 1);
    checkRecordLocation(recordLocationMap, 1, newSegment1, 4, 120, hashFunction);
    assertEquals(newValidDocIds1.getMutableRoaringBitmap().toArray(), new int[]{4});
    assertEquals(trackedSegments, Collections.singleton(newSegment1));
    Assert.assertTrue(newQueryableDocIds1.getMutableRoaringBitmap().isEmpty());

    // Close the metadata manager
    upsertMetadataManager.close();
  }

  private List<RecordInfo> getRecordInfoList(int numRecords, int[] primaryKeys, int[] timestamps,
      @Nullable boolean[] deleteRecordFlags) {
    List<RecordInfo> recordInfoList = new ArrayList<>();
    for (int i = 0; i < numRecords; i++) {
      recordInfoList.add(new RecordInfo(makePrimaryKey(primaryKeys[i]), i, new IntWrapper(timestamps[i]),
          deleteRecordFlags != null && deleteRecordFlags[i]));
    }
    return recordInfoList;
  }

  /**
   * Get recordInfo from validDocIdsSnapshot (enabledSnapshot = True).
   */
  private List<RecordInfo> getRecordInfoList(MutableRoaringBitmap validDocIdsSnapshot, int[] primaryKeys,
      int[] timestamps, @Nullable boolean[] deleteRecordFlags) {
    List<RecordInfo> recordInfoList = new ArrayList<>();
    Iterator<Integer> validDocIdsIterator = validDocIdsSnapshot.iterator();
    validDocIdsIterator.forEachRemaining((docId) -> recordInfoList.add(
        new RecordInfo(makePrimaryKey(primaryKeys[docId]), docId, new IntWrapper(timestamps[docId]),
            deleteRecordFlags != null && deleteRecordFlags[docId])));
    return recordInfoList;
  }

  private List<PrimaryKey> getPrimaryKeyList(int numRecords, int[] primaryKeys) {
    List<PrimaryKey> primaryKeyList = new ArrayList<>();
    for (int i = 0; i < numRecords; i++) {
      primaryKeyList.add(makePrimaryKey(primaryKeys[i]));
    }
    return primaryKeyList;
  }

  private static ImmutableSegmentImpl mockImmutableSegment(int sequenceNumber,
      ThreadSafeMutableRoaringBitmap validDocIds, @Nullable ThreadSafeMutableRoaringBitmap queryableDocIds,
      List<PrimaryKey> primaryKeys) {
    ImmutableSegmentImpl segment = mock(ImmutableSegmentImpl.class);
    when(segment.getSegmentName()).thenReturn(getSegmentName(sequenceNumber));
    when(segment.getValidDocIds()).thenReturn(validDocIds);
    when(segment.getQueryableDocIds()).thenReturn(queryableDocIds);
    DataSource dataSource = mock(DataSource.class);
    when(segment.getDataSource(anyString())).thenReturn(dataSource);
    ForwardIndexReader forwardIndex = mock(ForwardIndexReader.class);
    when(forwardIndex.isSingleValue()).thenReturn(true);
    when(forwardIndex.getStoredType()).thenReturn(DataType.INT);
    when(forwardIndex.getInt(anyInt(), any())).thenAnswer(
        invocation -> primaryKeys.get(invocation.getArgument(0)).getValues()[0]);
    when(dataSource.getForwardIndex()).thenReturn(forwardIndex);
    return segment;
  }

  private static EmptyIndexSegment mockEmptySegment(int sequenceNumber) {
    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segmentMetadata.getName()).thenReturn(getSegmentName(sequenceNumber));
    return new EmptyIndexSegment(segmentMetadata);
  }

  private static MutableSegment mockMutableSegment(int sequenceNumber, ThreadSafeMutableRoaringBitmap validDocIds,
      ThreadSafeMutableRoaringBitmap queryableDocIds) {
    MutableSegment segment = mock(MutableSegment.class);
    when(segment.getSegmentName()).thenReturn(getSegmentName(sequenceNumber));
    when(segment.getQueryableDocIds()).thenReturn(queryableDocIds);
    when(segment.getValidDocIds()).thenReturn(validDocIds);
    return segment;
  }

  private static String getSegmentName(int sequenceNumber) {
    return new LLCSegmentName(RAW_TABLE_NAME, 0, sequenceNumber, System.currentTimeMillis()).toString();
  }

  private static PrimaryKey makePrimaryKey(int value) {
    return new PrimaryKey(new Object[]{value});
  }

  private static void checkRecordLocation(Map<Object, RecordLocation> recordLocationMap, int keyValue,
      IndexSegment segment, int docId, int comparisonValue, HashFunction hashFunction) {
    RecordLocation recordLocation =
        recordLocationMap.get(HashUtils.hashPrimaryKey(makePrimaryKey(keyValue), hashFunction));
    assertNotNull(recordLocation);
    assertSame(recordLocation.getSegment(), segment);
    assertEquals(recordLocation.getDocId(), docId);
    assertEquals(((IntWrapper) recordLocation.getComparisonValue())._value, comparisonValue);
  }

  @Test
  public void testAddRecord()
      throws IOException {
    verifyAddRecord(HashFunction.NONE);
    verifyAddRecord(HashFunction.MD5);
    verifyAddRecord(HashFunction.MURMUR3);
  }

  private void verifyAddRecord(HashFunction hashFunction)
      throws IOException {
    String comparisonColumn = "timeCol";
    ConcurrentMapPartitionUpsertMetadataManager upsertMetadataManager =
        new ConcurrentMapPartitionUpsertMetadataManager(REALTIME_TABLE_NAME, 0, Collections.singletonList("pk"),
            Collections.singletonList(comparisonColumn), null, hashFunction, null, false, mock(ServerMetrics.class));
    Map<Object, RecordLocation> recordLocationMap = upsertMetadataManager._primaryKeyToRecordLocationMap;

    // Add the first segment
    // segment1: 0 -> {0, 100}, 1 -> {1, 120}, 2 -> {2, 100}
    int numRecords = 3;
    int[] primaryKeys = new int[]{0, 1, 2};
    int[] timestamps = new int[]{100, 120, 100};
    ThreadSafeMutableRoaringBitmap validDocIds1 = new ThreadSafeMutableRoaringBitmap();
    ImmutableSegmentImpl segment1 =
        mockImmutableSegment(1, validDocIds1, null, getPrimaryKeyList(numRecords, primaryKeys));
    upsertMetadataManager.addSegment(segment1, validDocIds1, null,
        getRecordInfoList(numRecords, primaryKeys, timestamps, null).iterator());

    // Update records from the second segment
    ThreadSafeMutableRoaringBitmap validDocIds2 = new ThreadSafeMutableRoaringBitmap();
    MutableSegment segment2 = mockMutableSegment(1, validDocIds2, null);
    upsertMetadataManager.addRecord(segment2, new RecordInfo(makePrimaryKey(3), 0, new IntWrapper(100), false));

    // segment1: 0 -> {0, 100}, 1 -> {1, 120}, 2 -> {2, 100}
    // segment2: 3 -> {0, 100}
    checkRecordLocation(recordLocationMap, 0, segment1, 0, 100, hashFunction);
    checkRecordLocation(recordLocationMap, 1, segment1, 1, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 2, segment1, 2, 100, hashFunction);
    checkRecordLocation(recordLocationMap, 3, segment2, 0, 100, hashFunction);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{0, 1, 2});
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0});

    upsertMetadataManager.addRecord(segment2, new RecordInfo(makePrimaryKey(2), 1, new IntWrapper(120), false));

    // segment1: 0 -> {0, 100}, 1 -> {1, 120}
    // segment2: 2 -> {1, 120}, 3 -> {0, 100}
    checkRecordLocation(recordLocationMap, 0, segment1, 0, 100, hashFunction);
    checkRecordLocation(recordLocationMap, 1, segment1, 1, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 2, segment2, 1, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 3, segment2, 0, 100, hashFunction);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{0, 1});
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 1});

    upsertMetadataManager.addRecord(segment2, new RecordInfo(makePrimaryKey(1), 2, new IntWrapper(100), false));

    // segment1: 0 -> {0, 100}, 1 -> {1, 120}
    // segment2: 2 -> {1, 120}, 3 -> {0, 100}
    checkRecordLocation(recordLocationMap, 0, segment1, 0, 100, hashFunction);
    checkRecordLocation(recordLocationMap, 1, segment1, 1, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 2, segment2, 1, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 3, segment2, 0, 100, hashFunction);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{0, 1});
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 1});

    upsertMetadataManager.addRecord(segment2, new RecordInfo(makePrimaryKey(0), 3, new IntWrapper(100), false));

    // segment1: 1 -> {1, 120}
    // segment2: 0 -> {3, 100}, 2 -> {1, 120}, 3 -> {0, 100}
    checkRecordLocation(recordLocationMap, 0, segment2, 3, 100, hashFunction);
    checkRecordLocation(recordLocationMap, 1, segment1, 1, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 2, segment2, 1, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 3, segment2, 0, 100, hashFunction);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{1});
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 1, 3});

    // Stop the metadata manager
    upsertMetadataManager.stop();

    // Add record should be no-op
    upsertMetadataManager.addRecord(segment2, new RecordInfo(makePrimaryKey(0), 4, new IntWrapper(120), false));
    // segment1: 1 -> {1, 120}
    // segment2: 0 -> {3, 100}, 2 -> {1, 120}, 3 -> {0, 100}
    checkRecordLocation(recordLocationMap, 0, segment2, 3, 100, hashFunction);
    checkRecordLocation(recordLocationMap, 1, segment1, 1, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 2, segment2, 1, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 3, segment2, 0, 100, hashFunction);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{1});
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 1, 3});

    // Close the metadata manager
    upsertMetadataManager.close();
  }

  @Test
  public void testAddRecordWithDeleteColumn()
      throws IOException {
    verifyAddRecordWithDeleteColumn(HashFunction.NONE);
    verifyAddRecordWithDeleteColumn(HashFunction.MD5);
    verifyAddRecordWithDeleteColumn(HashFunction.MURMUR3);
  }
  private void verifyAddRecordWithDeleteColumn(HashFunction hashFunction)
      throws IOException {
    String comparisonColumn = "timeCol";
    String deleteColumn = "deleteCol";
    ConcurrentMapPartitionUpsertMetadataManager upsertMetadataManager =
        new ConcurrentMapPartitionUpsertMetadataManager(REALTIME_TABLE_NAME, 0, Collections.singletonList("pk"),
            Collections.singletonList(comparisonColumn), deleteColumn, hashFunction, null,
            false, mock(ServerMetrics.class));
    Map<Object, RecordLocation> recordLocationMap = upsertMetadataManager._primaryKeyToRecordLocationMap;

    // queryableDocIds is same as validDocIds in the absence of delete markers
    // Add the first segment
    // segment1: 0 -> {0, 100}, 1 -> {1, 120}, 2 -> {2, 100}
    int numRecords = 3;
    int[] primaryKeys = new int[]{0, 1, 2};
    int[] timestamps = new int[]{100, 120, 100};
    ThreadSafeMutableRoaringBitmap validDocIds1 = new ThreadSafeMutableRoaringBitmap();
    ThreadSafeMutableRoaringBitmap queryableDocIds1 = new ThreadSafeMutableRoaringBitmap();
    ImmutableSegmentImpl segment1 =
        mockImmutableSegment(1, validDocIds1, queryableDocIds1, getPrimaryKeyList(numRecords, primaryKeys));
    upsertMetadataManager.addSegment(segment1, validDocIds1, queryableDocIds1,
        getRecordInfoList(numRecords, primaryKeys, timestamps, null).iterator());

    // Update records from the second segment
    ThreadSafeMutableRoaringBitmap validDocIds2 = new ThreadSafeMutableRoaringBitmap();
    ThreadSafeMutableRoaringBitmap queryableDocIds2 = new ThreadSafeMutableRoaringBitmap();
    MutableSegment segment2 = mockMutableSegment(1, validDocIds2, queryableDocIds2);
    upsertMetadataManager.addRecord(segment2, new RecordInfo(makePrimaryKey(3), 0, new IntWrapper(100), false));

    // segment1: 0 -> {0, 100}, 1 -> {1, 120}, 2 -> {2, 100}
    // segment2: 3 -> {0, 100}
    checkRecordLocation(recordLocationMap, 0, segment1, 0, 100, hashFunction);
    checkRecordLocation(recordLocationMap, 1, segment1, 1, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 2, segment1, 2, 100, hashFunction);
    checkRecordLocation(recordLocationMap, 3, segment2, 0, 100, hashFunction);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{0, 1, 2});
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0});
    assertEquals(queryableDocIds1.getMutableRoaringBitmap().toArray(), new int[]{0, 1, 2});
    assertEquals(queryableDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0});

    // Mark a record with latest value in segment1 as deleted
    upsertMetadataManager.addRecord(segment2, new RecordInfo(makePrimaryKey(2), 1, new IntWrapper(120), true));

    // segment1: 0 -> {0, 100}, 1 -> {1, 120}
    // segment2: 2 -> {1, 120}, 3 -> {0, 100}
    checkRecordLocation(recordLocationMap, 0, segment1, 0, 100, hashFunction);
    checkRecordLocation(recordLocationMap, 1, segment1, 1, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 2, segment2, 1, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 3, segment2, 0, 100, hashFunction);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{0, 1});
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 1});
    assertEquals(queryableDocIds1.getMutableRoaringBitmap().toArray(), new int[]{0, 1});
    assertEquals(queryableDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0});

    // Mark a record with latest value in segment2 as deleted
    upsertMetadataManager.addRecord(segment2, new RecordInfo(makePrimaryKey(3), 2, new IntWrapper(150), true));

    // segment1: 0 -> {0, 100}, 1 -> {1, 120}
    // segment2: 2 -> {1, 120}, 3 -> {2, 150}
    checkRecordLocation(recordLocationMap, 0, segment1, 0, 100, hashFunction);
    checkRecordLocation(recordLocationMap, 1, segment1, 1, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 2, segment2, 1, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 3, segment2, 2, 150, hashFunction);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{0, 1});
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{1, 2});
    assertEquals(queryableDocIds1.getMutableRoaringBitmap().toArray(), new int[]{0, 1});
    assertEquals(queryableDocIds2.getMutableRoaringBitmap().toArray(), new int[]{});

    // Revive a deleted primary key (by providing a larger comparisonValue)
    upsertMetadataManager.addRecord(segment2, new RecordInfo(makePrimaryKey(3), 3, new IntWrapper(200), false));

    // segment1: 0 -> {0, 100}, 1 -> {1, 120}
    // segment2: 2 -> {1, 120}, 3 -> {3, 200}
    checkRecordLocation(recordLocationMap, 0, segment1, 0, 100, hashFunction);
    checkRecordLocation(recordLocationMap, 1, segment1, 1, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 2, segment2, 1, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 3, segment2, 3, 200, hashFunction);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{0, 1});
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{1, 3});
    assertEquals(queryableDocIds1.getMutableRoaringBitmap().toArray(), new int[]{0, 1});
    assertEquals(queryableDocIds2.getMutableRoaringBitmap().toArray(), new int[]{3});

    // Stop the metadata manager
    upsertMetadataManager.stop();

    // Add record should be no-op
    upsertMetadataManager.addRecord(segment2, new RecordInfo(makePrimaryKey(0), 4, new IntWrapper(120), false));
    // segment1: 0 -> {0, 100}, 1 -> {1, 120}
    // segment2: 2 -> {1, 120}, 3 -> {3, 200}
    checkRecordLocation(recordLocationMap, 0, segment1, 0, 100, hashFunction);
    checkRecordLocation(recordLocationMap, 1, segment1, 1, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 2, segment2, 1, 120, hashFunction);
    checkRecordLocation(recordLocationMap, 3, segment2, 3, 200, hashFunction);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{0, 1});
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{1, 3});
    assertEquals(queryableDocIds1.getMutableRoaringBitmap().toArray(), new int[]{0, 1});
    assertEquals(queryableDocIds2.getMutableRoaringBitmap().toArray(), new int[]{3});

    // Close the metadata manager
    upsertMetadataManager.close();
  }

  @Test
  public void testHashPrimaryKey() {
    PrimaryKey pk = new PrimaryKey(new Object[]{"uuid-1", "uuid-2", "uuid-3"});
    assertEquals(BytesUtils.toHexString(((ByteArray) HashUtils.hashPrimaryKey(pk, HashFunction.MD5)).getBytes()),
        "6ca926be8c2d1d980acf48ba48418e24");
    assertEquals(BytesUtils.toHexString(((ByteArray) HashUtils.hashPrimaryKey(pk, HashFunction.MURMUR3)).getBytes()),
        "e4540494e43b27e312d01f33208c6a4e");
    // reorder
    pk = new PrimaryKey(new Object[]{"uuid-3", "uuid-2", "uuid-1"});
    assertEquals(BytesUtils.toHexString(((ByteArray) HashUtils.hashPrimaryKey(pk, HashFunction.MD5)).getBytes()),
        "fc2159b78d07f803fdfb0b727315a445");
    assertEquals(BytesUtils.toHexString(((ByteArray) HashUtils.hashPrimaryKey(pk, HashFunction.MURMUR3)).getBytes()),
        "37fab5ef0ea39711feabcdc623cb8a4e");
  }

  /**
   * Use a wrapper class to ensure different value has different reference.
   */
  private static class IntWrapper implements Comparable<IntWrapper> {
    final int _value;

    IntWrapper(int value) {
      _value = value;
    }

    @Override
    public int compareTo(IntWrapper o) {
      return Integer.compare(_value, o._value);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof IntWrapper)) {
        return false;
      }
      IntWrapper that = (IntWrapper) o;
      return _value == that._value;
    }

    @Override
    public int hashCode() {
      return _value;
    }
  }
}
