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
package org.apache.pinot.common.utils;

import java.util.HashMap;
import java.util.HashSet;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.fail;


public class SegmentUtilsTest {
  private static final String TABLE_NAME_WITH_TYPE = "testTable_REALTIME";
  private static final String SEGMENT = "testSegment";
  private static final String PARTITION_COLUMN = "partitionColumn";

  @Test
  public void testGetSegmentCreationTimeMs() {
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(SEGMENT);
    segmentZKMetadata.setCreationTime(1000L);
    assertEquals(SegmentUtils.getSegmentCreationTimeMs(segmentZKMetadata), 1000L);
    segmentZKMetadata.setPushTime(2000L);
    assertEquals(SegmentUtils.getSegmentCreationTimeMs(segmentZKMetadata), 2000L);
  }

  @Test
  public void testGetRealtimeSegmentPartitionIdFromZkMetadata() {

    // mocks
    SegmentZKMetadata segmentZKMetadata = mock(SegmentZKMetadata.class);
    SegmentPartitionMetadata segmentPartitionMetadata = mock(SegmentPartitionMetadata.class);
    HashMap<String, ColumnPartitionMetadata> columnPartitionMetadataMap = new HashMap<>();
    HashSet<Integer> partitions = new HashSet<>();
    partitions.add(3);
    columnPartitionMetadataMap.put(PARTITION_COLUMN,
        new ColumnPartitionMetadata("modulo", 8, partitions, new HashMap<>()));

    when(segmentPartitionMetadata.getColumnPartitionMap()).thenReturn(columnPartitionMetadataMap);
    when(segmentZKMetadata.getPartitionMetadata()).thenReturn(segmentPartitionMetadata);

    HelixManager helixManager = mock(HelixManager.class);
    ZkHelixPropertyStore zkHelixPropertyStore = mock(ZkHelixPropertyStore.class);
    when(helixManager.getHelixPropertyStore()).thenReturn(zkHelixPropertyStore);

    // mock static ZKMetadataProvider.getSegmentZKMetadata
    try (MockedStatic<ZKMetadataProvider> zkMetadataProviderMockedStatic = Mockito.mockStatic(
        ZKMetadataProvider.class)) {
      when(ZKMetadataProvider.getSegmentZKMetadata(Mockito.any(ZkHelixPropertyStore.class), eq(TABLE_NAME_WITH_TYPE),
          eq(SEGMENT))).thenReturn(segmentZKMetadata);

      Integer partitionId =
          SegmentUtils.getRealtimeSegmentPartitionId(SEGMENT, TABLE_NAME_WITH_TYPE, helixManager, PARTITION_COLUMN);

      assertEquals(partitionId, 3);
    }
  }

  @Test
  void testGetRealtimeSegmentPartitionIdForUploadedRealtimeSegment() {
    String segmentName = "uploaded__table_name__3__100__1716185755000";

    try {
      // Check the util method that gets segmentZKMetadata via HelixManager for partition id.
      Integer partitionId =
          SegmentUtils.getRealtimeSegmentPartitionId(segmentName, "realtimeTableName", null, "partitionColumn");
      assertEquals(partitionId, 3);
    } catch (Exception e) {
      fail("Exception should not be thrown");
    }

    try {
      // Check the util method that has segmentZKMetadata passed in directly for partition id.
      Integer partitionId = SegmentUtils.getRealtimeSegmentPartitionId(segmentName, null, "partitionColumn");
      assertEquals(partitionId, 3);
    } catch (Exception e) {
      fail("Exception should not be thrown");
    }
  }

  @Test
  void testGetTableNameFromSegmentName() {
    String segmentName = "some_table_name__0__1240__20250419T0723Z";
    String tableName = SegmentUtils.getTableNameFromSegmentName(segmentName);
    assertEquals(tableName, "some_table_name");
  }
}
