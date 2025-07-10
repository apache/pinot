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

import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


public class SegmentUtilsTest {
  private static final String SEGMENT = "testSegment";

  @Test
  public void testGetSegmentCreationTimeMs() {
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(SEGMENT);
    segmentZKMetadata.setCreationTime(1000L);
    assertEquals(SegmentUtils.getSegmentCreationTimeMs(segmentZKMetadata), 1000L);
    segmentZKMetadata.setPushTime(2000L);
    assertEquals(SegmentUtils.getSegmentCreationTimeMs(segmentZKMetadata), 2000L);
  }

  @Test
  public void testGetPartitionIdFromSegmentPartitionMetadata() {
    // Single partition column
    SegmentPartitionMetadata segmentPartitionMetadata =
        new SegmentPartitionMetadata(Map.of("p1", new ColumnPartitionMetadata("modulo", 8, Set.of(3), null)));
    assertEquals(SegmentUtils.getPartitionIdFromSegmentPartitionMetadata(segmentPartitionMetadata, null), 3);
    assertEquals(SegmentUtils.getPartitionIdFromSegmentPartitionMetadata(segmentPartitionMetadata, "p1"), 3);
    assertNull(SegmentUtils.getPartitionIdFromSegmentPartitionMetadata(segmentPartitionMetadata, "p2"));

    // Multiple partition columns
    segmentPartitionMetadata = new SegmentPartitionMetadata(
        Map.of("p1", new ColumnPartitionMetadata("modulo", 8, Set.of(3), null), "p2",
            new ColumnPartitionMetadata("hash", 4, Set.of(1), null)));
    assertEquals(SegmentUtils.getPartitionIdFromSegmentPartitionMetadata(segmentPartitionMetadata, "p1"), 3);
    assertEquals(SegmentUtils.getPartitionIdFromSegmentPartitionMetadata(segmentPartitionMetadata, "p2"), 1);
    assertNull(SegmentUtils.getPartitionIdFromSegmentPartitionMetadata(segmentPartitionMetadata, null));
    assertNull(SegmentUtils.getPartitionIdFromSegmentPartitionMetadata(segmentPartitionMetadata, "p3"));

    // Multiple partition ids
    segmentPartitionMetadata =
        new SegmentPartitionMetadata(Map.of("p1", new ColumnPartitionMetadata("modulo", 8, Set.of(3, 4), null)));
    assertNull(SegmentUtils.getPartitionIdFromSegmentPartitionMetadata(segmentPartitionMetadata, null));
    assertNull(SegmentUtils.getPartitionIdFromSegmentPartitionMetadata(segmentPartitionMetadata, "p1"));
    assertNull(SegmentUtils.getPartitionIdFromSegmentPartitionMetadata(segmentPartitionMetadata, "p2"));
  }

  @Test
  void testGetPartitionIdFromSegmentName() {
    assertEquals(SegmentUtils.getPartitionIdFromSegmentName("table__3__100__1716185755000"), 3);
    assertEquals(SegmentUtils.getPartitionIdFromSegmentName("uploaded__table_name__3__100__1716185755000"), 3);
  }

  @Test
  void testGetTableNameFromSegmentName() {
    String segmentName = "some_table_name__0__1240__20250419T0723Z";
    String tableName = SegmentUtils.getTableNameFromSegmentName(segmentName);
    assertEquals(tableName, "some_table_name");
  }
}
