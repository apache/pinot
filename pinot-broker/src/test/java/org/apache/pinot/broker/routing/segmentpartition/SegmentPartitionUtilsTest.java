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
package org.apache.pinot.broker.routing.segmentpartition;

import java.util.Map;
import java.util.Set;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;


/// Tests for [SegmentPartitionUtils].
public class SegmentPartitionUtilsTest {
  private static final String TABLE_NAME = "testTable_OFFLINE";
  private static final String PARTITION_COLUMN = "memberId";
  private static final String OTHER_PARTITION_COLUMN = "groupId";

  /// Segments with equal partition function metadata share one partition function, and one-element partition sets
  /// are shared too.
  @Test
  public void testSharedPartitionFunctionsAndPartitionSets() {
    SegmentPartitionInfo info0 = extract("segment0", "Murmur", 8, Set.of(3), null);
    SegmentPartitionInfo info1 = extract("segment1", "Murmur", 8, Set.of(3), null);
    SegmentPartitionInfo info2 = extract("segment2", "Murmur", 8, Set.of(5), null);
    assertSame(info1.getPartitionFunction(), info0.getPartitionFunction());
    assertSame(info2.getPartitionFunction(), info0.getPartitionFunction());
    assertSame(info1.getPartitions(), info0.getPartitions());
    assertEquals(info0.getPartitions(), Set.of(3));
    assertEquals(info2.getPartitions(), Set.of(5));
    assertEquals(info0.getPartitionFunction().getName(), "Murmur");
    assertEquals(info0.getPartitionFunction().getNumPartitions(), 8);

    // A different number of partitions, function or function config is a different function
    SegmentPartitionInfo otherNumPartitions = extract("segment3", "Murmur", 16, Set.of(3), null);
    assertNotSame(otherNumPartitions.getPartitionFunction(), info0.getPartitionFunction());
    assertEquals(otherNumPartitions.getPartitionFunction().getNumPartitions(), 16);
    SegmentPartitionInfo otherFunction = extract("segment4", "Modulo", 8, Set.of(3), null);
    assertNotSame(otherFunction.getPartitionFunction(), info0.getPartitionFunction());
    assertEquals(otherFunction.getPartitionFunction().getName(), "Modulo");
    SegmentPartitionInfo otherConfig = extract("segment5", "Murmur", 8, Set.of(3), Map.of("useRawBytes", "true"));
    assertNotSame(otherConfig.getPartitionFunction(), info0.getPartitionFunction());

    // Multi-partition sets are kept as is
    SegmentPartitionInfo multiPartitions = extract("segment6", "Murmur", 8, Set.of(1, 2), null);
    assertEquals(multiPartitions.getPartitions(), Set.of(1, 2));
    assertSame(multiPartitions.getPartitionFunction(), info0.getPartitionFunction());
  }

  @Test
  public void testSharedPartitionFunctionsInPartitionInfoMap() {
    ZNRecord znRecord0 = znRecord("segment0", "Murmur", 8, Set.of(3), null);
    ZNRecord znRecord1 = znRecord("segment1", "Murmur", 8, Set.of(3), null);
    Set<String> partitionColumns = Set.of(PARTITION_COLUMN, OTHER_PARTITION_COLUMN);
    Map<String, SegmentPartitionInfo> infoMap0 =
        SegmentPartitionUtils.extractPartitionInfoMap(TABLE_NAME, partitionColumns, "segment0", znRecord0);
    Map<String, SegmentPartitionInfo> infoMap1 =
        SegmentPartitionUtils.extractPartitionInfoMap(TABLE_NAME, partitionColumns, "segment1", znRecord1);
    assertNotNull(infoMap0);
    assertNotNull(infoMap1);
    for (String partitionColumn : partitionColumns) {
      assertSame(infoMap1.get(partitionColumn).getPartitionFunction(),
          infoMap0.get(partitionColumn).getPartitionFunction());
      assertSame(infoMap1.get(partitionColumn).getPartitions(), infoMap0.get(partitionColumn).getPartitions());
      assertEquals(infoMap0.get(partitionColumn).getPartitions(), Set.of(3));
    }
  }

  private static SegmentPartitionInfo extract(String segment, String functionName, int numPartitions,
      Set<Integer> partitions, Map<String, String> functionConfig) {
    SegmentPartitionInfo info = SegmentPartitionUtils.extractPartitionInfo(TABLE_NAME, PARTITION_COLUMN, segment,
        znRecord(segment, functionName, numPartitions, partitions, functionConfig));
    assertNotNull(info);
    return info;
  }

  private static ZNRecord znRecord(String segment, String functionName, int numPartitions, Set<Integer> partitions,
      Map<String, String> functionConfig) {
    ColumnPartitionMetadata columnPartitionMetadata =
        new ColumnPartitionMetadata(functionName, numPartitions, partitions, functionConfig);
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segment);
    segmentZKMetadata.setPartitionMetadata(new SegmentPartitionMetadata(
        Map.of(PARTITION_COLUMN, columnPartitionMetadata, OTHER_PARTITION_COLUMN, columnPartitionMetadata)));
    return segmentZKMetadata.toZNRecord();
  }
}
