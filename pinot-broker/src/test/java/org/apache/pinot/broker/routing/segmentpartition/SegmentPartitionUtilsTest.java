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

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.partition.function.CustomPartitionFunction;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class SegmentPartitionUtilsTest {
  private static final String TABLE_NAME_WITH_TYPE = "testTable_OFFLINE";
  private static final String SEGMENT_NAME = "testSegment";

  @Test
  public void testExtractPartitionInfoReturnsInvalidForInvalidPartitionFunction()
      throws IOException {
    ZNRecord znRecord =
        getZnRecord(Map.of("memberId", getInvalidCustomPartitionMetadata("plus(otherColumn, 1)", Set.of(0))));

    SegmentPartitionInfo segmentPartitionInfo =
        SegmentPartitionUtils.extractPartitionInfo(TABLE_NAME_WITH_TYPE, "memberId", SEGMENT_NAME, znRecord);

    assertSame(segmentPartitionInfo, SegmentPartitionUtils.INVALID_PARTITION_INFO);
  }

  @Test
  public void testExtractPartitionInfoMapSkipsInvalidPartitionFunction()
      throws IOException {
    ZNRecord znRecord = getZnRecord(Map.of("memberId",
        getInvalidCustomPartitionMetadata("plus(otherColumn, 1)", Set.of(0)), "accountId",
        new ColumnPartitionMetadata("Modulo", 4, Set.of(2), null)));

    Map<String, SegmentPartitionInfo> partitionInfoMap = SegmentPartitionUtils.extractPartitionInfoMap(
        TABLE_NAME_WITH_TYPE, Set.of("memberId", "accountId"), SEGMENT_NAME, znRecord);

    assertNotSame(partitionInfoMap, SegmentPartitionUtils.INVALID_COLUMN_PARTITION_INFO_MAP);
    assertFalse(partitionInfoMap.containsKey("memberId"));
    SegmentPartitionInfo accountPartitionInfo = partitionInfoMap.get("accountId");
    assertNotNull(accountPartitionInfo);
    assertEquals(accountPartitionInfo.getPartitionColumn(), "accountId");
    assertEquals(accountPartitionInfo.getPartitionFunction().getName(), "Modulo");
    assertEquals(accountPartitionInfo.getPartitions(), Set.of(2));
  }

  private static ZNRecord getZnRecord(Map<String, ColumnPartitionMetadata> columnPartitionMap)
      throws IOException {
    ZNRecord znRecord = new ZNRecord(SEGMENT_NAME);
    znRecord.setSimpleField(CommonConstants.Segment.PARTITION_METADATA,
        new SegmentPartitionMetadata(columnPartitionMap).toJsonString());
    return znRecord;
  }

  private static ColumnPartitionMetadata getInvalidCustomPartitionMetadata(String partitionExpression,
      Set<Integer> partitions) {
    return new ColumnPartitionMetadata(CustomPartitionFunction.NAME, 4, partitions,
        Map.of(CustomPartitionFunction.PARTITION_EXPRESSION_CONFIG_KEY, partitionExpression));
  }
}
