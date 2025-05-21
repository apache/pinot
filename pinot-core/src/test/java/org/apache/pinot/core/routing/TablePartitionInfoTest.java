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
package org.apache.pinot.core.routing;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class TablePartitionInfoTest {
  @Test
  public void testGetSegmentsInPartition() {
    final int partitionId = 1;
    final int numPartitions = 4;
    BiFunction<List<String>, List<String>, TablePartitionInfo> newTpi = (partitionInfoSegments, excludedSegments) -> {
      TablePartitionInfo.PartitionInfo[] info = new TablePartitionInfo.PartitionInfo[4];
      for (int index = 0; index < info.length; index++) {
        if (index == partitionId) {
          info[index] = new TablePartitionInfo.PartitionInfo(Set.of("instance-1"), partitionInfoSegments);
        } else {
          // all segments but partitionId are prefixed with "generic".
          info[index] = new TablePartitionInfo.PartitionInfo(Set.of("instance-1"),
              List.of(String.format("generic-segment-%s", index)));
        }
      }
      return new TablePartitionInfo("testTable_REALTIME", "partCol", "murmur",
          numPartitions, info, List.of() /* segments with invalid partition */, Map.of(partitionId, excludedSegments));
    };
    // Test with the configured partitionId
    assertEquals(List.of("segment-1"), newTpi.apply(List.of(), List.of("segment-1"))
        .getSegmentsInPartition(partitionId));
    assertEquals(List.of("segment-1", "segment-2"), newTpi.apply(List.of("segment-1"), List.of("segment-2"))
        .getSegmentsInPartition(partitionId));
    // Test when the looked up partition does not have any entry in excludedNewSegments map
    assertEquals(List.of("generic-segment-0"), newTpi.apply(List.of("segment-1"), List.of())
        .getSegmentsInPartition(0));
  }
}
