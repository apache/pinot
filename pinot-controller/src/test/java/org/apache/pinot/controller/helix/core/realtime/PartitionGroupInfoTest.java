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
package org.apache.pinot.controller.helix.core.realtime;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.controller.helix.core.WatermarkInductionResult;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.PartitionGroupMetadata;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class PartitionGroupInfoTest {

  @Test
  public void testConversionFromWatermark() {
    int partitionGroupId = 123;
    long offset = 456;
    int sequenceNumber = 789;

    WatermarkInductionResult.Watermark watermark =
        new WatermarkInductionResult.Watermark(partitionGroupId, sequenceNumber, offset);

    PartitionGroupMetadata metadata = mock(PartitionGroupMetadata.class);
    when(metadata.getPartitionGroupId()).thenReturn(partitionGroupId);
    when(metadata.getStartOffset()).thenReturn(new LongMsgOffset(offset));

    Pair<PartitionGroupMetadata, Integer> pair = Pair.of(metadata, sequenceNumber);

    assertNotNull(pair);
    assertEquals(pair.getLeft().getPartitionGroupId(), partitionGroupId);
    assertEquals(pair.getRight().intValue(), sequenceNumber);
    assertNotNull(pair.getLeft().getStartOffset());
    assertEquals(pair.getLeft().getStartOffset().toString(), new LongMsgOffset(offset).toString());
  }
}
