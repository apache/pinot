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
package org.apache.pinot.common.metadata.segment;

import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


public class SegmentZKMetadataUtilsTest {
  @Test
  public void testCreateSegmentZKMetadataForNewLLCSegmentWithoutOffsets() {
    String realtimeTableName = "testTable_REALTIME";
    String llcSegmentName = new LLCSegmentName("testTable", 0, 1, System.currentTimeMillis()).getSegmentName();

    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(segmentMetadata.getName()).thenReturn(llcSegmentName);
    when(segmentMetadata.getStartOffset()).thenReturn(null);
    when(segmentMetadata.getEndOffset()).thenReturn(null);
    when(segmentMetadata.getCrc()).thenReturn(12345L);
    when(segmentMetadata.getDataCrc()).thenReturn(12345L);
    when(segmentMetadata.getIndexCreationTime()).thenReturn(System.currentTimeMillis());
    when(segmentMetadata.getTimeUnit()).thenReturn(TimeUnit.MILLISECONDS);
    when(segmentMetadata.getStartTime()).thenReturn(1000L);
    when(segmentMetadata.getEndTime()).thenReturn(2000L);
    when(segmentMetadata.getTotalDocs()).thenReturn(100);

    SegmentZKMetadata zkMetadata = SegmentZKMetadataUtils.createSegmentZKMetadata(
            realtimeTableName, segmentMetadata, "hdfs://fsName/seg", null, 1024);

    assertNotNull(zkMetadata);
    assertEquals(zkMetadata.getSegmentName(), llcSegmentName);
    assertNull(zkMetadata.getStartOffset());
    assertNull(zkMetadata.getEndOffset());
  }
}
