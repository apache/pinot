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
package org.apache.pinot.plugin.minion.tasks.upsertcompactmerge;

import java.util.Arrays;
import java.util.List;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;



public class UpsertCompactMergeTaskExecutorTest {
  private UpsertCompactMergeTaskExecutor _taskExecutor;

  @BeforeClass
  public void setUp() {
    _taskExecutor = new UpsertCompactMergeTaskExecutor(null);
  }

  @Test
  public void testValidateCRCForInputSegments() {
    SegmentMetadataImpl segment1 = Mockito.mock(SegmentMetadataImpl.class);
    SegmentMetadataImpl segment2 = Mockito.mock(SegmentMetadataImpl.class);

    Mockito.when(segment1.getCrc()).thenReturn("1000");
    Mockito.when(segment2.getCrc()).thenReturn("2000");

    List<SegmentMetadataImpl> segmentMetadataList = Arrays.asList(segment1, segment2);
    List<String> expectedCRCList = Arrays.asList("1000", "2000");

    _taskExecutor.validateCRCForInputSegments(segmentMetadataList, expectedCRCList);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testValidateCRCForInputSegmentsWithMismatchedCRC() {
    SegmentMetadataImpl segment1 = Mockito.mock(SegmentMetadataImpl.class);
    SegmentMetadataImpl segment2 = Mockito.mock(SegmentMetadataImpl.class);

    Mockito.when(segment1.getCrc()).thenReturn("1000");
    Mockito.when(segment2.getCrc()).thenReturn("3000");

    List<SegmentMetadataImpl> segmentMetadataList = Arrays.asList(segment1, segment2);
    List<String> expectedCRCList = Arrays.asList("1000", "2000");

    _taskExecutor.validateCRCForInputSegments(segmentMetadataList, expectedCRCList);
  }

  @Test
  public void testGetCommonPartitionIDForSegments() {
    SegmentMetadataImpl segment1 = Mockito.mock(SegmentMetadataImpl.class);
    SegmentMetadataImpl segment2 = Mockito.mock(SegmentMetadataImpl.class);
    SegmentMetadataImpl segment3 = Mockito.mock(SegmentMetadataImpl.class);

    Mockito.when(segment1.getName()).thenReturn("testTable__0__0__0");
    Mockito.when(segment2.getName()).thenReturn("testTable__0__1__0");
    Mockito.when(segment3.getName()).thenReturn("testTable__0__2__0");

    List<SegmentMetadataImpl> segmentMetadataList = Arrays.asList(segment1, segment2, segment3);

    int partitionID = _taskExecutor.getCommonPartitionIDForSegments(segmentMetadataList);
    Assert.assertEquals(partitionID, 0);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testGetCommonPartitionIDForSegmentsWithDifferentPartitionIDs() {
    SegmentMetadataImpl segment1 = Mockito.mock(SegmentMetadataImpl.class);
    SegmentMetadataImpl segment2 = Mockito.mock(SegmentMetadataImpl.class);

    Mockito.when(segment1.getName()).thenReturn("testTable__0__0__0");
    Mockito.when(segment2.getName()).thenReturn("testTable__1__0__0");

    List<SegmentMetadataImpl> segmentMetadataList = Arrays.asList(segment1, segment2);

    _taskExecutor.getCommonPartitionIDForSegments(segmentMetadataList);
  }
}
