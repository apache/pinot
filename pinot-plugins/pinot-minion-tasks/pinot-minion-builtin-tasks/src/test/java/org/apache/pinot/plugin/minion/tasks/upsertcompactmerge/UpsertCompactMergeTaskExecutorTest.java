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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.common.MinionConstants;
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

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*Max creation time "
      + "configuration is missing from task config.*")
  public void testGetMaxZKCreationTimeFromConfigWithNullValue() {
    // Test that the method handles null config gracefully with fallback to current time
    Map<String, String> configs = new HashMap<>();
    // Intentionally not setting MAX_ZK_CREATION_TIME_MILLIS_KEY to simulate null config during rollout

    long result = _taskExecutor.getMaxZKCreationTimeFromConfig(configs);

    // Should return current system time (within reasonable bounds)
    long currentTime = System.currentTimeMillis();
    Assert.assertTrue(result > 0, "Result should be positive");
    Assert.assertTrue(Math.abs(result - currentTime) < 1000, "Result should be close to current time");
  }

  @Test
  public void testGetMaxZKCreationTimeFromConfigWithValidValue() {
    // Test that the method correctly parses valid config value
    Map<String, String> configs = new HashMap<>();
    long expectedTime = 1234567890L;
    configs.put(MinionConstants.UpsertCompactMergeTask.MAX_ZK_CREATION_TIME_MILLIS_KEY, String.valueOf(expectedTime));

    long result = _taskExecutor.getMaxZKCreationTimeFromConfig(configs);

    Assert.assertEquals(result, expectedTime, "Should return the configured time");
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*Invalid max creation "
      + "time format.*")
  public void testGetMaxZKCreationTimeFromConfigWithInvalidFormat() {
    // Test that the method throws appropriate exception for invalid format
    Map<String, String> configs = new HashMap<>();
    configs.put(MinionConstants.UpsertCompactMergeTask.MAX_ZK_CREATION_TIME_MILLIS_KEY, "invalid_number");

    _taskExecutor.getMaxZKCreationTimeFromConfig(configs);
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*No valid creation time"
      + " found.*")
  public void testGetMaxZKCreationTimeFromConfigWithZeroValue() {
    // Test that the method throws appropriate exception for zero/negative values
    Map<String, String> configs = new HashMap<>();
    configs.put(MinionConstants.UpsertCompactMergeTask.MAX_ZK_CREATION_TIME_MILLIS_KEY, "0");

    _taskExecutor.getMaxZKCreationTimeFromConfig(configs);
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*No valid creation time"
      + " found.*")
  public void testGetMaxZKCreationTimeFromConfigWithNegativeValue() {
    // Test that the method throws appropriate exception for negative values
    Map<String, String> configs = new HashMap<>();
    configs.put(MinionConstants.UpsertCompactMergeTask.MAX_ZK_CREATION_TIME_MILLIS_KEY, "-100");

    _taskExecutor.getMaxZKCreationTimeFromConfig(configs);
  }
}
