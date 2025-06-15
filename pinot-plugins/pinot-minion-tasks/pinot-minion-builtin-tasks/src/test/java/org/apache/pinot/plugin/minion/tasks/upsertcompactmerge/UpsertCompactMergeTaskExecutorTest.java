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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.plugin.minion.tasks.SegmentConversionResult;
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

  @Test
  public void testValidateCRCForInputSegmentsWithStringCRC() {
    SegmentMetadataImpl segment1 = Mockito.mock(SegmentMetadataImpl.class);
    SegmentMetadataImpl segment2 = Mockito.mock(SegmentMetadataImpl.class);

    Mockito.when(segment1.getCrc()).thenReturn("abc123");
    Mockito.when(segment2.getCrc()).thenReturn("def456");

    List<SegmentMetadataImpl> segmentMetadataList = Arrays.asList(segment1, segment2);
    List<String> expectedCRCList = Arrays.asList("abc123", "def456");

    // Should not throw exception for matching string CRCs
    _taskExecutor.validateCRCForInputSegments(segmentMetadataList, expectedCRCList);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testValidateCRCForInputSegmentsWithNullCRC() {
    SegmentMetadataImpl segment1 = Mockito.mock(SegmentMetadataImpl.class);

    Mockito.when(segment1.getName()).thenReturn("testSegment");
    Mockito.when(segment1.getCrc()).thenReturn(null);

    List<SegmentMetadataImpl> segmentMetadataList = Arrays.asList(segment1);
    List<String> expectedCRCList = Arrays.asList("1000");

    _taskExecutor.validateCRCForInputSegments(segmentMetadataList, expectedCRCList);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testValidateCRCForInputSegmentsWithNullExpectedCRC() {
    SegmentMetadataImpl segment1 = Mockito.mock(SegmentMetadataImpl.class);

    Mockito.when(segment1.getName()).thenReturn("testSegment");
    Mockito.when(segment1.getCrc()).thenReturn("1000");

    List<SegmentMetadataImpl> segmentMetadataList = Arrays.asList(segment1);
    List<String> expectedCRCList = Arrays.asList((String) null);

    _taskExecutor.validateCRCForInputSegments(segmentMetadataList, expectedCRCList);
  }

  @Test
  public void testValidateCRCForInputSegmentsWithEmptyList() {
    List<SegmentMetadataImpl> segmentMetadataList = Arrays.asList();
    List<String> expectedCRCList = Arrays.asList();

    // Should not throw exception for empty lists
    _taskExecutor.validateCRCForInputSegments(segmentMetadataList, expectedCRCList);
  }

  @Test(expectedExceptions = IndexOutOfBoundsException.class)
  public void testValidateCRCForInputSegmentsWithMismatchedListSizes() {
    SegmentMetadataImpl segment1 = Mockito.mock(SegmentMetadataImpl.class);

    Mockito.when(segment1.getCrc()).thenReturn("1000");

    List<SegmentMetadataImpl> segmentMetadataList = Arrays.asList(segment1);
    List<String> expectedCRCList = Arrays.asList(); // Empty list

    _taskExecutor.validateCRCForInputSegments(segmentMetadataList, expectedCRCList);
  }

  @Test
  public void testGetCommonPartitionIDForSingleSegment() {
    SegmentMetadataImpl segment1 = Mockito.mock(SegmentMetadataImpl.class);

    Mockito.when(segment1.getName()).thenReturn("testTable__5__0__0");

    List<SegmentMetadataImpl> segmentMetadataList = Arrays.asList(segment1);

    int partitionID = _taskExecutor.getCommonPartitionIDForSegments(segmentMetadataList);
    Assert.assertEquals(partitionID, 5);
  }

  @Test
  public void testGetCommonPartitionIDForMultipleSegmentsWithHighPartitionID() {
    SegmentMetadataImpl segment1 = Mockito.mock(SegmentMetadataImpl.class);
    SegmentMetadataImpl segment2 = Mockito.mock(SegmentMetadataImpl.class);
    SegmentMetadataImpl segment3 = Mockito.mock(SegmentMetadataImpl.class);

    Mockito.when(segment1.getName()).thenReturn("testTable__100__0__0");
    Mockito.when(segment2.getName()).thenReturn("testTable__100__1__0");
    Mockito.when(segment3.getName()).thenReturn("testTable__100__2__0");

    List<SegmentMetadataImpl> segmentMetadataList = Arrays.asList(segment1, segment2, segment3);

    int partitionID = _taskExecutor.getCommonPartitionIDForSegments(segmentMetadataList);
    Assert.assertEquals(partitionID, 100);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testGetCommonPartitionIDForSegmentsWithInvalidSegmentName() {
    SegmentMetadataImpl segment1 = Mockito.mock(SegmentMetadataImpl.class);

    // Invalid segment name format
    Mockito.when(segment1.getName()).thenReturn("invalid_segment_name");

    List<SegmentMetadataImpl> segmentMetadataList = Arrays.asList(segment1);

    _taskExecutor.getCommonPartitionIDForSegments(segmentMetadataList);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testGetCommonPartitionIDForSegmentsWithMixedPartitionIDs() {
    SegmentMetadataImpl segment1 = Mockito.mock(SegmentMetadataImpl.class);
    SegmentMetadataImpl segment2 = Mockito.mock(SegmentMetadataImpl.class);
    SegmentMetadataImpl segment3 = Mockito.mock(SegmentMetadataImpl.class);

    Mockito.when(segment1.getName()).thenReturn("testTable__0__0__0");
    Mockito.when(segment2.getName()).thenReturn("testTable__0__1__0");
    Mockito.when(segment3.getName()).thenReturn("testTable__1__0__0"); // Different partition ID

    List<SegmentMetadataImpl> segmentMetadataList = Arrays.asList(segment1, segment2, segment3);

    _taskExecutor.getCommonPartitionIDForSegments(segmentMetadataList);
  }

  @Test
  public void testValidateCRCForInputSegmentsWithIdenticalCRCs() {
    SegmentMetadataImpl segment1 = Mockito.mock(SegmentMetadataImpl.class);
    SegmentMetadataImpl segment2 = Mockito.mock(SegmentMetadataImpl.class);
    SegmentMetadataImpl segment3 = Mockito.mock(SegmentMetadataImpl.class);

    Mockito.when(segment1.getCrc()).thenReturn("same_crc");
    Mockito.when(segment2.getCrc()).thenReturn("same_crc");
    Mockito.when(segment3.getCrc()).thenReturn("same_crc");

    List<SegmentMetadataImpl> segmentMetadataList = Arrays.asList(segment1, segment2, segment3);
    List<String> expectedCRCList = Arrays.asList("same_crc", "same_crc", "same_crc");

    // Should not throw exception for identical CRCs
    _taskExecutor.validateCRCForInputSegments(segmentMetadataList, expectedCRCList);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testValidateCRCForInputSegmentsWithPartialMismatch() {
    SegmentMetadataImpl segment1 = Mockito.mock(SegmentMetadataImpl.class);
    SegmentMetadataImpl segment2 = Mockito.mock(SegmentMetadataImpl.class);
    SegmentMetadataImpl segment3 = Mockito.mock(SegmentMetadataImpl.class);

    Mockito.when(segment1.getName()).thenReturn("segment1");
    Mockito.when(segment2.getName()).thenReturn("segment2");
    Mockito.when(segment3.getName()).thenReturn("segment3");

    Mockito.when(segment1.getCrc()).thenReturn("1000");
    Mockito.when(segment2.getCrc()).thenReturn("2000");
    Mockito.when(segment3.getCrc()).thenReturn("4000"); // Mismatch here

    List<SegmentMetadataImpl> segmentMetadataList = Arrays.asList(segment1, segment2, segment3);
    List<String> expectedCRCList = Arrays.asList("1000", "2000", "3000");

    _taskExecutor.validateCRCForInputSegments(segmentMetadataList, expectedCRCList);
  }

  @Test
  public void testValidateCRCForInputSegmentsWithLargeCRCs() {
    SegmentMetadataImpl segment1 = Mockito.mock(SegmentMetadataImpl.class);
    SegmentMetadataImpl segment2 = Mockito.mock(SegmentMetadataImpl.class);

    // Large CRC values
    String largeCrc1 = "123456789012345";
    String largeCrc2 = "987654321098765";

    Mockito.when(segment1.getCrc()).thenReturn(largeCrc1);
    Mockito.when(segment2.getCrc()).thenReturn(largeCrc2);

    List<SegmentMetadataImpl> segmentMetadataList = Arrays.asList(segment1, segment2);
    List<String> expectedCRCList = Arrays.asList(largeCrc1, largeCrc2);

    // Should not throw exception for matching large CRCs
    _taskExecutor.validateCRCForInputSegments(segmentMetadataList, expectedCRCList);
  }

    @Test
  public void testGetSegmentZKMetadataCustomMapModifier() {
    // Create mock PinotTaskConfig
    Map<String, String> configs = new HashMap<>();
    configs.put(MinionConstants.SEGMENT_NAME_KEY, "segment1,segment2,segment3");
    PinotTaskConfig pinotTaskConfig = new PinotTaskConfig(MinionConstants.UpsertCompactMergeTask.TASK_TYPE, configs);

    // Create mock SegmentConversionResult
    SegmentConversionResult segmentConversionResult = Mockito.mock(SegmentConversionResult.class);

    // Test the method
    SegmentZKMetadataCustomMapModifier modifier =
        _taskExecutor.getSegmentZKMetadataCustomMapModifier(pinotTaskConfig, segmentConversionResult);

    // Verify the modifier is created
    Assert.assertNotNull(modifier);

    // Test the modifier behavior by applying it to an empty map
    Map<String, String> originalMap = new HashMap<>();
    Map<String, String> modifiedMap = modifier.modifyMap(originalMap);
    Assert.assertNotNull(modifiedMap);
    Assert.assertEquals(modifiedMap.size(), 2);

    // Verify the merged segments list is stored
    String mergedSegmentsKey = MinionConstants.UpsertCompactMergeTask.TASK_TYPE
        + MinionConstants.UpsertCompactMergeTask.MERGED_SEGMENTS_ZK_SUFFIX;
    Assert.assertTrue(modifiedMap.containsKey(mergedSegmentsKey));
    Assert.assertEquals(modifiedMap.get(mergedSegmentsKey), "segment1,segment2,segment3");

    // Verify the task time is stored
    String taskTimeKey = MinionConstants.UpsertCompactMergeTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX;
    Assert.assertTrue(modifiedMap.containsKey(taskTimeKey));
    String taskTime = modifiedMap.get(taskTimeKey);
    Assert.assertNotNull(taskTime);
    long timestamp = Long.parseLong(taskTime);
    Assert.assertTrue(timestamp > 0);
  }

    @Test
  public void testGetSegmentZKMetadataCustomMapModifierWithEmptySegmentNames() {
    // Create mock PinotTaskConfig with empty segment names
    Map<String, String> configs = new HashMap<>();
    configs.put(MinionConstants.SEGMENT_NAME_KEY, "");
    PinotTaskConfig pinotTaskConfig = new PinotTaskConfig(MinionConstants.UpsertCompactMergeTask.TASK_TYPE, configs);

    // Create mock SegmentConversionResult
    SegmentConversionResult segmentConversionResult = Mockito.mock(SegmentConversionResult.class);

    // Test the method
    SegmentZKMetadataCustomMapModifier modifier =
        _taskExecutor.getSegmentZKMetadataCustomMapModifier(pinotTaskConfig, segmentConversionResult);

    // Verify the modifier
    Assert.assertNotNull(modifier);
    Map<String, String> originalMap = new HashMap<>();
    Map<String, String> modifiedMap = modifier.modifyMap(originalMap);
    Assert.assertNotNull(modifiedMap);

    // Verify empty segment names are handled
    String mergedSegmentsKey = MinionConstants.UpsertCompactMergeTask.TASK_TYPE
        + MinionConstants.UpsertCompactMergeTask.MERGED_SEGMENTS_ZK_SUFFIX;
    Assert.assertEquals(modifiedMap.get(mergedSegmentsKey), "");
  }

    @Test
  public void testGetSegmentZKMetadataCustomMapModifierWithSingleSegment() {
    // Create mock PinotTaskConfig with single segment
    Map<String, String> configs = new HashMap<>();
    configs.put(MinionConstants.SEGMENT_NAME_KEY, "singleSegment");
    PinotTaskConfig pinotTaskConfig = new PinotTaskConfig(MinionConstants.UpsertCompactMergeTask.TASK_TYPE, configs);

    // Create mock SegmentConversionResult
    SegmentConversionResult segmentConversionResult = Mockito.mock(SegmentConversionResult.class);

    // Test the method
    SegmentZKMetadataCustomMapModifier modifier =
        _taskExecutor.getSegmentZKMetadataCustomMapModifier(pinotTaskConfig, segmentConversionResult);

    // Verify the modifier
    Assert.assertNotNull(modifier);
    Map<String, String> originalMap = new HashMap<>();
    Map<String, String> modifiedMap = modifier.modifyMap(originalMap);
    Assert.assertNotNull(modifiedMap);

    // Verify single segment name is handled correctly
    String mergedSegmentsKey = MinionConstants.UpsertCompactMergeTask.TASK_TYPE
        + MinionConstants.UpsertCompactMergeTask.MERGED_SEGMENTS_ZK_SUFFIX;
    Assert.assertEquals(modifiedMap.get(mergedSegmentsKey), "singleSegment");
  }

  @Test
  public void testGetCommonPartitionIDForEmptySegmentList() {
    List<SegmentMetadataImpl> emptySegmentMetadataList = Arrays.asList();

    try {
      _taskExecutor.getCommonPartitionIDForSegments(emptySegmentMetadataList);
      Assert.fail("Expected exception for empty segment list");
    } catch (Exception e) {
      // Expected - empty list should cause an exception
      Assert.assertTrue(e instanceof RuntimeException || e instanceof IllegalStateException);
    }
  }

    @Test
  public void testValidateCRCForInputSegmentsPerformance() {
    // Test with a larger number of segments to ensure performance is acceptable
    List<SegmentMetadataImpl> segmentMetadataList = new ArrayList<>();
    List<String> expectedCRCList = new ArrayList<>();

    for (int i = 0; i < 100; i++) {
      SegmentMetadataImpl segment = Mockito.mock(SegmentMetadataImpl.class);
      String crc = "crc_" + i;
      Mockito.when(segment.getCrc()).thenReturn(crc);
      segmentMetadataList.add(segment);
      expectedCRCList.add(crc);
    }

    long startTime = System.currentTimeMillis();
    _taskExecutor.validateCRCForInputSegments(segmentMetadataList, expectedCRCList);
    long duration = System.currentTimeMillis() - startTime;

    // Validation should complete quickly (under 1 second for 100 segments)
    Assert.assertTrue(duration < 1000, "CRC validation took too long: " + duration + "ms");
  }
}
