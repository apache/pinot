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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.minion.MinionConf;
import org.apache.pinot.minion.event.MinionEventObserver;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class UpsertCompactMergeTaskExecutorTest {

  private UpsertCompactMergeTaskExecutor _taskExecutor;
  private File _tempDir;

  @Mock
  private MinionConf _minionConf;

  @Mock
  private MinionEventObserver _eventObserver;

  @BeforeClass
  public void setUpClass() {
    MockitoAnnotations.openMocks(this);
  }

  @BeforeMethod
  public void setUp()
      throws Exception {
    _tempDir = new File(FileUtils.getTempDirectory(),
        "UpsertCompactMergeTaskExecutorTest_" + System.currentTimeMillis());
    FileUtils.forceMkdir(_tempDir);

    _taskExecutor = new UpsertCompactMergeTaskExecutor(_minionConf);
    _taskExecutor.setMinionEventObserver(_eventObserver);
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    if (_tempDir != null && _tempDir.exists()) {
      FileUtils.deleteDirectory(_tempDir);
    }
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

  /**
   * Tests partition ID validation with null partition IDs.
   */
  @Test(expectedExceptions = IllegalStateException.class,
      expectedExceptionsMessageRegExp = ".*Partition id not found.*")
  public void testGetCommonPartitionIDForSegmentsWithNullPartitionId() {
    SegmentMetadataImpl segment1 = Mockito.mock(SegmentMetadataImpl.class);
    Mockito.when(segment1.getName()).thenReturn("testTable_invalidSegmentName");

    List<SegmentMetadataImpl> segmentMetadataList = Arrays.asList(segment1);

    _taskExecutor.getCommonPartitionIDForSegments(segmentMetadataList);
  }

  /**
   * Tests CRC validation with null CRC values.
   */
  @Test(expectedExceptions = IllegalStateException.class)
  public void testValidateCRCForInputSegmentsWithNullCrc() {
    SegmentMetadataImpl segment1 = Mockito.mock(SegmentMetadataImpl.class);
    Mockito.when(segment1.getCrc()).thenReturn(null);
    Mockito.when(segment1.getName()).thenReturn("segment1");

    List<SegmentMetadataImpl> segmentMetadataList = Arrays.asList(segment1);
    List<String> expectedCRCList = Arrays.asList("1000");

    _taskExecutor.validateCRCForInputSegments(segmentMetadataList, expectedCRCList);
  }

  /**
   * Tests handling of empty segment lists.
   */
  @Test(expectedExceptions = NoSuchElementException.class)
  public void testGetCommonPartitionIDForEmptySegmentList() {
    List<SegmentMetadataImpl> segmentMetadataList = Collections.emptyList();
    _taskExecutor.getCommonPartitionIDForSegments(segmentMetadataList);
  }

  /**
   * Tests validation with mismatched list sizes.
   */
  @Test(expectedExceptions = IndexOutOfBoundsException.class)
  public void testValidateCRCWithMismatchedListSizes() {
    SegmentMetadataImpl segment1 = Mockito.mock(SegmentMetadataImpl.class);
    SegmentMetadataImpl segment2 = Mockito.mock(SegmentMetadataImpl.class);

    Mockito.when(segment1.getCrc()).thenReturn("1000");
    Mockito.when(segment2.getCrc()).thenReturn("2000");

    List<SegmentMetadataImpl> segmentMetadataList = Arrays.asList(segment1, segment2);
    List<String> expectedCRCList = Arrays.asList("1000"); // Only one CRC

    _taskExecutor.validateCRCForInputSegments(segmentMetadataList, expectedCRCList);
  }

  /**
   * Tests handling of segments with special characters in names.
   */
  @Test
  public void testGetCommonPartitionIDWithSpecialCharacters() {
    SegmentMetadataImpl segment1 = Mockito.mock(SegmentMetadataImpl.class);
    SegmentMetadataImpl segment2 = Mockito.mock(SegmentMetadataImpl.class);

    // Segments with special characters but same partition
    Mockito.when(segment1.getName()).thenReturn("test-Table__5__0__12345");
    Mockito.when(segment2.getName()).thenReturn("test-Table__5__1__67890");

    List<SegmentMetadataImpl> segmentMetadataList = Arrays.asList(segment1, segment2);

    int partitionID = _taskExecutor.getCommonPartitionIDForSegments(segmentMetadataList);
    Assert.assertEquals(partitionID, 5);
  }

  /**
   * Tests max creation time with boundary values.
   */
  @Test
  public void testGetMaxZKCreationTimeFromConfigBoundaryValues() {
    Map<String, String> configs = new HashMap<>();

    // Test with Long.MAX_VALUE
    String maxKey = MinionConstants.UpsertCompactMergeTask.MAX_ZK_CREATION_TIME_MILLIS_KEY;
    configs.put(maxKey, String.valueOf(Long.MAX_VALUE));
    long result = _taskExecutor.getMaxZKCreationTimeFromConfig(configs);
    Assert.assertEquals(result, Long.MAX_VALUE);

    // Test with minimum valid value (1)
    configs.put(MinionConstants.UpsertCompactMergeTask.MAX_ZK_CREATION_TIME_MILLIS_KEY, "1");
    result = _taskExecutor.getMaxZKCreationTimeFromConfig(configs);
    Assert.assertEquals(result, 1L);
  }

  /**
   * Tests CRC validation with whitespace and empty strings.
   */
  @Test(expectedExceptions = IllegalStateException.class)
  public void testValidateCRCWithEmptyString() {
    SegmentMetadataImpl segment1 = Mockito.mock(SegmentMetadataImpl.class);
    Mockito.when(segment1.getCrc()).thenReturn("");
    Mockito.when(segment1.getName()).thenReturn("segment1");

    List<SegmentMetadataImpl> segmentMetadataList = Arrays.asList(segment1);
    List<String> expectedCRCList = Arrays.asList("1000");

    _taskExecutor.validateCRCForInputSegments(segmentMetadataList, expectedCRCList);
  }

  // Helper methods for testing

  /**
   * Creates simple test segments (for backward compatibility with existing tests).
   */
  private List<File> createTestSegments()
      throws IOException {
    List<File> segmentDirs = new ArrayList<>();

    for (int i = 0; i < 2; i++) {
      File segmentDir = new File(_tempDir, "segment" + i);
      FileUtils.forceMkdir(segmentDir);

      // Create dummy metadata file
      File metadataFile = new File(segmentDir, "metadata.properties");
      FileUtils.writeStringToFile(metadataFile, "segment.name=segment" + i + "\n");

      segmentDirs.add(segmentDir);
    }

    return segmentDirs;
  }
}
