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
package org.apache.pinot.core.data.manager.realtime;

import java.util.UUID;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class SegmentCompletionUtilsTest {

  @Test
  public void testGenerateSegmentFilePrefix() {
    String segmentName = "segment";
    assertEquals(SegmentCompletionUtils.getTmpSegmentNamePrefix(segmentName), "segment.tmp.");
  }

  @Test
  public void testGenerateTmpSegmentFileName() {
    String segmentName = "segment";
    String segmentNamePrefix = SegmentCompletionUtils.getTmpSegmentNamePrefix(segmentName);
    String firstSegmentFileName = SegmentCompletionUtils.generateTmpSegmentFileName(segmentName);
    String secondSegmentFileName = SegmentCompletionUtils.generateTmpSegmentFileName(segmentName);
    assertTrue(firstSegmentFileName.startsWith(segmentNamePrefix));
    assertTrue(secondSegmentFileName.startsWith(segmentNamePrefix));
    assertNotEquals(secondSegmentFileName, firstSegmentFileName);
    assertTrue(SegmentCompletionUtils.isTmpFile(firstSegmentFileName));
    assertTrue(SegmentCompletionUtils.isTmpFile(secondSegmentFileName));

    UUID segmentBuildId = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
    assertEquals(SegmentCompletionUtils.generateTmpSegmentFileName(segmentName, segmentBuildId),
        "segment.tmp.550e8400-e29b-41d4-a716-446655440000");
    assertEquals(SegmentCompletionUtils.generateTmpSegmentFileName(segmentName, segmentBuildId),
        SegmentCompletionUtils.generateTmpSegmentFileName(segmentName, segmentBuildId));
  }

  @Test
  public void testGenerateUploadId() {
    UUID uploadId = SegmentCompletionUtils.generateUploadId("server-1", "segment", "100");
    assertEquals(uploadId.version(), 8);
    assertEquals(uploadId.variant(), 2);
    assertEquals(SegmentCompletionUtils.generateUploadId("server-1", "segment", "100"), uploadId);
    assertNotEquals(SegmentCompletionUtils.generateUploadId("server-2", "segment", "100"), uploadId);
    assertNotEquals(SegmentCompletionUtils.generateUploadId("server-1", "segment", "101"), uploadId);
    assertNotEquals(SegmentCompletionUtils.generateUploadId("a", "bc", "d"),
        SegmentCompletionUtils.generateUploadId("ab", "c", "d"));
  }

  @Test
  public void testIsTmpFile() {
    assertTrue(SegmentCompletionUtils.isTmpFile("hdfs://foo.tmp.550e8400-e29b-41d4-a716-446655440000"));
    assertFalse(SegmentCompletionUtils.isTmpFile("hdfs://foo.tmp."));
    assertFalse(SegmentCompletionUtils.isTmpFile(".tmp.550e8400-e29b-41d4-a716-446655440000"));
    assertFalse(SegmentCompletionUtils.isTmpFile("hdfs://foo.tmp.55"));
  }
}
