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

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class SegmentCompletionUtilsTest {
  private static final String SEGMENT_NAME = "table__0__1__123";
  private static final String SERVER_A = "Server_host-a_8098";
  private static final String SERVER_B = "Server_host-b_8098";

  @Test
  public void testGenerateSegmentFilePrefix() {
    assertEquals(SegmentCompletionUtils.getTmpSegmentNamePrefix("segment"), "segment.tmp.");
  }

  @Test
  public void testSameServerRetryReusesTempKey() {
    String first = SegmentCompletionUtils.generateTmpSegmentFileName(SEGMENT_NAME, SERVER_A);
    String retry = SegmentCompletionUtils.generateTmpSegmentFileName(SEGMENT_NAME, SERVER_A);
    assertEquals(first, SEGMENT_NAME + ".tmp." + SERVER_A);
    assertEquals(retry, first);
  }

  @Test
  public void testTwoReplicasDoNotShareTempKey() {
    String replicaA = SegmentCompletionUtils.generateTmpSegmentFileName(SEGMENT_NAME, SERVER_A);
    String replicaB = SegmentCompletionUtils.generateTmpSegmentFileName(SEGMENT_NAME, SERVER_B);
    assertNotEquals(replicaA, replicaB);
    assertTrue(replicaA.endsWith(SERVER_A));
    assertTrue(replicaB.endsWith(SERVER_B));
    assertFalse(replicaA.contains(SERVER_B));
    assertFalse(replicaB.contains(SERVER_A));
  }

  @Test
  public void testUuidGeneratorStillMintsDistinctLeftoverNames() {
    String first = SegmentCompletionUtils.generateTmpSegmentFileName(SEGMENT_NAME);
    String second = SegmentCompletionUtils.generateTmpSegmentFileName(SEGMENT_NAME);
    assertTrue(first.startsWith(SEGMENT_NAME + ".tmp."));
    assertTrue(second.startsWith(SEGMENT_NAME + ".tmp."));
    assertNotEquals(first, second);
    assertTrue(SegmentCompletionUtils.isTmpFile(first));
    assertTrue(SegmentCompletionUtils.isTmpFile(second));
  }

  @Test
  public void testRejectsUnsafeInstanceId() {
    expectThrows(IllegalArgumentException.class,
        () -> SegmentCompletionUtils.generateTmpSegmentFileName(SEGMENT_NAME, ""));
    expectThrows(IllegalArgumentException.class,
        () -> SegmentCompletionUtils.generateTmpSegmentFileName(SEGMENT_NAME, "Server_host/8098"));
    expectThrows(IllegalArgumentException.class,
        () -> SegmentCompletionUtils.generateTmpSegmentFileName(SEGMENT_NAME, "Server_host\\8098"));
    expectThrows(IllegalArgumentException.class, () -> SegmentCompletionUtils.generateTmpSegmentFileName("", SERVER_A));
  }

  @Test
  public void testIsTmpFileRecognizesInstanceIdAndLeftoverUuid() {
    assertTrue(SegmentCompletionUtils.isTmpFile("hdfs://foo.tmp.550e8400-e29b-41d4-a716-446655440000"));
    assertTrue(SegmentCompletionUtils.isTmpFile("hdfs://foo/" + SEGMENT_NAME + ".tmp." + SERVER_A));
    assertTrue(SegmentCompletionUtils.isTmpFile("s3://bucket/" + SEGMENT_NAME + ".tmp.Server_foo.bar.com_8098"));
    assertFalse(SegmentCompletionUtils.isTmpFile("hdfs://foo.tmp."));
    assertFalse(SegmentCompletionUtils.isTmpFile(".tmp.550e8400-e29b-41d4-a716-446655440000"));
    assertFalse(SegmentCompletionUtils.isTmpFile("hdfs://foo/" + SEGMENT_NAME));
    assertFalse(SegmentCompletionUtils.isTmpFile("hdfs://foo.tmp.Server_host/8098"));
  }
}
