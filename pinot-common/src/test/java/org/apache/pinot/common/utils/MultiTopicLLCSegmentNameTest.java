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
package org.apache.pinot.common.utils;

import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class MultiTopicLLCSegmentNameTest {

  @Test
  public void testConstructFromComponents() {
    MultiTopicLLCSegmentName name = new MultiTopicLLCSegmentName("myTable", 2, 5, 3, 1700000000000L);
    assertEquals(name.getTableName(), "myTable");
    assertEquals(name.getConfigId(), 2);
    assertEquals(name.getStreamPartitionId(), 5);
    assertEquals(name.getSequenceNumber(), 3);
    assertEquals(name.getPartitionGroupId(), 20005);
    assertEquals(name.getSegmentName(), "myTable__2__5__3__20231114T2213Z");
  }

  @Test
  public void testConstructFromString() {
    MultiTopicLLCSegmentName name = new MultiTopicLLCSegmentName("myTable__2__5__3__20231114T2213Z");
    assertEquals(name.getTableName(), "myTable");
    assertEquals(name.getConfigId(), 2);
    assertEquals(name.getStreamPartitionId(), 5);
    assertEquals(name.getSequenceNumber(), 3);
    assertEquals(name.getPartitionGroupId(), 20005);
  }

  @Test
  public void testOfValidName() {
    MultiTopicLLCSegmentName name = MultiTopicLLCSegmentName.of("myTable__0__63__10__20231215T0000Z");
    assertNotNull(name);
    assertEquals(name.getConfigId(), 0);
    assertEquals(name.getStreamPartitionId(), 63);
    assertEquals(name.getSequenceNumber(), 10);
    assertEquals(name.getPartitionGroupId(), 63);
  }

  @Test
  public void testOfInvalidName() {
    // 4-part LLC format — should return null
    assertNull(MultiTopicLLCSegmentName.of("myTable__10005__3__20231215T0000Z"));
    // Uploaded realtime format — should return null (parts[3] is a date, not integer)
    assertNull(MultiTopicLLCSegmentName.of("uploaded__myTable__5__20231215T0000Z__suffix"));
    // Garbage
    assertNull(MultiTopicLLCSegmentName.of("not_a_segment"));
  }

  @Test
  public void testIsMultiTopicLLCSegment() {
    assertTrue(MultiTopicLLCSegmentName.isMultiTopicLLCSegment("myTable__2__5__3__20231215T0000Z"));
    assertTrue(MultiTopicLLCSegmentName.isMultiTopicLLCSegment("myTable__0__0__0__20231215T0000Z"));
    // 4-part LLC
    assertFalse(MultiTopicLLCSegmentName.isMultiTopicLLCSegment("myTable__10005__3__20231215T0000Z"));
    // Uploaded realtime — parts[3] is a date string, not parseable as integer
    assertFalse(MultiTopicLLCSegmentName.isMultiTopicLLCSegment("uploaded__myTable__5__20231215T0000Z__suffix"));
    // 3 parts
    assertFalse(MultiTopicLLCSegmentName.isMultiTopicLLCSegment("a__b__c"));
  }

  @Test
  public void testPartitionGroupIdEncoding() {
    // configId=0, partition=5 -> 5
    MultiTopicLLCSegmentName name0 = new MultiTopicLLCSegmentName("myTable", 0, 5, 0, 1700000000000L);
    assertEquals(name0.getPartitionGroupId(), 5);

    // configId=1, partition=3 -> 10003
    MultiTopicLLCSegmentName name1 = new MultiTopicLLCSegmentName("myTable", 1, 3, 0, 1700000000000L);
    assertEquals(name1.getPartitionGroupId(), 10003);

    // configId=5, partition=99 -> 50099
    MultiTopicLLCSegmentName name5 = new MultiTopicLLCSegmentName("myTable", 5, 99, 0, 1700000000000L);
    assertEquals(name5.getPartitionGroupId(), 50099);
  }

  @Test
  public void testToLLCSegmentName() {
    MultiTopicLLCSegmentName multiTopic = new MultiTopicLLCSegmentName("myTable", 1, 5, 3, 1700000000000L);
    LLCSegmentName llc = multiTopic.toLLCSegmentName();
    assertEquals(llc.getTableName(), "myTable");
    assertEquals(llc.getPartitionGroupId(), 10005);
    assertEquals(llc.getSequenceNumber(), 3);
  }

  @Test
  public void testCompareTo() {
    MultiTopicLLCSegmentName a = new MultiTopicLLCSegmentName("myTable", 0, 5, 1, 1700000000000L);
    MultiTopicLLCSegmentName b = new MultiTopicLLCSegmentName("myTable", 0, 5, 2, 1700000000000L);
    MultiTopicLLCSegmentName c = new MultiTopicLLCSegmentName("myTable", 1, 0, 0, 1700000000000L);

    assertTrue(a.compareTo(b) < 0);
    assertTrue(b.compareTo(a) > 0);
    assertTrue(a.compareTo(c) < 0); // configId 0 < configId 1
  }

  @Test
  public void testEqualsAndHashCode() {
    MultiTopicLLCSegmentName a = new MultiTopicLLCSegmentName("myTable", 1, 5, 3, 1700000000000L);
    MultiTopicLLCSegmentName b = new MultiTopicLLCSegmentName(a.getSegmentName());
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());

    MultiTopicLLCSegmentName c = new MultiTopicLLCSegmentName("myTable", 1, 5, 4, 1700000000000L);
    assertNotEquals(a, c);
  }

  @Test
  public void testLLCSegmentNameIsLLCSegmentIncludesMultiTopic() {
    // 4-part format
    assertTrue(LLCSegmentName.isLLCSegment("myTable__10005__3__20231215T0000Z"));
    // 5-part multi-topic format
    assertTrue(LLCSegmentName.isLLCSegment("myTable__1__5__3__20231215T0000Z"));
    // Uploaded realtime — should NOT be detected as LLC
    assertFalse(LLCSegmentName.isLLCSegment("uploaded__myTable__5__20231215T0000Z__suffix"));
  }

  @Test
  public void testSegmentUtilsPartitionId() {
    // Multi-topic segment returns encoded partition group ID
    assertEquals(SegmentUtils.getPartitionIdFromSegmentName("myTable__1__5__3__20231215T0000Z"),
        Integer.valueOf(10005));
    // Standard LLC segment
    assertEquals(SegmentUtils.getPartitionIdFromSegmentName("myTable__10005__3__20231215T0000Z"),
        Integer.valueOf(10005));
  }
}
