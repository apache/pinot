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

import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;


public class TopicPartitionIdTest {

  @Test
  public void testSingleArgConstructor() {
    TopicPartitionId id = new TopicPartitionId(5);
    assertEquals(id.getTopicId(), 0);
    assertEquals(id.getPartitionId(), 5);
  }

  @Test
  public void testTwoArgConstructor() {
    TopicPartitionId id = new TopicPartitionId(2, 7);
    assertEquals(id.getTopicId(), 2);
    assertEquals(id.getPartitionId(), 7);
  }

  @Test
  public void testEquality() {
    TopicPartitionId id1 = new TopicPartitionId(1, 3);
    TopicPartitionId id2 = new TopicPartitionId(1, 3);
    TopicPartitionId id3 = new TopicPartitionId(1, 4);
    TopicPartitionId id4 = new TopicPartitionId(2, 3);

    assertEquals(id1, id2);
    assertEquals(id1.hashCode(), id2.hashCode());
    assertNotEquals(id1, id3);
    assertNotEquals(id1, id4);
  }

  @Test
  public void testSingleArgEqualsZeroTopic() {
    TopicPartitionId id1 = new TopicPartitionId(5);
    TopicPartitionId id2 = new TopicPartitionId(0, 5);
    assertEquals(id1, id2);
    assertEquals(id1.hashCode(), id2.hashCode());
  }

  @Test
  public void testCompareTo() {
    TopicPartitionId a = new TopicPartitionId(0, 1);
    TopicPartitionId b = new TopicPartitionId(0, 2);
    TopicPartitionId c = new TopicPartitionId(1, 0);
    TopicPartitionId d = new TopicPartitionId(1, 1);

    assertTrue(a.compareTo(b) < 0);
    assertTrue(b.compareTo(a) > 0);
    assertTrue(a.compareTo(c) < 0);
    assertTrue(c.compareTo(d) < 0);
    assertEquals(a.compareTo(new TopicPartitionId(0, 1)), 0);
  }

  @Test
  public void testAsMapKey() {
    Map<TopicPartitionId, String> map = new HashMap<>();
    TopicPartitionId id1 = new TopicPartitionId(1, 3);
    map.put(id1, "value1");

    TopicPartitionId id2 = new TopicPartitionId(1, 3);
    assertEquals(map.get(id2), "value1");

    assertFalse(map.containsKey(new TopicPartitionId(1, 4)));
    assertFalse(map.containsKey(new TopicPartitionId(2, 3)));
  }

  @Test
  public void testToString() {
    TopicPartitionId id = new TopicPartitionId(2, 7);
    assertEquals(id.toString(), "2:7");
  }

  @Test
  public void testToMultiTopicPinotPartitionId() {
    assertEquals(new TopicPartitionId(0, 5).toMultiTopicPinotPartitionId(), 5);
    assertEquals(new TopicPartitionId(1, 3).toMultiTopicPinotPartitionId(), 10003);
    assertEquals(new TopicPartitionId(2, 0).toMultiTopicPinotPartitionId(), 20000);
    assertEquals(new TopicPartitionId(5).toMultiTopicPinotPartitionId(), 5);
  }

  @Test
  public void testFromMultiTopicPinotPartitionId() {
    TopicPartitionId id1 = TopicPartitionId.fromMultiTopicPinotPartitionId(10003);
    assertEquals(id1.getTopicId(), 1);
    assertEquals(id1.getPartitionId(), 3);

    TopicPartitionId id2 = TopicPartitionId.fromMultiTopicPinotPartitionId(3);
    assertEquals(id2.getTopicId(), 0);
    assertEquals(id2.getPartitionId(), 3);

    TopicPartitionId id3 = TopicPartitionId.fromMultiTopicPinotPartitionId(20000);
    assertEquals(id3.getTopicId(), 2);
    assertEquals(id3.getPartitionId(), 0);
  }

  @Test
  public void testRoundTrip() {
    TopicPartitionId original = new TopicPartitionId(1, 3);
    TopicPartitionId roundTripped =
        TopicPartitionId.fromMultiTopicPinotPartitionId(original.toMultiTopicPinotPartitionId());
    assertEquals(roundTripped, original);

    TopicPartitionId singleTopic = new TopicPartitionId(0, 42);
    TopicPartitionId roundTripped2 =
        TopicPartitionId.fromMultiTopicPinotPartitionId(singleTopic.toMultiTopicPinotPartitionId());
    assertEquals(roundTripped2, singleTopic);
  }
}
