/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.utils;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static com.linkedin.pinot.common.utils.SegmentNameBuilder.Realtime.*;
import static org.testng.Assert.fail;


/**
 * Tests for the realtime segment name builder.
 */
public class SegmentNameBuilderTest {
  @Test
  public void testSegmentNameBuilder() {
    String oldV1Name = "myTable_REALTIME__Server_1.2.3.4_1234__myTable_REALTIME_1234567_0__0__23456789";
    String shortV1Name = SegmentNameBuilder.Realtime.buildHighLevelConsumerSegmentName("myTable_REALTIME_1234567_0", "ALL", "1234567");
    String v2Name = SegmentNameBuilder.Realtime.buildLowLevelConsumerSegmentName("myTable_REALTIME", "0", "1", 1465508537069L);

    assertEquals(shortV1Name, "myTable_REALTIME_1234567_0__ALL__1234567");
    assertEquals(v2Name, "myTable_REALTIME__0__1__20160609T2142Z");

    // Check v1/v2 format detection
    assertEquals(isRealtimeV1Name(oldV1Name), true);
    assertEquals(isRealtimeV1Name(shortV1Name), true);
    assertEquals(isRealtimeV1Name(v2Name), false);

    assertEquals(isRealtimeV2Name(oldV1Name), false);
    assertEquals(isRealtimeV2Name(shortV1Name), false);
    assertEquals(isRealtimeV2Name(v2Name), true);

    // Check table name
    assertEquals(extractTableName(oldV1Name), "myTable_REALTIME");
    assertEquals(extractTableName(shortV1Name), "myTable_REALTIME");
    assertEquals(extractTableName(v2Name), "myTable_REALTIME");

    // Check partition range
    assertEquals(extractPartitionRange(oldV1Name), "0");
    assertEquals(extractPartitionRange(shortV1Name), "ALL");
    assertEquals(extractPartitionRange(v2Name), "0");

    // Check groupId
    assertEquals(extractGroupIdName(oldV1Name), "myTable_REALTIME_1234567_0");
    assertEquals(extractGroupIdName(shortV1Name), "myTable_REALTIME_1234567_0");
    try {
      extractGroupIdName(v2Name);
      fail("extractGroupIdName with a v2 name did not throw an exception");
    } catch (Exception e) {
      // Expected
    }

    // Check sequence number
    assertEquals(extractSequenceNumber(oldV1Name), "23456789");
    assertEquals(extractSequenceNumber(shortV1Name), "1234567");
    assertEquals(extractSequenceNumber(v2Name), "1");
  }

  @Test
  public void testExceptions() {
    // Nulls should not work
    try {
      SegmentNameBuilder.Realtime.buildHighLevelConsumerSegmentName(null, null, null);
      fail("Exception not thrown");
    } catch (Exception e) {
      // Expected
    }

    try {
      SegmentNameBuilder.Realtime.buildLowLevelConsumerSegmentName(null, null, null, 1234L);
      fail("Exception not thrown");
    } catch (Exception e) {
      // Expected
    }

    // Double underscores should not work
    try {
      SegmentNameBuilder.Realtime.buildHighLevelConsumerSegmentName("__", "a", "b");
      fail("Exception not thrown");
    } catch (Exception e) {
      // Expected
    }

    try {
      SegmentNameBuilder.Realtime.buildLowLevelConsumerSegmentName("a", "b", "__", 1234L);
      fail("Exception not thrown");
    } catch (Exception e) {
      // Expected
    }
  }
}
