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
package org.apache.pinot.segment.spi.creator.name;

import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class SimpleSegmentNameGeneratorTest {
  private static final String MALFORMED_TABLE_NAME = "test/Table";
  private static final String TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME_POSTFIX = "postfix";
  private static final String MALFORMED_SEGMENT_NAME_POSTFIX = "post*fix";
  private static final int INVALID_SEQUENCE_ID = -1;
  private static final int VALID_SEQUENCE_ID = 0;
  private static final long MIN_TIME_VALUE = 1234L;
  private static final long MAX_TIME_VALUE = 5678L;
  private static final String MALFORMED_TIME_VALUE = "12|34";

  @Test
  public void testWithoutSegmentNamePostfix() {
    SegmentNameGenerator segmentNameGenerator = new SimpleSegmentNameGenerator(TABLE_NAME, null);
    assertEquals(segmentNameGenerator.toString(),
        "SimpleSegmentNameGenerator: tableName=testTable, appendUUIDToSegmentName=false, "
            + "excludeTimeInSegmentName=false");
    assertEquals(segmentNameGenerator.generateSegmentName(INVALID_SEQUENCE_ID, null, null), "testTable");
    assertEquals(segmentNameGenerator.generateSegmentName(INVALID_SEQUENCE_ID, MIN_TIME_VALUE, MAX_TIME_VALUE),
        "testTable_1234_5678");
    assertEquals(segmentNameGenerator.generateSegmentName(VALID_SEQUENCE_ID, null, null), "testTable_0");
    assertEquals(segmentNameGenerator.generateSegmentName(VALID_SEQUENCE_ID, MIN_TIME_VALUE, MAX_TIME_VALUE),
        "testTable_1234_5678_0");
  }

  @Test
  public void testWithSegmentNamePostfix() {
    SegmentNameGenerator segmentNameGenerator = new SimpleSegmentNameGenerator(TABLE_NAME, SEGMENT_NAME_POSTFIX);
    assertEquals(segmentNameGenerator.toString(),
        "SimpleSegmentNameGenerator: tableName=testTable, segmentNamePostfix=postfix, appendUUIDToSegmentName=false, "
            + "excludeTimeInSegmentName=false");
    assertEquals(segmentNameGenerator.generateSegmentName(INVALID_SEQUENCE_ID, null, null), "testTable_postfix");
    assertEquals(segmentNameGenerator.generateSegmentName(INVALID_SEQUENCE_ID, MIN_TIME_VALUE, MAX_TIME_VALUE),
        "testTable_1234_5678_postfix");
    assertEquals(segmentNameGenerator.generateSegmentName(VALID_SEQUENCE_ID, null, null), "testTable_postfix_0");
    assertEquals(segmentNameGenerator.generateSegmentName(VALID_SEQUENCE_ID, MIN_TIME_VALUE, MAX_TIME_VALUE),
        "testTable_1234_5678_postfix_0");
  }

  @Test
  public void testWithMalFormedTableNameSegmentNamePostfixTimeValue() {
    try {
      new SimpleSegmentNameGenerator(MALFORMED_TABLE_NAME, SEGMENT_NAME_POSTFIX);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
    try {
      new SimpleSegmentNameGenerator(TABLE_NAME, MALFORMED_SEGMENT_NAME_POSTFIX);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
    try {
      SegmentNameGenerator segmentNameGenerator = new SimpleSegmentNameGenerator(TABLE_NAME, SEGMENT_NAME_POSTFIX);
      segmentNameGenerator.generateSegmentName(VALID_SEQUENCE_ID, MIN_TIME_VALUE, MALFORMED_TIME_VALUE);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // Expected
      assertEquals(e.getMessage(), "Invalid partial or full segment name: 12|34");
    }
  }

  @Test
  public void testWithExcludeTimeInSegmentName() {
    SegmentNameGenerator segmentNameGenerator = new SimpleSegmentNameGenerator(TABLE_NAME, null, false, true);
    segmentNameGenerator.generateSegmentName(VALID_SEQUENCE_ID, MIN_TIME_VALUE, MAX_TIME_VALUE);
    assertEquals(segmentNameGenerator.toString(),
        "SimpleSegmentNameGenerator: tableName=testTable, appendUUIDToSegmentName=false, "
            + "excludeTimeInSegmentName=true");
    assertEquals(segmentNameGenerator.generateSegmentName(INVALID_SEQUENCE_ID, null, null), "testTable");
    assertEquals(segmentNameGenerator.generateSegmentName(INVALID_SEQUENCE_ID, MIN_TIME_VALUE, MAX_TIME_VALUE),
        "testTable");
    assertEquals(segmentNameGenerator.generateSegmentName(VALID_SEQUENCE_ID, null, null), "testTable_0");
    assertEquals(segmentNameGenerator.generateSegmentName(VALID_SEQUENCE_ID, MIN_TIME_VALUE, MAX_TIME_VALUE),
        "testTable_0");
  }
}
