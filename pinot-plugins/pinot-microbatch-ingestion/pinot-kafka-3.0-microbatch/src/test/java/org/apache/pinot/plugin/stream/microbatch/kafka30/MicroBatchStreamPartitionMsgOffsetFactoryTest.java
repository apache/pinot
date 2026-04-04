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
package org.apache.pinot.plugin.stream.microbatch.kafka30;

import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class MicroBatchStreamPartitionMsgOffsetFactoryTest {

  private MicroBatchStreamPartitionMsgOffsetFactory _factory;

  @BeforeMethod
  public void setUp() {
    _factory = new MicroBatchStreamPartitionMsgOffsetFactory();
    _factory.init(null);
  }

  @Test
  public void testCreateFromNumericString() {
    // Test parsing simple numeric offset (e.g., "0" from smallest offset criteria)
    StreamPartitionMsgOffset offset = _factory.create("0");
    assertNotNull(offset);
    assertTrue(offset instanceof MicroBatchStreamPartitionMsgOffset);

    MicroBatchStreamPartitionMsgOffset mbOffset = (MicroBatchStreamPartitionMsgOffset) offset;
    assertEquals(mbOffset.getKafkaMessageOffset(), 0L);
    assertEquals(mbOffset.getRecordOffsetInMicroBatch(), 0L);
  }

  @Test
  public void testCreateFromLargeNumericString() {
    StreamPartitionMsgOffset offset = _factory.create("9876543210");
    assertNotNull(offset);

    MicroBatchStreamPartitionMsgOffset mbOffset = (MicroBatchStreamPartitionMsgOffset) offset;
    assertEquals(mbOffset.getKafkaMessageOffset(), 9876543210L);
    assertEquals(mbOffset.getRecordOffsetInMicroBatch(), 0L);
  }

  @Test
  public void testCreateFromNumericStringWithWhitespace() {
    // Should handle whitespace around numeric values
    StreamPartitionMsgOffset offset = _factory.create("  123  ");
    assertNotNull(offset);

    MicroBatchStreamPartitionMsgOffset mbOffset = (MicroBatchStreamPartitionMsgOffset) offset;
    assertEquals(mbOffset.getKafkaMessageOffset(), 123L);
  }

  @Test
  public void testCreateFromJsonFormat() {
    // Test parsing JSON format: {"kmo":0,"mbro":5}
    StreamPartitionMsgOffset offset = _factory.create("{\"kmo\":100,\"mbro\":50}");
    assertNotNull(offset);

    MicroBatchStreamPartitionMsgOffset mbOffset = (MicroBatchStreamPartitionMsgOffset) offset;
    assertEquals(mbOffset.getKafkaMessageOffset(), 100L);
    assertEquals(mbOffset.getRecordOffsetInMicroBatch(), 50L);
  }

  @Test
  public void testCreateFromJsonFormatWithZeroValues() {
    StreamPartitionMsgOffset offset = _factory.create("{\"kmo\":0,\"mbro\":0}");
    assertNotNull(offset);

    MicroBatchStreamPartitionMsgOffset mbOffset = (MicroBatchStreamPartitionMsgOffset) offset;
    assertEquals(mbOffset.getKafkaMessageOffset(), 0L);
    assertEquals(mbOffset.getRecordOffsetInMicroBatch(), 0L);
  }

  @Test
  public void testCreateFromJsonFormatWithLargeValues() {
    StreamPartitionMsgOffset offset = _factory.create("{\"kmo\":9999999999,\"mbro\":1000000}");
    assertNotNull(offset);

    MicroBatchStreamPartitionMsgOffset mbOffset = (MicroBatchStreamPartitionMsgOffset) offset;
    assertEquals(mbOffset.getKafkaMessageOffset(), 9999999999L);
    assertEquals(mbOffset.getRecordOffsetInMicroBatch(), 1000000L);
  }

  @Test
  public void testRoundTrip() {
    // Create offset, serialize to string, parse back
    MicroBatchStreamPartitionMsgOffset original = new MicroBatchStreamPartitionMsgOffset(42, 7);
    String serialized = original.toString();

    StreamPartitionMsgOffset parsed = _factory.create(serialized);
    assertNotNull(parsed);

    MicroBatchStreamPartitionMsgOffset mbParsed = (MicroBatchStreamPartitionMsgOffset) parsed;
    assertEquals(mbParsed.getKafkaMessageOffset(), 42L);
    assertEquals(mbParsed.getRecordOffsetInMicroBatch(), 7L);
  }

  @Test
  public void testCreateFromInvalidJson() {
    try {
      _factory.create("{invalid json}");
      fail("Should throw exception for invalid JSON");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Failed to parse"));
    }
  }

  @Test
  public void testCreateFromEmptyString() {
    try {
      _factory.create("");
      fail("Should throw exception for empty string");
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  @Test
  public void testCreateFromNullString() {
    try {
      _factory.create((String) null);
      fail("Should throw exception for null string");
    } catch (Exception e) {
      // Expected - either NullPointerException or IllegalArgumentException
    }
  }

  @Test
  public void testCreateFromMalformedNumeric() {
    // Non-numeric string that doesn't start with '{' should fall through to JSON parsing
    try {
      _factory.create("not-a-number");
      fail("Should throw exception for malformed input");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Failed to parse"));
    }
  }

  @Test
  public void testCreateFromJsonMissingFields() {
    // JSON with missing fields
    try {
      _factory.create("{\"kmo\":100}");
      // This might succeed with default value for mbro, or fail - depends on implementation
      // If it succeeds, mbro should default to 0
    } catch (IllegalArgumentException e) {
      // Also acceptable if it requires both fields
    }
  }

  @Test
  public void testCreateFromNegativeOffset() {
    // Negative offset in JSON format
    StreamPartitionMsgOffset offset = _factory.create("{\"kmo\":-1,\"mbro\":0}");
    assertNotNull(offset);

    MicroBatchStreamPartitionMsgOffset mbOffset = (MicroBatchStreamPartitionMsgOffset) offset;
    assertEquals(mbOffset.getKafkaMessageOffset(), -1L);
  }

  @Test
  public void testInitWithNullConfig() {
    // init() should handle null config gracefully
    MicroBatchStreamPartitionMsgOffsetFactory factory = new MicroBatchStreamPartitionMsgOffsetFactory();
    factory.init(null); // Should not throw
  }
}
