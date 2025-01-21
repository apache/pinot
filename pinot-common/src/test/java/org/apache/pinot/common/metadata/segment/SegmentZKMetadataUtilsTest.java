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
package org.apache.pinot.common.metadata.segment;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class SegmentZKMetadataUtilsTest {

  private SegmentZKMetadata _testMetadata;
  private static final String TEST_SEGMENT_NAME = "testSegment";
  private static final String TEST_TABLE_SEGMENT = "mytable__0__0__20220722T2342Z";

  @BeforeMethod
  public void setUp() {
    // Create a test metadata object with sample data
    _testMetadata = new SegmentZKMetadata(TEST_SEGMENT_NAME);
    _testMetadata.setStartTime(1234567890L);
    _testMetadata.setEndTime(1234567899L);
    _testMetadata.setTimeUnit(TimeUnit.SECONDS);
    _testMetadata.setIndexVersion("v1");
    _testMetadata.setTotalDocs(1000);
    _testMetadata.setSizeInBytes(1024 * 1024);
    _testMetadata.setCrc(123456L);
    _testMetadata.setCreationTime(System.currentTimeMillis());
  }

  @Test
  public void testSerialize() throws IOException {
    // Test successful serialization
    String serialized = SegmentZKMetadataUtils.serialize(_testMetadata);

    // Verify basic properties
    assertNotNull(serialized, "Serialized string should not be null");
    assertTrue(serialized.contains(TEST_SEGMENT_NAME), "Serialized string should contain segment name");
    assertTrue(serialized.contains("SECONDS"), "Serialized string should contain time unit");

    // Verify JSON structure
    ObjectNode jsonNode = (ObjectNode) SegmentZKMetadataUtils.MAPPER.readTree(serialized);
    assertTrue(jsonNode.has("simpleFields"), "Should contain simpleFields");
    assertTrue(jsonNode.has("mapFields"), "Should contain mapFields");
  }

  @Test
  public void testSerializeNull() throws IOException {
    assertNull(SegmentZKMetadataUtils.serialize(null), "Serializing null should return null");
  }

  @Test
  public void testDeserializeString() throws IOException {
    String errorStr = "{\"id\":\"" + TEST_TABLE_SEGMENT + "\",\"simpleFields\":"
        + "{\"segment.crc\":\"2624963047\",\"segment.creation.time\":\"1658533353347\","
        + "\"segment.download.url\":\"http://localhost:18998/segments/mytable/" + TEST_TABLE_SEGMENT + "\","
        + "\"segment.end.time\":\"1405296000000\",\"segment.flush.threshold.size\":\"2500\","
        + "\"segment.index.version\":\"v3\",\"segment.realtime.endOffset\":\"2500\","
        + "\"segment.realtime.numReplicas\":\"1\",\"segment.realtime.startOffset\":\"0\","
        + "\"segment.realtime.status\":\"DONE\",\"segment.start.time\":\"1404086400000\","
        + "\"segment.time.unit\":\"MILLISECONDS\",\"segment.total.docs\":\"2500\"},"
        + "\"mapFields\":{},\"listFields\":{}}";

    SegmentZKMetadata segmentZKMetadata = SegmentZKMetadataUtils.deserialize(errorStr);

    // Verify deserialized properties
    assertEquals(segmentZKMetadata.getSegmentName(), TEST_TABLE_SEGMENT,
        "Segment name should match expected value");
    assertEquals(segmentZKMetadata.getEndTimeMs(), 1405296000000L,
        "End time should match expected value");
    assertEquals(segmentZKMetadata.getCrc(), 2624963047L,
        "CRC should match expected value");
  }

  @Test
  public void testDeserializeObjectNode() throws IOException {
    String errorStr = "{\"id\":\"" + TEST_TABLE_SEGMENT + "\",\"simpleFields\":"
        + "{\"segment.crc\":\"2624963047\",\"segment.creation.time\":\"1658533353347\","
        + "\"segment.download.url\":\"http://localhost:18998/segments/mytable/" + TEST_TABLE_SEGMENT + "\","
        + "\"segment.end.time\":\"1405296000000\",\"segment.flush.threshold.size\":\"2500\","
        + "\"segment.index.version\":\"v3\",\"segment.realtime.endOffset\":\"2500\","
        + "\"segment.realtime.numReplicas\":\"1\",\"segment.realtime.startOffset\":\"0\","
        + "\"segment.realtime.status\":\"DONE\",\"segment.start.time\":\"1404086400000\","
        + "\"segment.time.unit\":\"MILLISECONDS\",\"segment.total.docs\":\"2500\"},"
        + "\"mapFields\":{},\"listFields\":{}}";

    JsonNode zkChildren = SegmentZKMetadataUtils.MAPPER.readTree(errorStr);
    SegmentZKMetadata segmentZKMetadata = SegmentZKMetadataUtils.deserialize((ObjectNode) zkChildren);

    assertEquals(segmentZKMetadata.getSegmentName(), TEST_TABLE_SEGMENT,
        "Segment name should match expected value");
    assertEquals(segmentZKMetadata.getEndTimeMs(), 1405296000000L,
        "End time should match expected value");
  }

  @Test
  public void testDeserializeBytes() throws IOException {
    String serialized = SegmentZKMetadataUtils.serialize(_testMetadata);
    byte[] bytes = serialized.getBytes();

    SegmentZKMetadata deserialized = SegmentZKMetadataUtils.deserialize(bytes);

    assertNotNull(deserialized, "Deserialized object should not be null");
    assertEquals(deserialized.getSegmentName(), _testMetadata.getSegmentName(),
        "Segment names should match");
  }
}
