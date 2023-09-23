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
package org.apache.pinot.controller.api;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class SegmentCompletionProtocolDeserTest {
  private static final StreamPartitionMsgOffset OFFSET = new LongMsgOffset(1L);
  private static final long BUILD_TIME_MILLIS = 123;
  private static final String SEGMENT_LOCATION = "file.tmp";
  private static final String CONTROLLER_VIP_URL = "http://localhost:8998";

  @Test
  public void testCompleteResponseParams() {
    // Test with all params
    SegmentCompletionProtocol.Response.Params params =
        new SegmentCompletionProtocol.Response.Params().withBuildTimeSeconds(BUILD_TIME_MILLIS)
            .withStreamPartitionMsgOffset(OFFSET.toString()).withSegmentLocation(SEGMENT_LOCATION)
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);

    SegmentCompletionProtocol.Response response = new SegmentCompletionProtocol.Response(params);
    assertEquals(response.getBuildTimeSeconds(), BUILD_TIME_MILLIS);
    assertEquals(new LongMsgOffset(response.getStreamPartitionMsgOffset()).compareTo(OFFSET), 0);
    assertEquals(response.getSegmentLocation(), SEGMENT_LOCATION);
    assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);
  }

  @Test
  public void testIncompleteResponseParams() {
    // Test with reduced params
    SegmentCompletionProtocol.Response.Params params =
        new SegmentCompletionProtocol.Response.Params().withBuildTimeSeconds(BUILD_TIME_MILLIS)
            .withStreamPartitionMsgOffset(OFFSET.toString())
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);

    SegmentCompletionProtocol.Response response = new SegmentCompletionProtocol.Response(params);
    assertEquals(response.getBuildTimeSeconds(), BUILD_TIME_MILLIS);
    assertEquals(new LongMsgOffset(response.getStreamPartitionMsgOffset()).compareTo(OFFSET), 0);
    assertNull(response.getSegmentLocation());
    assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);
  }

  @Test
  public void testJsonResponseWithAllParams() {
    // Test with all params
    SegmentCompletionProtocol.Response.Params params =
        new SegmentCompletionProtocol.Response.Params().withBuildTimeSeconds(BUILD_TIME_MILLIS)
            .withStreamPartitionMsgOffset(OFFSET.toString()).withSegmentLocation(SEGMENT_LOCATION)
            .withControllerVipUrl(CONTROLLER_VIP_URL)
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);

    SegmentCompletionProtocol.Response response = new SegmentCompletionProtocol.Response(params);
    JsonNode jsonNode = JsonUtils.objectToJsonNode(response);

    assertEquals(jsonNode.get("streamPartitionMsgOffset").asText(), OFFSET.toString());
    assertEquals(jsonNode.get("segmentLocation").asText(), SEGMENT_LOCATION);
    assertTrue(jsonNode.get("isSplitCommitType").asBoolean());
    assertEquals(jsonNode.get("status").asText(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT.toString());
    assertEquals(jsonNode.get("controllerVipUrl").asText(), CONTROLLER_VIP_URL);
  }

  @Test
  public void testJsonNullSegmentLocationAndVip() {
    SegmentCompletionProtocol.Response.Params params =
        new SegmentCompletionProtocol.Response.Params().withBuildTimeSeconds(BUILD_TIME_MILLIS)
            .withStreamPartitionMsgOffset(OFFSET.toString())
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);

    SegmentCompletionProtocol.Response response = new SegmentCompletionProtocol.Response(params);
    JsonNode jsonNode = JsonUtils.objectToJsonNode(response);

    assertEquals(jsonNode.get("streamPartitionMsgOffset").asText(), OFFSET.toString());
    assertNull(jsonNode.get("segmentLocation"));
    assertTrue(jsonNode.get("isSplitCommitType").asBoolean());
    assertEquals(jsonNode.get("status").asText(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT.toString());
    assertNull(jsonNode.get("controllerVipUrl"));
  }

  @Test
  public void testJsonResponseWithSegmentLocationNullVip() {
    // Should never happen because if split commit, should have both location and VIP, but testing deserialization
    // regardless
    SegmentCompletionProtocol.Response.Params params =
        new SegmentCompletionProtocol.Response.Params().withBuildTimeSeconds(BUILD_TIME_MILLIS)
            .withStreamPartitionMsgOffset(OFFSET.toString()).withSegmentLocation(SEGMENT_LOCATION)
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);

    SegmentCompletionProtocol.Response response = new SegmentCompletionProtocol.Response(params);
    JsonNode jsonNode = JsonUtils.objectToJsonNode(response);

    assertEquals(jsonNode.get("streamPartitionMsgOffset").asText(), OFFSET.toString());
    assertEquals(jsonNode.get("segmentLocation").asText(), SEGMENT_LOCATION);
    assertTrue(jsonNode.get("isSplitCommitType").asBoolean());
    assertEquals(jsonNode.get("status").asText(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT.toString());
    assertNull(jsonNode.get("controllerVipUrl"));
  }

  @Test
  public void testJsonResponseWithVipAndNullSegmentLocation() {
    // Should never happen because if split commit, should have both location and VIP, but testing deserialization
    // regardless
    SegmentCompletionProtocol.Response.Params params =
        new SegmentCompletionProtocol.Response.Params().withBuildTimeSeconds(BUILD_TIME_MILLIS)
            .withStreamPartitionMsgOffset(OFFSET.toString()).withControllerVipUrl(CONTROLLER_VIP_URL)
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);

    SegmentCompletionProtocol.Response response = new SegmentCompletionProtocol.Response(params);
    JsonNode jsonNode = JsonUtils.objectToJsonNode(response);

    assertEquals(jsonNode.get("streamPartitionMsgOffset").asText(), OFFSET.toString());
    assertNull(jsonNode.get("segmentLocation"));
    assertTrue(jsonNode.get("isSplitCommitType").asBoolean());
    assertEquals(jsonNode.get("status").asText(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT.toString());
    assertEquals(jsonNode.get("controllerVipUrl").asText(), CONTROLLER_VIP_URL);
  }
}
