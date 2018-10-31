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
package com.linkedin.pinot.controller.api.resources;

import com.alibaba.fastjson.JSON;
import com.linkedin.pinot.common.protocols.SegmentCompletionProtocol;
import org.json.JSONException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SegmentCompletionProtocolDeserTest {
  private final int OFFSET = 1;
  private final long BUILD_TIME_MILLIS = 123;
  private final String SEGMENT_LOCATION = "file.tmp";
  private final String CONTROLLER_VIP_URL = "http://localhost:8998";

  @Test
  public void testCompleteResponseParams() {
    // Test with all params
    SegmentCompletionProtocol.Response.Params params = new SegmentCompletionProtocol.Response.Params()
        .withBuildTimeSeconds(BUILD_TIME_MILLIS)
        .withOffset(OFFSET)
        .withSegmentLocation(SEGMENT_LOCATION)
        .withSplitCommit(true)
        .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);

    SegmentCompletionProtocol.Response response = new SegmentCompletionProtocol.Response(params);
    Assert.assertEquals(response.getBuildTimeSeconds(), BUILD_TIME_MILLIS);
    Assert.assertEquals(response.getOffset(), OFFSET);
    Assert.assertEquals(response.getSegmentLocation(), SEGMENT_LOCATION);
    Assert.assertEquals(response.getIsSplitCommit(), true);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);
  }

  @Test
  public void testIncompleteResponseParams() {
    // Test with reduced params
    SegmentCompletionProtocol.Response.Params params = new SegmentCompletionProtocol.Response.Params()
        .withBuildTimeSeconds(BUILD_TIME_MILLIS)
        .withOffset(OFFSET)
        .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);

    SegmentCompletionProtocol.Response response = new SegmentCompletionProtocol.Response(params);
    Assert.assertEquals(response.getBuildTimeSeconds(), BUILD_TIME_MILLIS);
    Assert.assertEquals(response.getOffset(), OFFSET);
    Assert.assertEquals(response.getSegmentLocation(), null);
    Assert.assertEquals(response.getIsSplitCommit(), false);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);
  }

  @Test
  public void testJsonResponseWithAllParams() throws JSONException {
    // Test with all params
    SegmentCompletionProtocol.Response.Params params = new SegmentCompletionProtocol.Response.Params()
        .withBuildTimeSeconds(BUILD_TIME_MILLIS)
        .withOffset(OFFSET)
        .withSegmentLocation(SEGMENT_LOCATION)
        .withSplitCommit(true)
        .withControllerVipUrl(CONTROLLER_VIP_URL).withStatus(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);

    SegmentCompletionProtocol.Response response = new SegmentCompletionProtocol.Response(params);

    com.alibaba.fastjson.JSONObject jsonObject = JSON.parseObject(response.toJsonString());

    Assert.assertEquals(jsonObject.get("offset"), OFFSET);
    Assert.assertEquals(jsonObject.get("segmentLocation"), SEGMENT_LOCATION);
    Assert.assertEquals(jsonObject.get("isSplitCommitType"), true);
    Assert.assertEquals(jsonObject.get("status"), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT.toString());
    Assert.assertEquals(jsonObject.get("controllerVipUrl"), CONTROLLER_VIP_URL);
  }

  @Test
  public void testJsonNullSegmentLocationAndVip() throws JSONException {
    SegmentCompletionProtocol.Response.Params params = new SegmentCompletionProtocol.Response.Params()
        .withBuildTimeSeconds(BUILD_TIME_MILLIS)
        .withOffset(OFFSET)
        .withSplitCommit(false)
        .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);

    SegmentCompletionProtocol.Response response = new SegmentCompletionProtocol.Response(params);

    com.alibaba.fastjson.JSONObject jsonObject = JSON.parseObject(response.toJsonString());

    Assert.assertEquals(jsonObject.get("offset"), OFFSET);
    Assert.assertEquals(jsonObject.get("segmentLocation"), null);
    Assert.assertEquals(jsonObject.get("isSplitCommitType"), false);
    Assert.assertEquals(jsonObject.get("status"), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT.toString());
    Assert.assertEquals(jsonObject.get("controllerVipUrl"), null);
  }

  @Test
  public void testJsonResponseWithoutSplitCommit() throws JSONException {
    SegmentCompletionProtocol.Response.Params params = new SegmentCompletionProtocol.Response.Params()
        .withBuildTimeSeconds(BUILD_TIME_MILLIS)
        .withOffset(OFFSET)
        .withSplitCommit(false)
        .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);

    SegmentCompletionProtocol.Response response = new SegmentCompletionProtocol.Response(params);

    com.alibaba.fastjson.JSONObject jsonObject = JSON.parseObject(response.toJsonString());

    Assert.assertEquals(jsonObject.get("offset"), OFFSET);
    Assert.assertEquals(jsonObject.get("isSplitCommitType"), false);
    Assert.assertEquals(jsonObject.get("status"), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT.toString());
    Assert.assertEquals(jsonObject.get("controllerVipUrl"), null);
  }

  @Test
  public void testJsonResponseWithSegmentLocationNullVip() throws JSONException {
    // Should never happen because if split commit, should have both location and VIP, but testing deserialization regardless
    SegmentCompletionProtocol.Response.Params params = new SegmentCompletionProtocol.Response.Params()
        .withBuildTimeSeconds(BUILD_TIME_MILLIS)
        .withOffset(OFFSET)
        .withSegmentLocation(SEGMENT_LOCATION)
        .withSplitCommit(false)
        .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);

    SegmentCompletionProtocol.Response response = new SegmentCompletionProtocol.Response(params);

    com.alibaba.fastjson.JSONObject jsonObject = JSON.parseObject(response.toJsonString());

    Assert.assertEquals(jsonObject.get("offset"), OFFSET);
    Assert.assertEquals(jsonObject.get("isSplitCommitType"), false);
    Assert.assertEquals(jsonObject.get("segmentLocation"), SEGMENT_LOCATION);
    Assert.assertEquals(jsonObject.get("status"), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT.toString());
    Assert.assertEquals(jsonObject.get("controllerVipUrl"), null);
  }

  @Test
  public void testJsonResponseWithVipAndNullSegmentLocation() throws JSONException {
    // Should never happen because if split commit, should have both location and VIP, but testing deserialization regardless
    SegmentCompletionProtocol.Response.Params params = new SegmentCompletionProtocol.Response.Params()
        .withBuildTimeSeconds(BUILD_TIME_MILLIS)
        .withOffset(OFFSET)
        .withControllerVipUrl(CONTROLLER_VIP_URL)
        .withSplitCommit(false)
        .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);

    SegmentCompletionProtocol.Response response = new SegmentCompletionProtocol.Response(params);

    com.alibaba.fastjson.JSONObject jsonObject = JSON.parseObject(response.toJsonString());

    Assert.assertEquals(jsonObject.get("offset"), OFFSET);
    Assert.assertEquals(jsonObject.get("isSplitCommitType"), false);
    Assert.assertEquals(jsonObject.get("segmentLocation"), null);
    Assert.assertEquals(jsonObject.get("status"), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT.toString());
    Assert.assertEquals(jsonObject.get("controllerVipUrl"), CONTROLLER_VIP_URL);
  }
}
