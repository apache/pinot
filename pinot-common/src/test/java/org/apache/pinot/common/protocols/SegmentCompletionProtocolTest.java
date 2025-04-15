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
package org.apache.pinot.common.protocols;

import java.net.URI;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.common.utils.URIUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SegmentCompletionProtocolTest {

  @Test
  public void testRequestURL()
      throws Exception {

    // test default params
    SegmentCompletionProtocol.Request.Params params = new SegmentCompletionProtocol.Request.Params();
    SegmentCompletionProtocol.ExtendBuildTimeRequest extendBuildTimeRequest =
        new SegmentCompletionProtocol.ExtendBuildTimeRequest(params);
    URI uri = new URI(extendBuildTimeRequest.getUrl("localhost:8080", "http"));
    Assert.assertEquals(uri.getScheme(), "http");
    Assert.assertEquals(uri.getHost(), "localhost");
    Assert.assertEquals(uri.getPort(), 8080);
    Assert.assertEquals(uri.getPath(), String.format("/%s", SegmentCompletionProtocol.MSG_TYPE_EXTEND_BUILD_TIME));
    Map<String, String> paramsMap =
        Arrays.stream(uri.getQuery().split("&")).collect(Collectors.toMap(e -> e.split("=")[0], e -> e.split("=")[1]));
    Assert.assertEquals(paramsMap.get(SegmentCompletionProtocol.PARAM_SEGMENT_NAME), "UNKNOWN_SEGMENT");
    Assert.assertEquals(paramsMap.get(SegmentCompletionProtocol.PARAM_INSTANCE_ID), "UNKNOWN_INSTANCE");
    Assert.assertNull(paramsMap.get(SegmentCompletionProtocol.PARAM_REASON));
    Assert.assertNull(paramsMap.get(SegmentCompletionProtocol.PARAM_BUILD_TIME_MILLIS));
    Assert.assertNull(paramsMap.get(SegmentCompletionProtocol.PARAM_WAIT_TIME_MILLIS));
    Assert.assertNull(paramsMap.get(SegmentCompletionProtocol.PARAM_EXTRA_TIME_SEC));
    Assert.assertNull(paramsMap.get(SegmentCompletionProtocol.PARAM_MEMORY_USED_BYTES));
    Assert.assertNull(paramsMap.get(SegmentCompletionProtocol.PARAM_SEGMENT_SIZE_BYTES));
    Assert.assertNull(paramsMap.get(SegmentCompletionProtocol.PARAM_ROW_COUNT));
    Assert.assertNull(paramsMap.get(SegmentCompletionProtocol.PARAM_SEGMENT_LOCATION));
    Assert.assertNull(paramsMap.get(SegmentCompletionProtocol.PARAM_STREAM_PARTITION_MSG_OFFSET));

    // test that params set only if valid values
    params = new SegmentCompletionProtocol.Request.Params().withSegmentName("foo__0__0__12345Z")
        .withInstanceId("Server_localhost_8099").withReason(null).withBuildTimeMillis(-100).withWaitTimeMillis(0)
        .withExtraTimeSec(-1).withMemoryUsedBytes(0).withSegmentSizeBytes(-12345).withNumRows(0)
        .withSegmentLocation(null).withStreamPartitionMsgOffset(null);
    SegmentCompletionProtocol.SegmentConsumedRequest segmentConsumedRequest =
        new SegmentCompletionProtocol.SegmentConsumedRequest(params);
    uri = new URI(segmentConsumedRequest.getUrl("localhost:8080", "http"));
    paramsMap =
        Arrays.stream(uri.getQuery().split("&")).collect(Collectors.toMap(e -> e.split("=")[0], e -> e.split("=")[1]));
    Assert.assertEquals(paramsMap.get(SegmentCompletionProtocol.PARAM_SEGMENT_NAME), "foo__0__0__12345Z");
    Assert.assertEquals(paramsMap.get(SegmentCompletionProtocol.PARAM_INSTANCE_ID), "Server_localhost_8099");
    Assert.assertNull(paramsMap.get(SegmentCompletionProtocol.PARAM_REASON));
    Assert.assertNull(paramsMap.get(SegmentCompletionProtocol.PARAM_BUILD_TIME_MILLIS));
    Assert.assertNull(paramsMap.get(SegmentCompletionProtocol.PARAM_WAIT_TIME_MILLIS));
    Assert.assertNull(paramsMap.get(SegmentCompletionProtocol.PARAM_EXTRA_TIME_SEC));
    Assert.assertNull(paramsMap.get(SegmentCompletionProtocol.PARAM_MEMORY_USED_BYTES));
    Assert.assertNull(paramsMap.get(SegmentCompletionProtocol.PARAM_SEGMENT_SIZE_BYTES));
    Assert.assertNull(paramsMap.get(SegmentCompletionProtocol.PARAM_ROW_COUNT));
    Assert.assertNull(paramsMap.get(SegmentCompletionProtocol.PARAM_SEGMENT_LOCATION));
    Assert.assertNull(paramsMap.get(SegmentCompletionProtocol.PARAM_STREAM_PARTITION_MSG_OFFSET));

    params = new SegmentCompletionProtocol.Request.Params().withSegmentName("foo__0__0__12345Z")
        .withInstanceId("Server_localhost_8099").withReason("ROW_LIMIT").withBuildTimeMillis(1000)
        .withWaitTimeMillis(2000).withExtraTimeSec(3000).withMemoryUsedBytes(4000).withSegmentSizeBytes(5000)
        .withNumRows(6000).withSegmentLocation("/tmp/segment").withStreamPartitionMsgOffset("7000");
    SegmentCompletionProtocol.SegmentCommitRequest segmentCommitRequest =
        new SegmentCompletionProtocol.SegmentCommitRequest(params);
    uri = new URI(segmentCommitRequest.getUrl("localhost:8080", "http"));
    paramsMap =
        Arrays.stream(uri.getQuery().split("&")).collect(Collectors.toMap(e -> e.split("=")[0], e -> e.split("=")[1]));
    Assert.assertEquals(paramsMap.get(SegmentCompletionProtocol.PARAM_SEGMENT_NAME), "foo__0__0__12345Z");
    Assert.assertEquals(paramsMap.get(SegmentCompletionProtocol.PARAM_INSTANCE_ID), "Server_localhost_8099");
    Assert.assertEquals(paramsMap.get(SegmentCompletionProtocol.PARAM_REASON), "ROW_LIMIT");
    Assert.assertEquals(paramsMap.get(SegmentCompletionProtocol.PARAM_BUILD_TIME_MILLIS), "1000");
    Assert.assertEquals(paramsMap.get(SegmentCompletionProtocol.PARAM_WAIT_TIME_MILLIS), "2000");
    Assert.assertEquals(paramsMap.get(SegmentCompletionProtocol.PARAM_EXTRA_TIME_SEC), "3000");
    Assert.assertEquals(paramsMap.get(SegmentCompletionProtocol.PARAM_MEMORY_USED_BYTES), "4000");
    Assert.assertEquals(paramsMap.get(SegmentCompletionProtocol.PARAM_SEGMENT_SIZE_BYTES), "5000");
    Assert.assertEquals(paramsMap.get(SegmentCompletionProtocol.PARAM_ROW_COUNT), "6000");
    Assert.assertEquals(paramsMap.get(SegmentCompletionProtocol.PARAM_SEGMENT_LOCATION), "/tmp/segment");
    Assert.assertEquals(paramsMap.get(SegmentCompletionProtocol.PARAM_STREAM_PARTITION_MSG_OFFSET), "7000");

    // test param encoding
    params = new SegmentCompletionProtocol.Request.Params().withSegmentName("foo%%__0__0__12345Z")
        .withInstanceId("Server_localhost_8099").withReason("{\"type\":\"ROW_LIMIT\", \"value\":1000}")
        .withBuildTimeMillis(1000).withWaitTimeMillis(2000).withExtraTimeSec(3000).withMemoryUsedBytes(4000)
        .withSegmentSizeBytes(5000).withNumRows(6000).withSegmentLocation("s3://my.bucket/segment")
        .withStreamPartitionMsgOffset(
            "{\"shardId-000000000001\":\"49615238429973311938200772279310862572716999467690098706\"}");
    SegmentCompletionProtocol.SegmentCommitStartRequest segmentCommitStartRequest =
        new SegmentCompletionProtocol.SegmentCommitStartRequest(params);
    String url = segmentCommitStartRequest.getUrl("localhost:8080", "http");
    Assert.assertEquals(url,
    // CHECKSTYLE:OFF
        "http://localhost:8080/segmentCommitStart?extraTimeSec=3000&segmentSizeBytes=5000&reason=%7B%22type%22%3A%22ROW_LIMIT%22%2C%20%22value%22%3A1000%7D&buildTimeMillis=1000&streamPartitionMsgOffset=%7B%22shardId-000000000001%22%3A%2249615238429973311938200772279310862572716999467690098706%22%7D&instance=Server_localhost_8099&waitTimeMillis=2000&name=foo%25%25__0__0__12345Z&location=s3%3A%2F%2Fmy.bucket%2Fsegment&rowCount=6000&memoryUsedBytes=4000");
    // CHECKSTYLE:ON

    paramsMap = Arrays.stream(url.split("\\?")[1].split("&"))
        .collect(Collectors.toMap(e -> e.split("=")[0], e -> e.split("=")[1]));
    Assert.assertEquals(paramsMap.get(SegmentCompletionProtocol.PARAM_SEGMENT_NAME),
        URIUtils.encode("foo%%__0__0__12345Z"));
    Assert.assertEquals(paramsMap.get(SegmentCompletionProtocol.PARAM_INSTANCE_ID),
        URIUtils.encode("Server_localhost_8099"));
    Assert.assertEquals(paramsMap.get(SegmentCompletionProtocol.PARAM_REASON),
        "%7B%22type%22%3A%22ROW_LIMIT%22%2C%20%22value%22%3A1000%7D");
    Assert.assertEquals(paramsMap.get(SegmentCompletionProtocol.PARAM_BUILD_TIME_MILLIS), "1000");
    Assert.assertEquals(paramsMap.get(SegmentCompletionProtocol.PARAM_WAIT_TIME_MILLIS), "2000");
    Assert.assertEquals(paramsMap.get(SegmentCompletionProtocol.PARAM_EXTRA_TIME_SEC), "3000");
    Assert.assertEquals(paramsMap.get(SegmentCompletionProtocol.PARAM_MEMORY_USED_BYTES), "4000");
    Assert.assertEquals(paramsMap.get(SegmentCompletionProtocol.PARAM_SEGMENT_SIZE_BYTES), "5000");
    Assert.assertEquals(paramsMap.get(SegmentCompletionProtocol.PARAM_ROW_COUNT), "6000");
    Assert.assertEquals(paramsMap.get(SegmentCompletionProtocol.PARAM_SEGMENT_LOCATION),
        URIUtils.encode("s3://my.bucket/segment"));
    Assert.assertEquals(paramsMap.get(SegmentCompletionProtocol.PARAM_STREAM_PARTITION_MSG_OFFSET),
        URIUtils.encode("{\"shardId-000000000001\":\"49615238429973311938200772279310862572716999467690098706\"}"));
  }
}
