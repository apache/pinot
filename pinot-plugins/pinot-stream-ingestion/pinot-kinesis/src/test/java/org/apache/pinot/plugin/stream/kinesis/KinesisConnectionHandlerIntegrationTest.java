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
package org.apache.pinot.plugin.stream.kinesis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.pinot.spi.stream.StreamConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DeleteStreamRequest;

public class KinesisConnectionHandlerIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(KinesisConnectionHandlerIntegrationTest.class);
  private static final String TEST_STREAM_PREFIX = "pinot-test-stream-";
  private static final String AWS_REGION = System.getenv("AWS_REGION");
  private static final String AWS_ACCESS_KEY = System.getenv("AWS_ACCESS_KEY_ID");
  private static final String AWS_SECRET_KEY = System.getenv("AWS_SECRET_ACCESS_KEY");

  private KinesisConnectionHandler _connectionHandler;
  private String _testStreamName;
  private KinesisClient _kinesisClient;

  @BeforeClass
  public void setUp() {
    // Skip tests if credentials are not provided
    if (AWS_ACCESS_KEY == null || AWS_SECRET_KEY == null || AWS_REGION == null) {
      throw new SkipException(
          "Skipping integration test because AWS credentials or region are not set in environment variables");
    }

    // Create test stream name
    _testStreamName = TEST_STREAM_PREFIX + UUID.randomUUID();

    // Create connection handler
    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", "kinesis");
    streamConfigMap.put("region", AWS_REGION);
    streamConfigMap.put("accessKey", AWS_ACCESS_KEY);
    streamConfigMap.put("secretKey", AWS_SECRET_KEY);
    streamConfigMap.put("stream.kinesis.topic.name", _testStreamName);
    streamConfigMap.put("stream.kinesis.decoder.class.name",
        "org.apache.pinot.plugin.stream.kinesis.KinesisMessageDecoder");

    StreamConfig streamConfig = new StreamConfig("testTable", streamConfigMap);
    KinesisConfig kinesisConfig = new KinesisConfig(streamConfig);
    _connectionHandler = new KinesisConnectionHandler(kinesisConfig);
    _kinesisClient = _connectionHandler.getClient();

    // Create test stream
    createTestStream();
  }

  private void createTestStream() {
    try {
      _kinesisClient.createStream(CreateStreamRequest.builder()
          .streamName(_testStreamName)
          .shardCount(1)
          .build());

      // Wait for stream to become active
      LOGGER.info("Waiting for stream {} to become active", _testStreamName);
      _kinesisClient.waiter().waitUntilStreamExists(builder -> builder.streamName(_testStreamName));
      LOGGER.info("Stream {} is now active", _testStreamName);
    } catch (Exception e) {
      LOGGER.error("Failed to create test stream", e);
      throw new RuntimeException(e);
    }
  }

  @AfterClass
  public void cleanup() {
    try {
      if (_kinesisClient != null && _testStreamName != null) {
        _kinesisClient.deleteStream(DeleteStreamRequest.builder().streamName(_testStreamName).build());
        LOGGER.info("Deleted test stream: {}", _testStreamName);
      }
    } catch (Exception e) {
      LOGGER.error("Failed to delete test stream", e);
    }
  }

  @Test
  public void testGetStreamNames() {
    List<String> streams = _connectionHandler.getStreamNames();
    Assert.assertNotNull(streams);
    Assert.assertTrue(streams.contains(_testStreamName),
        "Expected to find test stream " + _testStreamName + " in list of streams: " + streams);
  }

  @Test(dependsOnMethods = "testGetStreamNames", enabled = false)
  public void testGetStreamNamesWithPagination() {
    // Create additional test streams to test pagination
    String[] additionalStreams = new String[3];
    for (int i = 0; i < additionalStreams.length; i++) {
      additionalStreams[i] = TEST_STREAM_PREFIX + UUID.randomUUID();
      _kinesisClient.createStream(CreateStreamRequest.builder()
          .streamName(additionalStreams[i])
          .shardCount(1)
          .build());
    }

    // Wait for all streams to become active
    for (String streamName : additionalStreams) {
      _kinesisClient.waiter().waitUntilStreamExists(builder -> builder.streamName(streamName));
    }

    try {
      // Get all streams and verify
      List<String> streams = _connectionHandler.getStreamNames();
      Assert.assertNotNull(streams);
      Assert.assertTrue(streams.contains(_testStreamName));
      for (String streamName : additionalStreams) {
        Assert.assertTrue(streams.contains(streamName), "Expected to find stream: " + streamName);
      }
    } finally {
      // Cleanup additional streams
      for (String streamName : additionalStreams) {
        _kinesisClient.deleteStream(DeleteStreamRequest.builder().streamName(streamName).build());
      }
    }
  }
}
