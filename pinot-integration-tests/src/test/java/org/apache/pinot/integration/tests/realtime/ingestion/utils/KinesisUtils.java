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
package org.apache.pinot.integration.tests.realtime.ingestion.utils;

import java.math.BigInteger;
import java.time.Duration;
import java.util.List;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.MergeShardsRequest;
import software.amazon.awssdk.services.kinesis.model.MergeShardsResponse;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.SplitShardRequest;
import software.amazon.awssdk.services.kinesis.model.SplitShardResponse;


public class KinesisUtils {

  private KinesisUtils() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(KinesisUtils.class);

  public static void splitNthShard(KinesisClient kinesisClient, String stream, int index) {
    List<Shard> shards = getShards(kinesisClient, stream);
    int initialSize = shards.size();
    splitShard(kinesisClient, stream, shards.get(index));
    LOGGER.info("Splitted shard with ID: " + shards.get(index).shardId());

    TestUtils.waitForCondition((avoid) -> isKinesisStreamActive(kinesisClient, stream)
            && getShards(kinesisClient, stream).size() == initialSize + 2,
        2000, Duration.ofMinutes(1).toMillis(), "Waiting for Kinesis stream to be active and shards to be split");
  }

  public static void mergeShards(KinesisClient kinesisClient, String stream, int index1, int index2) {
    List<Shard> shards = getShards(kinesisClient, stream);
    int initialSize = shards.size();
    mergeShard(kinesisClient, stream, shards.get(index1), shards.get(index2));
    LOGGER.info("Merged shard with ID: " + shards.get(index1).shardId() + " and " + shards.get(index2).shardId());

    TestUtils.waitForCondition((avoid) -> isKinesisStreamActive(kinesisClient, stream)
            && getShards(kinesisClient, stream).size() == initialSize + 1,
        2000, Duration.ofMinutes(1).toMillis(), "Waiting for Kinesis stream to be active and shards to be merged");
  }

  public static boolean isKinesisStreamActive(KinesisClient kinesisClient, String streamName) {
    try {
      String kinesisStreamStatus = getKinesisStreamStatus(kinesisClient, streamName);
      boolean isActive = kinesisStreamStatus.contentEquals("ACTIVE");
      if (!isActive) {
        LOGGER.warn("Kinesis stream " + streamName + " in state" + kinesisStreamStatus);
      }
      return isActive;
    } catch (ResourceNotFoundException e) {
      LOGGER.warn("Kinesis stream " + streamName + " not found");
      return false;
    }
  }

  public static String getKinesisStreamStatus(KinesisClient kinesisClient, String streamName) {
    return kinesisClient.describeStream(DescribeStreamRequest.builder().streamName(streamName).build())
        .streamDescription().streamStatusAsString();
  }

  private static List<Shard> getShards(KinesisClient kinesisClient, String stream) {
    ListShardsResponse listShardsResponse =
        kinesisClient.listShards(ListShardsRequest.builder().streamName(stream).build());
    return listShardsResponse.shards();
  }

  private static SplitShardResponse splitShard(KinesisClient kinesisClient, String stream, Shard shard) {
    BigInteger startHash = new BigInteger(shard.hashKeyRange().startingHashKey());
    BigInteger endHash = new BigInteger(shard.hashKeyRange().endingHashKey());
    BigInteger newStartingHashKey = startHash.add(endHash).divide(new BigInteger("2"));
    return kinesisClient.splitShard(SplitShardRequest.builder()
        .shardToSplit(shard.shardId())
        .streamName(stream)
        .newStartingHashKey(newStartingHashKey.toString())
        .build());
  }

  private static MergeShardsResponse mergeShard(KinesisClient kinesisClient, String stream, Shard shard1,
      Shard shard2) {
    return kinesisClient.mergeShards(MergeShardsRequest.builder()
        .shardToMerge(shard1.shardId())
        .adjacentShardToMerge(shard2.shardId())
        .streamName(stream)
        .build());
  }
}
