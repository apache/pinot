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
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.SplitShardRequest;

public class KinesisUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(KinesisUtils.class);

  public static void splitNthShard(KinesisClient kinesisClient, String stream, int index) {
    List<Shard> shards = getShards(kinesisClient, stream);
    int initialSize = shards.size();
    splitShard(kinesisClient, stream, shards.get(index));
    LOGGER.info("Splitted shard with ID: " + shards.get(index).shardId());

    TestUtils.waitForCondition((avoid) -> isKinesisStreamActive(kinesisClient, stream) && getShards(kinesisClient, stream).size() == initialSize + 2,
        Duration.ofMinutes(2).toMillis(), "Waiting for Kinesis stream to be active and shards to be split");
  }

  public static boolean isKinesisStreamActive(KinesisClient kinesisClient, String streamName) {
    try {
      String kinesisStreamStatus =
          kinesisClient.describeStream(DescribeStreamRequest.builder().streamName(streamName).build())
              .streamDescription().streamStatusAsString();

      return kinesisStreamStatus.contentEquals("ACTIVE");
    } catch (Exception e) {
      LOGGER.warn("Could not fetch kinesis stream status", e);
      return false;
    }
  }

  private static List<Shard> getShards(KinesisClient kinesisClient, String stream) {
    ListShardsResponse listShardsResponse = kinesisClient.listShards(ListShardsRequest.builder().streamName(stream).build());
    return listShardsResponse.shards();
  }

  private static void splitShard(KinesisClient kinesisClient, String stream, Shard shard) {
    BigInteger startHash = new BigInteger(shard.hashKeyRange().startingHashKey());
    BigInteger endHash = new BigInteger(shard.hashKeyRange().endingHashKey());
    BigInteger newStartingHashKey = startHash.add(endHash).divide(new BigInteger("2"));
    kinesisClient.splitShard(SplitShardRequest.builder()
        .shardToSplit(shard.shardId())
        .streamName(stream)
        .newStartingHashKey(newStartingHashKey.toString())
        .build());
  }
}
