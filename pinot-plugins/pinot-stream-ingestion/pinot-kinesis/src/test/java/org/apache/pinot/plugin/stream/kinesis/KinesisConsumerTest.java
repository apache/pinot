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

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class KinesisConsumerTest {
  private static final String STREAM_TYPE = "kinesis";
  private static final String TABLE_NAME_WITH_TYPE = "kinesisTest_REALTIME";
  private static final String STREAM_NAME = "kinesis-test";
  private static final String AWS_REGION = "us-west-2";
  private static final int TIMEOUT = 1000;
  private static final int NUM_RECORDS = 10;
  private static final String DUMMY_RECORD_PREFIX = "DUMMY_RECORD-";
  private static final String PARTITION_KEY_PREFIX = "PARTITION_KEY-";
  private static final String PLACEHOLDER = "DUMMY";
  private static final int MAX_RECORDS_TO_FETCH = 20;
  private static final double DOUBLE_COMPARISON_DELTA = 0.000001;
  private static final KinesisConsumer.RequestRateLimiter NO_OP_REQUEST_RATE_LIMITER =
      (streamName, shardId, requestType, rpsLimit, timeoutMs) -> true;

  private KinesisConfig _kinesisConfig;
  private List<Record> _records;

  private KinesisConfig getKinesisConfig() {
    return getKinesisConfig(Map.of());
  }

  private KinesisConfig getKinesisConfig(Map<String, String> overrides) {
    Map<String, String> props = new HashMap<>();
    props.put(StreamConfigProperties.STREAM_TYPE, STREAM_TYPE);
    props.put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE, StreamConfigProperties.STREAM_TOPIC_NAME),
        STREAM_NAME);
    props.put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE,
        StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS), KinesisConsumerFactory.class.getName());
    props.put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE, StreamConfigProperties.STREAM_DECODER_CLASS),
        "org.apache.pinot.plugin.inputformat.json.JSONMessageDecoder");
    props.put(KinesisConfig.REGION, AWS_REGION);
    props.put(KinesisConfig.MAX_RECORDS_TO_FETCH, String.valueOf(MAX_RECORDS_TO_FETCH));
    props.put(KinesisConfig.SHARD_ITERATOR_TYPE, ShardIteratorType.AT_SEQUENCE_NUMBER.toString());
    props.putAll(overrides);
    return new KinesisConfig(new StreamConfig(TABLE_NAME_WITH_TYPE, props));
  }

  @BeforeClass
  public void setUp() {
    _kinesisConfig = getKinesisConfig();
    _records = new ArrayList<>(NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      Record record =
          Record.builder().data(SdkBytes.fromUtf8String(DUMMY_RECORD_PREFIX + i)).partitionKey(PARTITION_KEY_PREFIX + i)
              .approximateArrivalTimestamp(Instant.now()).sequenceNumber(String.valueOf(i + 1)).build();
      _records.add(record);
    }
  }

  @Test
  public void testBasicConsumer() {
    KinesisClient kinesisClient = mock(KinesisClient.class);
    when(kinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(
        GetShardIteratorResponse.builder().shardIterator(PLACEHOLDER).build());
    when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(
        GetRecordsResponse.builder().nextShardIterator(PLACEHOLDER).records(_records).build());

    KinesisConsumer kinesisConsumer = newTestConsumer(kinesisClient);

    // Fetch first batch
    KinesisPartitionGroupOffset startOffset = new KinesisPartitionGroupOffset("0", "1");
    KinesisMessageBatch kinesisMessageBatch = kinesisConsumer.fetchMessages(startOffset, TIMEOUT);
    assertEquals(kinesisMessageBatch.getMessageCount(), NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      assertEquals(baToString(kinesisMessageBatch.getStreamMessage(i).getValue()), DUMMY_RECORD_PREFIX + i);
    }
    assertFalse(kinesisMessageBatch.isEndOfPartitionGroup());
    assertTrue(kinesisMessageBatch.getSizeInBytes() > 0);

    // Fetch second batch
    kinesisMessageBatch = kinesisConsumer.fetchMessages(kinesisMessageBatch.getOffsetOfNextBatch(), TIMEOUT);
    assertEquals(kinesisMessageBatch.getMessageCount(), NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      assertEquals(baToString(kinesisMessageBatch.getStreamMessage(i).getValue()), DUMMY_RECORD_PREFIX + i);
    }
    assertFalse(kinesisMessageBatch.isEndOfPartitionGroup());
    assertTrue(kinesisMessageBatch.getSizeInBytes() > 0);

    // Expect only 1 call to get shard iterator and 2 calls to get records
    verify(kinesisClient, times(1)).getShardIterator(any(GetShardIteratorRequest.class));
    verify(kinesisClient, times(2)).getRecords(any(GetRecordsRequest.class));
  }

  @Test
  public void testEndOfShard() {
    KinesisClient kinesisClient = mock(KinesisClient.class);
    when(kinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(
        GetShardIteratorResponse.builder().shardIterator(PLACEHOLDER).build());
    when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(
        GetRecordsResponse.builder().nextShardIterator(null).records(_records).build());

    KinesisConsumer kinesisConsumer = newTestConsumer(kinesisClient);

    // Fetch first batch
    KinesisPartitionGroupOffset startOffset = new KinesisPartitionGroupOffset("0", "1");
    KinesisMessageBatch kinesisMessageBatch = kinesisConsumer.fetchMessages(startOffset, TIMEOUT);
    assertEquals(kinesisMessageBatch.getMessageCount(), NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      assertEquals(baToString(kinesisMessageBatch.getStreamMessage(i).getValue()), DUMMY_RECORD_PREFIX + i);
    }
    assertTrue(kinesisMessageBatch.isEndOfPartitionGroup());

    // Fetch second batch
    kinesisMessageBatch = kinesisConsumer.fetchMessages(kinesisMessageBatch.getOffsetOfNextBatch(), TIMEOUT);
    assertEquals(kinesisMessageBatch.getMessageCount(), 0);
    assertTrue(kinesisMessageBatch.isEndOfPartitionGroup());

    // Expect only 1 call to get shard iterator and 1 call to get records
    verify(kinesisClient, times(1)).getShardIterator(any(GetShardIteratorRequest.class));
    verify(kinesisClient, times(1)).getRecords(any(GetRecordsRequest.class));
  }

  public String baToString(byte[] bytes) {
    return SdkBytes.fromByteArray(bytes).asUtf8String();
  }

  @Test
  public void testRpsLimitConfig() {
    KinesisConfig defaultConfig = getKinesisConfig();
    assertEquals(defaultConfig.getRpsLimitPerSecond(), 1.0, DOUBLE_COMPARISON_DELTA);
    assertEquals(defaultConfig.getRpsLimit(), 1);

    KinesisConfig integerConfig = getKinesisConfig(Map.of(KinesisConfig.RPS_LIMIT, "2"));
    assertEquals(integerConfig.getRpsLimitPerSecond(), 2.0, DOUBLE_COMPARISON_DELTA);
    assertEquals(integerConfig.getRpsLimit(), 2);

    KinesisConfig decimalConfig = getKinesisConfig(Map.of(KinesisConfig.RPS_LIMIT, "0.25"));
    assertEquals(decimalConfig.getRpsLimitPerSecond(), 0.25, DOUBLE_COMPARISON_DELTA);
    assertEquals(decimalConfig.getRpsLimit(), 1);

    assertEquals(getKinesisConfig(Map.of(KinesisConfig.RPS_LIMIT, "0")).getRpsLimitPerSecond(), 1.0,
        DOUBLE_COMPARISON_DELTA);
    assertEquals(getKinesisConfig(Map.of(KinesisConfig.RPS_LIMIT, "-1")).getRpsLimitPerSecond(), 1.0,
        DOUBLE_COMPARISON_DELTA);
  }

  @Test
  public void testFetchRetriesGetRecordsRateLimitExceeded() {
    KinesisClient kinesisClient = mock(KinesisClient.class);
    when(kinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(
        GetShardIteratorResponse.builder().shardIterator(PLACEHOLDER).build());
    when(kinesisClient.getRecords(any(GetRecordsRequest.class)))
        .thenThrow(rateLimitException())
        .thenThrow(rateLimitException())
        .thenReturn(GetRecordsResponse.builder().nextShardIterator(PLACEHOLDER).records(_records).build());

    TestKinesisConsumer kinesisConsumer =
        new TestKinesisConsumer(_kinesisConfig, kinesisClient, NO_OP_REQUEST_RATE_LIMITER);

    KinesisPartitionGroupOffset startOffset = new KinesisPartitionGroupOffset("0", "1");
    KinesisMessageBatch kinesisMessageBatch = kinesisConsumer.fetchMessages(startOffset, 10_000);

    assertEquals(kinesisMessageBatch.getMessageCount(), NUM_RECORDS);
    assertFalse(kinesisMessageBatch.isEndOfPartitionGroup());
    assertEquals(kinesisConsumer.getSleepMsList(), List.of(1000L, 2000L));
    verify(kinesisClient, times(1)).getShardIterator(any(GetShardIteratorRequest.class));
    verify(kinesisClient, times(3)).getRecords(any(GetRecordsRequest.class));
  }

  @Test
  public void testFetchReturnsEmptyBatchWhenGetRecordsRateLimitExceedsTimeout() {
    KinesisClient kinesisClient = mock(KinesisClient.class);
    when(kinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(
        GetShardIteratorResponse.builder().shardIterator(PLACEHOLDER).build());
    when(kinesisClient.getRecords(any(GetRecordsRequest.class)))
        .thenThrow(rateLimitException())
        .thenThrow(rateLimitException());

    TestKinesisConsumer kinesisConsumer =
        new TestKinesisConsumer(_kinesisConfig, kinesisClient, NO_OP_REQUEST_RATE_LIMITER);

    KinesisPartitionGroupOffset startOffset = new KinesisPartitionGroupOffset("0", "1");
    KinesisMessageBatch kinesisMessageBatch = kinesisConsumer.fetchMessages(startOffset, 2500);

    assertEquals(kinesisMessageBatch.getMessageCount(), 0);
    assertEquals(kinesisMessageBatch.getOffsetOfNextBatch(), startOffset);
    assertFalse(kinesisMessageBatch.isEndOfPartitionGroup());
    assertEquals(kinesisConsumer.getSleepMsList(), List.of(1000L, 1500L));
    verify(kinesisClient, times(1)).getShardIterator(any(GetShardIteratorRequest.class));
    verify(kinesisClient, times(2)).getRecords(any(GetRecordsRequest.class));
  }

  @Test
  public void testFetchRetriesGetShardIteratorRateLimitExceeded() {
    KinesisClient kinesisClient = mock(KinesisClient.class);
    when(kinesisClient.getShardIterator(any(GetShardIteratorRequest.class)))
        .thenThrow(rateLimitException())
        .thenReturn(GetShardIteratorResponse.builder().shardIterator(PLACEHOLDER).build());
    when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(
        GetRecordsResponse.builder().nextShardIterator(PLACEHOLDER).records(_records).build());

    CapturingRequestRateLimiter requestRateLimiter = new CapturingRequestRateLimiter();
    TestKinesisConsumer kinesisConsumer = new TestKinesisConsumer(_kinesisConfig, kinesisClient, requestRateLimiter);

    KinesisPartitionGroupOffset startOffset = new KinesisPartitionGroupOffset("0", "1");
    KinesisMessageBatch kinesisMessageBatch = kinesisConsumer.fetchMessages(startOffset, 10_000);

    assertEquals(kinesisMessageBatch.getMessageCount(), NUM_RECORDS);
    assertEquals(kinesisConsumer.getSleepMsList(), List.of(1000L));
    assertEquals(requestRateLimiter.getRequestTypes(),
        List.of(KinesisConsumer.RequestType.GET_SHARD_ITERATOR, KinesisConsumer.RequestType.GET_SHARD_ITERATOR,
            KinesisConsumer.RequestType.GET_RECORDS));
    verify(kinesisClient, times(2)).getShardIterator(any(GetShardIteratorRequest.class));
    verify(kinesisClient, times(1)).getRecords(any(GetRecordsRequest.class));
  }

  @Test
  public void testFetchReturnsEmptyBatchWhenRequestLimiterExceedsRemainingTimeout() {
    KinesisClient kinesisClient = mock(KinesisClient.class);
    when(kinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(
        GetShardIteratorResponse.builder().shardIterator(PLACEHOLDER).build());

    AdvancingRequestRateLimiter requestRateLimiter = new AdvancingRequestRateLimiter();
    TestKinesisConsumer kinesisConsumer = new TestKinesisConsumer(_kinesisConfig, kinesisClient, requestRateLimiter);
    requestRateLimiter.setKinesisConsumer(kinesisConsumer);

    KinesisPartitionGroupOffset startOffset = new KinesisPartitionGroupOffset("0", "1");
    KinesisMessageBatch kinesisMessageBatch = kinesisConsumer.fetchMessages(startOffset, 5000);

    assertEquals(kinesisMessageBatch.getMessageCount(), 0);
    assertEquals(kinesisMessageBatch.getOffsetOfNextBatch(), startOffset);
    assertFalse(kinesisMessageBatch.isEndOfPartitionGroup());
    assertEquals(requestRateLimiter.getRequestTypes(),
        List.of(KinesisConsumer.RequestType.GET_SHARD_ITERATOR, KinesisConsumer.RequestType.GET_RECORDS));
    assertEquals(requestRateLimiter.getTimeoutMsList(), List.of(5000L, 1000L));
    verify(kinesisClient, times(1)).getShardIterator(any(GetShardIteratorRequest.class));
    verify(kinesisClient, times(0)).getRecords(any(GetRecordsRequest.class));
  }

  @Test
  public void testSharedLimiterUsesSameLimiterForSameStreamShardOperation() {
    KinesisConsumer.SharedKinesisRequestRateLimiter requestRateLimiter =
        new KinesisConsumer.SharedKinesisRequestRateLimiter();

    assertTrue(requestRateLimiter.tryAcquire(STREAM_NAME, "0", KinesisConsumer.RequestType.GET_RECORDS, 1_000_000.0,
        1000));
    assertTrue(requestRateLimiter.tryAcquire(STREAM_NAME, "0", KinesisConsumer.RequestType.GET_RECORDS, 500_000.0,
        1000));

    assertEquals(requestRateLimiter.getLimiterCountForTesting(), 1);
    assertEquals(requestRateLimiter.getRateForTesting(STREAM_NAME, "0", KinesisConsumer.RequestType.GET_RECORDS),
        500_000.0, DOUBLE_COMPARISON_DELTA);

    assertTrue(requestRateLimiter.tryAcquire(STREAM_NAME, "0", KinesisConsumer.RequestType.GET_SHARD_ITERATOR,
        250_000.0, 1000));
    assertEquals(requestRateLimiter.getLimiterCountForTesting(), 2);
  }

  private KinesisConsumer newTestConsumer(KinesisClient kinesisClient) {
    return new TestKinesisConsumer(_kinesisConfig, kinesisClient, NO_OP_REQUEST_RATE_LIMITER);
  }

  private ProvisionedThroughputExceededException rateLimitException() {
    return ProvisionedThroughputExceededException.builder().message("throttled").build();
  }

  private static class TestKinesisConsumer extends KinesisConsumer {
    private final List<Long> _sleepMsList = new ArrayList<>();
    private long _currentTimeMs;

    TestKinesisConsumer(KinesisConfig config, KinesisClient kinesisClient,
        KinesisConsumer.RequestRateLimiter requestRateLimiter) {
      super(config, kinesisClient, requestRateLimiter);
    }

    @Override
    void sleep(long sleepMs) {
      _sleepMsList.add(sleepMs);
      _currentTimeMs += sleepMs;
    }

    @Override
    long currentTimeMillis() {
      return _currentTimeMs;
    }

    @Override
    long getRateLimitBackoffJitterMs(long maxJitterMs) {
      return 0L;
    }

    List<Long> getSleepMsList() {
      return _sleepMsList;
    }

    void advanceTimeMs(long timeMs) {
      _currentTimeMs += timeMs;
    }
  }

  private static class CapturingRequestRateLimiter implements KinesisConsumer.RequestRateLimiter {
    private final List<KinesisConsumer.RequestType> _requestTypes = new ArrayList<>();
    private final List<Long> _timeoutMsList = new ArrayList<>();

    @Override
    public boolean tryAcquire(String streamName, String shardId, KinesisConsumer.RequestType requestType,
        double rpsLimit, long timeoutMs) {
      _requestTypes.add(requestType);
      _timeoutMsList.add(timeoutMs);
      return true;
    }

    List<KinesisConsumer.RequestType> getRequestTypes() {
      return _requestTypes;
    }

    List<Long> getTimeoutMsList() {
      return _timeoutMsList;
    }
  }

  private static class AdvancingRequestRateLimiter extends CapturingRequestRateLimiter {
    private TestKinesisConsumer _kinesisConsumer;

    void setKinesisConsumer(TestKinesisConsumer kinesisConsumer) {
      _kinesisConsumer = kinesisConsumer;
    }

    @Override
    public boolean tryAcquire(String streamName, String shardId, KinesisConsumer.RequestType requestType,
        double rpsLimit, long timeoutMs) {
      super.tryAcquire(streamName, shardId, requestType, rpsLimit, timeoutMs);
      if (requestType == KinesisConsumer.RequestType.GET_SHARD_ITERATOR) {
        _kinesisConsumer.advanceTimeMs(4000);
        return true;
      }
      return false;
    }
  }
}
