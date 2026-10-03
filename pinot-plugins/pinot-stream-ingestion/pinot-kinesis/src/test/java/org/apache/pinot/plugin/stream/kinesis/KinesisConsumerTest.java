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
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


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
      (shardId, requestType, timeoutMs) -> true;

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
  public void testSharedLimiterRestoresRateAndRemovesLastRegistration() {
    KinesisConsumer.SharedKinesisRequestRateLimiter registry = new KinesisConsumer.SharedKinesisRequestRateLimiter();
    KinesisConfig lowConfig = getKinesisConfig(Map.of(KinesisConfig.RPS_LIMIT, "0.25"));
    KinesisConsumer.RequestRateLimiter low = registry.forConsumer(lowConfig);
    KinesisConsumer.RequestRateLimiter high = registry.forConsumer(_kinesisConfig);
    KinesisConsumer.RequestType operation = KinesisConsumer.RequestType.GET_RECORDS;
    low.tryAcquire("0", operation, 1);
    high.tryAcquire("0", operation, 1);
    assertEquals(registry.getLimiterCountForTesting(), 1);
    assertEquals(registry.getRateForTesting(_kinesisConfig, "0", operation), 0.25);
    low.close();
    assertEquals(registry.getRateForTesting(_kinesisConfig, "0", operation), 1.0);
    low.close();
    assertEquals(registry.getLimiterCountForTesting(), 1);
    high.close();
    assertEquals(registry.getLimiterCountForTesting(), 0);

    KinesisConsumer.RequestRateLimiter replacement = registry.forConsumer(_kinesisConfig);
    replacement.tryAcquire("0", operation, 1);
    assertEquals(registry.getRateForTesting(_kinesisConfig, "0", operation), 1.0);
    replacement.close();
    expectThrows(IllegalStateException.class, () -> low.tryAcquire("0", operation, 1));
    assertEquals(registry.getLimiterCountForTesting(), 0);
  }

  @Test
  public void testSharedLimiterEnforcesFractionalBudgetAndSeparatesOperations() {
    KinesisConsumer.SharedKinesisRequestRateLimiter registry = new KinesisConsumer.SharedKinesisRequestRateLimiter();
    // A long permit interval makes the rejection independent of scheduler pauses during the test.
    KinesisConfig config = getKinesisConfig(Map.of(KinesisConfig.RPS_LIMIT, "0.000001"));
    KinesisConsumer.RequestRateLimiter first = registry.forConsumer(config);
    KinesisConsumer.RequestRateLimiter second = registry.forConsumer(config);
    KinesisConsumer.RequestType records = KinesisConsumer.RequestType.GET_RECORDS;
    assertFalse(first.tryAcquire("0", records, 0));
    assertEquals(registry.getLimiterCountForTesting(), 0);
    assertTrue(first.tryAcquire("0", records, 1));
    assertFalse(second.tryAcquire("0", records, 1));
    assertTrue(second.tryAcquire("1", records, 1));
    assertTrue(second.tryAcquire("0", KinesisConsumer.RequestType.GET_SHARD_ITERATOR, 1));
    assertEquals(registry.getLimiterCountForTesting(), 3);
    first.close();
    second.close();
    assertEquals(registry.getLimiterCountForTesting(), 0);
  }

  @Test
  public void testConsumerReplacementPreservesPermitDebt() {
    KinesisConsumer.SharedKinesisRequestRateLimiter registry = new KinesisConsumer.SharedKinesisRequestRateLimiter();
    KinesisConfig config = getKinesisConfig(Map.of(KinesisConfig.RPS_LIMIT, "0.000001"));
    KinesisConsumer.RequestRateLimiter first = registry.forConsumer(config);
    assertTrue(first.tryAcquire("0", KinesisConsumer.RequestType.GET_RECORDS, 1));
    first.close();
    assertEquals(registry.getLimiterCountForTesting(), 0);
    for (int i = 0; i < 3; i++) {
      KinesisConsumer.RequestRateLimiter replacement = registry.forConsumer(config);
      assertFalse(replacement.tryAcquire("0", KinesisConsumer.RequestType.GET_RECORDS, 1));
      replacement.close();
      assertEquals(registry.getLimiterCountForTesting(), 0);
    }
  }

  @Test
  public void testSharedLimiterSeparatesConnectionNamespaces() {
    KinesisConsumer.SharedKinesisRequestRateLimiter registry = new KinesisConsumer.SharedKinesisRequestRateLimiter();
    List<Map<String, String>> namespaces = List.of(
        Map.of(),
        Map.of(KinesisConfig.REGION, "us-east-1"),
        Map.of(KinesisConfig.ENDPOINT, "http://localhost:4566"),
        Map.of(KinesisConfig.ACCESS_KEY, "account-a-key", KinesisConfig.SECRET_KEY, "secret-a"),
        Map.of(KinesisConfig.ACCESS_KEY, "account-b-key", KinesisConfig.SECRET_KEY, "secret-b"),
        Map.of(KinesisConfig.IAM_ROLE_BASED_ACCESS_ENABLED, "true", KinesisConfig.ROLE_ARN,
            "arn:aws:iam::111111111111:role/reader"),
        Map.of(KinesisConfig.IAM_ROLE_BASED_ACCESS_ENABLED, "true", KinesisConfig.ROLE_ARN,
            "arn:aws:iam::222222222222:role/reader"));
    List<KinesisConsumer.RequestRateLimiter> consumers = new ArrayList<>();
    for (Map<String, String> namespace : namespaces) {
      Map<String, String> overrides = new HashMap<>(namespace);
      overrides.put(KinesisConfig.RPS_LIMIT, "0.000001");
      KinesisConfig config = getKinesisConfig(overrides);
      KinesisConsumer.RequestRateLimiter consumer = registry.forConsumer(config);
      consumers.add(consumer);
      assertTrue(consumer.tryAcquire("0", KinesisConsumer.RequestType.GET_RECORDS, 1));
      KinesisConsumer.RequestRateLimiter sameNamespace = registry.forConsumer(getKinesisConfig(overrides));
      consumers.add(sameNamespace);
      assertFalse(sameNamespace.tryAcquire("0", KinesisConsumer.RequestType.GET_RECORDS, 1));
    }
    assertEquals(registry.getLimiterCountForTesting(), namespaces.size());
    consumers.forEach(KinesisConsumer.RequestRateLimiter::close);
    assertEquals(registry.getLimiterCountForTesting(), 0);
  }

  @Test
  public void testConcurrentRegistrationAndRemovalPreserveMinimum() throws Exception {
    KinesisConsumer.SharedKinesisRequestRateLimiter registry = new KinesisConsumer.SharedKinesisRequestRateLimiter();
    int count = 8;
    CyclicBarrier barrier = new CyclicBarrier(count);
    ExecutorService executor = Executors.newFixedThreadPool(count);
    List<KinesisConsumer.RequestRateLimiter> consumers = new ArrayList<>();
    List<Future<?>> futures = new ArrayList<>();
    try {
      for (int i = 1; i <= count; i++) {
        KinesisConfig config = getKinesisConfig(Map.of(KinesisConfig.RPS_LIMIT, String.valueOf(i * 0.125)));
        KinesisConsumer.RequestRateLimiter consumer = registry.forConsumer(config);
        consumers.add(consumer);
        futures.add(executor.submit(() -> {
          barrier.await(10, TimeUnit.SECONDS);
          consumer.tryAcquire("0", KinesisConsumer.RequestType.GET_RECORDS, 1);
          return null;
        }));
      }
      for (Future<?> future : futures) {
        future.get(10, TimeUnit.SECONDS);
      }
      assertEquals(registry.getLimiterCountForTesting(), 1);
      assertEquals(registry.getRateForTesting(_kinesisConfig, "0", KinesisConsumer.RequestType.GET_RECORDS), 0.125);
      futures.clear();
      // Keep the highest-rate consumer active while all the lower-rate consumers close concurrently.
      for (int i = 0; i < count - 1; i++) {
        KinesisConsumer.RequestRateLimiter consumer = consumers.get(i);
        futures.add(executor.submit(consumer::close));
      }
      for (Future<?> future : futures) {
        future.get(10, TimeUnit.SECONDS);
      }
      assertEquals(registry.getRateForTesting(_kinesisConfig, "0", KinesisConsumer.RequestType.GET_RECORDS), 1.0);
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
      consumers.forEach(KinesisConsumer.RequestRateLimiter::close);
    }
    assertEquals(registry.getLimiterCountForTesting(), 0);
  }

  @Test
  public void testConsumerCloseReleasesRegistrationsEvenWhenClientCloseFails() {
    KinesisConsumer.SharedKinesisRequestRateLimiter registry = new KinesisConsumer.SharedKinesisRequestRateLimiter();
    KinesisClient client = mock(KinesisClient.class);
    when(client.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(
        GetShardIteratorResponse.builder().shardIterator(PLACEHOLDER).build());
    when(client.getRecords(any(GetRecordsRequest.class))).thenReturn(
        GetRecordsResponse.builder().nextShardIterator(PLACEHOLDER).records(_records).build());
    KinesisConsumer consumer = new KinesisConsumer(_kinesisConfig, client, registry.forConsumer(_kinesisConfig));
    KinesisPartitionGroupOffset offset = new KinesisPartitionGroupOffset("0", "1");
    assertEquals(consumer.fetchMessages(offset, TIMEOUT).getMessageCount(), NUM_RECORDS);
    assertEquals(registry.getLimiterCountForTesting(), 2);
    doThrow(new IllegalStateException("client close failed")).when(client).close();
    expectThrows(IllegalStateException.class, consumer::close);
    assertEquals(registry.getLimiterCountForTesting(), 0);
    consumer.close();
    verify(client, times(1)).close();
    expectThrows(IllegalStateException.class, () -> consumer.fetchMessages(offset, TIMEOUT));
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
    public boolean tryAcquire(String shardId, KinesisConsumer.RequestType requestType, long timeoutMs) {
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
    public boolean tryAcquire(String shardId, KinesisConsumer.RequestType requestType, long timeoutMs) {
      super.tryAcquire(shardId, requestType, timeoutMs);
      if (requestType == KinesisConsumer.RequestType.GET_SHARD_ITERATOR) {
        _kinesisConsumer.advanceTimeMs(4000);
        return true;
      }
      return false;
    }
  }
}
