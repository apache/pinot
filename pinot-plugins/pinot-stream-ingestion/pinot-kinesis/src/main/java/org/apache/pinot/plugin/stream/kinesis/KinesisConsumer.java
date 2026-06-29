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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.RateLimiter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.pinot.common.utils.ThrottledLogger;
import org.apache.pinot.spi.stream.BytesStreamMessage;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;


/**
 * A {@link PartitionGroupConsumer} implementation for the Kinesis stream
 */
public class KinesisConsumer extends KinesisConnectionHandler implements PartitionGroupConsumer {
  private static final Logger LOGGER = LoggerFactory.getLogger(KinesisConsumer.class);
  private static final int INITIAL_RATE_LIMIT_BACKOFF_MS = 1000;
  private static final int MAX_RATE_LIMIT_BACKOFF_MS = 5000;
  private static final int RATE_LIMIT_BACKOFF_JITTER_BOUND_MS = 250;
  private static final RequestRateLimiter SHARED_REQUEST_RATE_LIMITER = new SharedKinesisRequestRateLimiter();
  private static final double RATE_LIMIT_LOG_RATE_PER_MIN = 5.0;

  private final ThrottledLogger _throttledLogger = new ThrottledLogger(LOGGER, RATE_LIMIT_LOG_RATE_PER_MIN);
  private String _nextStartSequenceNumber = null;
  private String _nextShardIterator = null;
  private final RequestRateLimiter _requestRateLimiter;

  public KinesisConsumer(KinesisConfig config) {
    super(config);
    _requestRateLimiter = SHARED_REQUEST_RATE_LIMITER;
    LOGGER.info("Created Kinesis consumer with topic: {}, RPS limit: {}, max records per fetch: {}",
        config.getStreamTopicName(), config.getRpsLimitPerSecond(), config.getNumMaxRecordsToFetch());
  }

  @VisibleForTesting
  public KinesisConsumer(KinesisConfig config, KinesisClient kinesisClient) {
    this(config, kinesisClient, SHARED_REQUEST_RATE_LIMITER);
  }

  @VisibleForTesting
  KinesisConsumer(KinesisConfig config, KinesisClient kinesisClient, RequestRateLimiter requestRateLimiter) {
    super(config, kinesisClient);
    _requestRateLimiter = requestRateLimiter;
  }

  /**
   * Based on Kinesis documentation, we might get a response with empty records but a non-null nextShardIterator.
   * Known cases are:
   *  1. When the shard has ended (has been split or merged) and we need a couple of calls to getRecords() to reach
   *  a null iterator
   *  2. When there are no new messages in the shard but the shard is active. We will continue to get a non-null
   *  nextShardIterator in this case
   *  3. When there are some messages in the shard, but we need a few iterations to get them.
   * This needs to be handled by the client based on appropriate retry strategy.
   */
  @Override
  public synchronized KinesisMessageBatch fetchMessages(StreamPartitionMsgOffset startMsgOffset, int timeoutMs) {
    KinesisPartitionGroupOffset startOffset = (KinesisPartitionGroupOffset) startMsgOffset;
    long deadlineMs = currentTimeMillis() + Math.max(timeoutMs, 0);
    int attempts = 0;
    KinesisRateLimitException lastRateLimitException = null;
    while (true) {
      if (lastRateLimitException != null && currentTimeMillis() >= deadlineMs) {
        logRateLimitTimeout(startOffset, attempts, lastRateLimitException);
        return new KinesisMessageBatch(List.of(), startOffset, false, 0);
      }
      try {
        return getKinesisMessageBatch(startOffset, deadlineMs);
      } catch (KinesisRateLimitException e) {
        lastRateLimitException = e;
        attempts++;
        long remainingMs = deadlineMs - currentTimeMillis();
        if (remainingMs <= 0) {
          logRateLimitTimeout(startOffset, attempts, e);
          return new KinesisMessageBatch(List.of(), startOffset, false, 0);
        }
        long backoffMs = Math.min(computeRateLimitBackoffMs(attempts), remainingMs);
        _throttledLogger.warn(
            String.format("Rate limit exceeded while fetching messages from Kinesis stream: %s, shard: %s, "
                    + "operation: %s, threshold: %s, attempt: %d, backing off for %d ms",
                _config.getStreamTopicName(), startOffset.getShardId(), e.getRequestType(),
                _config.getRpsLimitPerSecond(), attempts, backoffMs), e.getCause());
        sleep(backoffMs);
      } catch (KinesisRequestTimeoutException e) {
        logRequestLimiterTimeout(startOffset, e);
        return new KinesisMessageBatch(List.of(), startOffset, false, 0);
      }
    }
  }

  private KinesisMessageBatch getKinesisMessageBatch(KinesisPartitionGroupOffset startMsgOffset, long deadlineMs) {
    KinesisPartitionGroupOffset startOffset = startMsgOffset;
    String shardId = startOffset.getShardId();
    String startSequenceNumber = startOffset.getSequenceNumber();
    // Get the shard iterator
    String shardIterator;
    if (startSequenceNumber.equals(_nextStartSequenceNumber)) {
      shardIterator = _nextShardIterator;
    } else {
      GetShardIteratorRequest getShardIteratorRequest =
          GetShardIteratorRequest.builder().streamName(_config.getStreamTopicName()).shardId(shardId)
              .startingSequenceNumber(startSequenceNumber).shardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER)
              .build();
      shardIterator = executeKinesisRequest(shardId, RequestType.GET_SHARD_ITERATOR, deadlineMs,
          () -> _kinesisClient.getShardIterator(getShardIteratorRequest)).shardIterator();
    }
    if (shardIterator == null) {
      return new KinesisMessageBatch(List.of(), startOffset, true, 0);
    }
    _nextStartSequenceNumber = startSequenceNumber;
    _nextShardIterator = shardIterator;

    // Read records
    GetRecordsRequest getRecordRequest =
        GetRecordsRequest.builder().shardIterator(shardIterator).limit(_config.getNumMaxRecordsToFetch()).build();
    GetRecordsResponse getRecordsResponse = executeKinesisRequest(shardId, RequestType.GET_RECORDS, deadlineMs,
        () -> _kinesisClient.getRecords(getRecordRequest));

    List<Record> records = getRecordsResponse.records();
    List<BytesStreamMessage> messages;
    KinesisPartitionGroupOffset offsetOfNextBatch;
    long batchSizeInBytes = 0;
    if (!records.isEmpty()) {
      messages = new ArrayList<>();
      for (Record record: records) {
        BytesStreamMessage bytesStreamMessage = extractStreamMessage(record, shardId);
        batchSizeInBytes += bytesStreamMessage.getLength();
        messages.add(bytesStreamMessage);
      }
      offsetOfNextBatch =
          (KinesisPartitionGroupOffset) messages.get(messages.size() - 1).getMetadata().getNextOffset();
    } else {
      // Empty batch with non-null nextShardIterator: Kinesis may return empty results transiently even when
      // records are available. The framework re-fetches on the next cycle using the cached shard iterator.
      if (getRecordsResponse.nextShardIterator() != null) {
        LOGGER.debug("Empty batch returned from Kinesis shard: {} with non-null next shard iterator", shardId);
      }
      messages = List.of();
      offsetOfNextBatch = startOffset;
    }
    assert offsetOfNextBatch != null;
    _nextStartSequenceNumber = offsetOfNextBatch.getSequenceNumber();
    _nextShardIterator = getRecordsResponse.nextShardIterator();
    return new KinesisMessageBatch(messages, offsetOfNextBatch, _nextShardIterator == null, batchSizeInBytes);
  }

  private <T> T executeKinesisRequest(String shardId, RequestType requestType, long deadlineMs,
      Supplier<T> requestSupplier) {
    long remainingMs = deadlineMs - currentTimeMillis();
    if (remainingMs <= 0
        || !_requestRateLimiter.tryAcquire(_config.getStreamTopicName(), shardId, requestType,
            _config.getRpsLimitPerSecond(), remainingMs)) {
      throw new KinesisRequestTimeoutException(requestType);
    }
    try {
      return requestSupplier.get();
    } catch (ProvisionedThroughputExceededException pte) {
      throw new KinesisRateLimitException(requestType, pte);
    }
  }

  private long computeRateLimitBackoffMs(int attempts) {
    long baseBackoffMs = INITIAL_RATE_LIMIT_BACKOFF_MS * (1L << Math.min(attempts - 1, 20));
    long cappedBaseBackoffMs = Math.min(baseBackoffMs, MAX_RATE_LIMIT_BACKOFF_MS);
    long jitterMs = getRateLimitBackoffJitterMs(RATE_LIMIT_BACKOFF_JITTER_BOUND_MS);
    return Math.min(cappedBaseBackoffMs + jitterMs, MAX_RATE_LIMIT_BACKOFF_MS);
  }

  @VisibleForTesting
  long currentTimeMillis() {
    return System.currentTimeMillis();
  }

  @VisibleForTesting
  long getRateLimitBackoffJitterMs(long maxJitterMs) {
    return ThreadLocalRandom.current().nextLong(maxJitterMs + 1);
  }

  @VisibleForTesting
  void sleep(long backoffMs) {
    try {
      Thread.sleep(backoffMs);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while backing off after Kinesis rate limit exceeded", e);
    }
  }

  private void logRateLimitTimeout(KinesisPartitionGroupOffset startOffset, int attempts,
      KinesisRateLimitException rateLimitException) {
    _throttledLogger.warn(
        String.format("Rate limit exceeded while fetching messages from Kinesis stream: %s, shard: %s, "
                + "operation: %s, threshold: %s, attempts: %d. Fetch timeout exhausted; returning empty batch.",
            _config.getStreamTopicName(), startOffset.getShardId(), rateLimitException.getRequestType(),
            _config.getRpsLimitPerSecond(), attempts), rateLimitException.getCause());
  }

  private void logRequestLimiterTimeout(KinesisPartitionGroupOffset startOffset,
      KinesisRequestTimeoutException timeoutException) {
    _throttledLogger.warn(
        String.format("Timed out waiting for Kinesis request limiter while fetching messages from stream: %s, "
                + "shard: %s, operation: %s, threshold: %s. Fetch timeout exhausted; returning empty batch.",
            _config.getStreamTopicName(), startOffset.getShardId(), timeoutException.getRequestType(),
            _config.getRpsLimitPerSecond()), timeoutException);
  }

  private BytesStreamMessage extractStreamMessage(Record record, String shardId) {
    byte[] key = record.partitionKey().getBytes(StandardCharsets.UTF_8);
    byte[] value = record.data().asByteArray();
    long timestamp = record.approximateArrivalTimestamp().toEpochMilli();
    String sequenceNumber = record.sequenceNumber();
    KinesisPartitionGroupOffset offset = new KinesisPartitionGroupOffset(shardId, sequenceNumber);
    // NOTE: Use the same offset as next offset because the consumer starts consuming AFTER the start sequence number.
    StreamMessageMetadata.Builder builder =
        new StreamMessageMetadata.Builder().setRecordIngestionTimeMs(timestamp).setSerializedValueSize(value.length)
            .setOffset(offset, offset);
    if (_config.isPopulateMetadata()) {
      builder.setMetadata(Map.of(KinesisStreamMessageMetadata.APPRX_ARRIVAL_TIMESTAMP_KEY, String.valueOf(timestamp),
          KinesisStreamMessageMetadata.SEQUENCE_NUMBER_KEY, sequenceNumber));
    }
    StreamMessageMetadata metadata = builder.build();
    return new BytesStreamMessage(key, value, metadata);
  }

  @Override
  public void close() {
    super.close();
  }

  enum RequestType {
    GET_RECORDS,
    GET_SHARD_ITERATOR
  }

  @VisibleForTesting
  interface RequestRateLimiter {
    boolean tryAcquire(String streamName, String shardId, RequestType requestType, double rpsLimit, long timeoutMs);
  }

  private static class KinesisRateLimitException extends RuntimeException {
    private final RequestType _requestType;

    KinesisRateLimitException(RequestType requestType, ProvisionedThroughputExceededException cause) {
      super(cause);
      _requestType = requestType;
    }

    RequestType getRequestType() {
      return _requestType;
    }

    @Override
    public synchronized ProvisionedThroughputExceededException getCause() {
      return (ProvisionedThroughputExceededException) super.getCause();
    }
  }

  private static class KinesisRequestTimeoutException extends RuntimeException {
    private final RequestType _requestType;

    KinesisRequestTimeoutException(RequestType requestType) {
      _requestType = requestType;
    }

    RequestType getRequestType() {
      return _requestType;
    }
  }

  /**
   * Shared per-JVM request limiter for Kinesis read operations.
   * <p>
   * This class is thread-safe. Limiters are keyed by stream, shard, and operation so multiple consumers on the same
   * server share a single smooth request budget for the same AWS shard operation.
   */
  @VisibleForTesting
  static class SharedKinesisRequestRateLimiter implements RequestRateLimiter {
    private static final int RATE_LIMITER_EXPIRATION_HOURS = 1;
    private final Cache<RequestRateLimiterKey, RateLimiter> _rateLimiters;

    SharedKinesisRequestRateLimiter() {
      this(CacheBuilder.newBuilder().expireAfterAccess(RATE_LIMITER_EXPIRATION_HOURS, TimeUnit.HOURS).build());
    }

    @VisibleForTesting
    SharedKinesisRequestRateLimiter(Cache<RequestRateLimiterKey, RateLimiter> rateLimiters) {
      _rateLimiters = rateLimiters;
    }

    @Override
    public boolean tryAcquire(String streamName, String shardId, RequestType requestType, double rpsLimit,
        long timeoutMs) {
      if (timeoutMs <= 0) {
        return false;
      }
      RequestRateLimiterKey key = new RequestRateLimiterKey(streamName, shardId, requestType);
      RateLimiter rateLimiter = _rateLimiters.asMap().computeIfAbsent(key, ignored -> RateLimiter.create(rpsLimit));
      if (rpsLimit < rateLimiter.getRate()) {
        rateLimiter.setRate(rpsLimit);
      }
      return rateLimiter.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS);
    }

    @VisibleForTesting
    double getRateForTesting(String streamName, String shardId, RequestType requestType) {
      RateLimiter rateLimiter =
          _rateLimiters.getIfPresent(new RequestRateLimiterKey(streamName, shardId, requestType));
      return rateLimiter == null ? 0.0 : rateLimiter.getRate();
    }

    @VisibleForTesting
    long getLimiterCountForTesting() {
      return _rateLimiters.size();
    }
  }

  private static class RequestRateLimiterKey {
    private final String _streamName;
    private final String _shardId;
    private final RequestType _requestType;

    RequestRateLimiterKey(String streamName, String shardId, RequestType requestType) {
      _streamName = streamName;
      _shardId = shardId;
      _requestType = requestType;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof RequestRateLimiterKey)) {
        return false;
      }
      RequestRateLimiterKey that = (RequestRateLimiterKey) o;
      return _streamName.equals(that._streamName) && _shardId.equals(that._shardId)
          && _requestType == that._requestType;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_streamName, _shardId, _requestType);
    }
  }
}
