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
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
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


/// A [PartitionGroupConsumer] implementation for the Kinesis stream
public class KinesisConsumer extends KinesisConnectionHandler implements PartitionGroupConsumer {
  private static final Logger LOGGER = LoggerFactory.getLogger(KinesisConsumer.class);
  private static final int INITIAL_RATE_LIMIT_BACKOFF_MS = 1000;
  private static final int MAX_RATE_LIMIT_BACKOFF_MS = 5000;
  private static final int RATE_LIMIT_BACKOFF_JITTER_BOUND_MS = 250;
  private static final SharedKinesisRequestRateLimiter SHARED_REQUEST_RATE_LIMITER =
      new SharedKinesisRequestRateLimiter();
  private static final double RATE_LIMIT_LOG_RATE_PER_MIN = 5.0;

  private final ThrottledLogger _throttledLogger = new ThrottledLogger(LOGGER, RATE_LIMIT_LOG_RATE_PER_MIN);
  private String _nextStartSequenceNumber = null;
  private String _nextShardIterator = null;
  private final RequestRateLimiter _requestRateLimiter;
  private boolean _closed;

  public KinesisConsumer(KinesisConfig config) {
    super(config);
    _requestRateLimiter = SHARED_REQUEST_RATE_LIMITER.forConsumer(config);
    LOGGER.info("Created Kinesis consumer with topic: {}, RPS limit: {}, max records per fetch: {}",
        config.getStreamTopicName(), config.getRpsLimitPerSecond(), config.getNumMaxRecordsToFetch());
  }

  @VisibleForTesting
  public KinesisConsumer(KinesisConfig config, KinesisClient kinesisClient) {
    this(config, kinesisClient, SHARED_REQUEST_RATE_LIMITER.forConsumer(config));
  }

  @VisibleForTesting
  KinesisConsumer(KinesisConfig config, KinesisClient kinesisClient, RequestRateLimiter requestRateLimiter) {
    super(config, kinesisClient);
    _requestRateLimiter = requestRateLimiter;
  }

  /// Based on Kinesis documentation, we might get a response with empty records but a non-null nextShardIterator.
  /// Known cases are:
  ///  1. When the shard has ended (has been split or merged) and we need a couple of calls to getRecords() to reach
  ///  a null iterator
  ///  2. When there are no new messages in the shard but the shard is active. We will continue to get a non-null
  ///  nextShardIterator in this case
  ///  3. When there are some messages in the shard, but we need a few iterations to get them.
  /// This needs to be handled by the client based on appropriate retry strategy.
  @Override
  public synchronized KinesisMessageBatch fetchMessages(StreamPartitionMsgOffset startMsgOffset, int timeoutMs) {
    if (_closed) {
      throw new IllegalStateException("Kinesis consumer is closed");
    }
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
      // TODO: Revisit the offset handling logic. Reading after the start sequence number can lose the first message
      //       when consuming from a new partition because the initial start sequence number is inclusive.
      GetShardIteratorRequest getShardIteratorRequest =
          GetShardIteratorRequest.builder().streamName(_config.getStreamTopicName()).shardId(shardId)
              .startingSequenceNumber(startSequenceNumber).shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
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
      // TODO: Revisit whether Kinesis can return empty batch when there are available records. The consumer cna handle
      //       empty message batch, but it will treat it as fully caught up.
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
        || !_requestRateLimiter.tryAcquire(shardId, requestType, remainingMs)) {
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
  public synchronized void close() {
    if (!_closed) {
      _closed = true;
      try {
        super.close();
      } finally {
        _requestRateLimiter.close();
      }
    }
  }

  enum RequestType {
    GET_RECORDS,
    GET_SHARD_ITERATOR
  }

  @VisibleForTesting
  interface RequestRateLimiter {
    boolean tryAcquire(String shardId, RequestType requestType, long timeoutMs);

    default void close() {
    }
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

  /// Shared per-JVM request budgets. Registrations and effective-rate changes are serialized per key; permit waits
  /// happen outside the registry update. Entries live until their last registered consumer closes.
  @VisibleForTesting
  static class SharedKinesisRequestRateLimiter {
    private static final int IDLE_EXPIRATION_HOURS = 1;
    private final ConcurrentHashMap<RequestRateLimiterKey, SharedLimiter> _rateLimiters = new ConcurrentHashMap<>();
    // Preserve permit debt across short-lived consumers without retaining registrations after close.
    private final Cache<RequestRateLimiterKey, RateLimiter> _idleRateLimiters =
        CacheBuilder.newBuilder().expireAfterWrite(Duration.ofHours(IDLE_EXPIRATION_HOURS)).build();

    RequestRateLimiter forConsumer(KinesisConfig config) {
      return new ConsumerRequestRateLimiter(config);
    }

    @VisibleForTesting
    double getRateForTesting(KinesisConfig config, String shardId, RequestType requestType) {
      SharedLimiter limiter = _rateLimiters.get(new RequestRateLimiterKey(config, shardId, requestType));
      return limiter == null ? 0.0 : limiter._rateLimiter.getRate();
    }

    @VisibleForTesting
    int getLimiterCountForTesting() {
      return _rateLimiters.size();
    }

    /// Each handle belongs to one consumer. Synchronization also makes close safe for direct users of the handle.
    private class ConsumerRequestRateLimiter implements RequestRateLimiter {
      private final KinesisConfig _config;
      private final Map<RequestRateLimiterKey, SharedLimiter> _registrations = new HashMap<>();
      private boolean _closed;

      ConsumerRequestRateLimiter(KinesisConfig config) {
        _config = config;
      }

      @Override
      public synchronized boolean tryAcquire(String shardId, RequestType requestType, long timeoutMs) {
        if (_closed) {
          throw new IllegalStateException("Kinesis request limiter is closed");
        }
        if (timeoutMs <= 0) {
          return false;
        }
        RequestRateLimiterKey key = new RequestRateLimiterKey(_config, shardId, requestType);
        SharedLimiter limiter = _registrations.computeIfAbsent(key, ignored ->
            _rateLimiters.compute(key, (unused, current) -> {
              SharedLimiter shared = current;
              if (shared == null) {
                RateLimiter idle = _idleRateLimiters.asMap().remove(key);
                shared = new SharedLimiter(idle == null ? RateLimiter.create(_config.getRpsLimitPerSecond()) : idle);
              }
              shared._consumers.put(this, _config.getRpsLimitPerSecond());
              shared.updateRate();
              return shared;
            }));
        return limiter._rateLimiter.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS);
      }

      @Override
      public synchronized void close() {
        if (_closed) {
          return;
        }
        _closed = true;
        for (RequestRateLimiterKey key : _registrations.keySet()) {
          _rateLimiters.computeIfPresent(key, (unused, shared) -> {
            shared._consumers.remove(this);
            if (shared._consumers.isEmpty()) {
              _idleRateLimiters.put(key, shared._rateLimiter);
              return null;
            }
            shared.updateRate();
            return shared;
          });
        }
        _registrations.clear();
      }
    }

    /// Access the registrations only inside the registry's per-key compute operations.
    private static class SharedLimiter {
      private final RateLimiter _rateLimiter;
      private final Map<RequestRateLimiter, Double> _consumers = new HashMap<>();

      SharedLimiter(RateLimiter rateLimiter) {
        _rateLimiter = rateLimiter;
      }

      void updateRate() {
        double rate = _consumers.values().stream().mapToDouble(Double::doubleValue).min().orElseThrow();
        if (Double.compare(rate, _rateLimiter.getRate()) != 0) {
          _rateLimiter.setRate(rate);
        }
      }
    }
  }

  /// Configuration namespace for a stream. Never retains secret keys or temporary session credentials.
  /// Different credential configurations are deliberately isolated without an extra AWS identity lookup.
  private static class RequestRateLimiterKey {
    private final String _streamName;
    private final String _region;
    private final String _endpoint;
    private final String _credentialScope;
    private final String _shardId;
    private final RequestType _requestType;

    RequestRateLimiterKey(KinesisConfig config, String shardId, RequestType requestType) {
      _streamName = config.getStreamTopicName();
      _region = config.getAwsRegion();
      _endpoint = StringUtils.isBlank(config.getEndpoint()) ? "" : config.getEndpoint();
      if (config.isIamRoleBasedAccess()) {
        _credentialScope = "role:" + config.getRoleArn();
      } else if (StringUtils.isNotBlank(config.getAccessKey()) && StringUtils.isNotBlank(config.getSecretKey())) {
        _credentialScope = "access-key:" + config.getAccessKey();
      } else {
        _credentialScope = "default";
      }
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
      return _streamName.equals(that._streamName) && _region.equals(that._region)
          && _endpoint.equals(that._endpoint) && _credentialScope.equals(that._credentialScope)
          && _shardId.equals(that._shardId) && _requestType == that._requestType;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_streamName, _region, _endpoint, _credentialScope, _shardId, _requestType);
    }
  }
}
