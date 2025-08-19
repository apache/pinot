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
package org.apache.pinot.core.data.manager.realtime;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.AtomicDouble;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;
import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamConsumerFactoryProvider;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class is responsible for creating realtime consumption rate limiters.
 * It contains one rate limiter for the entire server and multiple table partition level rate limiters.
 * Server rate limiter is used to throttle the overall consumption rate of the server and configured via
 * cluster or server config.
 * For table partition level rate limiter, the rate limit value specified in StreamConfig of table config, is for the
 * entire topic. The effective rate limit for each partition is simply the specified rate limit divided by the
 * partition count.
 * This class leverages a cache for storing partition count for different topics as retrieving partition count from
 * stream is a bit expensive and also the same count will be used of all partition consumers of the same topic.
 */
public class RealtimeConsumptionRateManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeConsumptionRateManager.class);
  private static final int CACHE_ENTRY_EXPIRATION_TIME_IN_MINUTES = 10;

  private static final String SERVER_CONSUMPTION_RATE_METRIC_KEY_NAME =
      ServerMeter.REALTIME_ROWS_CONSUMED.getMeterName();
  private volatile ConsumptionRateLimiter _serverRateLimiter = NOOP_RATE_LIMITER;

  // stream config object is required for fetching the partition count from the stream
  private final LoadingCache<StreamConfig, Integer> _streamConfigToTopicPartitionCountMap;
  private volatile boolean _isThrottlingAllowed = false;

  @VisibleForTesting
  RealtimeConsumptionRateManager(LoadingCache<StreamConfig, Integer> streamConfigToTopicPartitionCountMap) {
    _streamConfigToTopicPartitionCountMap = streamConfigToTopicPartitionCountMap;
  }

  private static class InstanceHolder {
    private static final RealtimeConsumptionRateManager INSTANCE = new RealtimeConsumptionRateManager(
        buildCache(DEFAULT_PARTITION_COUNT_FETCHER, CACHE_ENTRY_EXPIRATION_TIME_IN_MINUTES, TimeUnit.MINUTES));
  }

  public static RealtimeConsumptionRateManager getInstance() {
    return InstanceHolder.INSTANCE;
  }

  public void enablePartitionRateLimiter() {
    _isThrottlingAllowed = true;
  }

  public ConsumptionRateLimiter createServerRateLimiter(PinotConfiguration serverConfig, ServerMetrics serverMetrics) {
    double serverRateLimit =
        serverConfig.getProperty(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT,
            CommonConstants.Server.DEFAULT_SERVER_CONSUMPTION_RATE_LIMIT);
    createOrUpdateServerRateLimiter(serverRateLimit, serverMetrics);
    return _serverRateLimiter;
  }

  private void createOrUpdateServerRateLimiter(double serverRateLimit, ServerMetrics serverMetrics) {
    LOGGER.info("Setting up ServerRateLimiter with rate limit: {}", serverRateLimit);
    ConsumptionRateLimiter currentRateLimiter = _serverRateLimiter;
    if (serverRateLimit > 0) {
      if (currentRateLimiter instanceof ServerRateLimiter) {
        ((ServerRateLimiter) currentRateLimiter).updateRateLimit(serverRateLimit);
      } else {
        _serverRateLimiter =
            new ServerRateLimiter(serverRateLimit, serverMetrics, SERVER_CONSUMPTION_RATE_METRIC_KEY_NAME);
      }
    } else {
      if (currentRateLimiter instanceof ServerRateLimiter) {
        ((ServerRateLimiter) currentRateLimiter).close();
        _serverRateLimiter = NOOP_RATE_LIMITER;
        // Note: The consumer threads already present before refer to the serverRateLimiter object (not
        // NOOP_RATE_LIMITER). These threads will keep calling throttle() method and they might get blocked as a
        // result of it, But the metric related to throttling won't be emitted since as a result of here above the
        // AsyncMetricEmitter will be closed. It's recommended to forceCommit segments to avoid this.
      }
    }
  }

  public void updateServerRateLimiter(double newRateLimit, ServerMetrics serverMetrics) {
    LOGGER.info("Updating serverRateLimiter from: {} to: {}", _serverRateLimiter, newRateLimit);
    createOrUpdateServerRateLimiter(newRateLimit, serverMetrics);
  }

  public ConsumptionRateLimiter getServerRateLimiter() {
    return _serverRateLimiter;
  }

  public ConsumptionRateLimiter createRateLimiter(StreamConfig streamConfig, String tableName,
      ServerMetrics serverMetrics, String metricKeyName) {
    if (streamConfig.getTopicConsumptionRateLimit().isEmpty()) {
      return NOOP_RATE_LIMITER;
    }
    int partitionCount;
    try {
      partitionCount = _streamConfigToTopicPartitionCountMap.get(streamConfig);
    } catch (ExecutionException e) {
      // Exception here means for some reason, partition count cannot be fetched from stream!
      throw new RuntimeException(e);
    }
    double topicRateLimit = streamConfig.getTopicConsumptionRateLimit().get();
    double partitionRateLimit = topicRateLimit / partitionCount;
    LOGGER.info("A consumption rate limiter is set up for topic {} in table {} with rate limit: {} "
            + "(topic rate limit: {}, partition count: {})", streamConfig.getTopicName(), tableName, partitionRateLimit,
        topicRateLimit, partitionCount);
    return new PartitionRateLimiter(partitionRateLimit, serverMetrics, metricKeyName);
  }

  @VisibleForTesting
  ConsumptionRateLimiter createRateLimiter(StreamConfig streamConfig, String tableName) {
    return createRateLimiter(streamConfig, tableName, null, null);
  }

  @VisibleForTesting
  static LoadingCache<StreamConfig, Integer> buildCache(PartitionCountFetcher partitionCountFetcher,
      long duration, TimeUnit unit) {
    return CacheBuilder.newBuilder().refreshAfterWrite(duration, unit)
        .build(new CacheLoader<StreamConfig, Integer>() {
          @Override
          public Integer load(StreamConfig key)
              throws Exception {
            // this method is called the first time cache is used for the given streamConfig
            Integer count = partitionCountFetcher.fetch(key);
            // if the count cannot be fetched, don't throw exception; return 1.
            // The overall consumption rate will be higher, but we prefer that over not consuming at all.
            return count != null ? count : 1;
          }

          @Override
          public ListenableFuture<Integer> reload(StreamConfig key, Integer oldValue)
              throws Exception {
            // if partition count fetcher cannot fetch the value, old value is returned
            Integer count = partitionCountFetcher.fetch(key);
            return Futures.immediateFuture(count != null ? count : oldValue);
          }
        });
  }

  /**
   * Tracks quota utilization for a stream consumer by aggregating the number of messages consumed and computing
   * the consumption rate against the configured rate limit.
   * <p>
   * This class maintains the message count over time and computes the quota utilization ratio once per minute.
   * The utilization is reported as a gauge metric to {@link ServerMetrics}.
   * <p>
   * Note: This class is not thread-safe and is intended to be used in contexts where concurrency control is
   * managed externally (e.g., per-partition rate limiter instances).
   * <p>
   * Example:
   *   - If 3000 messages are consumed in one minute and the rate limit is 50 msg/sec,
   *     the quota utilization = (3000 / 60) / 50 = 1.0 → 100%
   */
  static class QuotaUtilizationTracker {
    private long _previousMinute = -1;
    private int _aggregateNumMessages = 0;
    private final ServerMetrics _serverMetrics;
    private final String _metricKeyName;

    public QuotaUtilizationTracker(ServerMetrics serverMetrics, String metricKeyName) {
      _serverMetrics = serverMetrics;
      _metricKeyName = metricKeyName;
    }

    /**
     * Update count and return utilization ratio percentage (0 if not enough data yet).
     */
    public int update(int numMsgsConsumed, double rateLimit, Instant now) {
      int ratioPercentage = 0;
      long nowInMinutes = now.getEpochSecond() / 60;
      if (nowInMinutes == _previousMinute) {
        _aggregateNumMessages += numMsgsConsumed;
      } else {
        if (_previousMinute != -1) { // not first time
          double actualRate = _aggregateNumMessages / ((nowInMinutes - _previousMinute) * 60.0); // messages per second
          ratioPercentage = (int) Math.round(actualRate / rateLimit * 100);
          _serverMetrics.setValueOfTableGauge(_metricKeyName, ServerGauge.CONSUMPTION_QUOTA_UTILIZATION,
              ratioPercentage);
        }
        _aggregateNumMessages = numMsgsConsumed;
        _previousMinute = nowInMinutes;
      }
      return ratioPercentage;
    }

    @VisibleForTesting
    int getAggregateNumMessages() {
      return _aggregateNumMessages;
    }
  }

  @FunctionalInterface
  public interface ConsumptionRateLimiter {
    void throttle(int numMsgs);
  }

  @VisibleForTesting
  static final ConsumptionRateLimiter NOOP_RATE_LIMITER = n -> {
  };

  /**
   * {@code PartitionRateLimiter} is an implementation of {@link ConsumptionRateLimiter} that uses Guava's
   * {@link com.google.common.util.concurrent.RateLimiter} to throttle the rate of consumption at per partition
   * level based on a configurable rate limit (in permits per second).
   * <p>
   * <p>This class is NOT thread-safe
   */
  @VisibleForTesting
  static class PartitionRateLimiter implements ConsumptionRateLimiter {
    private final double _rate;
    private final RateLimiter _rateLimiter;
    private final QuotaUtilizationTracker _quotaUtilizationTracker;

    private PartitionRateLimiter(double rate, ServerMetrics serverMetrics, String metricKeyName) {
      _rate = rate;
      _rateLimiter = RateLimiter.create(rate);
      _quotaUtilizationTracker = new QuotaUtilizationTracker(serverMetrics, metricKeyName);
    }

    @Override
    public void throttle(int numMsgs) {
      if (InstanceHolder.INSTANCE._isThrottlingAllowed) {
        // Only emit metrics when throttling is allowed. Throttling is not enabled.
        // until the server has passed startup checks. Otherwise, we will see
        // consumption well over 100% during startup.
        _quotaUtilizationTracker.update(numMsgs, _rate, Clock.systemUTC().instant());
        if (numMsgs > 0) {
          _rateLimiter.acquire(numMsgs);
        }
      }
    }

    @VisibleForTesting
    double getRate() {
      return _rate;
    }

    @Override
    public String toString() {
      return "PartitionRateLimiter{"
          + "_rate=" + _rate
          + ", _rateLimiter=" + _rateLimiter
          + ", _quotaUtilizationTracker=" + _quotaUtilizationTracker
          + '}';
    }
  }

  /**
   * {@code ServerRateLimiter} is an implementation of {@link ConsumptionRateLimiter} that uses Guava's
   * {@link com.google.common.util.concurrent.RateLimiter} to throttle the rate of consumption at entire server
   * level based on a configurable rate limit (in permits per second).
   * <p>
   * It supports dynamically updating the rate limit and emits metrics asynchronously to track quota utilization
   * via {@link AsyncMetricEmitter}.
   *
   * <p>This class is thread-safe
   */
  @VisibleForTesting
  static class ServerRateLimiter implements ConsumptionRateLimiter {
    private final RateLimiter _rateLimiter;
    private final AsyncMetricEmitter _metricEmitter;

    public ServerRateLimiter(double initialRateLimit, ServerMetrics serverMetrics, String metricKeyName) {
      _rateLimiter = RateLimiter.create(initialRateLimit);
      _metricEmitter = new AsyncMetricEmitter(serverMetrics, metricKeyName, initialRateLimit);
      _metricEmitter.start(); // start background emission
    }

    public void throttle(int numMsgsConsumed) {
      _metricEmitter.record(numMsgsConsumed); // just incrementing counter (non-blocking)
      if (numMsgsConsumed > 0) {
        _rateLimiter.acquire(numMsgsConsumed); // blocks if needed
      }
    }

    public void updateRateLimit(double newRateLimit) {
      _rateLimiter.setRate(newRateLimit);
      _metricEmitter.updateRateLimit(newRateLimit);
    }

    public void close() {
      _metricEmitter.close();
    }

    @VisibleForTesting
    double getRate() {
      return _rateLimiter.getRate();
    }

    @VisibleForTesting
    AsyncMetricEmitter getMetricEmitter() {
      return _metricEmitter;
    }

    @Override
    public String toString() {
      return "ServerRateLimiter{"
          + "_rateLimiter=" + _rateLimiter
          + ", _metricEmitter=" + _metricEmitter
          + '}';
    }
  }

  @VisibleForTesting
  @FunctionalInterface
  interface PartitionCountFetcher {
    Integer fetch(StreamConfig streamConfig);
  }

  @VisibleForTesting
  static final PartitionCountFetcher DEFAULT_PARTITION_COUNT_FETCHER = streamConfig -> {
    String clientId =
        StreamConsumerFactory.getUniqueClientId(streamConfig.getTopicName() + "-consumption.rate.manager");
    StreamConsumerFactory factory = StreamConsumerFactoryProvider.create(streamConfig);
    try (StreamMetadataProvider streamMetadataProvider = factory.createStreamMetadataProvider(clientId)) {
      return streamMetadataProvider.fetchPartitionCount(/*maxWaitTimeMs*/10_000);
    } catch (Exception e) {
      LOGGER.warn("Error fetching metadata for topic {}", streamConfig.getTopicName(), e);
      return null;
    }
  };

  /**
   * Asynchronously emits the quota utilization metric for a shared rate limiter (e.g., server-wide).
   * <p>
   * This class aggregates consumed message counts over a fixed time interval (default: 60 seconds) using a
   * high-performance {@link java.util.concurrent.atomic.LongAdder}. A scheduled background task computes the
   * actual message rate and reports the quota utilization ratio as a gauge metric.
   * <p>
   * This design avoids contention from multiple threads calling emit logic and ensures non-blocking accumulation
   * of message counts. Thread-safe and suitable for shared use.
   * <p>
   * Usage:
   *   - Call {@link #record(int)} to record messages consumed.
   *   - Call {@link #start()} once to schedule metric emission.
   *   - Optionally call {@link #close()} to stop the emitter.
   * <p>
   * Thread-safe.
   */
  static class AsyncMetricEmitter {
    private static final int METRIC_EMIT_FREQUENCY_SEC = 60;
    private final AtomicDouble _rateLimit;
    private final LongAdder _messageCount = new LongAdder();
    private final ScheduledExecutorService _executor;
    private final AtomicBoolean _running = new AtomicBoolean(false);
    private final QuotaUtilizationTracker _tracker;

    public AsyncMetricEmitter(ServerMetrics serverMetrics, String metricKeyName, double initialRateLimit) {
      _rateLimit = new AtomicDouble(initialRateLimit);
      _tracker = new QuotaUtilizationTracker(serverMetrics, metricKeyName);
      _executor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "server-rate-limit-metric-emitter");
        t.setDaemon(true);
        return t;
      });
    }

    public void start() {
      if (_running.compareAndSet(false, true)) {
        _executor.scheduleAtFixedRate(this::emit, 0, METRIC_EMIT_FREQUENCY_SEC, TimeUnit.SECONDS);
      }
    }

    @VisibleForTesting
    void start(int initialDelayInSeconds, int emitFrequencyInSeconds) {
      if (_running.compareAndSet(false, true)) {
        _executor.scheduleAtFixedRate(this::emit, initialDelayInSeconds, emitFrequencyInSeconds, TimeUnit.SECONDS);
      }
    }

    public void updateRateLimit(double newRateLimit) {
      _rateLimit.set(newRateLimit);
    }

    public void record(int numMsgsConsumed) {
      _messageCount.add(numMsgsConsumed);
    }

    private void emit() {
      try {
        double rateLimit = _rateLimit.get();
        Instant now = Instant.now();
        int count = (int) _messageCount.sumThenReset();
        _tracker.update(count, rateLimit, now);
      } catch (Exception e) {
        LOGGER.warn("Encountered an error while emitting the metrics.", e);
      }
    }

    @VisibleForTesting
    LongAdder getMessageCount() {
      return _messageCount;
    }

    @VisibleForTesting
    QuotaUtilizationTracker getTracker() {
      return _tracker;
    }

    @VisibleForTesting
    double getRate() {
      return _rateLimit.get();
    }

    public void close() {
      if (_running.compareAndSet(true, false)) {
        _executor.shutdownNow();
      }
    }
  }
}
