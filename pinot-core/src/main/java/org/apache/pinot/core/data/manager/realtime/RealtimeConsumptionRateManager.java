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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;
import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamConsumerFactoryProvider;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class is responsible for creating realtime consumption rate limiters. The rate limit, specified in
 * StreamConfig of table config, is for the entire topic. The effective rate limit for each partition is simply the
 * specified rate limit divided by the partition count.
 * This class leverages a cache for storing partition count for different topics as retrieving partition count from
 * stream is a bit expensive and also the same count will be used of all partition consumers of the same topic.
 */
public class RealtimeConsumptionRateManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeConsumptionRateManager.class);
  private static final int CACHE_ENTRY_EXPIRATION_TIME_IN_MINUTES = 10;

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

  public void enableThrottling() {
    _isThrottlingAllowed = true;
  }

  public ConsumptionRateLimiter createRateLimiter(StreamConfig streamConfig, String tableName,
      ServerMetrics serverMetrics, String metricKeyName) {
    if (!streamConfig.getTopicConsumptionRateLimit().isPresent()) {
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
    MetricEmitter metricEmitter = new MetricEmitter(serverMetrics, metricKeyName);
    return new RateLimiterImpl(partitionRateLimit, metricEmitter);
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

  @FunctionalInterface
  public interface ConsumptionRateLimiter {
    void throttle(int numMsgs);
  }

  @VisibleForTesting
  static final ConsumptionRateLimiter NOOP_RATE_LIMITER = n -> {
  };

  @VisibleForTesting
  static class RateLimiterImpl implements ConsumptionRateLimiter {
    private final double _rate;
    private final RateLimiter _rateLimiter;
    private MetricEmitter _metricEmitter;

    private RateLimiterImpl(double rate, MetricEmitter metricEmitter) {
      _rate = rate;
      _rateLimiter = RateLimiter.create(rate);
      _metricEmitter = metricEmitter;
    }

    @Override
    public void throttle(int numMsgs) {
      if (InstanceHolder.INSTANCE._isThrottlingAllowed) {
        // Only emit metrics when throttling is allowed. Throttling is not enabled.
        // until the server has passed startup checks. Otherwise, we will see
        // consumption well over 100% during startup.
        _metricEmitter.emitMetric(numMsgs, _rate, Clock.systemUTC().instant());
        if (numMsgs > 0) {
          _rateLimiter.acquire(numMsgs);
        }
      }
    }

    @VisibleForTesting
    double getRate() {
      return _rate;
    }
  }

  @VisibleForTesting
  @FunctionalInterface
  interface PartitionCountFetcher {
    Integer fetch(StreamConfig streamConfig);
  }

  @VisibleForTesting
  static final PartitionCountFetcher DEFAULT_PARTITION_COUNT_FETCHER = streamConfig -> {
    String clientId = streamConfig.getTopicName() + "-consumption.rate.manager";
    StreamConsumerFactory factory = StreamConsumerFactoryProvider.create(streamConfig);
    try (StreamMetadataProvider streamMetadataProvider = factory.createStreamMetadataProvider(clientId)) {
      return streamMetadataProvider.fetchPartitionCount(/*maxWaitTimeMs*/10_000);
    } catch (Exception e) {
      LOGGER.warn("Error fetching metadata for topic " + streamConfig.getTopicName(), e);
      return null;
    }
  };

  /**
   * This class is responsible to emit a gauge metric for the ratio of the actual consumption rate to the rate limit.
   * Number of messages consumed are aggregated over one minute. Each minute the ratio percentage is calculated and
   * emitted.
   */
  @VisibleForTesting
  static class MetricEmitter {

    private final ServerMetrics _serverMetrics;
    private final String _metricKeyName;

    // state variables
    private long _previousMinute = -1;
    private int _aggregateNumMessages = 0;

    public MetricEmitter(ServerMetrics serverMetrics, String metricKeyName) {
      _serverMetrics = serverMetrics;
      _metricKeyName = metricKeyName;
    }

    int emitMetric(int numMsgsConsumed, double rateLimit, Instant now) {
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
  }
}
