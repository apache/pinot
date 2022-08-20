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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
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

  public ConsumptionRateLimiter createRateLimiter(StreamConfig streamConfig, String tableName) {
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
    return new RateLimiterImpl(partitionRateLimit);
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

    private RateLimiterImpl(double rate) {
      _rate = rate;
      _rateLimiter = RateLimiter.create(rate);
    }

    @Override
    public void throttle(int numMsgs) {
      if (InstanceHolder.INSTANCE._isThrottlingAllowed && numMsgs > 0) {
        _rateLimiter.acquire(numMsgs);
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
}
