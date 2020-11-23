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
 * This class is responsible for creating realtime consumption rate limiters. If rate limiter is used for
 * multi-partition topics, the provided rate in StreamConfig is divided by partition count. This class leverages a
 * cache for storing partition count for different topics as retrieving partition count from Kafka is a bit expensive
 * and also the same count will be used of all partition consumers of the same topic.
 */
public class RealtimeConsumptionRateManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeConsumptionRateManager.class);
  private static final RealtimeConsumptionRateManager INSTANCE = new RealtimeConsumptionRateManager();
  private static final int CACHE_ENTRY_EXPIRATION_TIME_IN_MINUTES = 10;

  private final LoadingCache<StreamConfig, Integer> _streamConfigToTopicPartitionCountMap = buildCache();
  private boolean _isThrottlingAllowed = false;

  private RealtimeConsumptionRateManager() {
  }

  public static RealtimeConsumptionRateManager getInstance() {
    return INSTANCE;
  }

  public void enableThrottling() {
    _isThrottlingAllowed = true;
  }

  public ConsumptionRateLimiter createRateLimiterForSinglePartitionTopic(StreamConfig streamConfig) {
    return createRateLimiter(streamConfig, false);
  }

  public ConsumptionRateLimiter createRateLimiterForMultiPartitionTopic(StreamConfig streamConfig) {
    return createRateLimiter(streamConfig, true);
  }

  private ConsumptionRateLimiter createRateLimiter(StreamConfig streamConfig, boolean multiPartitionTopic) {
    if (streamConfig.getTopicConsumptionRateLimit().isEmpty()) {
      return NOOP_RATE_LIMITER;
    }
    int partitionCount = 1;
    if (multiPartitionTopic) {
      try {
        partitionCount = _streamConfigToTopicPartitionCountMap.get(streamConfig);
      } catch (ExecutionException e) {
        // Exception here means that for some reason, partition count cannot be retrieved from Kafka broker!
        throw new RuntimeException(e);
      }
    }
    double topicRateLimit = streamConfig.getTopicConsumptionRateLimit().get();
    double partitionRateLimit = topicRateLimit / partitionCount;
    LOGGER.info("A rate limiter is setup for topic {} with rate limit: {} (topic rate limit: {}, partition count: {})",
        streamConfig.getTopicName(), partitionRateLimit, topicRateLimit, partitionCount);
    return new RateLimiterImpl(partitionRateLimit);
  }

  private LoadingCache<StreamConfig, Integer> buildCache() {
    return CacheBuilder.newBuilder().expireAfterWrite(CACHE_ENTRY_EXPIRATION_TIME_IN_MINUTES, TimeUnit.MINUTES)
        .build(new CacheLoader<StreamConfig, Integer>() {
          @Override
          public Integer load(StreamConfig streamConfig)
              throws Exception {
            String clientId = streamConfig.getTopicName() + "-consumption.rate.manager";
            StreamConsumerFactory factory = StreamConsumerFactoryProvider.create(streamConfig);
            try (StreamMetadataProvider streamMetadataProvider = factory.createStreamMetadataProvider(clientId)) {
              return streamMetadataProvider.fetchPartitionCount(/*maxWaitTimeMs*/10_000);
            }
          }
        });
  }

  @FunctionalInterface
  public interface ConsumptionRateLimiter {
    void throttle(int numMsgs);
  }

  private static final ConsumptionRateLimiter NOOP_RATE_LIMITER = n -> {
  };

  private static class RateLimiterImpl implements ConsumptionRateLimiter {

    private RateLimiter _rateLimiter;

    public RateLimiterImpl(double rateLimit) {
      _rateLimiter = RateLimiter.create(rateLimit);
    }

    @Override
    public void throttle(int numMsgs) {
      if (INSTANCE._isThrottlingAllowed && numMsgs > 0) {
        _rateLimiter.acquire(numMsgs);
      }
    }
  }
}
