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

import com.google.common.cache.LoadingCache;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.stream.StreamConfig;
import org.testng.annotations.Test;

import static org.apache.pinot.core.data.manager.realtime.RealtimeConsumptionRateManager.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class RealtimeConsumptionRateManagerTest {
  private static final int NUM_PARTITIONS_TOPIC_A = 10;
  private static final int NUM_PARTITIONS_TOPIC_B = 20;
  private static final Double RATE_LIMIT_FOR_ENTIRE_TOPIC = 50.0;
  private static final String TABLE_NAME = "table-XYZ";
  private static final double DELTA = 0.0001;
  private static final StreamConfig STREAM_CONFIG_A = mock(StreamConfig.class);
  private static final StreamConfig STREAM_CONFIG_B = mock(StreamConfig.class);
  private static final StreamConfig STREAM_CONFIG_C = mock(StreamConfig.class);
  private static RealtimeConsumptionRateManager _consumptionRateManager;

  static {
    LoadingCache<StreamConfig, Integer> cache = mock(LoadingCache.class);
    try {
      when(cache.get(STREAM_CONFIG_A)).thenReturn(NUM_PARTITIONS_TOPIC_A);
      when(cache.get(STREAM_CONFIG_B)).thenReturn(NUM_PARTITIONS_TOPIC_B);
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
    when(STREAM_CONFIG_A.getTopicConsumptionRateLimit()).thenReturn(Optional.of(RATE_LIMIT_FOR_ENTIRE_TOPIC));
    when(STREAM_CONFIG_B.getTopicConsumptionRateLimit()).thenReturn(Optional.of(RATE_LIMIT_FOR_ENTIRE_TOPIC));
    when(STREAM_CONFIG_C.getTopicConsumptionRateLimit()).thenReturn(Optional.empty());
    _consumptionRateManager = new RealtimeConsumptionRateManager(cache);
  }

  @Test
  public void testCreateRateLimiter() {
    // topic A
    ConsumptionRateLimiter rateLimiter = _consumptionRateManager.createRateLimiter(STREAM_CONFIG_A, TABLE_NAME);
    assertEquals(5.0, ((RateLimiterImpl) rateLimiter).getRate(), DELTA);

    // topic B
    rateLimiter = _consumptionRateManager.createRateLimiter(STREAM_CONFIG_B, TABLE_NAME);
    assertEquals(2.5, ((RateLimiterImpl) rateLimiter).getRate(), DELTA);

    // topic C
    rateLimiter = _consumptionRateManager.createRateLimiter(STREAM_CONFIG_C, TABLE_NAME);
    assertEquals(rateLimiter, NOOP_RATE_LIMITER);
  }

  @Test
  public void testBuildCache() throws Exception {
    PartitionCountFetcher partitionCountFetcher = mock(PartitionCountFetcher.class);
    LoadingCache<StreamConfig, Integer> cache = buildCache(partitionCountFetcher, 500, TimeUnit.MILLISECONDS);
    when(partitionCountFetcher.fetch(STREAM_CONFIG_A)).thenReturn(10);
    when(partitionCountFetcher.fetch(STREAM_CONFIG_B)).thenReturn(20);
    assertEquals((int) cache.get(STREAM_CONFIG_A), 10); // call fetcher in load method
    assertEquals((int) cache.get(STREAM_CONFIG_A), 10); // use cache
    assertEquals((int) cache.get(STREAM_CONFIG_A), 10); // use cache
    assertEquals((int) cache.get(STREAM_CONFIG_B), 20); // call fetcher in load method
    assertEquals((int) cache.get(STREAM_CONFIG_B), 20); // use cache
    verify(partitionCountFetcher, times(1)).fetch(STREAM_CONFIG_A); // count changes
    verify(partitionCountFetcher, times(1)).fetch(STREAM_CONFIG_B); // count changes
    when(partitionCountFetcher.fetch(STREAM_CONFIG_A)).thenReturn(11);
    when(partitionCountFetcher.fetch(STREAM_CONFIG_B)).thenReturn(21);
    assertEquals((int) cache.get(STREAM_CONFIG_A), 10); // use cache
    assertEquals((int) cache.get(STREAM_CONFIG_B), 20); // use cache
    Thread.sleep(550); // wait till cache expires
    assertEquals((int) cache.get(STREAM_CONFIG_A), 11); // call fetcher in reload method
    assertEquals((int) cache.get(STREAM_CONFIG_A), 11); // use cache
    assertEquals((int) cache.get(STREAM_CONFIG_A), 11); // use cache
    assertEquals((int) cache.get(STREAM_CONFIG_B), 21); // call fetcher in reload method
    assertEquals((int) cache.get(STREAM_CONFIG_B), 21); // use cache
    verify(partitionCountFetcher, times(2)).fetch(STREAM_CONFIG_A);
    verify(partitionCountFetcher, times(2)).fetch(STREAM_CONFIG_B);
    when(partitionCountFetcher.fetch(STREAM_CONFIG_A)).thenReturn(null); // unsuccessful fetch
    when(partitionCountFetcher.fetch(STREAM_CONFIG_B)).thenReturn(22);
    Thread.sleep(550); // wait till cache expires
    assertEquals((int) cache.get(STREAM_CONFIG_A), 11); // call fetcher in reload method
    assertEquals((int) cache.get(STREAM_CONFIG_A), 11); // use cache
    assertEquals((int) cache.get(STREAM_CONFIG_A), 11); // use cache
    assertEquals((int) cache.get(STREAM_CONFIG_B), 22); // call fetcher in reload method
    assertEquals((int) cache.get(STREAM_CONFIG_B), 22); // use cache
    verify(partitionCountFetcher, times(3)).fetch(STREAM_CONFIG_A);
    verify(partitionCountFetcher, times(3)).fetch(STREAM_CONFIG_B);

    // unsuccessful fetch in the first call for config C
    when(partitionCountFetcher.fetch(STREAM_CONFIG_C)).thenReturn(null); // unsuccessful fetch
    assertEquals((int) cache.get(STREAM_CONFIG_C), 1); // call fetcher in load method
    assertEquals((int) cache.get(STREAM_CONFIG_C), 1); // use cache
    assertEquals((int) cache.get(STREAM_CONFIG_C), 1); // use cache
    verify(partitionCountFetcher, times(1)).fetch(STREAM_CONFIG_C);
  }
}
