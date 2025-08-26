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
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
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
  private static final PinotConfiguration SERVER_CONFIG_1 = mock(PinotConfiguration.class);
  private static final PinotConfiguration SERVER_CONFIG_2 = mock(PinotConfiguration.class);
  private static final PinotConfiguration SERVER_CONFIG_3 = mock(PinotConfiguration.class);
  private static final PinotConfiguration SERVER_CONFIG_4 = mock(PinotConfiguration.class);
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

    when(SERVER_CONFIG_1.getProperty(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT,
        CommonConstants.Server.DEFAULT_SERVER_CONSUMPTION_RATE_LIMIT)).thenReturn(5.0);
    when(SERVER_CONFIG_2.getProperty(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT,
        CommonConstants.Server.DEFAULT_SERVER_CONSUMPTION_RATE_LIMIT)).thenReturn(2.5);
    when(SERVER_CONFIG_3.getProperty(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT,
        CommonConstants.Server.DEFAULT_SERVER_CONSUMPTION_RATE_LIMIT)).thenReturn(0.0);
    when(SERVER_CONFIG_4.getProperty(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT,
        CommonConstants.Server.DEFAULT_SERVER_CONSUMPTION_RATE_LIMIT)).thenReturn(-1.0);
  }

  @Test
  public void testCreateRateLimiter() {
    // topic A
    ConsumptionRateLimiter rateLimiter = _consumptionRateManager.createRateLimiter(STREAM_CONFIG_A, TABLE_NAME);
    assertEquals(5.0, ((PartitionRateLimiter) rateLimiter).getRate(), DELTA);

    // topic B
    rateLimiter = _consumptionRateManager.createRateLimiter(STREAM_CONFIG_B, TABLE_NAME);
    assertEquals(2.5, ((PartitionRateLimiter) rateLimiter).getRate(), DELTA);

    // topic C
    rateLimiter = _consumptionRateManager.createRateLimiter(STREAM_CONFIG_C, TABLE_NAME);
    assertEquals(rateLimiter, NOOP_RATE_LIMITER);
  }

  @Test
  public void testCreateServerRateLimiter() {
    // Server config 1
    ConsumptionRateLimiter rateLimiter = _consumptionRateManager.createServerRateLimiter(SERVER_CONFIG_1, null);
    ServerRateLimiter serverRateLimiter = (ServerRateLimiter) rateLimiter;
    try {
      assertEquals(serverRateLimiter.getRate(), 5.0, DELTA);
      assertEquals(serverRateLimiter.getMetricEmitter().getRate(), 5.0, DELTA);
    } finally {
      serverRateLimiter.close();
    }

    // Server config 2
    serverRateLimiter = (ServerRateLimiter) _consumptionRateManager.createServerRateLimiter(SERVER_CONFIG_2, null);
    try {
      assertEquals(((ServerRateLimiter) rateLimiter).getRate(), 2.5, DELTA);
    } finally {
      serverRateLimiter.close();
    }

    // Server config 3
    rateLimiter = _consumptionRateManager.createServerRateLimiter(SERVER_CONFIG_3, null);
    assertEquals(rateLimiter, NOOP_RATE_LIMITER);

    // Server config 4
    rateLimiter = _consumptionRateManager.createServerRateLimiter(SERVER_CONFIG_4, null);
    assertEquals(rateLimiter, NOOP_RATE_LIMITER);

    _consumptionRateManager.updateServerRateLimiter(1, null);
    serverRateLimiter = (ServerRateLimiter) _consumptionRateManager.getServerRateLimiter();
    try {
      assertEquals(serverRateLimiter.getRate(), 1);
      assertEquals(serverRateLimiter.getMetricEmitter().getRate(), 1);

      serverRateLimiter.updateRateLimit(12_000);
      assertEquals(serverRateLimiter.getRate(), 12_000);
      assertEquals(serverRateLimiter.getMetricEmitter().getRate(), 12_000);
    } finally {
      serverRateLimiter.close();
    }
  }

  @Test
  public void testBuildCache()
      throws Exception {
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

  @Test
  public void testMetricEmitter() {

    // setup metric emitter
    double rateLimit = 2; // unit: msgs/sec
    double rateLimitInMinutes = rateLimit * 60;
    ServerMetrics serverMetrics = mock(ServerMetrics.class);
    QuotaUtilizationTracker quotaUtilizationTracker =
        new QuotaUtilizationTracker(serverMetrics, "tableA-topicB-partition5");

    // 1st minute: no metrics should be emitted in the first minute
    int[] numMsgs = {10, 20, 5, 25};
    Instant now = Clock.fixed(Instant.parse("2022-08-10T12:00:02Z"), ZoneOffset.UTC).instant();
    assertEquals(quotaUtilizationTracker.update(numMsgs[0], rateLimit, now), 0);
    now = Clock.fixed(Instant.parse("2022-08-10T12:00:10Z"), ZoneOffset.UTC).instant();
    assertEquals(quotaUtilizationTracker.update(numMsgs[1], rateLimit, now), 0);
    now = Clock.fixed(Instant.parse("2022-08-10T12:00:30Z"), ZoneOffset.UTC).instant();
    assertEquals(quotaUtilizationTracker.update(numMsgs[2], rateLimit, now), 0);
    now = Clock.fixed(Instant.parse("2022-08-10T12:00:55Z"), ZoneOffset.UTC).instant();
    assertEquals(quotaUtilizationTracker.update(numMsgs[3], rateLimit, now), 0);

    // 2nd minute: metric should be emitted
    now = Clock.fixed(Instant.parse("2022-08-10T12:01:05Z"), ZoneOffset.UTC).instant();
    int sumOfMsgsInPrevMinute = sum(numMsgs);
    int expectedRatio = calcExpectedRatio(rateLimitInMinutes, sumOfMsgsInPrevMinute);
    numMsgs = new int[]{35};
    assertEquals(quotaUtilizationTracker.update(numMsgs[0], rateLimit, now), expectedRatio);

    // 3rd minute
    now = Clock.fixed(Instant.parse("2022-08-10T12:02:25Z"), ZoneOffset.UTC).instant();
    sumOfMsgsInPrevMinute = sum(numMsgs);
    expectedRatio = calcExpectedRatio(rateLimitInMinutes, sumOfMsgsInPrevMinute);
    numMsgs = new int[]{0};
    assertEquals(quotaUtilizationTracker.update(numMsgs[0], rateLimit, now), expectedRatio);

    // 4th minute
    now = Clock.fixed(Instant.parse("2022-08-10T12:03:15Z"), ZoneOffset.UTC).instant();
    sumOfMsgsInPrevMinute = sum(numMsgs);
    expectedRatio = calcExpectedRatio(rateLimitInMinutes, sumOfMsgsInPrevMinute);
    numMsgs = new int[]{10, 20};
    assertEquals(quotaUtilizationTracker.update(numMsgs[0], rateLimit, now), expectedRatio);
    now = Clock.fixed(Instant.parse("2022-08-10T12:03:20Z"), ZoneOffset.UTC).instant();
    assertEquals(quotaUtilizationTracker.update(numMsgs[1], rateLimit, now), expectedRatio);

    // 5th minute
    now = Clock.fixed(Instant.parse("2022-08-10T12:04:30Z"), ZoneOffset.UTC).instant();
    sumOfMsgsInPrevMinute = sum(numMsgs);
    expectedRatio = calcExpectedRatio(rateLimitInMinutes, sumOfMsgsInPrevMinute);
    numMsgs = new int[]{5};
    assertEquals(quotaUtilizationTracker.update(numMsgs[0], rateLimit, now), expectedRatio);
  }

  @Test
  public void testAsyncMetricEmitter()
      throws InterruptedException {
    AsyncMetricEmitter emitter = new AsyncMetricEmitter(mock(ServerMetrics.class), "testMetric", 10.0);
    try {
      emitter.start(0, 1);
      Thread.sleep(1500); // Let emitter run at-least once
      for (int i = 0; i < 20; i++) {
        CompletableFuture.runAsync(() -> emitter.record(1));
      }
      TestUtils.waitForCondition(
          aVoid -> (emitter.getMessageCount().intValue() == 0) && (emitter.getTracker().getAggregateNumMessages() > 0),
          5000,
          "Expected messageCount to be zero because messageCount is always reset before emitter calls "
              + "quotaUtilisationTracker");
    } finally {
      emitter.close();
    }

    AsyncMetricEmitter emitter1 = new AsyncMetricEmitter(mock(ServerMetrics.class), "testMetric", 10.0);
    try {
      emitter1.start(10, 10);
      for (int i = 0; i < 20; i++) {
        CompletableFuture.runAsync(() -> emitter1.record(1));
      }
      TestUtils.waitForCondition(
          aVoid -> ((emitter1.getMessageCount().intValue() > 0) && (emitter1.getTracker().getAggregateNumMessages()
              == 0)), 5000,
          "Expected messageCount to be greater than zero because messageCount will reset post initial delay (first "
              + "run).");
    } finally {
      emitter.close();
    }
  }

  private int calcExpectedRatio(double rateLimitInMinutes, int sumOfMsgsInPrevMinute) {
    return (int) Math.round(sumOfMsgsInPrevMinute / rateLimitInMinutes * 100);
  }

  private int sum(int[] numMsgs) {
    return Arrays.stream(numMsgs).sum();
  }
}
