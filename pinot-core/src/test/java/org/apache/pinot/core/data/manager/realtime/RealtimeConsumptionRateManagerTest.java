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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.Test;

import static org.apache.pinot.core.data.manager.realtime.RealtimeConsumptionRateManager.*;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


public class RealtimeConsumptionRateManagerTest {
  private static final int NUM_PARTITIONS_TOPIC_A = 10;
  private static final int NUM_PARTITIONS_TOPIC_B = 20;
  private static final double RATE_LIMIT_FOR_PARTITION = 4.0;
  private static final double RATE_LIMIT_FOR_ENTIRE_TOPIC = 50.0;
  private static final String TABLE_NAME = "table-XYZ";
  private static final double DELTA = 0.0001;
  private static final StreamConfig STREAM_CONFIG_A = mock(StreamConfig.class);
  private static final StreamConfig STREAM_CONFIG_B = mock(StreamConfig.class);
  private static final StreamConfig STREAM_CONFIG_C = mock(StreamConfig.class);
  private static final StreamConfig STREAM_CONFIG_D = mock(StreamConfig.class);
  private static final StreamConfig STREAM_CONFIG_E = mock(StreamConfig.class);
  private static final PinotConfiguration SERVER_CONFIG_1 = mock(PinotConfiguration.class);
  private static final PinotConfiguration SERVER_CONFIG_2 = mock(PinotConfiguration.class);
  private static final PinotConfiguration SERVER_CONFIG_3 = mock(PinotConfiguration.class);
  private static final PinotConfiguration SERVER_CONFIG_4 = mock(PinotConfiguration.class);
  private static final RealtimeConsumptionRateManager CONSUMPTION_RATE_MANAGER;

  static {
    LoadingCache<StreamConfig, Integer> cache = mock(LoadingCache.class);
    try {
      when(cache.get(STREAM_CONFIG_A)).thenReturn(NUM_PARTITIONS_TOPIC_A);
      when(cache.get(STREAM_CONFIG_B)).thenReturn(NUM_PARTITIONS_TOPIC_B);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
    when(STREAM_CONFIG_A.getTopicConsumptionRateLimit()).thenReturn(RATE_LIMIT_FOR_ENTIRE_TOPIC);
    when(STREAM_CONFIG_B.getTopicConsumptionRateLimit()).thenReturn(RATE_LIMIT_FOR_ENTIRE_TOPIC);
    when(STREAM_CONFIG_C.getTopicConsumptionRateLimit()).thenReturn(StreamConfig.CONSUMPTION_RATE_LIMIT_NOT_SPECIFIED);
    when(STREAM_CONFIG_D.getPartitionConsumptionRateLimit()).thenReturn(RATE_LIMIT_FOR_PARTITION);
    when(STREAM_CONFIG_E.getPartitionConsumptionRateLimit()).thenReturn(RATE_LIMIT_FOR_PARTITION);
    when(STREAM_CONFIG_E.getTopicConsumptionRateLimit()).thenReturn(RATE_LIMIT_FOR_ENTIRE_TOPIC);
    CONSUMPTION_RATE_MANAGER = new RealtimeConsumptionRateManager(cache);

    when(SERVER_CONFIG_1.getProperty(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT,
        CommonConstants.Server.DEFAULT_SERVER_CONSUMPTION_RATE_LIMIT)).thenReturn(5.0);
    if (Math.random() < 0.5) {
      when(SERVER_CONFIG_1.getProperty(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT_BYTES,
          CommonConstants.Server.DEFAULT_SERVER_CONSUMPTION_RATE_LIMIT)).thenReturn(5.0);
    }
    if (Math.random() < 0.5) {
      when(SERVER_CONFIG_2.getProperty(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT,
          CommonConstants.Server.DEFAULT_SERVER_CONSUMPTION_RATE_LIMIT)).thenReturn(2.5);
    }
    when(SERVER_CONFIG_2.getProperty(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT_BYTES,
        CommonConstants.Server.DEFAULT_SERVER_CONSUMPTION_RATE_LIMIT)).thenReturn(2.5);
    when(SERVER_CONFIG_3.getProperty(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT,
        CommonConstants.Server.DEFAULT_SERVER_CONSUMPTION_RATE_LIMIT)).thenReturn(0.0);
    when(SERVER_CONFIG_4.getProperty(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT,
        CommonConstants.Server.DEFAULT_SERVER_CONSUMPTION_RATE_LIMIT)).thenReturn(-1.0);
  }

  @Test
  public void testCreateRateLimiter() {
    // topic A
    ConsumptionRateLimiter rateLimiter = CONSUMPTION_RATE_MANAGER.createRateLimiter(STREAM_CONFIG_A, TABLE_NAME);
    assertEquals(((PartitionRateLimiter) rateLimiter).getRate(), 5.0, DELTA);

    // topic B
    rateLimiter = CONSUMPTION_RATE_MANAGER.createRateLimiter(STREAM_CONFIG_B, TABLE_NAME);
    assertEquals(((PartitionRateLimiter) rateLimiter).getRate(), 2.5, DELTA);

    // topic C
    rateLimiter = CONSUMPTION_RATE_MANAGER.createRateLimiter(STREAM_CONFIG_C, TABLE_NAME);
    assertEquals(rateLimiter, NOOP_RATE_LIMITER);

    // topic D: partition level rate limit is used directly, without fetching the partition count
    rateLimiter = CONSUMPTION_RATE_MANAGER.createRateLimiter(STREAM_CONFIG_D, TABLE_NAME);
    assertEquals(((PartitionRateLimiter) rateLimiter).getRate(), 4.0, DELTA);

    // topic E: partition level rate limit takes precedence over topic level rate limit
    rateLimiter = CONSUMPTION_RATE_MANAGER.createRateLimiter(STREAM_CONFIG_E, TABLE_NAME);
    assertEquals(((PartitionRateLimiter) rateLimiter).getRate(), 4.0, DELTA);
  }

  @Test
  public void testCreateServerRateLimiter() {
    // Server config 1
    ConsumptionRateLimiter rateLimiter = CONSUMPTION_RATE_MANAGER.createServerRateLimiter(SERVER_CONFIG_1, null);
    ServerRateLimiter serverRateLimiter = (ServerRateLimiter) rateLimiter;
    try {
      assertEquals(serverRateLimiter.getRate(), 5.0, DELTA);
      assertEquals(serverRateLimiter.getMetricEmitter().getRate(), 5.0, DELTA);
    } finally {
      serverRateLimiter.close();
    }

    // Server config 2
    serverRateLimiter = (ServerRateLimiter) CONSUMPTION_RATE_MANAGER.createServerRateLimiter(SERVER_CONFIG_2, null);
    try {
      assertEquals(((ServerRateLimiter) rateLimiter).getRate(), 2.5, DELTA);
      assertEquals(serverRateLimiter.getRate(), 2.5, DELTA);
    } finally {
      serverRateLimiter.close();
    }

    // Server config 3
    rateLimiter = CONSUMPTION_RATE_MANAGER.createServerRateLimiter(SERVER_CONFIG_3, null);
    assertEquals(rateLimiter, NOOP_RATE_LIMITER);

    // Server config 4
    rateLimiter = CONSUMPTION_RATE_MANAGER.createServerRateLimiter(SERVER_CONFIG_4, null);
    assertEquals(rateLimiter, NOOP_RATE_LIMITER);

    ServerRateLimitConfig serverRateLimitConfig = new ServerRateLimitConfig(1, MessageCountThrottlingStrategy.INSTANCE);
    CONSUMPTION_RATE_MANAGER.updateServerRateLimiter(serverRateLimitConfig, null);
    serverRateLimiter = (ServerRateLimiter) CONSUMPTION_RATE_MANAGER.getServerRateLimiter();
    try {
      assertEquals(serverRateLimiter.getRate(), 1);
      assertEquals(serverRateLimiter.getMetricEmitter().getRate(), 1);

      serverRateLimiter.updateRateLimit(12_000, ByteCountThrottlingStrategy.INSTANCE);
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
    AsyncMetricEmitter emitter = new AsyncMetricEmitter(mock(ServerMetrics.class), 10.0);
    try {
      emitter.start(0, 1);
      Thread.sleep(1500); // Let emitter run at-least once
      for (int i = 0; i < 20; i++) {
        CompletableFuture.runAsync(() -> emitter.record(1));
      }
      TestUtils.waitForCondition(
          aVoid -> (emitter.getMessageCount().intValue() == 0) && (emitter.getTracker().getAggregateUnits() > 0),
          5000,
          "Expected messageCount to be zero because messageCount is always reset before emitter calls "
              + "quotaUtilisationTracker");
    } finally {
      emitter.close();
    }

    AsyncMetricEmitter emitter1 = new AsyncMetricEmitter(mock(ServerMetrics.class), 10.0);
    try {
      emitter1.start(10, 10);
      for (int i = 0; i < 20; i++) {
        CompletableFuture.runAsync(() -> emitter1.record(1));
      }
      TestUtils.waitForCondition(
          aVoid -> ((emitter1.getMessageCount().intValue() > 0) && (emitter1.getTracker().getAggregateUnits()
              == 0)), 5000,
          "Expected messageCount to be greater than zero because messageCount will reset post initial delay (first "
              + "run).");
    } finally {
      emitter1.close();
    }
  }

  @Test
  public void testAsyncMetricEmitterByteOverflow()
      throws InterruptedException {
    // emit() previously cast the per-minute LongAdder sum to int; in byte mode the sum can exceed Integer.MAX_VALUE
    // and would overflow to a negative aggregate. This exercises the fixed emit() path end to end.
    AsyncMetricEmitter emitter = new AsyncMetricEmitter(mock(ServerMetrics.class), 100_000_000.0);
    try {
      emitter.record(2_000_000_000);
      emitter.record(2_000_000_000); // total 4e9, exceeds Integer.MAX_VALUE
      emitter.start(0, 3600); // fire emit() once, ~immediately
      TestUtils.waitForCondition(
          aVoid -> emitter.getMessageCount().sum() == 0 && emitter.getTracker().getAggregateUnits() == 4_000_000_000L,
          5000, "emit() must read the LongAdder sum as long without int truncation");
    } finally {
      emitter.close();
    }
  }

  @Test
  public void testQuotaUtilizationTrackerByteOverflow() {
    // In byte-throttling mode the per-minute aggregate counts bytes and can exceed Integer.MAX_VALUE. This is a
    // regression for a (int) overflow that previously turned high byte rates into a negative utilization.
    ServerMetrics serverMetrics = mock(ServerMetrics.class);
    QuotaUtilizationTracker tracker = new QuotaUtilizationTracker(serverMetrics); // server-level
    double byteRateLimit = 100_000_000; // 100 MB/s
    Instant now = Clock.fixed(Instant.parse("2022-08-10T12:00:02Z"), ZoneOffset.UTC).instant();
    assertEquals(tracker.update(2_000_000_000, byteRateLimit, now), 0); // first minute, no emit
    now = Clock.fixed(Instant.parse("2022-08-10T12:00:40Z"), ZoneOffset.UTC).instant();
    assertEquals(tracker.update(4_000_000_000L, byteRateLimit, now), 0); // same minute: aggregate = 6e9 (> 2^32)
    assertEquals(tracker.getAggregateUnits(), 6_000_000_000L); // long: not overflowed to a negative int
    // next minute: 6e9 bytes / 60s = 100 MB/s == limit -> 100% (pre-fix the overflow produced a negative ratio)
    now = Clock.fixed(Instant.parse("2022-08-10T12:01:02Z"), ZoneOffset.UTC).instant();
    assertEquals(tracker.update(0, byteRateLimit, now), 100);
  }

  @Test
  public void testQuotaUtilizationTrackerServerVsPartitionGauge() {
    Instant firstMinute = Clock.fixed(Instant.parse("2022-08-10T12:00:02Z"), ZoneOffset.UTC).instant();
    Instant secondMinute = Clock.fixed(Instant.parse("2022-08-10T12:01:02Z"), ZoneOffset.UTC).instant();

    // Server-level tracker emits the server-wide (global) gauge, never the per-table gauge.
    ServerMetrics serverMetrics = mock(ServerMetrics.class);
    QuotaUtilizationTracker serverTracker = new QuotaUtilizationTracker(serverMetrics);
    serverTracker.update(3000, 50, firstMinute);
    serverTracker.update(0, 50, secondMinute);
    verify(serverMetrics).setValueOfGlobalGauge(eq(ServerGauge.SERVER_CONSUMPTION_QUOTA_UTILIZATION), anyLong());
    verify(serverMetrics, never()).setValueOfTableGauge(anyString(),
        eq(ServerGauge.CONSUMPTION_QUOTA_UTILIZATION), anyLong());

    // Partition-level tracker emits the per-table/partition gauge, never the server-wide gauge.
    ServerMetrics partitionMetrics = mock(ServerMetrics.class);
    QuotaUtilizationTracker partitionTracker =
        new QuotaUtilizationTracker(partitionMetrics, "tableA_REALTIME-topicA-0");
    partitionTracker.update(3000, 50, firstMinute);
    partitionTracker.update(0, 50, secondMinute);
    verify(partitionMetrics).setValueOfTableGauge(eq("tableA_REALTIME-topicA-0"),
        eq(ServerGauge.CONSUMPTION_QUOTA_UTILIZATION), anyLong());
    verify(partitionMetrics, never()).setValueOfGlobalGauge(
        eq(ServerGauge.SERVER_CONSUMPTION_QUOTA_UTILIZATION), anyLong());
  }

  @Test
  public void testServerRateLimitGaugeEmitted() {
    RealtimeConsumptionRateManager manager = new RealtimeConsumptionRateManager(mock(LoadingCache.class));

    // Byte rate limit configured -> gauge exposes the configured cap.
    ServerMetrics metrics = mock(ServerMetrics.class);
    PinotConfiguration config = mock(PinotConfiguration.class);
    when(config.getProperty(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT,
        CommonConstants.Server.DEFAULT_SERVER_CONSUMPTION_RATE_LIMIT)).thenReturn(0.0);
    when(config.getProperty(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT_BYTES,
        CommonConstants.Server.DEFAULT_SERVER_CONSUMPTION_RATE_LIMIT)).thenReturn(112_000_000.0);
    ServerRateLimiter limiter = (ServerRateLimiter) manager.createServerRateLimiter(config, metrics);
    try {
      verify(metrics).setValueOfGlobalGauge(ServerGauge.SERVER_CONSUMPTION_RATE_LIMIT, 112_000_000L);
    } finally {
      limiter.close();
    }

    // Disabled -> gauge is set to -1 so operators can still see rate limiting is off.
    ServerMetrics disabledMetrics = mock(ServerMetrics.class);
    PinotConfiguration disabledConfig = mock(PinotConfiguration.class);
    when(disabledConfig.getProperty(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT,
        CommonConstants.Server.DEFAULT_SERVER_CONSUMPTION_RATE_LIMIT)).thenReturn(0.0);
    when(disabledConfig.getProperty(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT_BYTES,
        CommonConstants.Server.DEFAULT_SERVER_CONSUMPTION_RATE_LIMIT)).thenReturn(0.0);
    manager.createServerRateLimiter(disabledConfig, disabledMetrics);
    verify(disabledMetrics).setValueOfGlobalGauge(ServerGauge.SERVER_CONSUMPTION_RATE_LIMIT, -1L);
  }

  @Test
  public void testPartitionRateLimitGaugeEmitted() {
    // Topic-derived: STREAM_CONFIG_A topic rate limit 50 over 10 partitions -> per-partition rate limit 5.
    ServerMetrics metrics = mock(ServerMetrics.class);
    CONSUMPTION_RATE_MANAGER.createRateLimiter(STREAM_CONFIG_A, TABLE_NAME, metrics, "tableA_REALTIME-topicA-0");
    verify(metrics).setValueOfTableGauge("tableA_REALTIME-topicA-0", ServerGauge.CONSUMPTION_RATE_LIMIT, 5L);

    // Direct partition rate limit: STREAM_CONFIG_D specifies 4.0 per partition, used as-is.
    ServerMetrics directMetrics = mock(ServerMetrics.class);
    CONSUMPTION_RATE_MANAGER.createRateLimiter(STREAM_CONFIG_D, TABLE_NAME, directMetrics, "tableD_REALTIME-topicD-0");
    verify(directMetrics).setValueOfTableGauge("tableD_REALTIME-topicD-0", ServerGauge.CONSUMPTION_RATE_LIMIT, 4L);

    // No rate limit configured (STREAM_CONFIG_C) -> NOOP limiter, no gauge emitted, and any gauges left behind by a
    // previously configured (since removed) limit are cleaned up so they do not linger as stale series.
    ServerMetrics noneMetrics = mock(ServerMetrics.class);
    ConsumptionRateLimiter limiter =
        CONSUMPTION_RATE_MANAGER.createRateLimiter(STREAM_CONFIG_C, TABLE_NAME, noneMetrics, "keyC");
    assertEquals(limiter, NOOP_RATE_LIMITER);
    verify(noneMetrics, never()).setValueOfTableGauge(anyString(), eq(ServerGauge.CONSUMPTION_RATE_LIMIT), anyLong());
    verify(noneMetrics).removeTableGauge("keyC", ServerGauge.CONSUMPTION_RATE_LIMIT);
    verify(noneMetrics).removeTableGauge("keyC", ServerGauge.CONSUMPTION_QUOTA_UTILIZATION);
  }

  @Test
  public void testPartitionRateLimitGaugeLifecycle() {
    // set -> removed -> set again across consuming segments, against a real metrics registry to verify the gauge is
    // registered, removed and cleanly re-registered as each new consumer picks up the current config.
    ServerMetrics metrics = new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    String key = "tableL_REALTIME-topicL-3";
    String capGaugeName = ServerGauge.CONSUMPTION_RATE_LIMIT.getGaugeName() + "." + key;

    // Limit set (STREAM_CONFIG_D: direct partition limit 4): cap gauge registered with the configured value.
    CONSUMPTION_RATE_MANAGER.createRateLimiter(STREAM_CONFIG_D, TABLE_NAME, metrics, key);
    assertEquals(metrics.getGaugeValue(capGaugeName), Long.valueOf(4));

    // Limit removed (STREAM_CONFIG_C: no limit): the next consumer creation removes the series.
    CONSUMPTION_RATE_MANAGER.createRateLimiter(STREAM_CONFIG_C, TABLE_NAME, metrics, key);
    assertNull(metrics.getGaugeValue(capGaugeName));

    // Limit set again (STREAM_CONFIG_A: topic limit 50 over 10 partitions): gauge re-registers with the new value.
    CONSUMPTION_RATE_MANAGER.createRateLimiter(STREAM_CONFIG_A, TABLE_NAME, metrics, key);
    assertEquals(metrics.getGaugeValue(capGaugeName), Long.valueOf(5));
  }

  @Test
  public void testServerRateLimitGaugeEmittedOnUpdate() {
    RealtimeConsumptionRateManager manager = new RealtimeConsumptionRateManager(mock(LoadingCache.class));
    ServerMetrics metrics = mock(ServerMetrics.class);
    // Create an active server limiter, then update its cap -> gauge re-emitted with the new value.
    manager.updateServerRateLimiter(
        new ServerRateLimitConfig(100_000_000, ByteCountThrottlingStrategy.INSTANCE), metrics);
    try {
      manager.updateServerRateLimiter(
          new ServerRateLimitConfig(200_000_000, ByteCountThrottlingStrategy.INSTANCE), metrics);
      verify(metrics).setValueOfGlobalGauge(ServerGauge.SERVER_CONSUMPTION_RATE_LIMIT, 200_000_000L);
    } finally {
      ((ServerRateLimiter) manager.getServerRateLimiter()).close();
    }
  }

  private int calcExpectedRatio(double rateLimitInMinutes, int sumOfMsgsInPrevMinute) {
    return (int) Math.round(sumOfMsgsInPrevMinute / rateLimitInMinutes * 100);
  }

  private int sum(int[] numMsgs) {
    return Arrays.stream(numMsgs).sum();
  }
}
