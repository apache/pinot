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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamMetadataProvider;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;

public class IngestionDelayTrackerTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME);
  private static final int METRICS_CLEANUP_TICK_INTERVAL_MS = 100;
  private static final int METRICS_TRACKING_TICK_INTERVAL_MS = 100;

  private final ServerMetrics _serverMetrics = mock(ServerMetrics.class);
  private final RealtimeTableDataManager _realtimeTableDataManager = mock(RealtimeTableDataManager.class);

  private static Map<String, String> getStreamConfigs() {
    Map<String, String> streamConfigs = new HashMap<>();
    streamConfigs.put("streamType", "kafka");
    streamConfigs.put("stream.kafka.topic.name", "test");
    streamConfigs.put("stream.kafka.decoder.class.name",
        "org.apache.pinot.plugin.stream.kafka.KafkaJSONMessageDecoder");
    return streamConfigs;
  }

  private static class MockIngestionDelayTracker extends IngestionDelayTracker {

    private Map<Integer, Map<String, List<Long>>> _partitionToMetricToValues;
    private ScheduledExecutorService _scheduledExecutorService;

    public MockIngestionDelayTracker(ServerMetrics serverMetrics, String tableNameWithType,
        RealtimeTableDataManager realtimeTableDataManager, Supplier<Boolean> isServerReadyToServeQueries)
        throws RuntimeException {
      super(serverMetrics, tableNameWithType, realtimeTableDataManager, isServerReadyToServeQueries);
    }

    public MockIngestionDelayTracker(ServerMetrics serverMetrics, String tableNameWithType,
        RealtimeTableDataManager realtimeTableDataManager, int timerThreadTickIntervalMs, int metricTrackingIntervalMs,
        Supplier<Boolean> isServerReadyToServeQueries) {
      super(serverMetrics, tableNameWithType, realtimeTableDataManager, timerThreadTickIntervalMs,
          metricTrackingIntervalMs, isServerReadyToServeQueries);
    }

    @Override
    public void createStreamMetadataProvider(String tableNameWithType,
        RealtimeTableDataManager realtimeTableDataManager) {
      _streamMetadataProviderList =
          List.of(new FakeStreamMetadataProvider(new StreamConfig(tableNameWithType, getStreamConfigs())));
    }

    @Override
    public void createMetrics(int partitionId) {
      if (_partitionToMetricToValues == null) {
        _partitionToMetricToValues = new ConcurrentHashMap<>();
        _scheduledExecutorService = Executors.newScheduledThreadPool(2);
      }
      Map<String, List<Long>> metricToValues = new HashMap<>();
      _partitionToMetricToValues.put(partitionId, metricToValues);
      if (_streamMetadataProviderList.get(0).supportsOffsetLag()) {
        metricToValues.put(ServerGauge.REALTIME_INGESTION_OFFSET_LAG.getGaugeName(), new ArrayList<>());
        metricToValues.put(ServerGauge.REALTIME_INGESTION_CONSUMING_OFFSET.getGaugeName(), new ArrayList<>());
        metricToValues.put(ServerGauge.REALTIME_INGESTION_UPSTREAM_OFFSET.getGaugeName(), new ArrayList<>());
      }
      metricToValues.put(ServerGauge.REALTIME_INGESTION_DELAY_MS.getGaugeName(), new ArrayList<>());

      _scheduledExecutorService.scheduleWithFixedDelay(() -> _partitionToMetricToValues.compute(partitionId, (k, v) -> {
        Map<String, List<Long>> metricToValuesForPartition = _partitionToMetricToValues.get(partitionId);
        if (_streamMetadataProviderList.get(0).supportsOffsetLag()) {
          metricToValuesForPartition.get(ServerGauge.REALTIME_INGESTION_OFFSET_LAG.getGaugeName())
              .add(getPartitionIngestionOffsetLag(partitionId));
          metricToValuesForPartition.get(ServerGauge.REALTIME_INGESTION_CONSUMING_OFFSET.getGaugeName())
              .add(getPartitionIngestionConsumingOffset(partitionId));
          metricToValuesForPartition.get(ServerGauge.REALTIME_INGESTION_UPSTREAM_OFFSET.getGaugeName())
              .add(getLatestPartitionOffset(partitionId));
        }
        metricToValuesForPartition.get(ServerGauge.REALTIME_INGESTION_DELAY_MS.getGaugeName())
            .add(getPartitionIngestionDelayMs(partitionId));
        return metricToValuesForPartition;
      }), 0, 10, TimeUnit.MILLISECONDS);
    }

    void updatePartitionIdToLatestOffset(Map<Integer, StreamPartitionMsgOffset> partitionIdToLatestOffset) {
      _partitionIdToLatestOffset = partitionIdToLatestOffset;
    }

    @Override
    public void shutdown() {
      if (_scheduledExecutorService != null) {
        _scheduledExecutorService.shutdown();
      }
      super.shutdown();
    }
  }

  private IngestionDelayTracker createTracker() {
    return new MockIngestionDelayTracker(_serverMetrics, REALTIME_TABLE_NAME, _realtimeTableDataManager, () -> true);
  }

  @Test
  public void testTrackerConstructors() {
    // Test regular constructor
    IngestionDelayTracker ingestionDelayTracker =
        new MockIngestionDelayTracker(_serverMetrics, REALTIME_TABLE_NAME, _realtimeTableDataManager, () -> true);

    Clock clock = Clock.systemUTC();
    ingestionDelayTracker.setClock(clock);

    Assert.assertTrue(ingestionDelayTracker.getPartitionIngestionDelayMs(0) <= clock.millis());
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(0), Long.MIN_VALUE);
    ingestionDelayTracker.shutdown();
    // Test constructor with timer arguments
    ingestionDelayTracker =
        new MockIngestionDelayTracker(_serverMetrics, REALTIME_TABLE_NAME, _realtimeTableDataManager,
            METRICS_CLEANUP_TICK_INTERVAL_MS, METRICS_TRACKING_TICK_INTERVAL_MS, () -> true);
    Assert.assertTrue(ingestionDelayTracker.getPartitionIngestionDelayMs(0) <= clock.millis());
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(0), Long.MIN_VALUE);
    // Test bad timer args to the constructor
    try {
      new IngestionDelayTracker(_serverMetrics, REALTIME_TABLE_NAME, _realtimeTableDataManager, 0, 0, () -> true);
      Assert.fail("Must have asserted due to invalid arguments"); // Constructor must assert
    } catch (Exception e) {
      if ((e instanceof NullPointerException) || !(e instanceof RuntimeException)) {
        Assert.fail(String.format("Unexpected exception: %s:%s", e.getClass(), e.getMessage()));
      }
    }
  }

  @Test
  public void testRecordIngestionDelayWithNoAging() {
    final long maxTestDelay = 100;
    final int partition0 = 0;
    final String segment0 = new LLCSegmentName(RAW_TABLE_NAME, partition0, 0, 123).getSegmentName();
    final int partition1 = 1;
    final String segment1 = new LLCSegmentName(RAW_TABLE_NAME, partition1, 0, 234).getSegmentName();

    IngestionDelayTracker ingestionDelayTracker = createTracker();
    // Use fixed clock so samples don't age
    Instant now = Instant.now();
    ZoneId zoneId = ZoneId.systemDefault();
    Clock clock = Clock.fixed(now, zoneId);
    ingestionDelayTracker.setClock(clock);

    // Test we follow a single partition up and down
    for (long ingestionTimeMs = 0; ingestionTimeMs <= maxTestDelay; ingestionTimeMs++) {
      ingestionDelayTracker.updateMetrics(segment0, partition0, ingestionTimeMs, null);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partition0),
          clock.millis() - ingestionTimeMs);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(partition0), ingestionTimeMs);
    }

    // Test tracking down a measure for a given partition
    for (long ingestionTimeMs = maxTestDelay; ingestionTimeMs >= 0; ingestionTimeMs--) {
      ingestionDelayTracker.updateMetrics(segment0, partition0, ingestionTimeMs, null);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partition0),
          clock.millis() - ingestionTimeMs);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(partition0), ingestionTimeMs);
    }

    // Make the current partition maximum
    ingestionDelayTracker.updateMetrics(segment0, partition0, maxTestDelay, null);

    // Bring up partition1 delay up and verify values
    for (long ingestionTimeMs = 0; ingestionTimeMs <= 2 * maxTestDelay; ingestionTimeMs++) {
      long firstStreamIngestionTimeMs = ingestionTimeMs + 1;
      ingestionDelayTracker.updateMetrics(segment1, partition1, ingestionTimeMs, null);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partition1),
          clock.millis() - ingestionTimeMs);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(partition1), ingestionTimeMs);
    }

    // Bring down values of partition1 and verify values
    for (long ingestionTimeMs = 2 * maxTestDelay; ingestionTimeMs >= 0; ingestionTimeMs--) {
      ingestionDelayTracker.updateMetrics(segment1, partition1, ingestionTimeMs, null);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partition1),
          clock.millis() - ingestionTimeMs);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(partition1), ingestionTimeMs);
    }

    ingestionDelayTracker.shutdown();
    Assert.assertTrue(true);
  }

  @Test
  public void testRecordIngestionDelayWithAging() {
    final int partition0 = 0;
    final String segment0 = new LLCSegmentName(RAW_TABLE_NAME, partition0, 0, 123).getSegmentName();
    final long partition0Delay0 = 1000;
    final long partition0Delay1 = 10; // record lower delay to make sure max gets reduced
    final long partition0Offset0Ms = 300;
    final long partition0Offset1Ms = 1000;
    final int partition1 = 1;
    final String segment1 = new LLCSegmentName(RAW_TABLE_NAME, partition1, 0, 234).getSegmentName();
    final long partition1Delay0 = 11;
    final long partition1Offset0Ms = 150;

    IngestionDelayTracker ingestionDelayTracker = createTracker();

    // With samples for a single partition, test that sample is aged as expected
    Instant now = Instant.now();
    ZoneId zoneId = ZoneId.systemDefault();
    Clock clock = Clock.fixed(now, zoneId);
    ingestionDelayTracker.setClock(clock);
    long ingestionTimeMs = clock.millis() - partition0Delay0;
    ingestionDelayTracker.updateMetrics(segment0, partition0, ingestionTimeMs, null);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partition0), partition0Delay0);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(partition0), ingestionTimeMs);

    // Advance clock and test aging
    Clock offsetClock = Clock.offset(clock, Duration.ofMillis(partition0Offset0Ms));
    ingestionDelayTracker.setClock(offsetClock);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partition0),
        (partition0Delay0 + partition0Offset0Ms));
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(partition0), ingestionTimeMs);

    ingestionTimeMs = offsetClock.millis() - partition0Delay1;
    ingestionDelayTracker.updateMetrics(segment0, partition0, ingestionTimeMs, null);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partition0), partition0Delay1);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(partition0), ingestionTimeMs);

    // Add some offset to the last sample and make sure we age that measure properly
    offsetClock = Clock.offset(offsetClock, Duration.ofMillis(partition0Offset1Ms));
    ingestionDelayTracker.setClock(offsetClock);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partition0),
        (partition0Delay1 + partition0Offset1Ms));

    ingestionTimeMs = offsetClock.millis() - partition1Delay0;
    ingestionDelayTracker.updateMetrics(segment1, partition1, ingestionTimeMs, null);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partition1), partition1Delay0);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(partition1), ingestionTimeMs);

    // Add some offset to the last sample and make sure we age that measure properly
    offsetClock = Clock.offset(offsetClock, Duration.ofMillis(partition1Offset0Ms));
    ingestionDelayTracker.setClock(offsetClock);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partition1),
        (partition1Delay0 + partition1Offset0Ms));

    ingestionDelayTracker.shutdown();
  }

  @Test
  public void testStopTrackingIngestionDelay() {
    final long maxTestDelay = 100;
    final int maxPartition = 100;

    IngestionDelayTracker ingestionDelayTracker = createTracker();
    // Use fixed clock so samples don't age
    Instant now = Instant.now();
    ZoneId zoneId = ZoneId.systemDefault();
    Clock clock = Clock.fixed(now, zoneId);
    ingestionDelayTracker.setClock(clock);

    // Record a number of partitions with delay equal to partition id
    for (int partitionId = 0; partitionId <= maxTestDelay; partitionId++) {
      String segmentName = new LLCSegmentName(RAW_TABLE_NAME, partitionId, 0, 123).getSegmentName();
      long ingestionTimeMs = clock.millis() - partitionId;
      ingestionDelayTracker.updateMetrics(segmentName, partitionId, ingestionTimeMs, null);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partitionId), partitionId);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(partitionId), ingestionTimeMs);
    }
    for (int partitionId = maxPartition; partitionId >= 0; partitionId--) {
      ingestionDelayTracker.stopTrackingPartition(partitionId);
    }
    for (int partitionId = 0; partitionId <= maxTestDelay; partitionId++) {
      // Untracked partitions must return 0
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partitionId), clock.millis());
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(partitionId), Long.MIN_VALUE);
    }
  }

  @Test
  public void testStopTrackingIngestionDelayWithSegment() {
    IngestionDelayTracker ingestionDelayTracker = createTracker();
    // Use fixed clock so samples don't age
    Instant now = Instant.now();
    ZoneId zoneId = ZoneId.systemDefault();
    Clock clock = Clock.fixed(now, zoneId);
    ingestionDelayTracker.setClock(clock);

    String segmentName = new LLCSegmentName(RAW_TABLE_NAME, 0, 0, 123).getSegmentName();
    long ingestionTimeMs = clock.millis() - 10;
    ingestionDelayTracker.updateMetrics(segmentName, 0, ingestionTimeMs, null);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(0), 10);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(0), ingestionTimeMs);

    ingestionDelayTracker.stopTrackingPartition(segmentName);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(0), clock.millis());
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(0), Long.MIN_VALUE);

    // Should not update metrics for removed segment
    ingestionDelayTracker.updateMetrics(segmentName, 0, ingestionTimeMs, null);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(0), clock.millis());
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(0), Long.MIN_VALUE);
  }

  @Test
  public void testShutdown() {
    final long maxTestDelay = 100;

    IngestionDelayTracker ingestionDelayTracker = createTracker();
    // Use fixed clock so samples don't age
    Instant now = Instant.now();
    ZoneId zoneId = ZoneId.systemDefault();
    Clock clock = Clock.fixed(now, zoneId);
    ingestionDelayTracker.setClock(clock);

    // Test Shutdown with partitions active
    for (int partitionId = 0; partitionId <= maxTestDelay; partitionId++) {
      String segmentName = new LLCSegmentName(RAW_TABLE_NAME, partitionId, 0, 123).getSegmentName();
      long ingestionTimeMs = clock.millis() - partitionId;
      ingestionDelayTracker.updateMetrics(segmentName, partitionId, ingestionTimeMs, null);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partitionId), partitionId);
    }
    ingestionDelayTracker.shutdown();

    // Test shutdown with no partitions
    ingestionDelayTracker = createTracker();
    ingestionDelayTracker.shutdown();
  }

  @Test
  public void testRecordIngestionDelayOffset() {
    final int partition0 = 0;
    final String segment0 = new LLCSegmentName(RAW_TABLE_NAME, partition0, 0, 123).getSegmentName();
    final int partition1 = 1;
    final String segment1 = new LLCSegmentName(RAW_TABLE_NAME, partition1, 0, 234).getSegmentName();

    IngestionDelayTracker ingestionDelayTracker = createTracker();
    Map<Integer, StreamPartitionMsgOffset> partitionMsgOffsetMap = new HashMap<>();
    ((MockIngestionDelayTracker) ingestionDelayTracker).updatePartitionIdToLatestOffset(partitionMsgOffsetMap);

    // Test tracking offset lag for a single partition
    StreamPartitionMsgOffset msgOffset0 = new LongMsgOffset(50);
    StreamPartitionMsgOffset latestOffset0 = new LongMsgOffset(150);
    partitionMsgOffsetMap.put(partition0, latestOffset0);
    ingestionDelayTracker.updateMetrics(segment0, partition0, Long.MIN_VALUE, msgOffset0);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionOffsetLag(partition0), 100);
    Assert.assertEquals(ingestionDelayTracker.getLatestPartitionOffset(partition0), 150);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionConsumingOffset(partition0), 50);

    // Test tracking offset lag for another partition
    StreamPartitionMsgOffset msgOffset1 = new LongMsgOffset(50);
    StreamPartitionMsgOffset latestOffset1 = new LongMsgOffset(150);
    partitionMsgOffsetMap.put(partition1, latestOffset1);
    ingestionDelayTracker.updateMetrics(segment1, partition1, Long.MIN_VALUE, msgOffset1);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionOffsetLag(partition1), 100);
    Assert.assertEquals(ingestionDelayTracker.getLatestPartitionOffset(partition1), 150);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionConsumingOffset(partition1), 50);

    // Update offset lag for partition0
    msgOffset0 = new LongMsgOffset(150);
    latestOffset0 = new LongMsgOffset(200);
    partitionMsgOffsetMap.put(partition0, latestOffset0);
    ingestionDelayTracker.updateMetrics(segment0, partition0, Long.MIN_VALUE, msgOffset0);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionOffsetLag(partition0), 50);
    Assert.assertEquals(ingestionDelayTracker.getLatestPartitionOffset(partition0), 200);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionConsumingOffset(partition0), 150);

    ingestionDelayTracker.shutdown();
  }

  @Test
  public void testIngestionDelay() {
    MockIngestionDelayTracker ingestionDelayTracker =
        new MockIngestionDelayTracker(_serverMetrics, REALTIME_TABLE_NAME, _realtimeTableDataManager,
            METRICS_CLEANUP_TICK_INTERVAL_MS, METRICS_TRACKING_TICK_INTERVAL_MS, () -> true);

    final int partition0 = 0;
    final String segment0 = new LLCSegmentName(RAW_TABLE_NAME, partition0, 0, 123).getSegmentName();
    final int partition1 = 1;

    ingestionDelayTracker._partitionsHostedByThisServer.add(partition0);
    ingestionDelayTracker._partitionsHostedByThisServer.add(partition1);
    ingestionDelayTracker.updateMetrics(segment0, partition0, System.currentTimeMillis(), new LongMsgOffset(50));

    ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    Map<Integer, StreamPartitionMsgOffset> partitionIdVsLatestOffset = new HashMap<>();
    partitionIdVsLatestOffset.put(partition0, new LongMsgOffset(200));
    partitionIdVsLatestOffset.put(partition1, new LongMsgOffset(200));
    ingestionDelayTracker.updatePartitionIdToLatestOffset(partitionIdVsLatestOffset);

    scheduledExecutorService.scheduleWithFixedDelay(() -> {
      partitionIdVsLatestOffset.put(partition0,
          new LongMsgOffset(((LongMsgOffset) (partitionIdVsLatestOffset.get(partition0))).getOffset() + 50));
      partitionIdVsLatestOffset.put(partition1,
          new LongMsgOffset(((LongMsgOffset) (partitionIdVsLatestOffset.get(partition1))).getOffset() + 50));
    }, 0, 10, TimeUnit.MILLISECONDS);

    Map<Integer, Map<String, List<Long>>> partitionToMetricToValues = ingestionDelayTracker._partitionToMetricToValues;
    TestUtils.waitForCondition((aVoid) -> {
      try {
        verifyMetrics(partitionToMetricToValues);
      } catch (Error e) {
        return false;
      }
      return true;
    }, 10, 2000, "Failed to verify the ingestion delay metrics.");
    scheduledExecutorService.shutdown();
    ingestionDelayTracker.shutdown();
  }

  private void verifyMetrics(Map<Integer, Map<String, List<Long>>> partitionToMetricToValues) {
    Assert.assertEquals(partitionToMetricToValues.size(), 2);
    verifyPartition0(partitionToMetricToValues.get(0));
    verifyPartition1(partitionToMetricToValues.get(1));
  }

  private void verifyPartition0(Map<String, List<Long>> metrics) {
    assertMinMax(metrics, ServerGauge.REALTIME_INGESTION_OFFSET_LAG.getGaugeName(), 150L, 300L);
    assertMinMax(metrics, ServerGauge.REALTIME_INGESTION_UPSTREAM_OFFSET.getGaugeName(), 200L, 350L);
    assertEqualsFirstAndLast(metrics, ServerGauge.REALTIME_INGESTION_CONSUMING_OFFSET.getGaugeName(), 50L);
    assertIncreasing(metrics, ServerGauge.REALTIME_INGESTION_DELAY_MS.getGaugeName());
    Assert.assertTrue(metrics.get(ServerGauge.REALTIME_INGESTION_DELAY_MS.getGaugeName()).get(0) > 0);
  }

  private void verifyPartition1(Map<String, List<Long>> metrics) {
    assertMinMax(metrics, ServerGauge.REALTIME_INGESTION_OFFSET_LAG.getGaugeName(), 200L, 350L);
    assertMinMax(metrics, ServerGauge.REALTIME_INGESTION_UPSTREAM_OFFSET.getGaugeName(), 200L, 350L);
    assertEqualsFirstAndLast(metrics, ServerGauge.REALTIME_INGESTION_CONSUMING_OFFSET.getGaugeName(), 0L);
    assertIncreasing(metrics, ServerGauge.REALTIME_INGESTION_DELAY_MS.getGaugeName());
    Assert.assertTrue(metrics.get(ServerGauge.REALTIME_INGESTION_DELAY_MS.getGaugeName()).get(0) > 0);
  }

  private void assertMinMax(Map<String, List<Long>> metrics, String key, long minFirst, long minLast) {
    List<Long> values = metrics.get(key);
    Assert.assertTrue(values.get(0) >= minFirst, key + " first value too small");
    Assert.assertTrue(values.get(values.size() - 1) >= minLast, key + " last value too small");
  }

  private void assertEqualsFirstAndLast(Map<String, List<Long>> metrics, String key, long expected) {
    List<Long> values = metrics.get(key);
    Assert.assertEquals(values.get(0), expected, key + " first value mismatch");
    Assert.assertEquals(values.get(values.size() - 1), expected, key + " last value mismatch");
  }

  private void assertIncreasing(Map<String, List<Long>> metrics, String key) {
    List<Long> values = metrics.get(key);
    Assert.assertTrue(values.get(values.size() - 1) > values.get(0), key + " not increasing");
  }
}
