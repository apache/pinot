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

import com.google.common.base.Supplier;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamConsumerFactoryProvider;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class IngestionDelayTrackerTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME);
  private static final int TIMER_THREAD_TICK_INTERVAL_MS = 100;

  private ServerMetrics _serverMetrics;
  private RealtimeTableDataManager _realtimeTableDataManager;

  // Mocks for StreamConsumerFactory and StreamMetadataProvider
  private StreamConsumerFactory _mockStreamConsumerFactory;
  private StreamMetadataProvider _mockStreamMetadataProvider;

  // MockedStatic for StreamConsumerFactoryProvider
  private MockedStatic<StreamConsumerFactoryProvider> _mockedFactoryProvider;

  @BeforeMethod
  public void setUp() throws Exception {
    _serverMetrics = mock(ServerMetrics.class);
    _realtimeTableDataManager = mock(RealtimeTableDataManager.class);

    // Initialize mocks for StreamConsumerFactory and StreamMetadataProvider
    _mockStreamConsumerFactory = mock(StreamConsumerFactory.class);
    _mockStreamMetadataProvider = mock(StreamMetadataProvider.class);

    // Mock the static method StreamConsumerFactoryProvider.create()
    _mockedFactoryProvider = Mockito.mockStatic(StreamConsumerFactoryProvider.class);
    _mockedFactoryProvider.when(() -> StreamConsumerFactoryProvider.create(any()))
        .thenReturn(_mockStreamConsumerFactory);

    // When createPartitionMetadataProvider is called, return the mock StreamMetadataProvider
    when(_mockStreamConsumerFactory.createPartitionMetadataProvider(anyString(), anyInt()))
        .thenReturn(_mockStreamMetadataProvider);

    // Default behavior for fetchStreamPartitionOffset
    when(_mockStreamMetadataProvider.fetchStreamPartitionOffset(any(), anyLong()))
        .thenReturn(new LongMsgOffset(100L));
  }

  @AfterMethod
  public void tearDown() {
    // Close the mocked static after each test
    _mockedFactoryProvider.close();
  }

  /**
   * Helper method to create a TableConfig with necessary stream configurations.
   */
  private TableConfig createTableConfig() {
    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", "kafka");
    streamConfigMap.put("stream.kafka.topic.name", RAW_TABLE_NAME);
    streamConfigMap.put("stream.kafka.decoder.class.name",
        "org.apache.pinot.plugin.stream.kafka.KafkaJSONMessageDecoder");

    StreamIngestionConfig streamIngestionConfig = new StreamIngestionConfig(Collections.singletonList(streamConfigMap));
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setStreamIngestionConfig(streamIngestionConfig);
    return new TableConfigBuilder(TableType.REALTIME)
        .setTableName(RAW_TABLE_NAME)
        .setIngestionConfig(ingestionConfig)
        .build();
  }

  /**
   * Updated createTracker method to pass TableConfig and handle mocks.
   */
  private IngestionDelayTracker createTracker() {
    TableConfig tableConfig = createTableConfig();

    // Supplier to indicate the server is ready to serve queries
    Supplier<Boolean> isServerReadyToServeQueries = () -> true;

    // Create the IngestionDelayTracker with the new constructor
    IngestionDelayTracker ingestionDelayTracker =
        new IngestionDelayTracker(_serverMetrics, REALTIME_TABLE_NAME, _realtimeTableDataManager,
            TIMER_THREAD_TICK_INTERVAL_MS, isServerReadyToServeQueries, tableConfig);

    return ingestionDelayTracker;
  }

  @Test
  public void testTrackerConstructors() throws Exception {
    // Test regular constructor with TableConfig
    IngestionDelayTracker ingestionDelayTracker = createTracker();

    Clock clock = Clock.systemUTC();
    ingestionDelayTracker.setClock(clock);

    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(0), 0);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(0), Long.MIN_VALUE);
    ingestionDelayTracker.shutdown();

    // Test constructor with timer arguments and TableConfig
    ingestionDelayTracker = new IngestionDelayTracker(_serverMetrics, REALTIME_TABLE_NAME, _realtimeTableDataManager,
        TIMER_THREAD_TICK_INTERVAL_MS, () -> true, createTableConfig());
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(0), 0);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(0), Long.MIN_VALUE);

    // Test bad timer args to the constructor
    try {
      new IngestionDelayTracker(_serverMetrics, REALTIME_TABLE_NAME, _realtimeTableDataManager, 0, () -> true,
          createTableConfig());
      Assert.fail("Must have asserted due to invalid arguments"); // Constructor must assert
    } catch (RuntimeException e) {
      // Expected exception, test passes
    } catch (Exception e) {
      Assert.fail(String.format("Unexpected exception: %s:%s", e.getClass(), e.getMessage()));
    }
  }

  @Test
  public void testRecordIngestionDelayWithNoAging() throws Exception {
    final long maxTestDelay = 100;
    final int partition0 = 0;
    final String segment0 = new LLCSegmentName(RAW_TABLE_NAME, partition0, 0, 123).getSegmentName();
    final int partition1 = 1;
    final String segment1 = new LLCSegmentName(RAW_TABLE_NAME, partition1, 0, 234).getSegmentName();

    IngestionDelayTracker ingestionDelayTracker = createTracker();
    // Use fixed clock so samples don't age
    Instant now = Instant.now();
    ZoneId zoneId = ZoneId.systemDefault();
    Clock fixedClock = Clock.fixed(now, zoneId);
    ingestionDelayTracker.setClock(fixedClock);

    // Mock fetchStreamPartitionOffset to return a fixed latest offset for testing
    when(_mockStreamMetadataProvider.fetchStreamPartitionOffset(any(), anyLong()))
        .thenReturn(new LongMsgOffset(200L)); // Example latest offset

    // Test we follow a single partition up and down
    for (long ingestionTimeMs = 0; ingestionTimeMs <= maxTestDelay; ingestionTimeMs++) {
      long firstStreamIngestionTimeMs = ingestionTimeMs + 1;
      ingestionDelayTracker.updateIngestionMetrics(segment0, partition0, ingestionTimeMs, firstStreamIngestionTimeMs,
          new LongMsgOffset(ingestionTimeMs)); // currentOffset as ingestionTimeMs for testing
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partition0),
          fixedClock.millis() - ingestionTimeMs);
      Assert.assertEquals(ingestionDelayTracker.getPartitionEndToEndIngestionDelayMs(partition0),
          fixedClock.millis() - firstStreamIngestionTimeMs);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(partition0), ingestionTimeMs);
    }

    // Test tracking another partition
    for (long ingestionTimeMs = 0; ingestionTimeMs <= maxTestDelay; ingestionTimeMs++) {
      long firstStreamIngestionTimeMs = ingestionTimeMs + 1;
      ingestionDelayTracker.updateIngestionMetrics(segment1, partition1, ingestionTimeMs, firstStreamIngestionTimeMs,
          new LongMsgOffset(ingestionTimeMs));
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partition1),
          fixedClock.millis() - ingestionTimeMs);
      Assert.assertEquals(ingestionDelayTracker.getPartitionEndToEndIngestionDelayMs(partition1),
          fixedClock.millis() - firstStreamIngestionTimeMs);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(partition1), ingestionTimeMs);
    }

    ingestionDelayTracker.shutdown();
    Assert.assertTrue(true);
  }

  @Test
  public void testRecordIngestionDelayWithAging() throws Exception {
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

    // Use fixed clock so samples don't age
    Instant now = Instant.now();
    ZoneId zoneId = ZoneId.systemDefault();
    Clock fixedClock = Clock.fixed(now, zoneId);
    ingestionDelayTracker.setClock(fixedClock);

    // Mock fetchStreamPartitionOffset to return latest offsets
    when(_mockStreamMetadataProvider.fetchStreamPartitionOffset(any(), anyLong()))
        .thenReturn(new LongMsgOffset(500L)); // Example latest offset

    long ingestionTimeMs = fixedClock.millis() - partition0Delay0;
    ingestionDelayTracker.updateIngestionMetrics(segment0, partition0, ingestionTimeMs, ingestionTimeMs,
        new LongMsgOffset(ingestionTimeMs));
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partition0), partition0Delay0);
    Assert.assertEquals(ingestionDelayTracker.getPartitionEndToEndIngestionDelayMs(partition0), partition0Delay0);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(partition0), ingestionTimeMs);

    // Advance clock and test aging
    Clock offsetClock = Clock.offset(fixedClock, Duration.ofMillis(partition0Offset0Ms));
    ingestionDelayTracker.setClock(offsetClock);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partition0),
        partition0Delay0 + partition0Offset0Ms);
    Assert.assertEquals(ingestionDelayTracker.getPartitionEndToEndIngestionDelayMs(partition0),
        partition0Delay0 + partition0Offset0Ms);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(partition0), ingestionTimeMs);

    ingestionTimeMs = offsetClock.millis() - partition0Delay1;
    ingestionDelayTracker.updateIngestionMetrics(segment0, partition0, ingestionTimeMs, ingestionTimeMs,
        new LongMsgOffset(ingestionTimeMs));
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partition0), partition0Delay1);
    Assert.assertEquals(ingestionDelayTracker.getPartitionEndToEndIngestionDelayMs(partition0), partition0Delay1);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(partition0), ingestionTimeMs);

    // Add some offset to the last sample and make sure we age that measure properly
    offsetClock = Clock.offset(offsetClock, Duration.ofMillis(partition0Offset1Ms));
    ingestionDelayTracker.setClock(offsetClock);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partition0),
        partition0Delay1 + partition0Offset1Ms);

    ingestionTimeMs = offsetClock.millis() - partition1Delay0;
    ingestionDelayTracker.updateIngestionMetrics(segment1, partition1, ingestionTimeMs, ingestionTimeMs,
        new LongMsgOffset(ingestionTimeMs));
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partition1), partition1Delay0);
    Assert.assertEquals(ingestionDelayTracker.getPartitionEndToEndIngestionDelayMs(partition1), partition1Delay0);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(partition1), ingestionTimeMs);

    // Add some offset to the last sample and make sure we age that measure properly
    offsetClock = Clock.offset(offsetClock, Duration.ofMillis(partition1Offset0Ms));
    ingestionDelayTracker.setClock(offsetClock);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partition1),
        partition1Delay0 + partition1Offset0Ms);

    ingestionDelayTracker.shutdown();
  }

  @Test
  public void testStopTrackingIngestionDelay() throws Exception {
    final long maxTestDelay = 100;
    final int maxPartition = 100;

    IngestionDelayTracker ingestionDelayTracker = createTracker();
    // Use fixed clock so samples don't age
    Instant now = Instant.now();
    ZoneId zoneId = ZoneId.systemDefault();
    Clock fixedClock = Clock.fixed(now, zoneId);
    ingestionDelayTracker.setClock(fixedClock);

    // Mock fetchStreamPartitionOffset to return latest offsets
    when(_mockStreamMetadataProvider.fetchStreamPartitionOffset(any(), anyLong()))
        .thenReturn(new LongMsgOffset(500L));

    // Record a number of partitions with delay equal to partition id
    for (int partitionId = 0; partitionId <= maxTestDelay; partitionId++) {
      String segmentName = new LLCSegmentName(RAW_TABLE_NAME, partitionId, 0, 123).getSegmentName();
      long ingestionTimeMs = fixedClock.millis() - partitionId;
      ingestionDelayTracker.updateIngestionMetrics(segmentName, partitionId, ingestionTimeMs, ingestionTimeMs,
          new LongMsgOffset(ingestionTimeMs));
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partitionId), partitionId);
      Assert.assertEquals(ingestionDelayTracker.getPartitionEndToEndIngestionDelayMs(partitionId), partitionId);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(partitionId), ingestionTimeMs);
    }
    for (int partitionId = maxPartition; partitionId >= 0; partitionId--) {
      ingestionDelayTracker.stopTrackingPartitionIngestionDelay(partitionId);
    }
    for (int partitionId = 0; partitionId <= maxTestDelay; partitionId++) {
      // Untracked partitions must return 0
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partitionId), 0);
      Assert.assertEquals(ingestionDelayTracker.getPartitionEndToEndIngestionDelayMs(partitionId), 0);
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(partitionId), Long.MIN_VALUE);
    }
  }

  @Test
  public void testStopTrackingIngestionDelayWithSegment() throws Exception {
    IngestionDelayTracker ingestionDelayTracker = createTracker();
    // Use fixed clock so samples don't age
    Instant now = Instant.now();
    ZoneId zoneId = ZoneId.systemDefault();
    Clock fixedClock = Clock.fixed(now, zoneId);
    ingestionDelayTracker.setClock(fixedClock);

    // Mock fetchStreamPartitionOffset to return latest offsets
    when(_mockStreamMetadataProvider.fetchStreamPartitionOffset(any(), anyLong()))
        .thenReturn(new LongMsgOffset(500L));

    String segmentName = new LLCSegmentName(RAW_TABLE_NAME, 0, 0, 123).getSegmentName();
    long ingestionTimeMs = fixedClock.millis() - 10;
    ingestionDelayTracker.updateIngestionMetrics(segmentName, 0, ingestionTimeMs, ingestionTimeMs,
        new LongMsgOffset(ingestionTimeMs));
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(0), 10);
    Assert.assertEquals(ingestionDelayTracker.getPartitionEndToEndIngestionDelayMs(0), 10);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(0), ingestionTimeMs);

    ingestionDelayTracker.stopTrackingPartitionIngestionDelay(segmentName);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(0), 0);
    Assert.assertEquals(ingestionDelayTracker.getPartitionEndToEndIngestionDelayMs(0), 0);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(0), Long.MIN_VALUE);

    // Should not update metrics for removed segment
    ingestionDelayTracker.updateIngestionMetrics(segmentName, 0, ingestionTimeMs, ingestionTimeMs,
        new LongMsgOffset(ingestionTimeMs));
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(0), 0);
    Assert.assertEquals(ingestionDelayTracker.getPartitionEndToEndIngestionDelayMs(0), 0);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionTimeMs(0), Long.MIN_VALUE);
  }

  @Test
  public void testShutdown() throws Exception {
    final long maxTestDelay = 100;

    IngestionDelayTracker ingestionDelayTracker = createTracker();
    // Use fixed clock so samples don't age
    Instant now = Instant.now();
    ZoneId zoneId = ZoneId.systemDefault();
    Clock fixedClock = Clock.fixed(now, zoneId);
    ingestionDelayTracker.setClock(fixedClock);

    // Mock fetchStreamPartitionOffset to return latest offsets
    when(_mockStreamMetadataProvider.fetchStreamPartitionOffset(any(), anyLong()))
        .thenReturn(new LongMsgOffset(500L));

    // Test Shutdown with partitions active
    for (int partitionId = 0; partitionId <= maxTestDelay; partitionId++) {
      String segmentName = new LLCSegmentName(RAW_TABLE_NAME, partitionId, 0, 123).getSegmentName();
      long ingestionTimeMs = fixedClock.millis() - partitionId;
      ingestionDelayTracker.updateIngestionMetrics(segmentName, partitionId, ingestionTimeMs, ingestionTimeMs,
          new LongMsgOffset(ingestionTimeMs));
      Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionDelayMs(partitionId), partitionId);
      Assert.assertEquals(ingestionDelayTracker.getPartitionEndToEndIngestionDelayMs(partitionId), partitionId);
    }
    ingestionDelayTracker.shutdown();

    // Test shutdown with no partitions
    ingestionDelayTracker = createTracker();
    ingestionDelayTracker.shutdown();
  }

  @Test
  public void testRecordIngestionDelayOffset() throws Exception {
    final int partition0 = 0;
    final String segment0 = new LLCSegmentName(RAW_TABLE_NAME, partition0, 0, 123).getSegmentName();
    final int partition1 = 1;
    final String segment1 = new LLCSegmentName(RAW_TABLE_NAME, partition1, 0, 234).getSegmentName();

    IngestionDelayTracker ingestionDelayTracker = createTracker();

    // Mock fetchStreamPartitionOffset to return latest offsets
    when(_mockStreamMetadataProvider.fetchStreamPartitionOffset(any(), anyLong()))
        .thenReturn(new LongMsgOffset(200L)); // Example latest offset

    // Test tracking offset lag for a single partition
    StreamPartitionMsgOffset msgOffset0 = new LongMsgOffset(100);
    ingestionDelayTracker.updateIngestionMetrics(segment0, partition0, Long.MIN_VALUE, Long.MIN_VALUE, msgOffset0);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionOffsetLag(partition0), 100); // 200 - 100

    // Test tracking offset lag for another partition
    StreamPartitionMsgOffset msgOffset1 = new LongMsgOffset(50);
    ingestionDelayTracker.updateIngestionMetrics(segment1, partition1, Long.MIN_VALUE, Long.MIN_VALUE, msgOffset1);
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionOffsetLag(partition1), 150); // 200 - 50

    // Update offset lag for partition0
    when(_mockStreamMetadataProvider.fetchStreamPartitionOffset(any(), anyLong())).thenReturn(
        new LongMsgOffset(150L)); // New latest offset
    ingestionDelayTracker.updateIngestionMetrics(segment0, partition0, Long.MIN_VALUE, Long.MIN_VALUE,
        new LongMsgOffset(150L));
    Assert.assertEquals(ingestionDelayTracker.getPartitionIngestionOffsetLag(partition0), 50); // 150 - 100

    ingestionDelayTracker.shutdown();
  }
}
