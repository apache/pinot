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

import java.io.File;
import java.net.SocketTimeoutException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.data.manager.provider.DefaultTableDataManagerProvider;
import org.apache.pinot.core.data.manager.provider.TableDataManagerProvider;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamMessageDecoder;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamMetadataProvider;
import org.apache.pinot.segment.local.segment.creator.Fixtures;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.segment.local.utils.ServerReloadJobStatusCache;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamMessage;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Verifies how a consuming segment reacts to being stopped while its consumer is blocked in the stream fetch. This
/// is the shape of every deliberate teardown: Helix `CONSUMING -> OFFLINE` and `CONSUMING -> DROPPED` both land on
/// `offloadSegment`, which interrupts the consumer thread while it is parked inside the stream client, so the
/// interrupt surfaces as a fetch failure — and must not be mistaken for one.
///
/// The table data manager, the consumer thread, the stop/interrupt/join handshake, and the consume loop are all real.
/// Only the stream is supplied by the test, through the [StreamConsumerFactory] extension point named in the table
/// config, which is what makes the timing deterministic: the consumer is guaranteed to be inside `fetchMessages` when
/// the interrupt lands.
public class RealtimeConsumerStopTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "RealtimeConsumerStopTest");
  private static final String RAW_TABLE_NAME = "Coffee";
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME);
  private static final LLCSegmentName SEGMENT_NAME = new LLCSegmentName(RAW_TABLE_NAME, 0, 0, 1600000000000L);
  private static final String SEGMENT_NAME_STR = SEGMENT_NAME.getSegmentName();
  private static final LongMsgOffset START_OFFSET = new LongMsgOffset(0L);
  /// `MAX_CONSECUTIVE_ERROR_COUNT` retries, plus the attempt that takes the count past the limit.
  private static final int ATTEMPTS_BEFORE_GIVING_UP = 6;

  @BeforeClass
  public void setUp() {
    FileUtils.deleteQuietly(TEMP_DIR);
    SegmentBuildTimeLeaseExtender.initExecutor();
  }

  @AfterClass
  public void tearDown() {
    SegmentBuildTimeLeaseExtender.shutdownExecutor();
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  @AfterMethod
  public void resetStream() {
    TestStreamConsumerFactory.reset();
  }

  /// Offloading a consuming segment interrupts its consumer mid-fetch. Nothing is wrong with the stream, so the
  /// interrupt must not be metered as a consumption exception, and no replacement consumer may be built.
  @Test
  public void testOffloadingAConsumingSegmentIsNotCountedAsAStreamError()
      throws Exception {
    ServerMetrics serverMetrics = new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    RealtimeTableDataManager tableDataManager = createTableDataManager();
    try {
      startConsumingSegment(tableDataManager, serverMetrics);
      assertTrue(TestStreamConsumerFactory.awaitFetch(), "Consumer never reached the stream fetch");
      long consumptionExceptions = consumptionExceptions(serverMetrics);
      long stoppedByInterrupt = stoppedByInterrupt(serverMetrics);

      // The call the Helix CONSUMING -> OFFLINE and CONSUMING -> DROPPED transitions make on the server
      tableDataManager.offloadSegment(SEGMENT_NAME_STR);

      assertEquals(consumptionExceptions(serverMetrics), consumptionExceptions,
          "Stopping a consumer must not be counted as a stream error");
      assertEquals(stoppedByInterrupt(serverMetrics), stoppedByInterrupt + 1,
          "Stopping a consumer must be counted on the deliberate stop meter");
      assertEquals(TestStreamConsumerFactory.CONSUMERS_CREATED.get(), 1,
          "No replacement consumer should be built for a segment being offloaded");
      assertEquals(TestStreamConsumerFactory.CONSUMERS_CLOSED.get(), 1, "The stream consumer should be closed");
    } finally {
      tableDataManager.shutDown();
    }
  }

  /// `goOnlineFromConsuming` stops the consumer and then re-enters the consume loop on the caller's own thread to
  /// reach the committed end offset, clearing the stop flag so that loop is allowed to run. An interrupt arriving
  /// there — the state transition thread itself being interrupted, as happens when a server shuts down mid
  /// transition — is still part of the teardown, which is why the stop has to be remembered across that reset.
  @Test
  public void testInterruptWhileCatchingUpIsNotCountedAsAStreamError()
      throws Exception {
    ServerMetrics serverMetrics = new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    RealtimeTableDataManager tableDataManager = createTableDataManager();
    try {
      RealtimeSegmentDataManager segmentDataManager = startConsumingSegment(tableDataManager, serverMetrics);
      assertTrue(TestStreamConsumerFactory.awaitFetch(), "Consumer never reached the stream fetch");
      long consumptionExceptions = consumptionExceptions(serverMetrics);
      long stoppedByInterrupt = stoppedByInterrupt(serverMetrics);
      TestStreamConsumerFactory.fetchesBehaveAs(TestStreamConsumerFactory.FetchBehaviour.INTERRUPT);
      // Count only the fetches the catch-up loop makes, not the ones the consumer thread already made
      TestStreamConsumerFactory.FETCHES.set(0);

      SegmentZKMetadata committedMetadata = new SegmentZKMetadata(SEGMENT_NAME_STR);
      committedMetadata.setEndOffset(new LongMsgOffset(100L).toString());
      committedMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
      // Catch-up cannot reach the end offset, so the replica falls back to downloading the segment. There is no deep
      // store behind this harness, so that download is what fails; the point of the test is what happened before it.
      Assert.assertThrows(Exception.class, () -> segmentDataManager.goOnlineFromConsuming(committedMetadata));
      // Catch-up ran on this thread, so clear the interrupt flag the stream re-armed on it
      Thread.interrupted();

      assertEquals(consumptionExceptions(serverMetrics), consumptionExceptions,
          "An interrupt on the catch-up path must not be counted as a stream error");
      // Once for the consumer thread that stop() interrupted, once for the catch-up loop on this thread
      assertEquals(stoppedByInterrupt(serverMetrics), stoppedByInterrupt + 2,
          "Both the consumer thread and the catch-up loop should be counted on the deliberate stop meter");
      assertEquals(TestStreamConsumerFactory.FETCHES.get(), 1, "Catch-up should give up on the first interrupt");
      assertEquals(TestStreamConsumerFactory.CONSUMERS_CREATED.get(), 1,
          "No replacement consumer should be built while stopping");
    } finally {
      tableDataManager.shutDown();
    }
  }

  /// The counterpart of the cases above, and the reason the distinction has to be drawn on the thread's interrupt
  /// status rather than on the fact that a stop is in flight: a real fetch failure is still metered and still retried
  /// behind a fresh consumer, even though it surfaces from the same catch block.
  @Test
  public void testStreamFailureIsCountedAsAStreamError()
      throws Exception {
    ServerMetrics serverMetrics = new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    RealtimeTableDataManager tableDataManager = createTableDataManager();
    try {
      TestStreamConsumerFactory.fetchesBehaveAs(TestStreamConsumerFactory.FetchBehaviour.FAIL);
      long consumptionExceptions = consumptionExceptions(serverMetrics);
      long stoppedByInterrupt = stoppedByInterrupt(serverMetrics);
      long retriesExhausted = retriesExhausted(serverMetrics);
      startConsumingSegment(tableDataManager, serverMetrics);

      // The transient handler sleeps and rebuilds the consumer, so a second consumer proves the retry happened
      TestUtils.waitForCondition(aVoid -> TestStreamConsumerFactory.CONSUMERS_CREATED.get() > 1, 30_000L,
          "Consumer was not recreated after a stream failure");

      assertTrue(consumptionExceptions(serverMetrics) > consumptionExceptions,
          "A stream failure must be counted as a consumption exception");
      assertEquals(stoppedByInterrupt(serverMetrics), stoppedByInterrupt,
          "A stream failure must not be counted as a deliberate stop");
      assertEquals(retriesExhausted(serverMetrics), retriesExhausted,
          "A failure that still has retries left must not be counted as an exhausted retry budget");
    } finally {
      tableDataManager.shutDown();
    }
  }

  /// A stream that keeps failing eventually runs the retry budget out, which is the point at which the segment is
  /// abandoned. `realtimeConsumptionExceptions` moves on every attempt, including ones that would have self-healed,
  /// so the exhaustion is what alerts hang on and it has to be counted exactly once per abandoned segment.
  @Test
  public void testExhaustingTheRetryBudgetIsCountedOnce()
      throws Exception {
    ServerMetrics serverMetrics = new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    RealtimeTableDataManager tableDataManager = createTableDataManager();
    try {
      TestStreamConsumerFactory.fetchesBehaveAs(TestStreamConsumerFactory.FetchBehaviour.FAIL);
      long consumptionExceptions = consumptionExceptions(serverMetrics);
      long retriesExhausted = retriesExhausted(serverMetrics);
      startConsumingSegment(tableDataManager, serverMetrics);

      // Each attempt backs off for a second before the next, so the budget takes a few seconds to run out
      TestUtils.waitForCondition(aVoid -> retriesExhausted(serverMetrics) > retriesExhausted, 60_000L,
          "Retry budget was never reported as exhausted");

      assertEquals(retriesExhausted(serverMetrics), retriesExhausted + 1,
          "Exhausting the retry budget should be counted once, not once per attempt");
      assertEquals(consumptionExceptions(serverMetrics), consumptionExceptions + ATTEMPTS_BEFORE_GIVING_UP,
          "Every failed attempt should be counted as a consumption exception");
    } finally {
      tableDataManager.shutDown();
    }
  }

  private static long consumptionExceptions(ServerMetrics serverMetrics) {
    return serverMetrics.getMeteredValue(ServerMeter.REALTIME_CONSUMPTION_EXCEPTIONS).count();
  }

  private static long retriesExhausted(ServerMetrics serverMetrics) {
    return serverMetrics.getMeteredValue(ServerMeter.REALTIME_CONSUMPTION_RETRIES_EXHAUSTED).count();
  }

  private static long stoppedByInterrupt(ServerMetrics serverMetrics) {
    return serverMetrics.getMeteredValue(ServerMeter.REALTIME_CONSUMPTION_STOPPED_BY_INTERRUPT).count();
  }

  private static InstanceDataManagerConfig createInstanceDataManagerConfig() {
    InstanceDataManagerConfig config = mock(InstanceDataManagerConfig.class);
    when(config.getInstanceId()).thenReturn("server-1");
    when(config.getInstanceDataDir()).thenReturn(TEMP_DIR.getAbsolutePath());
    when(config.getConfig()).thenReturn(new PinotConfiguration());
    when(config.getUpsertConfig()).thenReturn(new PinotConfiguration());
    when(config.getDedupConfig()).thenReturn(new PinotConfiguration());
    return config;
  }

  private static TableConfig createTableConfig()
      throws Exception {
    TableConfig tableConfig = Fixtures.createTableConfig(TestStreamConsumerFactory.class.getName(),
        FakeStreamMessageDecoder.class.getName());
    // Upsert would make consumption wait for every segment to load through Helix, which is out of scope here
    tableConfig.setUpsertConfig(null);
    return tableConfig;
  }

  /// Builds a fully initialized [RealtimeTableDataManager] through the production provider. The only stand-ins are
  /// the instance config, the Helix manager and the reload-status cache, none of which take part in offloading.
  private static RealtimeTableDataManager createTableDataManager()
      throws Exception {
    TableDataManagerProvider provider = new DefaultTableDataManagerProvider();
    provider.init(createInstanceDataManagerConfig(), mock(HelixManager.class), new SegmentLocks(), null,
        mock(ServerReloadJobStatusCache.class));
    RealtimeTableDataManager tableDataManager =
        (RealtimeTableDataManager) provider.getTableDataManager(createTableConfig(), Fixtures.createSchema());
    tableDataManager.start();
    return tableDataManager;
  }

  private static RealtimeSegmentDataManager startConsumingSegment(RealtimeTableDataManager tableDataManager,
      ServerMetrics serverMetrics)
      throws Exception {
    TableConfig tableConfig = createTableConfig();
    Schema schema = Fixtures.createSchema();
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(SEGMENT_NAME_STR);
    segmentZKMetadata.setStartOffset(START_OFFSET.toString());
    segmentZKMetadata.setCreationTime(System.currentTimeMillis());
    segmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
    RealtimeSegmentDataManager segmentDataManager =
        new RealtimeSegmentDataManager(segmentZKMetadata, tableConfig, tableDataManager,
            new File(TEMP_DIR, REALTIME_TABLE_NAME).getAbsolutePath(),
            new IndexLoadingConfig(createInstanceDataManagerConfig(), tableConfig), schema, SEGMENT_NAME,
            new ConsumerCoordinator(false, tableDataManager), serverMetrics, null, null, () -> true);
    tableDataManager.registerSegment(SEGMENT_NAME_STR, segmentDataManager);
    segmentDataManager.startConsumption();
    return segmentDataManager;
  }

  /// A stream whose fetch blocks like a caught-up client, or fails outright, depending on the mode selected by the
  /// test. Registered through the table config, so the segment data manager builds it exactly as it builds a Kafka
  /// or Kinesis consumer.
  public static class TestStreamConsumerFactory extends StreamConsumerFactory {
    /// Park like a caught-up client, fail like an unreachable broker, or fail the way a client does when the thread
    /// calling it is interrupted.
    enum FetchBehaviour {
      BLOCK, FAIL, INTERRUPT
    }

    static final AtomicInteger CONSUMERS_CREATED = new AtomicInteger();
    static final AtomicInteger CONSUMERS_CLOSED = new AtomicInteger();
    static final AtomicInteger FETCHES = new AtomicInteger();
    private static volatile CountDownLatch _fetchEntered = new CountDownLatch(1);
    private static volatile FetchBehaviour _fetchBehaviour = FetchBehaviour.BLOCK;

    static void reset() {
      CONSUMERS_CREATED.set(0);
      CONSUMERS_CLOSED.set(0);
      FETCHES.set(0);
      _fetchEntered = new CountDownLatch(1);
      _fetchBehaviour = FetchBehaviour.BLOCK;
    }

    static void fetchesBehaveAs(FetchBehaviour fetchBehaviour) {
      _fetchBehaviour = fetchBehaviour;
    }

    static boolean awaitFetch()
        throws InterruptedException {
      return _fetchEntered.await(30, TimeUnit.SECONDS);
    }

    @Override
    public StreamMetadataProvider createPartitionMetadataProvider(String clientId, int partition) {
      return new FakeStreamMetadataProvider(_streamConfig);
    }

    @Override
    public StreamMetadataProvider createStreamMetadataProvider(String clientId) {
      return new FakeStreamMetadataProvider(_streamConfig);
    }

    @Override
    public PartitionGroupConsumer createPartitionGroupConsumer(String clientId,
        PartitionGroupConsumptionStatus partitionGroupConsumptionStatus) {
      CONSUMERS_CREATED.incrementAndGet();
      return new TestConsumer();
    }

    private static class TestConsumer implements PartitionGroupConsumer {
      /// Never counted down, so the only way out of a [FetchBehaviour#BLOCK] fetch is the interrupt under test. A
      /// fetch bounded by `timeoutMs` could instead return and start a second fetch while a test was still arranging
      /// the stop, and that second fetch would observe the arranged behaviour before `stop()` had been called at all.
      private static final CountDownLatch RECORDS_NEVER_ARRIVE = new CountDownLatch(1);

      @Override
      public MessageBatch fetchMessages(StreamPartitionMsgOffset startOffset, int timeoutMs) {
        FETCHES.incrementAndGet();
        _fetchEntered.countDown();
        switch (_fetchBehaviour) {
          case FAIL:
            throw new RuntimeException("Stream is unreachable", new SocketTimeoutException());
          case INTERRUPT:
            throw interrupted();
          default:
            break;
        }
        try {
          RECORDS_NEVER_ARRIVE.await();
        } catch (InterruptedException e) {
          throw interrupted();
        }
        return new EmptyMessageBatch(startOffset);
      }

      /// Mirrors `org.apache.kafka.common.errors.InterruptException`: re-arm the thread interrupt flag and rethrow the
      /// interrupt wrapped in an unchecked exception. The re-arm is what the consumer reads to recognize a stop.
      private static RuntimeException interrupted() {
        Thread.currentThread().interrupt();
        return new RuntimeException("Interrupted while polling the stream", new InterruptedException());
      }

      @Override
      public void close() {
        CONSUMERS_CLOSED.incrementAndGet();
      }
    }

    /// What a caught-up client returns when a poll yields no records.
    private record EmptyMessageBatch(StreamPartitionMsgOffset _offsetOfNextBatch) implements MessageBatch<byte[]> {
      @Override
      public int getMessageCount() {
        return 0;
      }

      @Override
      public StreamMessage<byte[]> getStreamMessage(int index) {
        throw new IndexOutOfBoundsException("Empty batch has no message at index: " + index);
      }

      @Override
      public StreamPartitionMsgOffset getOffsetOfNextBatch() {
        return _offsetOfNextBatch;
      }

      @Override
      public long getSizeInBytes() {
        return 0;
      }
    }
  }
}
