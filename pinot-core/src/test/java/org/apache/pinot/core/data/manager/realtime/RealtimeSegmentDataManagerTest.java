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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.restlet.resources.SegmentErrorInfo;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.data.manager.provider.DefaultTableDataManagerProvider;
import org.apache.pinot.core.data.manager.provider.TableDataManagerProvider;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConsumerFactory;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamMessageDecoder;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentStatsHistory;
import org.apache.pinot.segment.local.segment.creator.Fixtures;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.UpsertContext;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.segment.local.utils.ServerReloadJobStatusCache;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.ParallelSegmentConsumptionPolicy;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.stream.BytesStreamMessage;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.LongMsgOffsetFactory;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PermanentConsumerException;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.stream.StreamMessage;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


// TODO Re-write this test using the stream abstraction
public class RealtimeSegmentDataManagerTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "RealtimeSegmentDataManagerTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME);
  private static final int PARTITION_GROUP_ID = 0;
  private static final int SEQUENCE_ID = 945;
  private static final long SEG_TIME_MS = 98347869999L;
  private static final LLCSegmentName SEGMENT_NAME =
      new LLCSegmentName(RAW_TABLE_NAME, PARTITION_GROUP_ID, SEQUENCE_ID, SEG_TIME_MS);
  private static final String SEGMENT_NAME_STR = SEGMENT_NAME.getSegmentName();
  private static final long START_OFFSET_VALUE = 198L;
  private static final LongMsgOffset START_OFFSET = new LongMsgOffset(START_OFFSET_VALUE);

  private final Map<Integer, ConsumerCoordinator> _partitionGroupIdToConsumerCoordinatorMap =
      new ConcurrentHashMap<>();

  private static TableConfig createTableConfig()
      throws Exception {
    return Fixtures.createTableConfig(FakeStreamConsumerFactory.class.getName(),
        FakeStreamMessageDecoder.class.getName());
  }

  private RealtimeTableDataManager createTableDataManager(TableConfig tableConfig) {
    final String instanceId = "server-1";
    SegmentBuildTimeLeaseExtender.getOrCreate(instanceId, new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry()),
        tableConfig.getTableName());
    RealtimeTableDataManager tableDataManager = mock(RealtimeTableDataManager.class);
    when(tableDataManager.getInstanceId()).thenReturn(instanceId);
    when(tableDataManager.getSegmentLock(any())).thenReturn(mock(Lock.class));
    RealtimeSegmentStatsHistory statsHistory = mock(RealtimeSegmentStatsHistory.class);
    when(statsHistory.getEstimatedCardinality(anyString())).thenReturn(200);
    when(statsHistory.getEstimatedAvgColSize(anyString())).thenReturn(32);
    when(tableDataManager.getStatsHistory()).thenReturn(statsHistory);
    when(tableDataManager.getConsumerDir()).thenReturn(TEMP_DIR.getAbsolutePath() + "/consumerDir");
    return tableDataManager;
  }

  private SegmentZKMetadata createZkMetadata() {
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(SEGMENT_NAME_STR);
    segmentZKMetadata.setStartOffset(START_OFFSET.toString());
    segmentZKMetadata.setCreationTime(System.currentTimeMillis());
    segmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
    return segmentZKMetadata;
  }

  private FakeRealtimeSegmentDataManager createFakeSegmentManager()
      throws Exception {
    return createFakeSegmentManager(false, new TimeSupplier(), null, null, null);
  }

  @Test
  public void testInitializationErrorStopMsgSkippedWhenDifferentSegmentManagerRegistered()
      throws Exception {
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      RealtimeTableDataManager tableDataManager = segmentDataManager.getTableDataManager();
      SegmentDataManager registeredSegmentDataManager = mock(SegmentDataManager.class);
      when(tableDataManager.getSegmentDataManager(SEGMENT_NAME_STR)).thenReturn(registeredSegmentDataManager);

      segmentDataManager.postStopConsumedMsgForInitializationError();

      Assert.assertFalse(segmentDataManager._postConsumeStoppedCalled);
    }
  }

  @Test
  public void testInitializationErrorStopMsgSentWhenCurrentSegmentManagerRegistered()
      throws Exception {
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      RealtimeTableDataManager tableDataManager = segmentDataManager.getTableDataManager();
      when(tableDataManager.getSegmentDataManager(SEGMENT_NAME_STR)).thenReturn(segmentDataManager);

      segmentDataManager.postStopConsumedMsgForInitializationError();

      Assert.assertTrue(segmentDataManager._postConsumeStoppedCalled);
    }
  }

  @Test
  public void testInitializationErrorStopMsgSkippedAfterTableShutdown()
      throws Exception {
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      RealtimeTableDataManager tableDataManager = segmentDataManager.getTableDataManager();
      // Shutdown drained the old table's segment map, so nothing is registered under this segment name even though a
      // recreated same-name table may already be consuming it.
      when(tableDataManager.getSegmentDataManager(SEGMENT_NAME_STR)).thenReturn(null);
      when(tableDataManager.isShutDown()).thenReturn(true);

      segmentDataManager.postStopConsumedMsgForInitializationError();

      Assert.assertFalse(segmentDataManager._postConsumeStoppedCalled);
    }
  }

  @Test(timeOut = 10_000)
  public void testStopConsumedMsgRetryStopsAfterTableShutdown()
      throws Exception {
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      RealtimeTableDataManager tableDataManager = segmentDataManager.getTableDataManager();
      // The controller rejects the first attempt (e.g. the table's ideal state is already gone) and the table is shut
      // down before the retry: the loop must give up instead of sleeping and retrying on behalf of a recreated table.
      segmentDataManager._stopConsumedResponseStatus = SegmentCompletionProtocol.ControllerResponseStatus.FAILED;
      when(tableDataManager.isShutDown()).thenReturn(true);

      segmentDataManager.invokePostStopConsumedMsg("Consuming segment initialization error");

      Assert.assertEquals(segmentDataManager._postConsumeStoppedAttempts, 1);
    }
  }

  @Test
  public void testDestroyBeforeConsumptionStarts()
      throws Exception {
    FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
    // Never started: the real stop() must tolerate the missing consumer thread (the fake normally overrides it), and
    // destroy() must still close the stream consumer.
    segmentDataManager.invokeRealStop();
    segmentDataManager.destroy();
    Assert.assertTrue(segmentDataManager.isStreamConsumerClosed());
  }

  @Test
  public void testPostStopConsumedMsgDoesNotCheckRegisteredSegmentManager()
      throws Exception {
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      RealtimeTableDataManager tableDataManager = segmentDataManager.getTableDataManager();
      SegmentDataManager registeredSegmentDataManager = mock(SegmentDataManager.class);
      when(tableDataManager.getSegmentDataManager(SEGMENT_NAME_STR)).thenReturn(registeredSegmentDataManager);

      segmentDataManager.invokePostStopConsumedMsg("consumer error");

      Assert.assertTrue(segmentDataManager._postConsumeStoppedCalled);
    }
  }

  /// A stream that reports data loss on every poll must not emit an ERROR line and a segment error entry per poll:
  /// the first detection is reported right away, later ones only once per interval with a count of the batches in
  /// between.
  @Test
  public void testDataLossReportIsRateLimited()
      throws Exception {
    TimeSupplier timeSupplier = new TimeSupplier();
    try (FakeRealtimeSegmentDataManager segmentDataManager =
        createFakeSegmentManager(false, timeSupplier, null, null, null)) {
      RealtimeTableDataManager tableDataManager = segmentDataManager.getTableDataManager();
      MessageBatch<?> batchWithLoss = mock(MessageBatch.class);
      when(batchWithLoss.hasDataLoss()).thenReturn(true);
      when(batchWithLoss.getFirstMessageOffset()).thenReturn(new LongMsgOffset(197));
      ArgumentCaptor<SegmentErrorInfo> errorCaptor = ArgumentCaptor.forClass(SegmentErrorInfo.class);

      // The first detection is reported immediately
      segmentDataManager.reportDataLoss(batchWithLoss);
      verify(tableDataManager, times(1)).addSegmentError(eq(SEGMENT_NAME_STR), errorCaptor.capture());
      String message = errorCaptor.getValue().getErrorMessage();
      Assert.assertTrue(message.contains("batchFirstOffset: 197"), message);
      Assert.assertTrue(message.contains("batches with loss since last report: 1)"), message);

      // Detections within the interval are counted but not reported
      for (int i = 0; i < 10; i++) {
        timeSupplier.add(TimeUnit.SECONDS.toMillis(5));
        segmentDataManager.reportDataLoss(batchWithLoss);
      }
      verify(tableDataManager, times(1)).addSegmentError(eq(SEGMENT_NAME_STR), any(SegmentErrorInfo.class));

      // Once the interval has elapsed, the next detection is reported along with how many batches it stands for
      timeSupplier.add(TimeUnit.SECONDS.toMillis(10));
      segmentDataManager.reportDataLoss(batchWithLoss);
      verify(tableDataManager, times(2)).addSegmentError(eq(SEGMENT_NAME_STR), errorCaptor.capture());
      message = errorCaptor.getValue().getErrorMessage();
      Assert.assertTrue(message.contains("batches with loss since last report: 11)"), message);

      // Batches without loss are never reported
      MessageBatch<?> batchWithoutLoss = mock(MessageBatch.class);
      segmentDataManager.reportDataLoss(batchWithoutLoss);
      verify(tableDataManager, times(2)).addSegmentError(eq(SEGMENT_NAME_STR), any(SegmentErrorInfo.class));
    }
  }

  /// A second, distinct loss event that falls inside the rate-limit interval after a report and that the stream then
  /// recovers from must still reach the log and the segment error cache: the first batch processed after the interval
  /// reports it, even though that batch is healthy.
  @Test
  public void testSuppressedDataLossIsReportedAfterRecovery()
      throws Exception {
    TimeSupplier timeSupplier = new TimeSupplier();
    try (FakeRealtimeSegmentDataManager segmentDataManager =
        createFakeSegmentManager(false, timeSupplier, null, null, null)) {
      RealtimeTableDataManager tableDataManager = segmentDataManager.getTableDataManager();
      MessageBatch<?> firstLoss = mock(MessageBatch.class);
      when(firstLoss.hasDataLoss()).thenReturn(true);
      when(firstLoss.getFirstMessageOffset()).thenReturn(new LongMsgOffset(197));
      MessageBatch<?> secondLoss = mock(MessageBatch.class);
      when(secondLoss.hasDataLoss()).thenReturn(true);
      when(secondLoss.getFirstMessageOffset()).thenReturn(new LongMsgOffset(481));
      MessageBatch<?> healthyBatch = mock(MessageBatch.class);
      ArgumentCaptor<SegmentErrorInfo> errorCaptor = ArgumentCaptor.forClass(SegmentErrorInfo.class);

      segmentDataManager.reportDataLoss(firstLoss);
      verify(tableDataManager, times(1)).addSegmentError(eq(SEGMENT_NAME_STR), any(SegmentErrorInfo.class));

      // A distinct loss within the interval is suppressed, and healthy batches within the interval do not report it
      timeSupplier.add(TimeUnit.SECONDS.toMillis(5));
      segmentDataManager.reportDataLoss(secondLoss);
      timeSupplier.add(TimeUnit.SECONDS.toMillis(10));
      segmentDataManager.reportDataLoss(healthyBatch);
      verify(tableDataManager, times(1)).addSegmentError(eq(SEGMENT_NAME_STR), any(SegmentErrorInfo.class));

      // The first healthy batch after the interval reports the suppressed loss
      timeSupplier.add(TimeUnit.SECONDS.toMillis(45));
      segmentDataManager.reportDataLoss(healthyBatch);
      verify(tableDataManager, times(2)).addSegmentError(eq(SEGMENT_NAME_STR), errorCaptor.capture());
      String message = errorCaptor.getValue().getErrorMessage();
      Assert.assertTrue(message.contains("batchFirstOffset: 481"), message);
      Assert.assertTrue(message.contains("batches with loss since last report: 1)"), message);

      // The deferred report restarts the interval: a new loss right after it is suppressed, then reported by the first
      // (healthy) batch after the interval
      timeSupplier.add(TimeUnit.SECONDS.toMillis(10));
      segmentDataManager.reportDataLoss(firstLoss);
      verify(tableDataManager, times(2)).addSegmentError(eq(SEGMENT_NAME_STR), any(SegmentErrorInfo.class));
      timeSupplier.add(TimeUnit.SECONDS.toMillis(50));
      segmentDataManager.reportDataLoss(healthyBatch);
      verify(tableDataManager, times(3)).addSegmentError(eq(SEGMENT_NAME_STR), errorCaptor.capture());
      message = errorCaptor.getValue().getErrorMessage();
      Assert.assertTrue(message.contains("batchFirstOffset: 197"), message);

      // Nothing is left to report afterwards
      timeSupplier.add(TimeUnit.MINUTES.toMillis(5));
      segmentDataManager.reportDataLoss(healthyBatch);
      verify(tableDataManager, times(3)).addSegmentError(eq(SEGMENT_NAME_STR), any(SegmentErrorInfo.class));
    }
  }

  /// An empty batch whose next-batch offset is ahead of the current offset means the stream moved past offsets it
  /// will never deliver (a Kafka consumer skipping a transaction commit marker, for example). The current offset
  /// must follow, so an idle consumer parks past the marker instead of on it, and the next fetch asks for the new
  /// offset. An empty batch that reports the requested offset, no offset, or an offset behind leaves it alone.
  @Test
  public void testEmptyBatchAheadOfCurrentOffsetAdvancesCurrentOffset()
      throws Exception {
    try (FakeRealtimeSegmentDataManager segmentDataManager =
        createFakeSegmentManager(true, new TimeSupplier(), null, null, null)) {
      segmentDataManager._stubConsumeLoop = false;
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.INITIAL_CONSUMING);
      // consumeLoop() is called directly, so set the time limit run() would have set
      segmentDataManager.setConsumeEndTime(System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1));
      ScriptedPartitionGroupConsumer consumer = new ScriptedPartitionGroupConsumer(segmentDataManager,
          // The stream skipped START_OFFSET_VALUE and START_OFFSET_VALUE + 1 without delivering anything
          emptyBatch(START_OFFSET_VALUE + 2),
          // Nothing moved
          emptyBatch(START_OFFSET_VALUE + 2),
          // A stream that does not report where the next batch starts
          emptyBatch(null),
          // Behind the current offset: never move backwards
          emptyBatch(START_OFFSET_VALUE + 1));
      segmentDataManager.setPartitionGroupConsumer(consumer);

      segmentDataManager.consumeLoop();

      Assert.assertEquals(consumer._requestedOffsets, List.of(START_OFFSET_VALUE, START_OFFSET_VALUE + 2,
          START_OFFSET_VALUE + 2, START_OFFSET_VALUE + 2));
      Assert.assertEquals(((LongMsgOffset) segmentDataManager.getCurrentOffset()).getOffset(),
          START_OFFSET_VALUE + 2);
      Assert.assertEquals(segmentDataManager.getSegment().getNumDocsIndexed(), 0);
    }
  }

  @DataProvider
  public Object[][] protocolCatchupEmptyPositions() {
    return new Object[][]{{10L, false}, {20L, false}, {50L, false}, {20L, true}};
  }

  /// An empty consumer position is not proof that the follower has the committer's rows. In particular, equality
  /// with the controller target must not make the protocol select this replica to COMMIT an unverified rowset.
  @Test(dataProvider = "protocolCatchupEmptyPositions")
  public void testEmptyBatchesDoNotCompleteProtocolCatchup(long advance, boolean hasDataLoss)
      throws Exception {
    try (FakeRealtimeSegmentDataManager segmentDataManager =
        createFakeSegmentManager(true, new TimeSupplier(), null, null, null)) {
      segmentDataManager._stubConsumeLoop = false;
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.CATCHING_UP);
      long target = START_OFFSET_VALUE + 20;
      segmentDataManager.setFinalOffset(target);
      ScriptedPartitionGroupConsumer consumer = new ScriptedPartitionGroupConsumer(segmentDataManager,
          new ScriptedMessageBatch(START_OFFSET_VALUE + advance, hasDataLoss), emptyBatch(null));
      segmentDataManager.setPartitionGroupConsumer(consumer);
      segmentDataManager._responses.add(new SegmentCompletionProtocol.Response(
          new SegmentCompletionProtocol.Response.Params().withStreamPartitionMsgOffset(Long.toString(target))
              .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT)));
      // Keep the fake's stop condition from masking a spurious COMMIT after a synthetic target match.
      segmentDataManager._responses.add(new SegmentCompletionProtocol.Response(
          new SegmentCompletionProtocol.Response.Params()
              .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.HOLD)));

      segmentDataManager.createPartitionConsumer().run();

      assertFalse(segmentDataManager._commitSegmentCalled, "an empty position must not confer commit eligibility");
      assertFalse(segmentDataManager._buildSegmentCalled);
      assertEquals(((LongMsgOffset) segmentDataManager.getCurrentOffset()).getOffset(), START_OFFSET_VALUE);
      assertEquals(consumer._requestedOffsets, List.of(START_OFFSET_VALUE, START_OFFSET_VALUE));
      assertEquals(segmentDataManager._responses.size(), 2,
          "the protocol must not enter HOLDING from an empty advance");
      if (!hasDataLoss) {
        verify(segmentDataManager._tableDataManager, never()).addSegmentError(eq(SEGMENT_NAME_STR), any());
      }
    }
  }

  @DataProvider
  public Object[][] recordOvershootDataLoss() {
    return new Object[][]{{false}, {true}};
  }

  /// Protocol catch-up retains ordinary record advancement and its existing overshoot/download recovery.
  @Test(dataProvider = "recordOvershootDataLoss")
  public void testMessagePastFinalOffsetOvershootsWhileCatchingUp(boolean hasDataLoss)
      throws Exception {
    try (FakeRealtimeSegmentDataManager segmentDataManager =
        createFakeSegmentManager(true, new TimeSupplier(), null, null, null)) {
      segmentDataManager._stubConsumeLoop = false;
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.CATCHING_UP);
      segmentDataManager.setFinalOffset(START_OFFSET_VALUE + 1);
      ScriptedPartitionGroupConsumer consumer = new ScriptedPartitionGroupConsumer(segmentDataManager,
          new ScriptedMessageBatch(START_OFFSET_VALUE + 3, hasDataLoss, START_OFFSET_VALUE + 2),
          emptyBatch(START_OFFSET_VALUE + 3));
      segmentDataManager.setPartitionGroupConsumer(consumer);

      assertThrows(RuntimeException.class, segmentDataManager::consumeLoop);
      assertEquals(consumer._requestedOffsets, List.of(START_OFFSET_VALUE));
      assertEquals(((LongMsgOffset) segmentDataManager.getCurrentOffset()).getOffset(), START_OFFSET_VALUE + 3);
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager.goOnlineFromConsuming(crcMetadata(START_OFFSET_VALUE + 1, 12345L));
      assertTrue(segmentDataManager._downloadAndReplaceCalled);
      assertFalse(segmentDataManager._buildAndReplaceCalled);
    }
  }

  /// An empty batch; `nextBatchOffset` is the consumer position, not proof of the skipped interval's contents.
  private static MessageBatch<byte[]> emptyBatch(@Nullable Long nextBatchOffset) {
    return new ScriptedMessageBatch(nextBatchOffset, false);
  }

  /// A batch of empty messages at the given offsets (each ending at the following offset), reporting the given
  /// next-batch offset (`null` for a stream that does not report one) and data-loss flag. The messages are never
  /// decoded by the tests using this batch, which stop in front of them.
  private static class ScriptedMessageBatch implements MessageBatch<byte[]> {
    private final Long _nextBatchOffset;
    private final boolean _hasDataLoss;
    private final long[] _messageOffsets;

    ScriptedMessageBatch(@Nullable Long nextBatchOffset, boolean hasDataLoss, long... messageOffsets) {
      _nextBatchOffset = nextBatchOffset;
      _hasDataLoss = hasDataLoss;
      _messageOffsets = messageOffsets;
    }

    @Override
    public int getMessageCount() {
      return _messageOffsets.length;
    }

    @Override
    public StreamMessage<byte[]> getStreamMessage(int index) {
      long offset = _messageOffsets[index];
      return new BytesStreamMessage(new byte[0], new StreamMessageMetadata.Builder()
          .setOffset(new LongMsgOffset(offset), new LongMsgOffset(offset + 1)).build());
    }

    @Override
    public StreamPartitionMsgOffset getOffsetOfNextBatch() {
      return _nextBatchOffset != null ? new LongMsgOffset(_nextBatchOffset) : null;
    }

    @Override
    public boolean hasDataLoss() {
      return _hasDataLoss;
    }

    @Override
    public long getSizeInBytes() {
      return 0;
    }
  }

  /// Hands out the given batches in order, recording the offset requested for each, and stops the data manager once
  /// the last one has been handed out so consumeLoop() returns.
  private static class ScriptedPartitionGroupConsumer implements PartitionGroupConsumer {
    final List<Long> _requestedOffsets = new ArrayList<>();
    private final FakeRealtimeSegmentDataManager _segmentDataManager;
    private final LinkedList<MessageBatch<byte[]>> _batches;

    @SafeVarargs
    ScriptedPartitionGroupConsumer(FakeRealtimeSegmentDataManager segmentDataManager,
        MessageBatch<byte[]>... batches) {
      _segmentDataManager = segmentDataManager;
      _batches = new LinkedList<>(List.of(batches));
    }

    @Override
    public MessageBatch<byte[]> fetchMessages(StreamPartitionMsgOffset startOffset, int timeoutMs) {
      _requestedOffsets.add(((LongMsgOffset) startOffset).getOffset());
      MessageBatch<byte[]> batch = _batches.remove();
      if (_batches.isEmpty()) {
        _segmentDataManager.stop();
      }
      return batch;
    }

    @Override
    public void close() {
    }
  }

  private FakeRealtimeSegmentDataManager createFakeSegmentManager(boolean noUpsert, TimeSupplier timeSupplier,
      @Nullable String maxRows, @Nullable String maxDuration, @Nullable TableConfig tableConfig)
      throws Exception {
    return createFakeSegmentManager(noUpsert, timeSupplier, maxRows, maxDuration, tableConfig,
        new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry()));
  }

  private FakeRealtimeSegmentDataManager createFakeSegmentManager(boolean noUpsert, TimeSupplier timeSupplier,
      @Nullable String maxRows, @Nullable String maxDuration, @Nullable TableConfig tableConfig,
      ServerMetrics serverMetrics)
      throws Exception {
    SegmentZKMetadata segmentZKMetadata = createZkMetadata();
    if (tableConfig == null) {
      tableConfig = createTableConfig();
    }
    if (noUpsert) {
      tableConfig.setUpsertConfig(null);
    }
    if (maxRows != null) {
      tableConfig.getIndexingConfig().getStreamConfigs()
          .put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, maxRows);
    }
    if (maxDuration != null) {
      tableConfig.getIndexingConfig().getStreamConfigs()
          .put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_TIME, maxDuration);
    }
    if (tableConfig.getIngestionConfig() == null) {
      tableConfig.setIngestionConfig(new IngestionConfig());
    }
    tableConfig.getIngestionConfig().setRetryOnSegmentBuildPrecheckFailure(true);
    RealtimeTableDataManager tableDataManager = createTableDataManager(tableConfig);
    LLCSegmentName llcSegmentName = new LLCSegmentName(SEGMENT_NAME_STR);
    _partitionGroupIdToConsumerCoordinatorMap.putIfAbsent(PARTITION_GROUP_ID,
        new ConsumerCoordinator(false, tableDataManager));
    Schema schema = Fixtures.createSchema();
    return new FakeRealtimeSegmentDataManager(segmentZKMetadata, tableConfig, tableDataManager,
        new File(TEMP_DIR, REALTIME_TABLE_NAME).getAbsolutePath(), schema, llcSegmentName,
        _partitionGroupIdToConsumerCoordinatorMap, serverMetrics, timeSupplier);
  }

  @BeforeClass
  public void setUp() {
    ServerMetrics.register(mock(ServerMetrics.class));

    FileUtils.deleteQuietly(TEMP_DIR);
    SegmentBuildTimeLeaseExtender.initExecutor();
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
    SegmentBuildTimeLeaseExtender.shutdownExecutor();
  }

  @Test
  public void testSnapshotTriggerContextBeforeConsumption()
      throws Exception {
    for (double metadataTtl : new double[]{0, 1}) {
      try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
        PartitionUpsertMetadataManager manager = mockUpsertMetadataManager(segmentDataManager, metadataTtl);
        doAnswer(invocation -> {
          assertEquals(segmentDataManager.getCurrentOffset().toString(), START_OFFSET.toString());
          return null;
        }).when(manager).takeSnapshot(SEGMENT_NAME_STR, START_OFFSET.toString());
        segmentDataManager._consumeOffsets.add(new LongMsgOffset(START_OFFSET_VALUE + 1));
        segmentDataManager.createPartitionConsumer().run();
        InOrder order = inOrder(manager);
        if (metadataTtl > 0) {
          order.verify(manager).takeSnapshot(SEGMENT_NAME_STR, START_OFFSET.toString());
          order.verify(manager).removeExpiredPrimaryKeys();
        } else {
          order.verify(manager).removeExpiredPrimaryKeys();
          order.verify(manager).takeSnapshot(SEGMENT_NAME_STR, START_OFFSET.toString());
        }
      }
    }
  }

  @DataProvider
  public Object[][] unsealedReleases() {
    long ahead = START_OFFSET_VALUE + 50;
    long behind = START_OFFSET_VALUE - 50;
    return new Object[][]{
        // The previous segment discarded its copy after consuming past this segment's start offset.
        {SEQUENCE_ID - 1, ahead, ahead},
        // The previous segment could not catch up and is downloading, so its rows end before this segment's start.
        {SEQUENCE_ID - 1, behind, behind},
        // An older segment was never offloaded, but this replica consumed the segments after it.
        {SEQUENCE_ID - 2, behind, START_OFFSET_VALUE}
    };
  }

  @Test(dataProvider = "unsealedReleases")
  public void testSnapshotTriggerUsesWhereUnsealedPreviousSegmentStopped(int releasedSequence, long stoppedAt,
      long expectedConsumedUpTo)
      throws Exception {
    _partitionGroupIdToConsumerCoordinatorMap.remove(PARTITION_GROUP_ID);
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      // The released segment is still mutable in the upsert metadata when this segment starts consuming.
      ConsumerCoordinator consumerCoordinator = _partitionGroupIdToConsumerCoordinatorMap.get(PARTITION_GROUP_ID);
      Assert.assertTrue(consumerCoordinator.getSemaphore().tryAcquire());
      String releasedSegment =
          new LLCSegmentName(RAW_TABLE_NAME, PARTITION_GROUP_ID, releasedSequence, SEG_TIME_MS).getSegmentName();
      consumerCoordinator.releaseUnsealed(releasedSegment, new LongMsgOffset(stoppedAt));
      PartitionUpsertMetadataManager manager = mockUpsertMetadataManager(segmentDataManager, 0);
      segmentDataManager._consumeOffsets.add(new LongMsgOffset(START_OFFSET_VALUE + 1));
      segmentDataManager.createPartitionConsumer().run();
      verify(manager).takeSnapshot(SEGMENT_NAME_STR, Long.toString(expectedConsumedUpTo));
    } finally {
      _partitionGroupIdToConsumerCoordinatorMap.remove(PARTITION_GROUP_ID);
    }
  }

  private static PartitionUpsertMetadataManager mockUpsertMetadataManager(
      FakeRealtimeSegmentDataManager segmentDataManager, double metadataTtl)
      throws Exception {
    PartitionUpsertMetadataManager manager = mock(PartitionUpsertMetadataManager.class);
    UpsertContext context = mock(UpsertContext.class);
    when(manager.getContext()).thenReturn(context);
    when(context.getMetadataTTL()).thenReturn(metadataTtl);
    Field managerField = RealtimeSegmentDataManager.class.getDeclaredField("_partitionUpsertMetadataManager");
    managerField.setAccessible(true);
    managerField.set(segmentDataManager, manager);
    return manager;
  }

  // Test that we are in HOLDING state as long as the controller responds HOLD to our segmentConsumed() message.
  // we should not consume when holding.
  @Test
  public void testHolding()
      throws Exception {
    FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
    RealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
    final LongMsgOffset endOffset = new LongMsgOffset(START_OFFSET_VALUE + 500);
    // We should consume initially...
    segmentDataManager._consumeOffsets.add(endOffset);
    final SegmentCompletionProtocol.Response response = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStatus(
                SegmentCompletionProtocol.ControllerResponseStatus.HOLD)
            .withStreamPartitionMsgOffset(endOffset.toString()));
    // And then never consume as long as we get a hold response, 100 times.
    for (int i = 0; i < 100; i++) {
      segmentDataManager._responses.add(response);
    }

    consumer.run();

    Assert.assertTrue(segmentDataManager._responses.isEmpty());
    Assert.assertTrue(segmentDataManager._consumeOffsets.isEmpty());
    Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    Assert.assertFalse(segmentDataManager._buildSegmentCalled);
    Assert.assertFalse(segmentDataManager._commitSegmentCalled);
    Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
    Assert.assertEquals(segmentDataManager._state.get(segmentDataManager), RealtimeSegmentDataManager.State.HOLDING);
    segmentDataManager.close();
  }

  // Test that we go to commit when the controller responds commit after 2 holds.
  @Test
  public void testCommitAfterHold()
      throws Exception {
    FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
    RealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
    final LongMsgOffset endOffset = new LongMsgOffset(START_OFFSET_VALUE + 500);
    // We should consume initially...
    segmentDataManager._consumeOffsets.add(endOffset);
    final SegmentCompletionProtocol.Response holdResponse = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStreamPartitionMsgOffset(endOffset.toString())
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.HOLD));
    final SegmentCompletionProtocol.Response commitResponse = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStreamPartitionMsgOffset(endOffset.toString())
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT));
    // And then never consume as long as we get a hold response, 100 times.
    segmentDataManager._responses.add(holdResponse);
    segmentDataManager._responses.add(commitResponse);

    consumer.run();

    Assert.assertTrue(segmentDataManager._responses.isEmpty());
    Assert.assertTrue(segmentDataManager._consumeOffsets.isEmpty());
    Assert.assertTrue(segmentDataManager._buildSegmentCalled);
    Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
    Assert.assertTrue(segmentDataManager._commitSegmentCalled);
    Assert.assertEquals(segmentDataManager._state.get(segmentDataManager), RealtimeSegmentDataManager.State.COMMITTED);
    segmentDataManager.close();
  }

  @Test
  public void testSegmentBuildException()
      throws Exception {
    FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
    RealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
    final LongMsgOffset endOffset = new LongMsgOffset(START_OFFSET_VALUE + 500);
    // We should consume initially...
    segmentDataManager._consumeOffsets.add(endOffset);
    final SegmentCompletionProtocol.Response commitResponse = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStreamPartitionMsgOffset(endOffset.toString())
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT));
    segmentDataManager._responses.add(commitResponse);
    segmentDataManager._failSegmentBuild = true;

    consumer.run();
    Assert.assertTrue(segmentDataManager._buildSegmentCalled);
    Assert.assertTrue(segmentDataManager._notifySegmentBuildFailedWithDeterministicErrorCalled);
    Assert.assertEquals(segmentDataManager._state.get(segmentDataManager), RealtimeSegmentDataManager.State.ERROR);
    segmentDataManager.close();
  }

  // Test hold, catchup. hold, commit
  @Test
  public void testCommitAfterCatchup()
      throws Exception {
    FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
    RealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
    final LongMsgOffset firstOffset = new LongMsgOffset(START_OFFSET_VALUE + 500);
    final LongMsgOffset catchupOffset = new LongMsgOffset(firstOffset.getOffset() + 10);
    // We should consume initially...
    segmentDataManager._consumeOffsets.add(firstOffset);
    segmentDataManager._consumeOffsets.add(catchupOffset); // Offset after catchup
    final SegmentCompletionProtocol.Response holdResponse1 = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStatus(
                SegmentCompletionProtocol.ControllerResponseStatus.HOLD)
            .withStreamPartitionMsgOffset(firstOffset.toString()));
    final SegmentCompletionProtocol.Response catchupResponse = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStatus(
                SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP)
            .withStreamPartitionMsgOffset(catchupOffset.toString()));
    final SegmentCompletionProtocol.Response holdResponse2 = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStreamPartitionMsgOffset(catchupOffset.toString())
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.HOLD));
    final SegmentCompletionProtocol.Response commitResponse = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStreamPartitionMsgOffset(catchupOffset.toString())
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT));
    // And then never consume as long as we get a hold response, 100 times.
    segmentDataManager._responses.add(holdResponse1);
    segmentDataManager._responses.add(catchupResponse);
    segmentDataManager._responses.add(holdResponse2);
    segmentDataManager._responses.add(commitResponse);

    consumer.run();

    Assert.assertTrue(segmentDataManager._responses.isEmpty());
    Assert.assertTrue(segmentDataManager._consumeOffsets.isEmpty());
    Assert.assertTrue(segmentDataManager._buildSegmentCalled);
    Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
    Assert.assertTrue(segmentDataManager._commitSegmentCalled);
    Assert.assertEquals(segmentDataManager._state.get(segmentDataManager), RealtimeSegmentDataManager.State.COMMITTED);
    segmentDataManager.close();
  }

  @Test
  public void testCommitAfterCatchupWithPeriodOffset()
      throws Exception {
    TableConfig tableConfig = createTableConfig();
    tableConfig.getIndexingConfig().getStreamConfigs().put(
        StreamConfigProperties.constructStreamProperty(StreamConfigProperties.STREAM_CONSUMER_OFFSET_CRITERIA,
            "fakeStream"), "2d");
    FakeRealtimeSegmentDataManager segmentDataManager =
        createFakeSegmentManager(false, new TimeSupplier(), null, null, tableConfig);
    RealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
    final LongMsgOffset firstOffset = new LongMsgOffset(START_OFFSET_VALUE + 500);
    final LongMsgOffset catchupOffset = new LongMsgOffset(firstOffset.getOffset() + 10);
    // We should consume initially...
    segmentDataManager._consumeOffsets.add(firstOffset);
    segmentDataManager._consumeOffsets.add(catchupOffset); // Offset after catchup
    final SegmentCompletionProtocol.Response holdResponse1 = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStatus(
                SegmentCompletionProtocol.ControllerResponseStatus.HOLD)
            .withStreamPartitionMsgOffset(firstOffset.toString()));
    final SegmentCompletionProtocol.Response catchupResponse = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStatus(
                SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP)
            .withStreamPartitionMsgOffset(catchupOffset.toString()));
    final SegmentCompletionProtocol.Response holdResponse2 = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStreamPartitionMsgOffset(catchupOffset.toString())
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.HOLD));
    final SegmentCompletionProtocol.Response commitResponse = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStreamPartitionMsgOffset(catchupOffset.toString())
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT));
    // And then never consume as long as we get a hold response, 100 times.
    segmentDataManager._responses.add(holdResponse1);
    segmentDataManager._responses.add(catchupResponse);
    segmentDataManager._responses.add(holdResponse2);
    segmentDataManager._responses.add(commitResponse);

    consumer.run();

    Assert.assertTrue(segmentDataManager._responses.isEmpty());
    Assert.assertTrue(segmentDataManager._consumeOffsets.isEmpty());
    Assert.assertTrue(segmentDataManager._buildSegmentCalled);
    Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
    Assert.assertTrue(segmentDataManager._commitSegmentCalled);
    Assert.assertEquals(segmentDataManager._state.get(segmentDataManager), RealtimeSegmentDataManager.State.COMMITTED);
    segmentDataManager.close();
  }

  @Test
  public void testCommitAfterCatchupWithTimestampOffset()
      throws Exception {
    TableConfig tableConfig = createTableConfig();
    tableConfig.getIndexingConfig().getStreamConfigs().put(
        StreamConfigProperties.constructStreamProperty(StreamConfigProperties.STREAM_CONSUMER_OFFSET_CRITERIA,
            "fakeStream"), Instant.now().toString());
    FakeRealtimeSegmentDataManager segmentDataManager =
        createFakeSegmentManager(false, new TimeSupplier(), null, null, tableConfig);
    RealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
    final LongMsgOffset firstOffset = new LongMsgOffset(START_OFFSET_VALUE + 500);
    final LongMsgOffset catchupOffset = new LongMsgOffset(firstOffset.getOffset() + 10);
    // We should consume initially...
    segmentDataManager._consumeOffsets.add(firstOffset);
    segmentDataManager._consumeOffsets.add(catchupOffset); // Offset after catchup
    final SegmentCompletionProtocol.Response holdResponse1 = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStatus(
                SegmentCompletionProtocol.ControllerResponseStatus.HOLD)
            .withStreamPartitionMsgOffset(firstOffset.toString()));
    final SegmentCompletionProtocol.Response catchupResponse = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStatus(
                SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP)
            .withStreamPartitionMsgOffset(catchupOffset.toString()));
    final SegmentCompletionProtocol.Response holdResponse2 = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStreamPartitionMsgOffset(catchupOffset.toString())
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.HOLD));
    final SegmentCompletionProtocol.Response commitResponse = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStreamPartitionMsgOffset(catchupOffset.toString())
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT));
    // And then never consume as long as we get a hold response, 100 times.
    segmentDataManager._responses.add(holdResponse1);
    segmentDataManager._responses.add(catchupResponse);
    segmentDataManager._responses.add(holdResponse2);
    segmentDataManager._responses.add(commitResponse);

    consumer.run();

    Assert.assertTrue(segmentDataManager._responses.isEmpty());
    Assert.assertTrue(segmentDataManager._consumeOffsets.isEmpty());
    Assert.assertTrue(segmentDataManager._buildSegmentCalled);
    Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
    Assert.assertTrue(segmentDataManager._commitSegmentCalled);
    Assert.assertEquals(segmentDataManager._state.get(segmentDataManager), RealtimeSegmentDataManager.State.COMMITTED);
    segmentDataManager.close();
  }

  @Test
  public void testDiscarded()
      throws Exception {
    FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
    RealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
    final LongMsgOffset endOffset = new LongMsgOffset(START_OFFSET_VALUE + 500);
    segmentDataManager._consumeOffsets.add(endOffset);
    final SegmentCompletionProtocol.Response discardResponse = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStreamPartitionMsgOffset(endOffset.toString())
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.DISCARD));
    segmentDataManager._responses.add(discardResponse);

    consumer.run();

    Assert.assertTrue(segmentDataManager._responses.isEmpty());
    Assert.assertTrue(segmentDataManager._consumeOffsets.isEmpty());
    Assert.assertFalse(segmentDataManager._buildSegmentCalled);
    Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
    Assert.assertFalse(segmentDataManager._commitSegmentCalled);
    Assert.assertEquals(segmentDataManager._state.get(segmentDataManager), RealtimeSegmentDataManager.State.DISCARDED);
    segmentDataManager.close();
  }

  @Test
  public void testRetained()
      throws Exception {
    FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
    RealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
    final LongMsgOffset endOffset = new LongMsgOffset(START_OFFSET_VALUE + 500);
    segmentDataManager._consumeOffsets.add(endOffset);
    SegmentCompletionProtocol.Response.Params params = new SegmentCompletionProtocol.Response.Params();
    params.withStreamPartitionMsgOffset(endOffset.toString())
        .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.KEEP);
    final SegmentCompletionProtocol.Response keepResponse = new SegmentCompletionProtocol.Response(params);
    segmentDataManager._responses.add(keepResponse);

    consumer.run();

    Assert.assertTrue(segmentDataManager._responses.isEmpty());
    Assert.assertTrue(segmentDataManager._consumeOffsets.isEmpty());
    Assert.assertFalse(segmentDataManager._buildSegmentCalled);
    Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
    Assert.assertTrue(segmentDataManager._buildAndReplaceCalled);
    Assert.assertFalse(segmentDataManager._commitSegmentCalled);
    Assert.assertEquals(segmentDataManager._state.get(segmentDataManager), RealtimeSegmentDataManager.State.RETAINED);
    segmentDataManager.close();
  }

  @Test
  public void testNotLeader()
      throws Exception {
    FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
    RealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
    final LongMsgOffset endOffset = new LongMsgOffset(START_OFFSET_VALUE + 500);
    // We should consume initially...
    segmentDataManager._consumeOffsets.add(endOffset);
    final SegmentCompletionProtocol.Response response = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStreamPartitionMsgOffset(endOffset.toString())
            .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.NOT_LEADER));
    // And then never consume as long as we get a Not leader response, 100 times.
    for (int i = 0; i < 100; i++) {
      segmentDataManager._responses.add(response);
    }

    consumer.run();

    Assert.assertTrue(segmentDataManager._responses.isEmpty());
    Assert.assertTrue(segmentDataManager._consumeOffsets.isEmpty());
    Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    Assert.assertFalse(segmentDataManager._buildSegmentCalled);
    Assert.assertFalse(segmentDataManager._commitSegmentCalled);
    Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
    Assert.assertEquals(segmentDataManager._state.get(segmentDataManager), RealtimeSegmentDataManager.State.HOLDING);
    segmentDataManager.close();
  }

  @Test
  public void testConsumingException()
      throws Exception {
    FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
    RealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();

    segmentDataManager._throwExceptionFromConsume = true;
    segmentDataManager._postConsumeStoppedCalled = false;
    consumer.run();
    Assert.assertTrue(segmentDataManager._postConsumeStoppedCalled);
    segmentDataManager.close();
  }

  // Tests to go online from consuming state

  // If the state is COMMITTED or RETAINED, nothing to do
  // If discarded or error state, then downloadAndReplace the segment
  @Test
  public void testOnlineTransitionAfterStop()
      throws Exception {
    SegmentZKMetadata metadata = new SegmentZKMetadata(SEGMENT_NAME_STR);
    final long finalOffsetValue = START_OFFSET_VALUE + 600;
    final LongMsgOffset finalOffset = new LongMsgOffset(finalOffsetValue);
    metadata.setEndOffset(finalOffset.toString());

    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      segmentDataManager.getConsumerSemaphoreAcquired().set(true);
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.COMMITTED);
      segmentDataManager.goOnlineFromConsuming(metadata);
      Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
      Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    }

    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      segmentDataManager.getConsumerSemaphoreAcquired().set(true);
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.RETAINED);
      segmentDataManager.goOnlineFromConsuming(metadata);
      Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
      Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    }

    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      segmentDataManager.getConsumerSemaphoreAcquired().set(true);
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.DISCARDED);
      segmentDataManager.goOnlineFromConsuming(metadata);
      Assert.assertTrue(segmentDataManager._downloadAndReplaceCalled);
      Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    }

    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      segmentDataManager.getConsumerSemaphoreAcquired().set(true);
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.ERROR);
      segmentDataManager.goOnlineFromConsuming(metadata);
      Assert.assertTrue(segmentDataManager._downloadAndReplaceCalled);
      Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    }

    // If holding, but we have overshot the expected final offset, the download and replace
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      segmentDataManager.getConsumerSemaphoreAcquired().set(true);
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.HOLDING);
      segmentDataManager.setCurrentOffset(finalOffsetValue + 1);
      segmentDataManager.goOnlineFromConsuming(metadata);
      Assert.assertTrue(segmentDataManager._downloadAndReplaceCalled);
      Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    }

    // If catching up, but we have overshot the expected final offset, the download and replace
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      segmentDataManager.getConsumerSemaphoreAcquired().set(true);
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.CATCHING_UP);
      segmentDataManager.setCurrentOffset(finalOffsetValue + 1);
      segmentDataManager.goOnlineFromConsuming(metadata);
      Assert.assertTrue(segmentDataManager._downloadAndReplaceCalled);
      Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    }

    // If catching up, but we did not get to the final offset, then download and replace
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      segmentDataManager.getConsumerSemaphoreAcquired().set(true);
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.CATCHING_UP);
      segmentDataManager._consumeOffsets.add(new LongMsgOffset(finalOffsetValue - 1));
      segmentDataManager.goOnlineFromConsuming(metadata);
      Assert.assertTrue(segmentDataManager._downloadAndReplaceCalled);
      Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    }

    // But then if we get to the exact offset, we get to build and replace, not download
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      segmentDataManager.getConsumerSemaphoreAcquired().set(true);
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.CATCHING_UP);
      segmentDataManager._consumeOffsets.add(finalOffset);
      segmentDataManager.goOnlineFromConsuming(metadata);
      Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
      Assert.assertTrue(segmentDataManager._buildAndReplaceCalled);
    }

    // But then if we get to the exact offset, we download the segment because consumer semaphore was never acquired.
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.CATCHING_UP);
      segmentDataManager._consumeOffsets.add(finalOffset);
      segmentDataManager.goOnlineFromConsuming(metadata);
      Assert.assertTrue(segmentDataManager._downloadAndReplaceCalled);
      Assert.assertFalse(segmentDataManager._buildAndReplaceCalled);
    }

    // Test downloadAndReplace is called after buildAndReplace fails.
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      segmentDataManager._failSegmentBuildAndReplace = true;
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.HOLDING);
      segmentDataManager.setCurrentOffset(finalOffsetValue);
      segmentDataManager.goOnlineFromConsuming(metadata);
      Assert.assertTrue(segmentDataManager._downloadAndReplaceCalled);
      Assert.assertTrue(segmentDataManager._buildAndReplaceCalled);
    }
  }

  @Test
  public void testOffloadRemovesSegmentMetadataBeforeReleasingConsumerSemaphore()
      throws Exception {
    // Use a fresh coordinator. Other tests release the shared semaphore without acquiring it, which inflates permits.
    _partitionGroupIdToConsumerCoordinatorMap.remove(PARTITION_GROUP_ID);
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      Semaphore semaphore = _partitionGroupIdToConsumerCoordinatorMap.get(PARTITION_GROUP_ID).getSemaphore();
      Assert.assertTrue(semaphore.tryAcquire());
      segmentDataManager.getConsumerSemaphoreAcquired().set(true);

      // MutableSegmentImpl.offload() removes the segment from the upsert/dedup metadata managers. For partial upsert in
      // PROTECTED mode that is where the primary keys are reverted to their previous locations, so it has to run
      // (1) after the stream consumer is closed, so nothing can consume into the offloaded segment, and
      // (2) while the consumer semaphore is still held, so the next consuming segment cannot replay against
      //     un-reverted state.
      AtomicInteger permitsWhenMetadataRemoved = new AtomicInteger(-1);
      AtomicBoolean streamConsumerClosedWhenMetadataRemoved = new AtomicBoolean(false);
      MutableSegmentImpl realtimeSegment = spy((MutableSegmentImpl) segmentDataManager.getSegment());
      doAnswer(invocation -> {
        permitsWhenMetadataRemoved.set(semaphore.availablePermits());
        streamConsumerClosedWhenMetadataRemoved.set(segmentDataManager.isStreamConsumerClosed());
        return null;
      }).when(realtimeSegment).offload();
      segmentDataManager.setRealtimeSegment(realtimeSegment);

      segmentDataManager.offload();

      verify(realtimeSegment).offload();
      Assert.assertTrue(streamConsumerClosedWhenMetadataRemoved.get(),
          "Stream consumer must be closed before the segment metadata is removed");
      Assert.assertEquals(permitsWhenMetadataRemoved.get(), 0,
          "Segment metadata must be removed while the consumer semaphore is still held");
      Assert.assertEquals(semaphore.availablePermits(), 1, "Consumer semaphore must be released after offload");
      Assert.assertFalse(segmentDataManager.getConsumerSemaphoreAcquired().get());
    }
  }

  @Test
  public void testOffloadAfterParallelConsumptionReleaseDoesNotReleaseSemaphoreTwice()
      throws Exception {
    _partitionGroupIdToConsumerCoordinatorMap.remove(PARTITION_GROUP_ID);
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      Semaphore semaphore = _partitionGroupIdToConsumerCoordinatorMap.get(PARTITION_GROUP_ID).getSemaphore();
      Assert.assertTrue(semaphore.tryAcquire());
      segmentDataManager.getConsumerSemaphoreAcquired().set(true);

      // With ALLOW_DURING_BUILD_ONLY / ALLOW_ALWAYS, buildSegmentInternal() and downloadSegmentAndReplace() let the
      // next consuming segment start early by closing the consumer and releasing the semaphore together.
      segmentDataManager.closeStreamConsumerAndReleaseSemaphore();
      Assert.assertTrue(segmentDataManager.isStreamConsumerClosed());
      Assert.assertEquals(semaphore.availablePermits(), 1,
          "Consumer semaphore must be released for parallel consumption");
      Assert.assertFalse(segmentDataManager.getConsumerSemaphoreAcquired().get());
      ConsumerCoordinator.UnsealedRelease unsealedRelease =
          _partitionGroupIdToConsumerCoordinatorMap.get(PARTITION_GROUP_ID).getUnsealedRelease();
      Assert.assertNotNull(unsealedRelease, "The next consumer must see where this still mutable segment stopped");
      Assert.assertEquals(unsealedRelease.segmentName(), SEGMENT_NAME_STR);
      Assert.assertEquals(unsealedRelease.stoppedAtOffset().toString(), START_OFFSET.toString());

      MutableSegmentImpl realtimeSegment = spy((MutableSegmentImpl) segmentDataManager.getSegment());
      doAnswer(invocation -> null).when(realtimeSegment).offload();
      segmentDataManager.setRealtimeSegment(realtimeSegment);

      segmentDataManager.offload();

      verify(realtimeSegment).offload();
      Assert.assertEquals(semaphore.availablePermits(), 1, "Offload must not release the consumer semaphore twice");
      Assert.assertNull(_partitionGroupIdToConsumerCoordinatorMap.get(PARTITION_GROUP_ID).getUnsealedRelease(),
          "Offload must clear the unsealed release");
    }
  }

  @Test
  public void testOffloadReleasesConsumerSemaphoreWhenMetadataRemovalFails()
      throws Exception {
    _partitionGroupIdToConsumerCoordinatorMap.remove(PARTITION_GROUP_ID);
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      Semaphore semaphore = _partitionGroupIdToConsumerCoordinatorMap.get(PARTITION_GROUP_ID).getSemaphore();
      Assert.assertTrue(semaphore.tryAcquire());
      segmentDataManager.getConsumerSemaphoreAcquired().set(true);

      MutableSegmentImpl realtimeSegment = spy((MutableSegmentImpl) segmentDataManager.getSegment());
      doThrow(new RuntimeException("metadata removal failed")).when(realtimeSegment).offload();
      segmentDataManager.setRealtimeSegment(realtimeSegment);

      try {
        segmentDataManager.offload();
        Assert.fail("Expected the metadata removal failure to propagate");
      } catch (RuntimeException e) {
        Assert.assertEquals(e.getMessage(), "metadata removal failed");
      }
      // A failed metadata removal must not leave the semaphore held, or the partition can never consume again.
      Assert.assertEquals(semaphore.availablePermits(), 1,
          "Consumer semaphore must be released even when metadata removal fails");
    }
  }

  @Test
  public void testOnlineTransitionSkipsLocalBuildOnCrcMismatch()
      throws Exception {
    long finalOffsetValue = START_OFFSET_VALUE + 600;

    // Upsert + committed CRC set + local CRC mismatch -> discard local build, download the committed segment.
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      when(segmentDataManager._tableDataManager.isUpsertEnabled()).thenReturn(true);
      runGoOnlineForCrcGuard(segmentDataManager, crcMetadata(finalOffsetValue, 12345L), false, finalOffsetValue);
      Assert.assertTrue(segmentDataManager._buildAndReplaceCalled);
      Assert.assertTrue(segmentDataManager._downloadAndReplaceCalled);
    }

    // Non-upsert + committed CRC set + local CRC mismatch -> discard local build, download the committed segment.
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      when(segmentDataManager._tableDataManager.isUpsertEnabled()).thenReturn(false);
      runGoOnlineForCrcGuard(segmentDataManager, crcMetadata(finalOffsetValue, 12345L), false, finalOffsetValue);
      Assert.assertTrue(segmentDataManager._buildAndReplaceCalled);
      Assert.assertTrue(segmentDataManager._downloadAndReplaceCalled);
    }

    // Upsert + committed CRC set + local CRC match -> build locally, no download, swap in committed metadata.
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      when(segmentDataManager._tableDataManager.isUpsertEnabled()).thenReturn(true);
      SegmentZKMetadata committedMetadata = crcMetadata(finalOffsetValue, 12345L);
      runGoOnlineForCrcGuard(segmentDataManager, committedMetadata, true, finalOffsetValue);
      Assert.assertTrue(segmentDataManager._buildAndReplaceCalled);
      Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
      verify(segmentDataManager._tableDataManager)
          .replaceConsumingSegment(eq(SEGMENT_NAME_STR), same(committedMetadata));
    }

    // Non-upsert + committed CRC set + local CRC match -> build locally, no download, swap in committed metadata.
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      when(segmentDataManager._tableDataManager.isUpsertEnabled()).thenReturn(false);
      SegmentZKMetadata committedMetadata = crcMetadata(finalOffsetValue, 12345L);
      runGoOnlineForCrcGuard(segmentDataManager, committedMetadata, true, finalOffsetValue);
      Assert.assertTrue(segmentDataManager._buildAndReplaceCalled);
      Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
      verify(segmentDataManager._tableDataManager)
          .replaceConsumingSegment(eq(SEGMENT_NAME_STR), same(committedMetadata));
    }

    // Committed CRC unset (-1) -> guard skipped, build locally with construction-time metadata.
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      when(segmentDataManager._tableDataManager.isUpsertEnabled()).thenReturn(false);
      SegmentZKMetadata committedMetadata = crcMetadata(finalOffsetValue, -1L);
      runGoOnlineForCrcGuard(segmentDataManager, committedMetadata, false, finalOffsetValue);
      Assert.assertTrue(segmentDataManager._buildAndReplaceCalled);
      Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
      verify(segmentDataManager._tableDataManager)
          .replaceConsumingSegment(eq(SEGMENT_NAME_STR), argThat(m -> m != committedMetadata));
    }

    // Pauseless table -> guard skipped, build locally with construction-time metadata.
    try (FakeRealtimeSegmentDataManager segmentDataManager =
        createFakeSegmentManager(false, new TimeSupplier(), null, null, createPauselessTableConfig())) {
      when(segmentDataManager._tableDataManager.isUpsertEnabled()).thenReturn(false);
      SegmentZKMetadata committedMetadata = crcMetadata(finalOffsetValue, 12345L);
      runGoOnlineForCrcGuard(segmentDataManager, committedMetadata, false, finalOffsetValue);
      Assert.assertTrue(segmentDataManager._buildAndReplaceCalled);
      Assert.assertFalse(segmentDataManager._downloadAndReplaceCalled);
      verify(segmentDataManager._tableDataManager)
          .replaceConsumingSegment(eq(SEGMENT_NAME_STR), argThat(m -> m != committedMetadata));
    }
  }

  @DataProvider
  public Object[][] unverifiedOnlineAdvances() {
    return new Object[][]{
        {"initial", "match", false}, {"initial", "mismatch", false},
        {"initial", "pauseless", false}, {"initial", "unset", false}, {"initial", "unset", true},
        {"empty", "match", false}, {"empty", "mismatch", false},
        {"empty", "pauseless", false}, {"empty", "unset", false}, {"empty", "unset", true},
        {"message", "match", false}, {"message", "mismatch", false},
        {"message", "pauseless", false}, {"message", "unset", false}
    };
  }

  /// Exercise real consumeLoop and ONLINE replacement, rather than setting or asserting the provenance flag.
  @Test(dataProvider = "unverifiedOnlineAdvances")
  public void testUnverifiedAdvanceRequiresCommittedCrcForOnlineReuse(String advanceKind, String crcPolicy,
      boolean hasDataLoss)
      throws Exception {
    TableConfig tableConfig = "pauseless".equals(crcPolicy) ? createPauselessTableConfig() : null;
    try (FakeRealtimeSegmentDataManager segmentDataManager =
        createFakeSegmentManager(true, new TimeSupplier(), null, null, tableConfig)) {
      long target = START_OFFSET_VALUE + 20;
      if ("initial".equals(advanceKind)) {
        consumeEmptyPosition(segmentDataManager, target, hasDataLoss);
        // A later loop and actual consumer recreation must not erase segment-level provenance.
        consumeEmptyPosition(segmentDataManager, target, false);
        segmentDataManager.invokeRecreateStreamConsumer();
      } else {
        segmentDataManager._stubConsumeLoop = false;
        long nextOffset = hasDataLoss ? target : target + 1;
        MessageBatch<byte[]> batch = "empty".equals(advanceKind)
            ? new ScriptedMessageBatch(nextOffset, hasDataLoss)
            : new ScriptedMessageBatch(nextOffset, false, target);
        segmentDataManager.setPartitionGroupConsumer(
            new ScriptedPartitionGroupConsumer(segmentDataManager, batch, emptyBatch(null)));
      }
      segmentDataManager._useRealBuildAndReplace = true;
      segmentDataManager._localSegmentCrcMatchesZk = !"mismatch".equals(crcPolicy);
      segmentDataManager.getConsumerSemaphoreAcquired().set(true);
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.HOLDING);
      SegmentZKMetadata committed = crcMetadata(target, "unset".equals(crcPolicy) ? -1L : 12345L);

      segmentDataManager.goOnlineFromConsuming(committed);

      assertEquals(((LongMsgOffset) segmentDataManager.getCurrentOffset()).getOffset(), target);
      boolean reuse = "match".equals(crcPolicy);
      assertEquals(segmentDataManager._downloadAndReplaceCalled, !reuse);
      if (reuse) {
        verify(segmentDataManager._tableDataManager).replaceConsumingSegment(eq(SEGMENT_NAME_STR), same(committed));
      } else {
        verify(segmentDataManager._tableDataManager, never()).replaceConsumingSegment(anyString(), any());
      }
    }
  }

  @Test
  public void testKeepCannotRetainAnUnverifiedAdvanceWithoutCommittedMetadata()
      throws Exception {
    try (FakeRealtimeSegmentDataManager segmentDataManager =
        createFakeSegmentManager(true, new TimeSupplier(), null, null, null)) {
      long target = START_OFFSET_VALUE + 20;
      consumeEmptyPosition(segmentDataManager, target, false);
      segmentDataManager._useRealBuildAndReplace = true;
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.HOLDING);
      segmentDataManager._responses.add(new SegmentCompletionProtocol.Response(
          new SegmentCompletionProtocol.Response.Params().withStreamPartitionMsgOffset(Long.toString(target))
              .withStatus(SegmentCompletionProtocol.ControllerResponseStatus.KEEP)));

      segmentDataManager.createPartitionConsumer().run();

      assertEquals(segmentDataManager._state.get(segmentDataManager), RealtimeSegmentDataManager.State.ERROR);
      verify(segmentDataManager._tableDataManager, never()).replaceConsumingSegment(anyString(), any());
      segmentDataManager.goOnlineFromConsuming(crcMetadata(target, 12345L));
      assertTrue(segmentDataManager._downloadAndReplaceCalled);
    }
  }

  @Test
  public void testUnverifiedAdvanceDoesNotHideDownloadFailure()
      throws Exception {
    try (FakeRealtimeSegmentDataManager segmentDataManager =
        createFakeSegmentManager(true, new TimeSupplier(), null, null, null)) {
      long target = START_OFFSET_VALUE + 20;
      consumeEmptyPosition(segmentDataManager, target, false);
      segmentDataManager._useRealBuildAndReplace = true;
      segmentDataManager._useRealDownloadAndReplace = true;
      segmentDataManager._stopWaitTimeMs = 0;
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.HOLDING);
      SegmentZKMetadata committed = crcMetadata(target, -1L);
      doThrow(new IllegalStateException("download failed"))
          .when(segmentDataManager._tableDataManager).downloadAndReplaceConsumingSegment(committed);

      assertThrows(IllegalStateException.class, () -> segmentDataManager.goOnlineFromConsuming(committed));

      assertTrue(segmentDataManager._downloadAndReplaceCalled);
      verify(segmentDataManager._tableDataManager, never()).replaceConsumingSegment(anyString(), any());
    }
  }

  private void consumeEmptyPosition(FakeRealtimeSegmentDataManager segmentDataManager, long nextOffset,
      boolean hasDataLoss)
      throws Exception {
    segmentDataManager._stubConsumeLoop = false;
    segmentDataManager._shouldStop.set(segmentDataManager, false);
    segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.INITIAL_CONSUMING);
    segmentDataManager.setConsumeEndTime(segmentDataManager._timeSupplier.get() + TimeUnit.HOURS.toMillis(1));
    segmentDataManager.setPartitionGroupConsumer(new ScriptedPartitionGroupConsumer(segmentDataManager,
        new ScriptedMessageBatch(nextOffset, hasDataLoss)));
    segmentDataManager.consumeLoop();
    segmentDataManager._shouldStop.set(segmentDataManager, false);
  }

  private void runGoOnlineForCrcGuard(FakeRealtimeSegmentDataManager segmentDataManager, SegmentZKMetadata metadata,
      boolean localCrcMatchesZk, long currentOffsetValue)
      throws Exception {
    segmentDataManager._useRealBuildAndReplace = true;
    segmentDataManager._localSegmentCrcMatchesZk = localCrcMatchesZk;
    segmentDataManager.getConsumerSemaphoreAcquired().set(true);
    segmentDataManager._stopWaitTimeMs = 0;
    segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.HOLDING);
    segmentDataManager.setCurrentOffset(currentOffsetValue);
    segmentDataManager.goOnlineFromConsuming(metadata);
  }

  private SegmentZKMetadata crcMetadata(long endOffsetValue, long crc) {
    SegmentZKMetadata metadata = new SegmentZKMetadata(SEGMENT_NAME_STR);
    metadata.setEndOffset(new LongMsgOffset(endOffsetValue).toString());
    if (crc >= 0) {
      metadata.setCrc(crc);
    }
    return metadata;
  }

  @Test
  public void testDownloadAfterLocalBuildFlagsUpsertRevertWhenConsumingDuringBuild()
      throws Exception {
    long finalOffsetValue = START_OFFSET_VALUE + 600;

    // ALLOW_DURING_BUILD_ONLY: the local build releases the consumer semaphore, its CRC mismatches the committed copy,
    // and the download that follows reverts upsert metadata while the next consuming segment can already be running.
    _partitionGroupIdToConsumerCoordinatorMap.remove(PARTITION_GROUP_ID);
    ServerMetrics serverMetrics = spy(new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry()));
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManagerForUpsertRevert(
        ParallelSegmentConsumptionPolicy.ALLOW_DURING_BUILD_ONLY, serverMetrics)) {
      Semaphore semaphore = _partitionGroupIdToConsumerCoordinatorMap.get(PARTITION_GROUP_ID).getSemaphore();
      Assert.assertTrue(semaphore.tryAcquire());
      // The policy would release, but the replace reverts upsert metadata, so the guard keeps the permit.
      segmentDataManager._releaseSemaphoreOnBuild = true;
      Assert.assertFalse(segmentDataManager.shouldReleaseSemaphoreBeforeBuild());
      runGoOnlineForCrcGuard(segmentDataManager, crcMetadata(finalOffsetValue, 12345L), false, finalOffsetValue);
      Assert.assertTrue(segmentDataManager._downloadAndReplaceCalled);
      Assert.assertEquals(semaphore.availablePermits(), 0);
      verify(serverMetrics, never()).addMeteredTableValue(anyString(),
          eq(ServerMeter.UPSERT_REVERT_WITH_CONSUMPTION_DURING_DOWNLOAD), anyLong());
    }

    // ALLOW_DURING_DOWNLOAD_ONLY: releasedDuringBuild is never true here, so only the override path can report it.
    _partitionGroupIdToConsumerCoordinatorMap.remove(PARTITION_GROUP_ID);
    serverMetrics = spy(new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry()));
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManagerForUpsertRevert(
        ParallelSegmentConsumptionPolicy.ALLOW_DURING_DOWNLOAD_ONLY, serverMetrics)) {
      Semaphore semaphore = _partitionGroupIdToConsumerCoordinatorMap.get(PARTITION_GROUP_ID).getSemaphore();
      Assert.assertTrue(semaphore.tryAcquire());
      segmentDataManager.getConsumerSemaphoreAcquired().set(true);
      Assert.assertFalse(segmentDataManager.shouldReleaseSemaphoreBeforeBuild());
      segmentDataManager.downloadSegmentAndReplace(crcMetadata(finalOffsetValue, 12345L));
      Assert.assertEquals(semaphore.availablePermits(), 0);
      verify(serverMetrics, never()).addMeteredTableValue(anyString(),
          eq(ServerMeter.UPSERT_REVERT_WITH_CONSUMPTION_DURING_DOWNLOAD), anyLong());
    }

    // ALLOW_DURING_BUILD_ONLY without a local build: the download still holds the semaphore, so nothing is flagged.
    _partitionGroupIdToConsumerCoordinatorMap.remove(PARTITION_GROUP_ID);
    serverMetrics = spy(new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry()));
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManagerForUpsertRevert(
        ParallelSegmentConsumptionPolicy.ALLOW_DURING_BUILD_ONLY, serverMetrics)) {
      Semaphore semaphore = _partitionGroupIdToConsumerCoordinatorMap.get(PARTITION_GROUP_ID).getSemaphore();
      Assert.assertTrue(semaphore.tryAcquire());
      segmentDataManager.getConsumerSemaphoreAcquired().set(true);
      segmentDataManager.downloadSegmentAndReplace(crcMetadata(finalOffsetValue, 12345L));
      Assert.assertEquals(semaphore.availablePermits(), 0);
      verify(serverMetrics, never()).addMeteredTableValue(anyString(),
          eq(ServerMeter.UPSERT_REVERT_WITH_CONSUMPTION_DURING_DOWNLOAD), anyLong());
    }

    // DISALLOW_ALWAYS never lets the next segment consume during the replace, even when this segment never acquired the
    // semaphore.
    serverMetrics = spy(new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry()));
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManagerForUpsertRevert(
        ParallelSegmentConsumptionPolicy.DISALLOW_ALWAYS, serverMetrics)) {
      segmentDataManager.downloadSegmentAndReplace(crcMetadata(finalOffsetValue, 12345L));
      verify(serverMetrics, never()).addMeteredTableValue(anyString(),
          eq(ServerMeter.UPSERT_REVERT_WITH_CONSUMPTION_DURING_DOWNLOAD), anyLong());
    }
  }

  private FakeRealtimeSegmentDataManager createFakeSegmentManagerForUpsertRevert(
      ParallelSegmentConsumptionPolicy policy, ServerMetrics serverMetrics)
      throws Exception {
    FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager(false, new TimeSupplier(), null, null,
        createTableConfigWithStreamIngestion(false, policy), serverMetrics);
    PartitionUpsertMetadataManager upsertMetadataManager = mock(PartitionUpsertMetadataManager.class);
    when(upsertMetadataManager.shouldRevertMetadataOnInconsistency(any())).thenReturn(true);
    segmentDataManager.setPartitionUpsertMetadataManager(upsertMetadataManager);
    segmentDataManager._useRealDownloadAndReplace = true;
    return segmentDataManager;
  }

  private TableConfig createPauselessTableConfig()
      throws Exception {
    return createTableConfigWithStreamIngestion(true, null);
  }

  private TableConfig createTableConfigWithStreamIngestion(boolean pauseless,
      @Nullable ParallelSegmentConsumptionPolicy policy)
      throws Exception {
    TableConfig tableConfig = createTableConfig();
    StreamIngestionConfig streamIngestionConfig =
        new StreamIngestionConfig(List.of(tableConfig.getIndexingConfig().getStreamConfigs()));
    streamIngestionConfig.setPauselessConsumptionEnabled(pauseless);
    streamIngestionConfig.setParallelSegmentConsumptionPolicy(policy);
    IngestionConfig ingestionConfig = tableConfig.getIngestionConfig();
    if (ingestionConfig == null) {
      ingestionConfig = new IngestionConfig();
      tableConfig.setIngestionConfig(ingestionConfig);
    }
    ingestionConfig.setStreamIngestionConfig(streamIngestionConfig);
    return tableConfig;
  }

  // Exercises the real CRC comparison (on-disk SegmentMetadataImpl read + hasSameCRC) rather than the stubbed branch.
  @Test
  public void testIsLocalSegmentCrcMatchingZk()
      throws Exception {
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      segmentDataManager._useRealCrcCheck = true;
      File resourceDir = new File(TEMP_DIR, REALTIME_TABLE_NAME);
      long localCrc = buildRealSegmentAndGetCrc(resourceDir, SEGMENT_NAME_STR);

      SegmentZKMetadata matchingMetadata = new SegmentZKMetadata(SEGMENT_NAME_STR);
      matchingMetadata.setCrc(localCrc);
      Assert.assertTrue(segmentDataManager.isLocalSegmentCrcMatchingZk(matchingMetadata));

      SegmentZKMetadata mismatchingMetadata = new SegmentZKMetadata(SEGMENT_NAME_STR);
      mismatchingMetadata.setCrc(localCrc + 1);
      Assert.assertFalse(segmentDataManager.isLocalSegmentCrcMatchingZk(mismatchingMetadata));
    }
  }

  // When the locally-built segment cannot be read (e.g. missing/corrupt), the CRC check must report a mismatch so the
  // ONLINE transition downloads the committed segment instead of throwing.
  @Test
  public void testIsLocalSegmentCrcMatchingZkTreatsUnreadableSegmentAsMismatch()
      throws Exception {
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      segmentDataManager._useRealCrcCheck = true;
      // No segment was built under the resource dir, so the metadata read fails.
      FileUtils.deleteQuietly(new File(new File(TEMP_DIR, REALTIME_TABLE_NAME), SEGMENT_NAME_STR));
      SegmentZKMetadata metadata = new SegmentZKMetadata(SEGMENT_NAME_STR);
      metadata.setCrc(12345L);
      Assert.assertFalse(segmentDataManager.isLocalSegmentCrcMatchingZk(metadata));
    }
  }

  private long buildRealSegmentAndGetCrc(File resourceDir, String segmentName)
      throws Exception {
    Schema schema = Fixtures.createSchema();
    TableConfig tableConfig = createTableConfig();
    SegmentGeneratorConfig generatorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    generatorConfig.setOutDir(resourceDir.getAbsolutePath());
    generatorConfig.setSegmentName(segmentName);
    List<GenericRow> rows = List.of(Fixtures.createSingleRow(1L), Fixtures.createSingleRow(2L));
    try (GenericRowRecordReader recordReader = new GenericRowRecordReader(rows)) {
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(generatorConfig, recordReader);
      driver.build();
    }
    return new SegmentMetadataImpl(new File(resourceDir, segmentName)).getCrc();
  }

  @Test
  public void testEndCriteriaChecking()
      throws Exception {
    // test reaching max row limit
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.INITIAL_CONSUMING);
      Assert.assertFalse(segmentDataManager.invokeEndCriteriaReached());
      segmentDataManager.setNumRowsIndexed(Fixtures.MAX_ROWS_IN_SEGMENT - 1);
      Assert.assertFalse(segmentDataManager.invokeEndCriteriaReached());
      segmentDataManager.setNumRowsIndexed(Fixtures.MAX_ROWS_IN_SEGMENT);
      Assert.assertTrue(segmentDataManager.invokeEndCriteriaReached());
      Assert.assertEquals(segmentDataManager.getStopReason(), SegmentCompletionProtocol.REASON_ROW_LIMIT);
    }
    // test reaching max time limit
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.INITIAL_CONSUMING);
      Assert.assertFalse(segmentDataManager.invokeEndCriteriaReached());
      // We should still get false because there is no messages fetched
      segmentDataManager._timeSupplier.add(Fixtures.MAX_TIME_FOR_SEGMENT_CLOSE_MS + 1);
      Assert.assertFalse(segmentDataManager.invokeEndCriteriaReached());
      // Once there are messages fetched, and the time exceeds the extended hour, we should get true
      setHasMessagesFetched(segmentDataManager, true);
      segmentDataManager._timeSupplier.add(TimeUnit.HOURS.toMillis(1));
      Assert.assertTrue(segmentDataManager.invokeEndCriteriaReached());
      Assert.assertEquals(segmentDataManager.getStopReason(), SegmentCompletionProtocol.REASON_TIME_LIMIT);
    }
    // In catching up state, test reaching final offset
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.CATCHING_UP);
      final long finalOffset = START_OFFSET_VALUE + 100;
      segmentDataManager.setFinalOffset(finalOffset);
      segmentDataManager.setCurrentOffset(finalOffset - 1);
      Assert.assertFalse(segmentDataManager.invokeEndCriteriaReached());
      segmentDataManager.setCurrentOffset(finalOffset);
      Assert.assertTrue(segmentDataManager.invokeEndCriteriaReached());
    }
    // In catching up state, test reaching final offset ignoring time
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      segmentDataManager._timeSupplier.add(Fixtures.MAX_TIME_FOR_SEGMENT_CLOSE_MS);
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.CATCHING_UP);
      final long finalOffset = START_OFFSET_VALUE + 100;
      segmentDataManager.setFinalOffset(finalOffset);
      segmentDataManager.setCurrentOffset(finalOffset - 1);
      Assert.assertFalse(segmentDataManager.invokeEndCriteriaReached());
      segmentDataManager.setCurrentOffset(finalOffset);
      Assert.assertTrue(segmentDataManager.invokeEndCriteriaReached());
    }
    // When we go from consuming to online state, time and final offset matter.
    // Case 1. We have reached final offset.
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      segmentDataManager._timeSupplier.add(1);
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.CONSUMING_TO_ONLINE);
      segmentDataManager.setConsumeEndTime(segmentDataManager._timeSupplier.get() + 10);
      final long finalOffset = START_OFFSET_VALUE + 100;
      segmentDataManager.setFinalOffset(finalOffset);
      segmentDataManager.setCurrentOffset(finalOffset - 1);
      Assert.assertFalse(segmentDataManager.invokeEndCriteriaReached());
      segmentDataManager.setCurrentOffset(finalOffset);
      Assert.assertTrue(segmentDataManager.invokeEndCriteriaReached());
    }
    // Case 2. We have reached time limit.
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager()) {
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.CONSUMING_TO_ONLINE);
      final long endTime = segmentDataManager._timeSupplier.get() + 10;
      segmentDataManager.setConsumeEndTime(endTime);
      final long finalOffset = START_OFFSET_VALUE + 100;
      segmentDataManager.setFinalOffset(finalOffset);
      segmentDataManager.setCurrentOffset(finalOffset - 1);
      segmentDataManager._timeSupplier.set(endTime - 1);
      Assert.assertFalse(segmentDataManager.invokeEndCriteriaReached());
      segmentDataManager._timeSupplier.set(endTime);
      Assert.assertTrue(segmentDataManager.invokeEndCriteriaReached());
    }

    // test end criteria reached if any of the index cannot take more rows
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager(false, new TimeSupplier(), null,
        null, null)) {
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.INITIAL_CONSUMING);
      Assert.assertFalse(segmentDataManager.invokeEndCriteriaReached());

      segmentDataManager.setIndexCapacityThresholdBreached(true);

      Assert.assertTrue(segmentDataManager.invokeEndCriteriaReached());
      Assert.assertEquals(segmentDataManager.getStopReason(),
          SegmentCompletionProtocol.REASON_INDEX_CAPACITY_THRESHOLD_BREACHED);
    }
  }

  private void setHasMessagesFetched(FakeRealtimeSegmentDataManager segmentDataManager, boolean hasMessagesFetched)
      throws Exception {
    Field field = RealtimeSegmentDataManager.class.getDeclaredField("_hasMessagesFetched");
    field.setAccessible(true);
    field.set(segmentDataManager, hasMessagesFetched);
  }

  // If commit fails, make sure that we do not re-build the segment when we try to commit again.
  @Test
  public void testReuseOfBuiltSegment()
      throws Exception {
    FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();
    consumeEmptyPosition(segmentDataManager, START_OFFSET_VALUE + 20, false);

    SegmentCompletionProtocol.Response.Params params = new SegmentCompletionProtocol.Response.Params();
    params.withStatus(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS);
    SegmentCompletionProtocol.Response commitSuccess = new SegmentCompletionProtocol.Response(params);
    params.withStatus(SegmentCompletionProtocol.ControllerResponseStatus.FAILED);
    SegmentCompletionProtocol.Response commitFailed = new SegmentCompletionProtocol.Response(params);

    // Set up the responses so that we get a failed respnse first and then a success response.
    segmentDataManager._responses.add(commitFailed);
    segmentDataManager._responses.add(commitSuccess);
    final long leaseTime = 50000L;

    // The first time we invoke build, it should go ahead and build the segment.
    File segmentTarFile = segmentDataManager.invokeBuildForCommit(leaseTime).getSegmentTarFile();
    Assert.assertNotNull(segmentTarFile);
    Assert.assertTrue(segmentDataManager._buildSegmentCalled);
    Assert.assertFalse(segmentDataManager.invokeCommit());
    Assert.assertTrue(segmentTarFile.exists());

    segmentDataManager._buildSegmentCalled = false;

    // This time around it should not build the segment.
    File segmentTarFile1 = segmentDataManager.invokeBuildForCommit(leaseTime).getSegmentTarFile();
    Assert.assertFalse(segmentDataManager._buildSegmentCalled);
    Assert.assertEquals(segmentTarFile1, segmentTarFile);
    Assert.assertTrue(segmentTarFile.exists());
    Assert.assertTrue(segmentDataManager.invokeCommit());
    Assert.assertFalse(segmentTarFile.exists());
    verify(segmentDataManager._tableDataManager).replaceConsumingSegment(eq(SEGMENT_NAME_STR), any());
    segmentDataManager.close();
  }

  // If commit fails, and we still have the file, make sure that we remove the file when we go
  // online.
  @Test
  public void testFileRemovedDuringOnlineTransition()
      throws Exception {
    FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager();

    SegmentCompletionProtocol.Response.Params params = new SegmentCompletionProtocol.Response.Params();
    params.withStatus(SegmentCompletionProtocol.ControllerResponseStatus.FAILED);
    SegmentCompletionProtocol.Response commitFailed = new SegmentCompletionProtocol.Response(params);

    // Set up the responses so that we get a failed response first and then a success response.
    segmentDataManager._responses.add(commitFailed);
    final long leaseTime = 50000L;
    final long finalOffset = START_OFFSET_VALUE + 600;
    segmentDataManager.setCurrentOffset(finalOffset);

    // We have set up commit to fail, so we should carry over the segment file.
    File segmentTarFile = segmentDataManager.invokeBuildForCommit(leaseTime).getSegmentTarFile();
    Assert.assertNotNull(segmentTarFile);
    Assert.assertTrue(segmentDataManager._buildSegmentCalled);
    Assert.assertFalse(segmentDataManager.invokeCommit());
    Assert.assertTrue(segmentTarFile.exists());

    // Now let the segment go ONLINE from CONSUMING, and ensure that the file is removed.
    SegmentZKMetadata metadata = new SegmentZKMetadata(SEGMENT_NAME_STR);
    metadata.setEndOffset(new LongMsgOffset(finalOffset).toString());
    segmentDataManager._stopWaitTimeMs = 0;
    segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.HOLDING);
    segmentDataManager.goOnlineFromConsuming(metadata);
    Assert.assertFalse(segmentTarFile.exists());
    segmentDataManager.close();
  }

  @Test
  public void testShutdownTableDataManagerWillNotShutdownLeaseExtenderExecutor()
      throws Exception {
    InstanceDataManagerConfig instanceDataManagerConfig = mock(InstanceDataManagerConfig.class);
    when(instanceDataManagerConfig.getInstanceDataDir()).thenReturn(TEMP_DIR.getAbsolutePath());
    when(instanceDataManagerConfig.getUpsertConfig()).thenReturn(new PinotConfiguration());
    when(instanceDataManagerConfig.getDedupConfig()).thenReturn(new PinotConfiguration());
    TableDataManagerProvider tableDataManagerProvider = new DefaultTableDataManagerProvider();
    tableDataManagerProvider.init(instanceDataManagerConfig,
        mock(HelixManager.class),
        new SegmentLocks(),
        null,
        mock(ServerReloadJobStatusCache.class));
    TableConfig tableConfig = createTableConfig();
    Schema schema = Fixtures.createSchema();
    TableDataManager tableDataManager = tableDataManagerProvider.getTableDataManager(tableConfig, schema);
    tableDataManager.start();
    tableDataManager.shutDown();
    Assert.assertFalse(SegmentBuildTimeLeaseExtender.isExecutorShutdown());
  }

  @Test
  public void testShouldNotSkipUnfilteredMessagesIfNotIndexedAndTimeThresholdIsReached()
      throws Exception {
    final int segmentTimeThresholdMins = 10;
    TimeSupplier timeSupplier = new TimeSupplier() {
      @Override
      public Long get() {
        long now = System.currentTimeMillis();
        // now() is called once in the run() method, then once on setting consumeStartTime, once before each batch
        // reading and once for every row indexation
        if (_timeCheckCounter.incrementAndGet() <= FakeStreamConfigUtils.SEGMENT_FLUSH_THRESHOLD_ROWS + 5) {
          return now;
        }
        // Exceed segment time threshold
        return now + TimeUnit.MINUTES.toMillis(segmentTimeThresholdMins + 1);
      }
    };
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager(true, timeSupplier,
        String.valueOf(FakeStreamConfigUtils.SEGMENT_FLUSH_THRESHOLD_ROWS * 2), segmentTimeThresholdMins + "m", null)) {
      segmentDataManager._stubConsumeLoop = false;
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.INITIAL_CONSUMING);

      RealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
      final LongMsgOffset endOffset =
          new LongMsgOffset(START_OFFSET_VALUE + FakeStreamConfigUtils.SEGMENT_FLUSH_THRESHOLD_ROWS);
      segmentDataManager._consumeOffsets.add(endOffset);
      final SegmentCompletionProtocol.Response response = new SegmentCompletionProtocol.Response(
          new SegmentCompletionProtocol.Response.Params().withStatus(
                  SegmentCompletionProtocol.ControllerResponseStatus.COMMIT)
              .withStreamPartitionMsgOffset(endOffset.toString()));
      segmentDataManager._responses.add(response);

      consumer.run();

      // millis() is called first in run before consumption, then once on setting consumeStartTime, then once for
      // each batch and once for each message in the batch, then once more when metrics are updated after each batch
      // is processed and then 4 more times in run() after consume loop
      Assert.assertEquals(timeSupplier._timeCheckCounter.get(), FakeStreamConfigUtils.SEGMENT_FLUSH_THRESHOLD_ROWS + 9);
      Assert.assertEquals(((LongMsgOffset) segmentDataManager.getCurrentOffset()).getOffset(),
          START_OFFSET_VALUE + FakeStreamConfigUtils.SEGMENT_FLUSH_THRESHOLD_ROWS);
      Assert.assertEquals(segmentDataManager.getSegment().getNumDocsIndexed(),
          FakeStreamConfigUtils.SEGMENT_FLUSH_THRESHOLD_ROWS);
      Assert.assertEquals(segmentDataManager.getSegment().getSegmentMetadata().getTotalDocs(),
          FakeStreamConfigUtils.SEGMENT_FLUSH_THRESHOLD_ROWS);
    }
  }

  @Test
  public void testShouldNotSkipUnfilteredMessagesIfNotIndexedAndRowCountThresholdIsReached()
      throws Exception {
    final int segmentTimeThresholdMins = 10;
    TimeSupplier timeSupplier = new TimeSupplier();
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager(true, timeSupplier,
        String.valueOf(FakeStreamConfigUtils.SEGMENT_FLUSH_THRESHOLD_ROWS), segmentTimeThresholdMins + "m", null)) {
      segmentDataManager._stubConsumeLoop = false;
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.INITIAL_CONSUMING);

      RealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
      final LongMsgOffset endOffset =
          new LongMsgOffset(START_OFFSET_VALUE + FakeStreamConfigUtils.SEGMENT_FLUSH_THRESHOLD_ROWS);
      segmentDataManager._consumeOffsets.add(endOffset);
      final SegmentCompletionProtocol.Response response = new SegmentCompletionProtocol.Response(
          new SegmentCompletionProtocol.Response.Params().withStatus(
                  SegmentCompletionProtocol.ControllerResponseStatus.COMMIT)
              .withStreamPartitionMsgOffset(endOffset.toString()));
      segmentDataManager._responses.add(response);

      consumer.run();

      // millis() is called first in run before consumption, then once on setting consumeStartTime, then once for
      // each batch and once for each message in the batch, then once for metrics updates and then 4 more times in
      // run() after consume loop
      Assert.assertEquals(timeSupplier._timeCheckCounter.get(), FakeStreamConfigUtils.SEGMENT_FLUSH_THRESHOLD_ROWS + 7);
      Assert.assertEquals(((LongMsgOffset) segmentDataManager.getCurrentOffset()).getOffset(),
          START_OFFSET_VALUE + FakeStreamConfigUtils.SEGMENT_FLUSH_THRESHOLD_ROWS);
      Assert.assertEquals(segmentDataManager.getSegment().getNumDocsIndexed(),
          FakeStreamConfigUtils.SEGMENT_FLUSH_THRESHOLD_ROWS);
      Assert.assertEquals(segmentDataManager.getSegment().getSegmentMetadata().getTotalDocs(),
          FakeStreamConfigUtils.SEGMENT_FLUSH_THRESHOLD_ROWS);
    }
  }

  @Test
  public void testServerIngestionOomProtectionWaitsAndResumesWhileInitialConsuming()
      throws Exception {
    TimeSupplier timeSupplier = new TimeSupplier();
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager(true, timeSupplier,
        String.valueOf(FakeStreamConfigUtils.SEGMENT_FLUSH_THRESHOLD_ROWS), "10m", null)) {
      segmentDataManager._stubConsumeLoop = false;
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.INITIAL_CONSUMING);

      ServerIngestionOomProtectionManager oomProtectionManager = mock(ServerIngestionOomProtectionManager.class);
      when(oomProtectionManager.waitIfProtectionNeeded(any())).thenReturn(true, false);
      segmentDataManager.setServerIngestionOomProtectionManager(oomProtectionManager);

      RealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
      final LongMsgOffset endOffset =
          new LongMsgOffset(START_OFFSET_VALUE + FakeStreamConfigUtils.SEGMENT_FLUSH_THRESHOLD_ROWS);
      segmentDataManager._consumeOffsets.add(endOffset);
      SegmentCompletionProtocol.Response response = new SegmentCompletionProtocol.Response(
          new SegmentCompletionProtocol.Response.Params().withStatus(
                  SegmentCompletionProtocol.ControllerResponseStatus.COMMIT)
              .withStreamPartitionMsgOffset(endOffset.toString()));
      segmentDataManager._responses.add(response);

      consumer.run();

      verify(oomProtectionManager, atLeast(1)).waitIfProtectionNeeded(any());
      Assert.assertEquals(segmentDataManager.getSegment().getNumDocsIndexed(),
          FakeStreamConfigUtils.SEGMENT_FLUSH_THRESHOLD_ROWS);
    }
  }

  @Test
  public void testServerIngestionOomProtectionStopPredicateDoesNotMutateEndCriteria()
      throws Exception {
    TimeSupplier timeSupplier = new TimeSupplier();
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager(true, timeSupplier,
        String.valueOf(FakeStreamConfigUtils.SEGMENT_FLUSH_THRESHOLD_ROWS), "10m", null)) {
      segmentDataManager._stubConsumeLoop = false;
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.INITIAL_CONSUMING);

      ServerIngestionOomProtectionManager oomProtectionManager = mock(ServerIngestionOomProtectionManager.class);
      when(oomProtectionManager.waitIfProtectionNeeded(any())).thenAnswer(invocation -> {
        BooleanSupplier stopCondition = invocation.getArgument(0);
        long consumeEndTime = segmentDataManager.getConsumeEndTime();
        timeSupplier.set(consumeEndTime);

        Assert.assertTrue(stopCondition.getAsBoolean());
        Assert.assertEquals(segmentDataManager.getConsumeEndTime(), consumeEndTime);
        return true;
      });
      segmentDataManager.setServerIngestionOomProtectionManager(oomProtectionManager);

      RealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
      final LongMsgOffset endOffset =
          new LongMsgOffset(START_OFFSET_VALUE + FakeStreamConfigUtils.SEGMENT_FLUSH_THRESHOLD_ROWS);
      segmentDataManager._consumeOffsets.add(endOffset);
      SegmentCompletionProtocol.Response response = new SegmentCompletionProtocol.Response(
          new SegmentCompletionProtocol.Response.Params().withStatus(
                  SegmentCompletionProtocol.ControllerResponseStatus.COMMIT)
              .withStreamPartitionMsgOffset(endOffset.toString()));
      segmentDataManager._responses.add(response);

      consumer.run();

      verify(oomProtectionManager, atLeast(1)).waitIfProtectionNeeded(any());
      Assert.assertEquals(segmentDataManager.getSegment().getNumDocsIndexed(),
          FakeStreamConfigUtils.SEGMENT_FLUSH_THRESHOLD_ROWS);
    }
  }

  @Test
  public void testServerIngestionOomProtectionIsSkippedWhileCatchingUp()
      throws Exception {
    TimeSupplier timeSupplier = new TimeSupplier();
    try (FakeRealtimeSegmentDataManager segmentDataManager = createFakeSegmentManager(true, timeSupplier,
        String.valueOf(FakeStreamConfigUtils.SEGMENT_FLUSH_THRESHOLD_ROWS * 2), "10m", null)) {
      segmentDataManager._stubConsumeLoop = false;
      segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.CATCHING_UP);
      LongMsgOffset finalOffset =
          new LongMsgOffset(START_OFFSET_VALUE + FakeStreamConfigUtils.SEGMENT_FLUSH_THRESHOLD_ROWS);
      Field finalOffsetField = RealtimeSegmentDataManager.class.getDeclaredField("_finalOffset");
      finalOffsetField.setAccessible(true);
      finalOffsetField.set(segmentDataManager, finalOffset);

      ServerIngestionOomProtectionManager oomProtectionManager = mock(ServerIngestionOomProtectionManager.class);
      when(oomProtectionManager.waitIfProtectionNeeded(any())).thenReturn(true);
      segmentDataManager.setServerIngestionOomProtectionManager(oomProtectionManager);

      RealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
      SegmentCompletionProtocol.Response response = new SegmentCompletionProtocol.Response(
          new SegmentCompletionProtocol.Response.Params().withStatus(
                  SegmentCompletionProtocol.ControllerResponseStatus.COMMIT)
              .withStreamPartitionMsgOffset(finalOffset.toString()));
      segmentDataManager._responses.add(response);

      consumer.run();

      verify(oomProtectionManager, never()).waitIfProtectionNeeded(any());
      Assert.assertEquals(((LongMsgOffset) segmentDataManager.getCurrentOffset()).getOffset(),
          START_OFFSET_VALUE + FakeStreamConfigUtils.SEGMENT_FLUSH_THRESHOLD_ROWS);
    }
  }

  @Test
  public void testCompletionModeDownloadWithUrlValidation()
      throws Exception {
    // Test the new validation logic for CompletionMode.DOWNLOAD

    // Test Case 1: Valid download URL - should proceed with DOWNLOAD mode
    testCompletionModeDownloadWithValidUrl();

    // Test Case 2: Missing/null download URL - should fall back to RETAINING mode
    testCompletionModeDownloadWithMissingUrl();

    // Test Case 3: Empty download URL - should fall back to RETAINING mode
    testCompletionModeDownloadWithEmptyUrl();
  }

  private void testCompletionModeDownloadWithValidUrl()
      throws Exception {
    String downloadUrl = "http://example.com/segment.tar.gz";
    TestResult expectedResult = new TestResult(
        RealtimeSegmentDataManager.State.DISCARDED, false);

    runCompletionModeDownloadTest(downloadUrl, downloadUrl, expectedResult);
  }

  private void testCompletionModeDownloadWithMissingUrl()
      throws Exception {
    TestResult expectedResult = new TestResult(
        RealtimeSegmentDataManager.State.RETAINED, true);

    runCompletionModeDownloadTest(null, null, expectedResult);
  }

  private void testCompletionModeDownloadWithEmptyUrl()
      throws Exception {
    TestResult expectedResult = new TestResult(
        RealtimeSegmentDataManager.State.RETAINED, true);

    runCompletionModeDownloadTest("", "", expectedResult);
  }

  /// Helper method to run completion mode download tests with different download URL scenarios.
  ///
  /// @param freshMetadataDownloadUrl The download URL returned by fetchZKMetadata (fresh metadata)
  /// @param initialSegmentDownloadUrl The download URL in the initial segment metadata
  /// @param expectedResult The expected test result (state and buildAndReplaceCalled flag)
  private void runCompletionModeDownloadTest(String freshMetadataDownloadUrl,
      String initialSegmentDownloadUrl, TestResult expectedResult)
      throws Exception {

    // Create table config with DOWNLOAD completion mode
    TableConfig tableConfig = createTableConfigWithDownloadCompletionMode();

    // Create mock table data manager with the specified download URL
    RealtimeTableDataManager mockTableDataManager = createMockTableDataManager(freshMetadataDownloadUrl);

    // Create segment data manager with DOWNLOAD completion mode
    SegmentZKMetadata segmentZKMetadata = createZkMetadata();
    if (initialSegmentDownloadUrl != null) {
      segmentZKMetadata.setDownloadUrl(initialSegmentDownloadUrl);
    }

    LLCSegmentName llcSegmentName = new LLCSegmentName(SEGMENT_NAME_STR);
    Schema schema = Fixtures.createSchema();
    ServerMetrics serverMetrics = new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry());

    try (FakeRealtimeSegmentDataManager segmentDataManager =
        new FakeRealtimeSegmentDataManager(segmentZKMetadata, tableConfig, mockTableDataManager,
            new File(TEMP_DIR, REALTIME_TABLE_NAME).getAbsolutePath(), schema, llcSegmentName,
            _partitionGroupIdToConsumerCoordinatorMap, serverMetrics, new TimeSupplier())) {

      // Execute the consumer with KEEP response
      executeConsumerWithKeepResponse(segmentDataManager);

      // Verify the expected results
      Assert.assertEquals(segmentDataManager._state.get(segmentDataManager),
          expectedResult._expectedState);
      Assert.assertEquals(segmentDataManager._buildAndReplaceCalled,
          expectedResult._expectedBuildAndReplaceCalled);
      Assert.assertTrue(segmentDataManager._responses.isEmpty());
      Assert.assertTrue(segmentDataManager._consumeOffsets.isEmpty());
    }
  }

  /// Creates a table config with DOWNLOAD completion mode.
  private TableConfig createTableConfigWithDownloadCompletionMode()
      throws Exception {
    TableConfig tableConfig = createTableConfig();
    tableConfig.getValidationConfig().setCompletionConfig(
        new org.apache.pinot.spi.config.table.CompletionConfig("DOWNLOAD"));
    return tableConfig;
  }

  /// Creates a mock table data manager that returns the specified download URL in fresh metadata.
  private RealtimeTableDataManager createMockTableDataManager(String downloadUrl) {
    RealtimeTableDataManager mockTableDataManager = mock(RealtimeTableDataManager.class);
    when(mockTableDataManager.getInstanceId()).thenReturn("server-1");
    when(mockTableDataManager.getSegmentLock(any())).thenReturn(mock(Lock.class));

    // Mock fetchZKMetadata to return metadata with the specified download URL
    SegmentZKMetadata metadata = createZkMetadata();
    metadata.setDownloadUrl(downloadUrl);
    when(mockTableDataManager.fetchZKMetadata(SEGMENT_NAME_STR)).thenReturn(metadata);

    // Set up stats history and consumer directory
    RealtimeSegmentStatsHistory statsHistory = mock(RealtimeSegmentStatsHistory.class);
    when(statsHistory.getEstimatedCardinality(anyString())).thenReturn(200);
    when(statsHistory.getEstimatedAvgColSize(anyString())).thenReturn(32);
    when(mockTableDataManager.getStatsHistory()).thenReturn(statsHistory);
    when(mockTableDataManager.getConsumerDir()).thenReturn(TEMP_DIR.getAbsolutePath() + "/consumerDir");

    // Set up consumer coordinator in the map
    _partitionGroupIdToConsumerCoordinatorMap.putIfAbsent(PARTITION_GROUP_ID,
        new ConsumerCoordinator(false, mockTableDataManager));

    return mockTableDataManager;
  }

  /// Executes the consumer with a KEEP response to trigger the completion mode logic.
  private void executeConsumerWithKeepResponse(FakeRealtimeSegmentDataManager segmentDataManager) {
    RealtimeSegmentDataManager.PartitionConsumer consumer = segmentDataManager.createPartitionConsumer();
    final LongMsgOffset endOffset = new LongMsgOffset(START_OFFSET_VALUE + 500);
    segmentDataManager._consumeOffsets.add(endOffset);

    final SegmentCompletionProtocol.Response response = new SegmentCompletionProtocol.Response(
        new SegmentCompletionProtocol.Response.Params().withStatus(
                SegmentCompletionProtocol.ControllerResponseStatus.KEEP)
            .withStreamPartitionMsgOffset(endOffset.toString()));
    segmentDataManager._responses.add(response);

    consumer.run();
  }

  /// Helper class to encapsulate expected test results for segment state transitions.
  ///
  /// This class holds the expected state of a [RealtimeSegmentDataManager] after a test,
  /// as well as whether the build-and-replace operation was expected to be called.
  private static class TestResult {
    /// The expected state of the [RealtimeSegmentDataManager] after the test execution.
    final RealtimeSegmentDataManager.State _expectedState;
    /// Whether the build-and-replace operation was expected to be called during the test.
    final boolean _expectedBuildAndReplaceCalled;

    /// Constructs a TestResult with the expected state and build-and-replace flag.
    ///
    /// @param expectedState The expected state of the segment manager after the test.
    /// @param expectedBuildAndReplaceCalled Whether build-and-replace was expected to be called.
    TestResult(RealtimeSegmentDataManager.State expectedState, boolean expectedBuildAndReplaceCalled) {
      _expectedState = expectedState;
      _expectedBuildAndReplaceCalled = expectedBuildAndReplaceCalled;
    }
  }

  private static class TimeSupplier implements Supplier<Long> {
    protected final AtomicInteger _timeCheckCounter = new AtomicInteger();
    protected long _timeNow = System.currentTimeMillis();

    @Override
    public Long get() {
      _timeCheckCounter.incrementAndGet();
      return _timeNow;
    }

    public void set(long millis) {
      _timeNow = millis;
    }

    public void add(long millis) {
      _timeNow += millis;
    }
  }

  // Implementing Closeable to make it easier to use in try-with-resources
  public static class FakeRealtimeSegmentDataManager extends RealtimeSegmentDataManager implements Closeable {

    public Field _state;
    public Field _shouldStop;
    public Field _stopReasonCode;
    public Field _segmentBuildFailedWithDeterministicError;
    public boolean _failSegmentBuildAndReplace = false;
    // When set, buildSegmentAndReplace runs the real implementation (including the upsert CRC guard) instead of being
    // short-circuited, and isLocalSegmentCrcMatchingZk returns _localSegmentCrcMatchesZk.
    public boolean _useRealBuildAndReplace = false;
    public boolean _localSegmentCrcMatchesZk = true;
    // When set, isLocalSegmentCrcMatchingZk runs the real on-disk metadata read + CRC comparison.
    public boolean _useRealCrcCheck = false;
    // When set, buildSegmentInternal releases the consumer semaphore like the real build does when the parallel
    // consumption policy allows consuming during build.
    public boolean _releaseSemaphoreOnBuild = false;
    // When set, downloadSegmentAndReplace runs the real implementation against the mocked table data manager.
    public boolean _useRealDownloadAndReplace = false;
    private Field _streamMsgOffsetFactory;
    public LinkedList<LongMsgOffset> _consumeOffsets = new LinkedList<>();
    public LinkedList<SegmentCompletionProtocol.Response> _responses = new LinkedList<>();
    public boolean _commitSegmentCalled = false;
    public boolean _buildSegmentCalled = false;
    public boolean _failSegmentBuild = false;
    public boolean _buildAndReplaceCalled = false;
    public int _stopWaitTimeMs = 100;
    private boolean _downloadAndReplaceCalled = false;
    private boolean _notifySegmentBuildFailedWithDeterministicErrorCalled = false;
    public boolean _throwExceptionFromConsume = false;
    public boolean _postConsumeStoppedCalled = false;
    public int _postConsumeStoppedAttempts = 0;
    public SegmentCompletionProtocol.ControllerResponseStatus _stopConsumedResponseStatus =
        SegmentCompletionProtocol.ControllerResponseStatus.PROCESSED;
    public Map<Integer, ConsumerCoordinator> _consumerCoordinatorMap;
    public boolean _stubConsumeLoop = true;
    public RealtimeTableDataManager _tableDataManager;
    private TimeSupplier _timeSupplier;
    private boolean _indexCapacityThresholdBreached;

    private static InstanceDataManagerConfig makeInstanceDataManagerConfig() {
      InstanceDataManagerConfig dataManagerConfig = mock(InstanceDataManagerConfig.class);
      when(dataManagerConfig.getReadMode()).thenReturn(null);
      when(dataManagerConfig.getAvgMultiValueCount()).thenReturn(null);
      when(dataManagerConfig.getSegmentFormatVersion()).thenReturn(null);
      when(dataManagerConfig.isRealtimeOffHeapAllocation()).thenReturn(false);
      when(dataManagerConfig.getConfig()).thenReturn(new PinotConfiguration());
      return dataManagerConfig;
    }

    public FakeRealtimeSegmentDataManager(SegmentZKMetadata segmentZKMetadata, TableConfig tableConfig,
        RealtimeTableDataManager realtimeTableDataManager, String resourceDataDir, Schema schema,
        LLCSegmentName llcSegmentName, Map<Integer, ConsumerCoordinator> consumerCoordinatorMap,
        ServerMetrics serverMetrics, TimeSupplier timeSupplier)
        throws Exception {
      super(segmentZKMetadata, tableConfig, realtimeTableDataManager, resourceDataDir,
          new IndexLoadingConfig(makeInstanceDataManagerConfig(), tableConfig), schema, llcSegmentName,
          consumerCoordinatorMap.get(llcSegmentName.getPartitionGroupId()), serverMetrics, null, null,
          () -> true);
      _tableDataManager = realtimeTableDataManager;
      _state = RealtimeSegmentDataManager.class.getDeclaredField("_state");
      _state.setAccessible(true);
      _shouldStop = RealtimeSegmentDataManager.class.getDeclaredField("_shouldStop");
      _shouldStop.setAccessible(true);
      _stopReasonCode = RealtimeSegmentDataManager.class.getDeclaredField("_stopReasonCode");
      _stopReasonCode.setAccessible(true);
      _segmentBuildFailedWithDeterministicError =
          RealtimeSegmentDataManager.class.getDeclaredField("_segmentBuildFailedWithDeterministicError");
      _segmentBuildFailedWithDeterministicError.setAccessible(true);
      _consumerCoordinatorMap = consumerCoordinatorMap;
      _streamMsgOffsetFactory = RealtimeSegmentDataManager.class.getDeclaredField("_streamPartitionMsgOffsetFactory");
      _streamMsgOffsetFactory.setAccessible(true);
      _streamMsgOffsetFactory.set(this, new LongMsgOffsetFactory());
      _timeSupplier = timeSupplier;
    }

    public RealtimeTableDataManager getTableDataManager() {
      return _tableDataManager;
    }

    /// Replaces the mutable segment so tests can observe or fail its offload().
    public void setRealtimeSegment(MutableSegmentImpl realtimeSegment)
        throws Exception {
      Field realtimeSegmentField = RealtimeSegmentDataManager.class.getDeclaredField("_realtimeSegment");
      realtimeSegmentField.setAccessible(true);
      realtimeSegmentField.set(this, realtimeSegment);
    }

    /// Replaces the stream consumer so tests can script the batches consumeLoop() sees.
    public void setPartitionGroupConsumer(PartitionGroupConsumer partitionGroupConsumer)
        throws Exception {
      Field partitionGroupConsumerField = RealtimeSegmentDataManager.class.getDeclaredField("_partitionGroupConsumer");
      partitionGroupConsumerField.setAccessible(true);
      partitionGroupConsumerField.set(this, partitionGroupConsumer);
    }

    public void invokeRecreateStreamConsumer()
        throws Exception {
      Method recreate = RealtimeSegmentDataManager.class.getDeclaredMethod("recreateStreamConsumer", String.class);
      recreate.setAccessible(true);
      recreate.invoke(this, "test segment provenance survives recreation");
    }

    /// Replaces the upsert metadata manager, which the constructor always leaves null.
    public void setPartitionUpsertMetadataManager(PartitionUpsertMetadataManager partitionUpsertMetadataManager)
        throws Exception {
      Field partitionUpsertMetadataManagerField =
          RealtimeSegmentDataManager.class.getDeclaredField("_partitionUpsertMetadataManager");
      partitionUpsertMetadataManagerField.setAccessible(true);
      partitionUpsertMetadataManagerField.set(this, partitionUpsertMetadataManager);
    }

    public boolean isStreamConsumerClosed()
        throws Exception {
      Field streamConsumerClosedField = RealtimeSegmentDataManager.class.getDeclaredField("_streamConsumerClosed");
      streamConsumerClosedField.setAccessible(true);
      return ((AtomicBoolean) streamConsumerClosedField.get(this)).get();
    }

    public String getStopReason() {
      try {
        SegmentCompletionProtocol.ReasonCode stopReasonCode =
            (SegmentCompletionProtocol.ReasonCode) _stopReasonCode.get(this);
        return stopReasonCode != null ? stopReasonCode.getReason() : null;
      } catch (Exception e) {
        Assert.fail();
      }
      return null;
    }

    public PartitionConsumer createPartitionConsumer() {
      return new PartitionConsumer();
    }

    public SegmentBuildDescriptor invokeBuildForCommit(long leaseTime)
        throws SegmentBuildFailureException {
      super.buildSegmentForCommit(leaseTime);
      return getSegmentBuildDescriptor();
    }

    public boolean invokeCommit()
        throws Exception {
      return super.commitSegment("dummyUrl");
    }

    private void terminateLoopIfNecessary() {
      if (_consumeOffsets.isEmpty() && _responses.isEmpty()) {
        setShouldStop();
      }
    }

    private void setShouldStop() {
      try {
        _shouldStop.set(this, true);
      } catch (Exception e) {
        Assert.fail();
      }
    }

    @Override
    public void startConsumption() {
      // Do nothing.
    }

    @Override
    protected boolean consumeLoop()
        throws Exception {
      if (_stubConsumeLoop) {
        if (_throwExceptionFromConsume) {
          throw new PermanentConsumerException(new Throwable("Offset out of range"));
        }
        setCurrentOffset(_consumeOffsets.remove().getOffset());
        terminateLoopIfNecessary();
        return true;
      }
      return super.consumeLoop();
    }

    @Override
    protected SegmentCompletionProtocol.Response postSegmentConsumedMsg() {
      return _responses.remove();
    }

    @Override
    protected SegmentCompletionProtocol.Response commit(String controllerVipUrl) {
      return _responses.remove();
    }

    @Override
    protected void postStopConsumedMsg(String reason) {
      _postConsumeStoppedCalled = true;
    }

    public void invokePostStopConsumedMsg(String reason) {
      super.postStopConsumedMsg(reason);
    }

    /// Runs the production stop() instead of this fake's override.
    public void invokeRealStop()
        throws InterruptedException {
      super.stop();
    }

    @Override
    SegmentCompletionProtocol.Response postSegmentStoppedConsuming(ConsumptionStopIndicator indicator) {
      _postConsumeStoppedCalled = true;
      _postConsumeStoppedAttempts++;
      return new SegmentCompletionProtocol.Response(
          new SegmentCompletionProtocol.Response.Params().withStatus(_stopConsumedResponseStatus));
    }

    @Override
    protected void notifySegmentBuildFailedWithDeterministicError() {
      _notifySegmentBuildFailedWithDeterministicErrorCalled = true;
    }


    // TODO: Some of the tests rely on specific number of calls to the `now()` method in the SegmentDataManager.
    // This is not a good coding practice and makes the code very fragile. This needs to be fixed.
    // Invoking now() in any part of RealtimeSegmentDataManager code will break the following tests:
    // 1. RealtimeSegmentDataManagerTest.testShouldNotSkipUnfilteredMessagesIfNotIndexedAndRowCountThresholdIsReached
    // 2. RealtimeSegmentDataManagerTest.testShouldNotSkipUnfilteredMessagesIfNotIndexedAndTimeThresholdIsReached
    @Override
    protected long now() {
      // now() is called in the constructor before _timeSupplier is set
      if (_timeSupplier == null) {
        return System.currentTimeMillis();
      }
      return _timeSupplier.get();
    }

    @Override
    protected void hold() {
      terminateLoopIfNecessary();
      _timeSupplier.add(5000L);
    }

    @Override
    protected boolean buildSegmentAndReplace(SegmentZKMetadata committedSegmentZKMetadata)
        throws Exception {
      terminateLoopIfNecessary();
      _buildAndReplaceCalled = true;
      if (_useRealBuildAndReplace) {
        return super.buildSegmentAndReplace(committedSegmentZKMetadata);
      }
      return !_failSegmentBuildAndReplace;
    }

    @Override
    protected boolean isLocalSegmentCrcMatchingZk(SegmentZKMetadata committedSegmentZKMetadata) {
      if (_useRealCrcCheck) {
        return super.isLocalSegmentCrcMatchingZk(committedSegmentZKMetadata);
      }
      return _localSegmentCrcMatchesZk;
    }

    @Override
    protected SegmentBuildDescriptor buildSegmentInternal(boolean forCommit) {
      terminateLoopIfNecessary();
      _buildSegmentCalled = true;
      // Through the real predicate, so the guard is exercised rather than bypassed.
      if (_releaseSemaphoreOnBuild && shouldReleaseSemaphoreBeforeBuild()) {
        closeStreamConsumerAndReleaseSemaphore();
      }
      if (_failSegmentBuild) {
        try {
          _segmentBuildFailedWithDeterministicError.set(this, true);
        } catch (Exception e) {
          Assert.fail();
        }
        return null;
      }
      if (!forCommit) {
        return new SegmentBuildDescriptor(null, null, getCurrentOffset(), 0, 0, -1);
      }
      File segmentTarFile = new File(new File(TEMP_DIR, REALTIME_TABLE_NAME), "segmentFile");
      try {
        segmentTarFile.createNewFile();
      } catch (IOException e) {
        Assert.fail("Could not create file " + segmentTarFile);
      }
      return new SegmentBuildDescriptor(segmentTarFile, null, getCurrentOffset(), 0, 0, -1);
    }

    @Override
    protected boolean commitSegment(String controllerVipUrl) {
      terminateLoopIfNecessary();
      _commitSegmentCalled = true;
      return true;
    }

    @Override
    protected void downloadSegmentAndReplace(SegmentZKMetadata metadata)
        throws Exception {
      terminateLoopIfNecessary();
      _downloadAndReplaceCalled = true;
      if (_useRealDownloadAndReplace) {
        super.downloadSegmentAndReplace(metadata);
      }
    }

    @Override
    public void stop() {
      setShouldStop();
      _timeSupplier.add(_stopWaitTimeMs);
    }

    public void setCurrentOffset(long offset) {
      setOffset(offset, "_currentOffset");
    }

    public void setConsumeEndTime(long endTime) {
      setLong(endTime, "_consumeEndTime");
    }

    public long getConsumeEndTime() {
      return getLong("_consumeEndTime");
    }

    public void setNumRowsConsumed(int numRows) {
      setInt(numRows, "_numRowsConsumed");
    }

    public void setNumRowsIndexed(int numRows) {
      setInt(numRows, "_numRowsIndexed");
    }

    public void setFinalOffset(long offset) {
      setOffset(offset, "_finalOffset");
    }

    @Override
    protected boolean canAddMore() {
      return !_indexCapacityThresholdBreached;
    }

    public void setIndexCapacityThresholdBreached(boolean indexCapacityThresholdBreached) {
      _indexCapacityThresholdBreached = indexCapacityThresholdBreached;
    }

    public boolean invokeEndCriteriaReached() {
      Method endCriteriaReached = null;
      try {
        endCriteriaReached = RealtimeSegmentDataManager.class.getDeclaredMethod("endCriteriaReached");
        endCriteriaReached.setAccessible(true);
        Boolean result = (Boolean) endCriteriaReached.invoke(this);
        return result;
      } catch (NoSuchMethodException e) {
        Assert.fail();
      } catch (InvocationTargetException e) {
        Assert.fail();
      } catch (IllegalAccessException e) {
        Assert.fail();
      }
      throw new RuntimeException("Cannot get here");
    }

    public void setSegmentMaxRowCount(int numRows) {
      setInt(numRows, "_segmentMaxRowCount");
    }

    private void setLong(long value, String fieldName) {
      try {
        Field field = RealtimeSegmentDataManager.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.setLong(this, value);
      } catch (NoSuchFieldException e) {
        Assert.fail();
      } catch (IllegalAccessException e) {
        Assert.fail();
      }
    }

    private long getLong(String fieldName) {
      try {
        Field field = RealtimeSegmentDataManager.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.getLong(this);
      } catch (NoSuchFieldException e) {
        Assert.fail();
      } catch (IllegalAccessException e) {
        Assert.fail();
      }
      throw new RuntimeException("Cannot get here");
    }

    private void setOffset(long value, String fieldName) {
      try {
        Field field = RealtimeSegmentDataManager.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        StreamPartitionMsgOffset offset = (StreamPartitionMsgOffset) field.get(this);
//        if (offset == null) {
        field.set(this, new LongMsgOffset(value));
//        } else {
//          offset.setOffset(value);
//        }
      } catch (NoSuchFieldException e) {
        Assert.fail();
      } catch (IllegalAccessException e) {
        Assert.fail();
      }
    }

    private void setInt(int value, String fieldName) {
      try {
        Field field = RealtimeSegmentDataManager.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.setInt(this, value);
      } catch (NoSuchFieldException e) {
        Assert.fail();
      } catch (IllegalAccessException e) {
        Assert.fail();
      }
    }

    @Override
    public void close() {
      offload();
      destroy();
    }
  }
}
