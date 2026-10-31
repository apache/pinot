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
import java.net.ConnectException;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConsumerFactory;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentStatsHistory;
import org.apache.pinot.segment.local.segment.creator.Fixtures;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorClassifier;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorReporter;
import org.apache.pinot.spi.ingestion.error.RefusedBySourceException;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.stream.BytesStreamMessage;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.PermanentConsumerException;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Which stream failures the consume loop records, and when: a refusal at once, an unreadable source only once Pinot
/// gives up on it.
public class RealtimeSegmentDataManagerSourceErrorTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), "RealtimeSegmentDataManagerSourceErrorTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME);
  private static final LLCSegmentName SEGMENT_NAME = new LLCSegmentName(RAW_TABLE_NAME, 0, 945, 98347869999L);
  private static final String AUTH_GAUGE =
      ServerGauge.INGESTION_SOURCE_AUTH_FAILURE_TIME_SECONDS.getGaugeName() + "." + REALTIME_TABLE_NAME + ".0";
  private static final String READ_GAUGE =
      ServerGauge.INGESTION_SOURCE_READ_FAILURE_TIME_SECONDS.getGaugeName() + "." + REALTIME_TABLE_NAME + ".0";
  private static final String REFUSED = "refused";
  private static final String MALFORMED = "malformed";
  // Stands in for a stream config value a decoder's exception can quote
  private static final String DECODER_CANARY = "fake-canary-value";

  /// What each fetch does, in order: throw a failure, or return a batch of these payloads (empty for none). A
  /// runnable runs during the fetch, before the next step decides it.
  private static final Deque<Object> SCRIPT = new ConcurrentLinkedDeque<>();
  private static volatile boolean _decoderInitRefused;
  private static volatile boolean _checkpointReportsRefusal;
  private static volatile boolean _createReportsRefusal;
  // Runs when a consumer is created, as the segment is told to stop mid-creation
  private static volatile Runnable _duringCreate;

  public static class ScriptedConsumerFactory extends FakeStreamConsumerFactory {
    @Override
    public PartitionGroupConsumer createPartitionGroupConsumer(String clientId,
        PartitionGroupConsumptionStatus partitionGroupConsumptionStatus) {
      Runnable duringCreate = _duringCreate;
      if (duringCreate != null) {
        // Interrupted retries: the plugin reports the failure its last attempt saw, and gives up
        duringCreate.run();
        IngestionSourceErrorReporter.report(new ConnectException("Connection refused"));
        throw new IllegalStateException("Failed to create the consumer");
      }
      if (_createReportsRefusal) {
        // A plugin that reports the failure it retried and throws without it, as Kafka's consumer creation does
        IngestionSourceErrorReporter.report(new RefusedBySourceException());
        throw new IllegalStateException("Failed to create the consumer");
      }
      return new PartitionGroupConsumer() {
        @Override
        public MessageBatch fetchMessages(StreamPartitionMsgOffset startOffset, int timeoutMs) {
          Object step = SCRIPT.poll();
          if (step instanceof Runnable) {
            ((Runnable) step).run();
            step = SCRIPT.poll();
          }
          if (step == null) {
            throw new PermanentConsumerException(new IllegalStateException("End of the test script"));
          }
          if (step instanceof Throwable) {
            // As thrown by the client, checked or not: a raw SocketTimeoutException reaches the consume loop today.
            throw RealtimeSegmentDataManagerSourceErrorTest.<RuntimeException>sneaky((Throwable) step);
          }
          @SuppressWarnings("unchecked")
          List<String> payloads = (List<String>) step;
          return batch(payloads, ((LongMsgOffset) startOffset).getOffset());
        }

        @Override
        public StreamPartitionMsgOffset checkpoint(StreamPartitionMsgOffset lastOffset) {
          // A plugin that validates on checkpoint and swallows what it finds, as Pub/Sub's snapshot validator does
          if (_checkpointReportsRefusal) {
            IngestionSourceErrorReporter.report(new RefusedBySourceException());
          }
          return lastOffset;
        }

        @Override
        public void close() {
        }
      };
    }
  }

  /// Fails a row whose payload says so: a registry refusal, or bytes that won't parse.
  public static class ScriptedDecoder implements StreamMessageDecoder<byte[]> {
    @Override
    public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName) {
      if (_decoderInitRefused) {
        throw new IllegalStateException("Registry refused " + DECODER_CANARY, new RefusedBySourceException());
      }
    }

    @Override
    public GenericRow decode(byte[] payload, GenericRow destination) {
      String value = new String(payload, StandardCharsets.UTF_8);
      if (value.equals(REFUSED)) {
        throw new RefusedBySourceException();
      }
      if (value.equals(MALFORMED)) {
        throw new IllegalArgumentException("Unknown magic byte");
      }
      destination.init(Fixtures.createSingleRow(System.currentTimeMillis()));
      return destination;
    }

    @Override
    public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
      return decode(payload, destination);
    }
  }

  @SuppressWarnings("unchecked")
  private static <E extends Throwable> E sneaky(Throwable t)
      throws E {
    throw (E) t;
  }

  private static MessageBatch<byte[]> batch(List<String> payloads, long startOffset) {
    return new MessageBatch<>() {
      @Override
      public int getMessageCount() {
        return payloads.size();
      }

      @Override
      public BytesStreamMessage getStreamMessage(int index) {
        return new BytesStreamMessage(payloads.get(index).getBytes(StandardCharsets.UTF_8),
            new StreamMessageMetadata.Builder()
                .setOffset(new LongMsgOffset(startOffset + index), new LongMsgOffset(startOffset + index + 1))
                .build());
      }

      @Override
      public StreamPartitionMsgOffset getOffsetOfNextBatch() {
        return new LongMsgOffset(startOffset + payloads.size());
      }

      @Override
      public long getSizeInBytes() {
        return 0;
      }
    };
  }

  @BeforeClass
  public void setUp() {
    FileUtils.deleteQuietly(TEMP_DIR);
    SegmentBuildTimeLeaseExtender.initExecutor();
    IngestionSourceErrorClassifier.registerRule(RefusedBySourceException.RULE);
  }

  @AfterClass
  public void tearDown() {
    IngestionSourceErrorClassifier.unregisterRule(RefusedBySourceException.RULE);
    SegmentBuildTimeLeaseExtender.shutdownExecutor();
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  @BeforeMethod
  public void resetScript() {
    SCRIPT.clear();
    _decoderInitRefused = false;
    _checkpointReportsRefusal = false;
    _createReportsRefusal = false;
    _duringCreate = null;
  }

  private static void stop(RealtimeSegmentDataManagerTest.FakeRealtimeSegmentDataManager segmentDataManager) {
    try {
      segmentDataManager._shouldStop.set(segmentDataManager, true);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }

  private RealtimeSegmentDataManagerTest.FakeRealtimeSegmentDataManager createSegmentDataManager(
      ServerMetrics serverMetrics)
      throws Exception {
    TableConfig tableConfig =
        Fixtures.createTableConfig(ScriptedConsumerFactory.class.getName(), ScriptedDecoder.class.getName());
    tableConfig.setUpsertConfig(null);
    String instanceId = "server-1";
    SegmentBuildTimeLeaseExtender.getOrCreate(instanceId, serverMetrics, REALTIME_TABLE_NAME);
    RealtimeTableDataManager tableDataManager = mock(RealtimeTableDataManager.class);
    when(tableDataManager.getInstanceId()).thenReturn(instanceId);
    when(tableDataManager.getSegmentLock(any())).thenReturn(mock(Lock.class));
    RealtimeSegmentStatsHistory statsHistory = mock(RealtimeSegmentStatsHistory.class);
    when(statsHistory.getEstimatedCardinality(anyString())).thenReturn(200);
    when(statsHistory.getEstimatedAvgColSize(anyString())).thenReturn(32);
    when(tableDataManager.getStatsHistory()).thenReturn(statsHistory);
    when(tableDataManager.getConsumerDir()).thenReturn(TEMP_DIR.getAbsolutePath() + "/consumerDir");
    // The recorder the table data manager owns, as in production
    when(tableDataManager.getIngestionSourceErrorRecorder()).thenReturn(
        new IngestionSourceErrorRecorder(serverMetrics, REALTIME_TABLE_NAME));

    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(SEGMENT_NAME.getSegmentName());
    segmentZKMetadata.setStartOffset("198");
    segmentZKMetadata.setCreationTime(System.currentTimeMillis());
    segmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
    Map<Integer, ConsumerCoordinator> consumerCoordinators = new ConcurrentHashMap<>();
    consumerCoordinators.put(0, new ConsumerCoordinator(false, tableDataManager));
    RealtimeSegmentDataManagerTest.FakeRealtimeSegmentDataManager segmentDataManager =
        new RealtimeSegmentDataManagerTest.FakeRealtimeSegmentDataManager(segmentZKMetadata, tableConfig,
            tableDataManager, new File(TEMP_DIR, REALTIME_TABLE_NAME).getAbsolutePath(), Fixtures.createSchema(),
            SEGMENT_NAME, consumerCoordinators, serverMetrics, new RealtimeSegmentDataManagerTest.TimeSupplier());
    segmentDataManager._stubConsumeLoop = false;
    segmentDataManager._state.set(segmentDataManager, RealtimeSegmentDataManager.State.INITIAL_CONSUMING);
    return segmentDataManager;
  }

  private static ServerMetrics newServerMetrics() {
    return new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
  }

  @Test
  public void refusalInterleavedWithEmptyFetchesIsRecordedAtOnce()
      throws Exception {
    // Every empty fetch resets the consume loop's error count, so it never gives up on this stream.
    Collections.addAll(SCRIPT, new RefusedBySourceException(), List.of(), new RefusedBySourceException(), List.of());
    ServerMetrics serverMetrics = newServerMetrics();
    RealtimeSegmentDataManagerTest.FakeRealtimeSegmentDataManager segmentDataManager =
        createSegmentDataManager(serverMetrics);
    expectThrows(PermanentConsumerException.class, segmentDataManager::consumeLoop);
    assertTrue(SCRIPT.isEmpty());
    assertNotNull(serverMetrics.getGaugeValue(AUTH_GAUGE));
    assertNull(serverMetrics.getGaugeValue(READ_GAUGE));

    // CONSUMING -> OFFLINE removes the partition's other metrics, but the record of the refusal stays.
    segmentDataManager.close();
    assertNotNull(serverMetrics.getGaugeValue(AUTH_GAUGE));
  }

  @Test
  public void refusalThePluginReportsWhileCheckpointingIsRecorded()
      throws Exception {
    // A transient fetch failure makes the consume loop checkpoint and recreate the consumer
    Collections.addAll(SCRIPT, new RuntimeException("transient"), List.of());
    _checkpointReportsRefusal = true;
    ServerMetrics serverMetrics = newServerMetrics();
    RealtimeSegmentDataManagerTest.FakeRealtimeSegmentDataManager segmentDataManager =
        createSegmentDataManager(serverMetrics);
    expectThrows(PermanentConsumerException.class, segmentDataManager::consumeLoop);
    assertTrue(SCRIPT.isEmpty());
    assertNotNull(serverMetrics.getGaugeValue(AUTH_GAUGE));
    segmentDataManager.close();
  }

  @Test
  public void unreachableSourceIsRecordedOnlyWhenPinotGivesUp()
      throws Exception {
    // Five transient failures and a successful fetch: Pinot recovered, so nothing is recorded.
    for (int i = 0; i < 5; i++) {
      SCRIPT.add(new ConnectException("Connection refused"));
    }
    SCRIPT.add(List.of());
    ServerMetrics serverMetrics = newServerMetrics();
    RealtimeSegmentDataManagerTest.FakeRealtimeSegmentDataManager segmentDataManager =
        createSegmentDataManager(serverMetrics);
    expectThrows(PermanentConsumerException.class, segmentDataManager::consumeLoop);
    assertTrue(SCRIPT.isEmpty());
    assertNull(serverMetrics.getGaugeValue(READ_GAUGE));
    segmentDataManager.close();

    // Six in a row: the consume loop gives up, and records it.
    for (int i = 0; i < 6; i++) {
      SCRIPT.add(new ConnectException("Connection refused"));
    }
    serverMetrics = newServerMetrics();
    segmentDataManager = createSegmentDataManager(serverMetrics);
    expectThrows(ConnectException.class, segmentDataManager::consumeLoop);
    assertNotNull(serverMetrics.getGaugeValue(READ_GAUGE));
    assertNull(serverMetrics.getGaugeValue(AUTH_GAUGE));
    segmentDataManager.close();
  }

  @Test
  public void failureWhileConsumptionIsStoppingIsNotRecorded()
      throws Exception {
    // Five transient failures, then the segment is told to stop, and the stop's interrupt surfaces as the sixth
    AtomicReference<RealtimeSegmentDataManagerTest.FakeRealtimeSegmentDataManager> stopping = new AtomicReference<>();
    for (int i = 0; i < 5; i++) {
      SCRIPT.add(new ConnectException("Connection refused"));
    }
    SCRIPT.add((Runnable) () -> stop(stopping.get()));
    SCRIPT.add(new SocketException("Socket closed"));
    ServerMetrics serverMetrics = newServerMetrics();
    stopping.set(createSegmentDataManager(serverMetrics));
    expectThrows(SocketException.class, stopping.get()::consumeLoop);
    assertNull(serverMetrics.getGaugeValue(READ_GAUGE));
    stopping.get().close();
  }

  @Test
  public void failureWhileStoppingDuringConsumerRecreationIsNotRecorded()
      throws Exception {
    // A transient failure makes the consume loop recreate its consumer, and the segment is told to stop meanwhile
    SCRIPT.add(new ConnectException("Connection refused"));
    ServerMetrics serverMetrics = newServerMetrics();
    RealtimeSegmentDataManagerTest.FakeRealtimeSegmentDataManager segmentDataManager =
        createSegmentDataManager(serverMetrics);
    _duringCreate = () -> stop(segmentDataManager);
    expectThrows(IllegalStateException.class, segmentDataManager::consumeLoop);
    assertNull(serverMetrics.getGaugeValue(READ_GAUGE));
    segmentDataManager.close();
  }

  @Test
  public void registryRefusalDuringDecodeIsRecordedWhileFetchesSucceed()
      throws Exception {
    // Bytes that won't parse come first in the batch: not the source's verdict, and they must not hide the refusal.
    List<String> payloads = new ArrayList<>(List.of(MALFORMED, "row", REFUSED, "row"));
    SCRIPT.add(payloads);
    SCRIPT.add(List.of("row"));
    ServerMetrics serverMetrics = newServerMetrics();
    RealtimeSegmentDataManagerTest.FakeRealtimeSegmentDataManager segmentDataManager =
        createSegmentDataManager(serverMetrics);
    expectThrows(PermanentConsumerException.class, segmentDataManager::consumeLoop);
    assertTrue(SCRIPT.isEmpty());
    assertNotNull(serverMetrics.getGaugeValue(AUTH_GAUGE));
    assertNull(serverMetrics.getGaugeValue(READ_GAUGE));
    segmentDataManager.close();

    // Bad bytes alone are content, not a source failure.
    SCRIPT.add(List.of(MALFORMED, "row"));
    serverMetrics = newServerMetrics();
    segmentDataManager = createSegmentDataManager(serverMetrics);
    expectThrows(PermanentConsumerException.class, segmentDataManager::consumeLoop);
    assertTrue(SCRIPT.isEmpty());
    assertNull(serverMetrics.getGaugeValue(AUTH_GAUGE));
    assertNull(serverMetrics.getGaugeValue(READ_GAUGE));
    segmentDataManager.close();
  }

  @Test
  public void decoderInitRefusedOnEveryAttemptIsClassifiedButKeptOutOfTheSegmentError()
      throws Exception {
    _decoderInitRefused = true;
    ServerMetrics serverMetrics = newServerMetrics();
    AttemptsExceededException failure =
        expectThrows(AttemptsExceededException.class, () -> createSegmentDataManager(serverMetrics));
    assertNotNull(serverMetrics.getGaugeValue(AUTH_GAUGE));
    // The thrown exception is the segment error, which the server's debug API serves
    String segmentError = ExceptionUtils.getStackTrace(failure);
    assertFalse(segmentError.contains(DECODER_CANARY), segmentError);
  }

  @Test
  public void failureReportedWhileCreatingTheConsumerIsRecorded()
      throws Exception {
    _createReportsRefusal = true;
    ServerMetrics serverMetrics = newServerMetrics();
    expectThrows(IllegalStateException.class, () -> createSegmentDataManager(serverMetrics));
    assertNotNull(serverMetrics.getGaugeValue(AUTH_GAUGE));
  }
}
