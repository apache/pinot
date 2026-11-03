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
package org.apache.pinot.segment.local.realtime.writer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImplTestUtils;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentStatsHistory;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamMessage;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests how [StatelessRealtimeSegmentWriter] stops consuming and how it handles `index()` failures.
public class StatelessRealtimeSegmentWriterTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String STREAM_TYPE = "test";
  // Counted down when the stream is fetched, so that consumption can be stopped while it is in progress
  private static final CountDownLatch FETCHED = new CountDownLatch(1);

  @Test(timeOut = 30_000)
  public void testStopConsumptionWithStreamIgnoringInterrupts()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension("col", DataType.STRING)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME)
        .setStreamConfigs(Map.of(StreamConfigProperties.STREAM_TYPE, STREAM_TYPE,
            StreamConfigProperties.constructStreamProperty(STREAM_TYPE, StreamConfigProperties.STREAM_TOPIC_NAME),
            "testTopic",
            StreamConfigProperties.constructStreamProperty(STREAM_TYPE,
                StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS), InterruptIgnoringConsumerFactory.class.getName(),
            StreamConfigProperties.constructStreamProperty(STREAM_TYPE, StreamConfigProperties.STREAM_DECODER_CLASS),
            NoOpDecoder.class.getName()))
        .build();
    SegmentZKMetadata segmentZKMetadata =
        new SegmentZKMetadata(new LLCSegmentName(RAW_TABLE_NAME, 0, 0, System.currentTimeMillis()).getSegmentName());
    // The stream never reaches the end offset
    segmentZKMetadata.setStartOffset("0");
    segmentZKMetadata.setEndOffset("100");

    try (StatelessRealtimeSegmentWriter writer = new StatelessRealtimeSegmentWriter(segmentZKMetadata,
        new IndexLoadingConfig(tableConfig, schema), null)) {
      writer.startConsumption();
      assertThat(FETCHED.await(10, TimeUnit.SECONDS)).isTrue();
      writer.stopConsumption();
      assertThat(writer.isSuccess()).isFalse();
      assertThat(writer.getConsumptionException()).hasMessageStartingWith("Consumption stopped at offset");
    }
  }

  // A published-then-rethrown index() must not abort stateless consumption (issue #16316).
  @Test
  public void testCapacityFillingRepairDoesNotAbort()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension("json", DataType.STRING).build();
    String segmentName = "testTable__0__0__0";
    Path consumerDir = Files.createTempDirectory("capacity-repair-consumer");
    RealtimeSegmentConfig config = new RealtimeSegmentConfig.Builder()
        .setTableNameWithType(RAW_TABLE_NAME + "_REALTIME").setSegmentName(segmentName)
        .setStreamName(STREAM_TYPE).setSchema(schema).setCapacity(1)
        .setIndex("json", StandardIndexes.json(), new JsonIndexConfig())
        .setSegmentZKMetadata(new SegmentZKMetadata(segmentName))
        .setConsumerDir(consumerDir.toString())
        .setMemoryManager(new DirectMemoryManager(segmentName))
        .setStatsHistory(mock(RealtimeSegmentStatsHistory.class)).setContinueOnError(false).build();
    MutableSegmentImpl segment = new MutableSegmentImpl(config, null);
    try {
      GenericRow row = new GenericRow();
      row.putValue("json", "{truncated");
      StatelessRealtimeSegmentWriter.indexTransformedRow(segment, row, null);
      assertEquals(segment.getNumDocsIndexed(), 1);
      assertFalse(segment.canTakeMoreRows());
      assertTrue(segment.canAddMore(), "Only row-count capacity should have been reached");
    } finally {
      segment.destroy();
      FileUtils.deleteDirectory(consumerDir.toFile());
    }
  }

  @Test
  public void testPublishedAggregateRepairDoesNotAbort()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension("dim", DataType.INT).addMetric("metric", DataType.LONG).build();
    MutableSegmentImpl segment = MutableSegmentImplTestUtils.createMutableSegmentImpl(schema, Set.of("metric"),
        Set.of(), Set.of(), true, false, false);
    try {
      GenericRow seed = new GenericRow();
      seed.putValue("dim", Integer.MIN_VALUE);
      seed.putValue("metric", 10L);
      StatelessRealtimeSegmentWriter.indexTransformedRow(segment, seed, null);
      GenericRow repaired = new GenericRow();
      repaired.putValue("dim", "not-an-int");
      repaired.putValue("metric", 3L);
      Exception failure = null;
      try {
        StatelessRealtimeSegmentWriter.indexTransformedRow(segment, repaired, null);
      } catch (Exception e) {
        failure = e;
      }
      assertEquals(segment.getNumDocsIndexed(), 1);
      assertEquals(segment.getRecord(0, new GenericRow()).getValue("metric"), 13L,
          "the repaired row was applied to the existing default-dimension group");
      assertFalse(segment.hasUnrecoverableIndexingFailure());
      assertNull(failure, "a completed aggregate repair must not abort stateless consumption");
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testPublishedRepairDoesNotAbort()
      throws Exception {
    MutableSegmentImpl segment = mock(MutableSegmentImpl.class);
    when(segment.wasLastIndexingOperationPublished()).thenReturn(true);
    when(segment.canAddMore()).thenReturn(true);
    when(segment.canTakeMoreRows()).thenReturn(true);
    when(segment.index(any(), any())).thenThrow(new RuntimeException("repaired then rethrown"));
    StatelessRealtimeSegmentWriter.indexTransformedRow(segment, new GenericRow(), mock(StreamMessageMetadata.class));
  }

  @Test
  public void testUnpublishedFailureRethrows()
      throws Exception {
    MutableSegmentImpl segment = mock(MutableSegmentImpl.class);
    when(segment.canAddMore()).thenReturn(true);
    when(segment.index(any(), any())).thenThrow(new RuntimeException("failed before mutation"));
    expectThrows(RuntimeException.class,
        () -> StatelessRealtimeSegmentWriter.indexTransformedRow(segment, new GenericRow(),
            mock(StreamMessageMetadata.class)));
  }

  @Test
  public void testTerminalPublishedRepairRethrows()
      throws Exception {
    MutableSegmentImpl segment = mock(MutableSegmentImpl.class);
    when(segment.wasLastIndexingOperationPublished()).thenReturn(true);
    when(segment.canAddMore()).thenReturn(false);
    when(segment.canTakeMoreRows()).thenReturn(false);
    when(segment.hasUnrecoverableIndexingFailure()).thenReturn(true);
    when(segment.index(any(), any())).thenThrow(new RuntimeException("unrecoverable"));
    expectThrows(RuntimeException.class,
        () -> StatelessRealtimeSegmentWriter.indexTransformedRow(segment, new GenericRow(),
            mock(StreamMessageMetadata.class)));
  }

  /// Stream that never returns a message and keeps polling when interrupted.
  public static class InterruptIgnoringConsumerFactory extends StreamConsumerFactory {
    @Nullable
    @Override
    public StreamMetadataProvider createPartitionMetadataProvider(String clientId, int partition) {
      return null;
    }

    @Nullable
    @Override
    public StreamMetadataProvider createStreamMetadataProvider(String clientId) {
      return null;
    }

    @Override
    public PartitionGroupConsumer createPartitionGroupConsumer(String clientId,
        PartitionGroupConsumptionStatus partitionGroupConsumptionStatus) {
      return new PartitionGroupConsumer() {
        @Override
        public MessageBatch<byte[]> fetchMessages(StreamPartitionMsgOffset startOffset, int timeoutMs) {
          FETCHED.countDown();
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            // Ignore the interrupt like a stream plugin that retries internally
          }
          return new EmptyMessageBatch(startOffset);
        }

        @Override
        public void close() {
        }
      };
    }
  }

  private static class EmptyMessageBatch implements MessageBatch<byte[]> {
    private final StreamPartitionMsgOffset _nextOffset;

    EmptyMessageBatch(StreamPartitionMsgOffset nextOffset) {
      _nextOffset = nextOffset;
    }

    @Override
    public int getMessageCount() {
      return 0;
    }

    @Override
    public StreamMessage<byte[]> getStreamMessage(int index) {
      throw new IndexOutOfBoundsException(index);
    }

    @Override
    public StreamPartitionMsgOffset getOffsetOfNextBatch() {
      return _nextOffset;
    }

    @Override
    public long getSizeInBytes() {
      return 0;
    }
  }

  public static class NoOpDecoder implements StreamMessageDecoder<byte[]> {
    @Override
    public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName) {
    }

    @Nullable
    @Override
    public GenericRow decode(byte[] payload, GenericRow destination) {
      return null;
    }

    @Nullable
    @Override
    public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
      return null;
    }
  }
}
