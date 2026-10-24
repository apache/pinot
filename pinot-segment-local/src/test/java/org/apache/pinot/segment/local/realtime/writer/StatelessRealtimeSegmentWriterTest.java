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

import java.io.File;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.BytesStreamMessage;
import org.apache.pinot.spi.stream.LongMsgOffset;
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
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Tests how [StatelessRealtimeSegmentWriter] stops consuming.
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

  @BeforeClass
  public void registerMetrics() {
    ServerMetrics.register(mock(ServerMetrics.class));
  }

  @DataProvider
  public Object[][] emptyAdvances() {
    return new Object[][]{{110, false}, {120, false}, {150, false}, {110, true}, {120, true}, {150, true}};
  }

  @Test(dataProvider = "emptyAdvances")
  public void testOpaqueEmptyAdvanceFailsReplay(int nextOffset, boolean hasDataLoss)
      throws Exception {
    try (Replay replay = replay(120, List.of(records(0, 100), empty(nextOffset, 0, hasDataLoss), records(110, 120)))) {
      replay.writer().startConsumption();
      awaitDone(replay.writer());
      assertTrue(indexedRows(replay.writer()) >= 100, "the failure must follow real nonempty row ingestion");
      if (replay.writer().isSuccess()) {
        // Prove the old incomplete rowset really builds, unlike the unrelated empty-segment builder failure.
        assertTrue(replay.writer().buildSegment().isFile());
      }
      assertFalse(replay.writer().isSuccess(), "an opaque empty jump cannot complete bounded replay");
      assertTrue(replay.writer().getConsumptionException() instanceof IllegalStateException);
      assertTrue(replay.writer().getConsumptionException().getMessage().contains("empty stream"));
      assertEquals(((LongMsgOffset) field(replay.writer(), "_currentOffset")).getOffset(), 100L);
      assertEquals(replay.requestedOffsets(), List.of(0L, 100L));
      verify(replay.consumer(), times(1)).close();
    }
  }

  @DataProvider
  public Object[][] validReplays() {
    return new Object[][]{{"records", 120}, {"nonmoving", 120}, {"filtered", 115}, {"alreadyAtEnd", 0}};
  }

  @Test(dataProvider = "validReplays")
  public void testValidReplayStillCompletes(String kind, int expectedRows)
      throws Exception {
    List<MessageBatch<byte[]>> batches = switch (kind) {
      case "records" -> List.of(records(0, 120));
      case "nonmoving" -> List.of(records(0, 100), empty(100, 0, false), records(100, 120));
      // Five fetched tombstones/filtered records legitimately advance the position without decoded rows.
      case "filtered" -> List.of(records(0, 100), empty(105, 5, false), records(105, 120));
      case "alreadyAtEnd" -> List.of();
      default -> throw new IllegalArgumentException(kind);
    };
    try (Replay replay = replay("alreadyAtEnd".equals(kind) ? 0 : 120, batches)) {
      replay.writer().startConsumption();
      awaitDone(replay.writer());
      assertTrue(replay.writer().isSuccess());
      assertNull(replay.writer().getConsumptionException());
      assertEquals(indexedRows(replay.writer()), expectedRows);
      verify(replay.consumer(), times(1)).close();
      if (expectedRows > 0) {
        assertTrue(replay.writer().buildSegment().isFile());
      } else {
        assertTrue(replay.requestedOffsets().isEmpty());
      }
    }
  }

  private static Replay replay(int endOffset, List<MessageBatch<byte[]>> batches)
      throws Exception {
    File root = Files.createTempDirectory(StatelessRealtimeSegmentWriterTest.class.getSimpleName()).toFile();
    TableConfig table = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setTimeColumnName("ts")
        .setStreamConfigs(Map.of(StreamConfigProperties.STREAM_TYPE, STREAM_TYPE,
            StreamConfigProperties.constructStreamProperty(STREAM_TYPE, StreamConfigProperties.STREAM_TOPIC_NAME),
            "testTopic",
            StreamConfigProperties.constructStreamProperty(STREAM_TYPE,
                StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS), ScriptedConsumerFactory.class.getName(),
            StreamConfigProperties.constructStreamProperty(STREAM_TYPE, StreamConfigProperties.STREAM_DECODER_CLASS),
            IntegerDecoder.class.getName(), StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, "1000"))
        .build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension("value", DataType.INT)
        .addDateTime("ts", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();
    IndexLoadingConfig config = new IndexLoadingConfig(table, schema);
    config.setTableDataDir(root.getAbsolutePath());
    SegmentZKMetadata metadata =
        new SegmentZKMetadata(new LLCSegmentName(RAW_TABLE_NAME, 0, 0, System.nanoTime()).getSegmentName());
    metadata.setStartOffset("0");
    metadata.setEndOffset(Integer.toString(endOffset));
    PartitionGroupConsumer consumer = mock(PartitionGroupConsumer.class);
    List<Long> requestedOffsets = new ArrayList<>();
    LinkedList<MessageBatch<byte[]>> remaining = new LinkedList<>(batches);
    when(consumer.fetchMessages(any(), anyInt())).thenAnswer(invocation -> {
      requestedOffsets.add(((LongMsgOffset) invocation.getArgument(0)).getOffset());
      return remaining.removeFirst();
    });
    ScriptedConsumerFactory.CONSUMER.set(consumer);
    try {
      return new Replay(new StatelessRealtimeSegmentWriter(metadata, config, null), consumer, requestedOffsets, root);
    } finally {
      ScriptedConsumerFactory.CONSUMER.remove();
    }
  }

  private static void awaitDone(StatelessRealtimeSegmentWriter writer)
      throws Exception {
    TestUtils.waitForCondition(ignored -> writer.isDoneConsuming(), 10_000L, "writer thread did not finish");
    assertTrue(writer.isDoneConsuming());
  }

  private static int indexedRows(StatelessRealtimeSegmentWriter writer)
      throws Exception {
    return ((MutableSegmentImpl) field(writer, "_realtimeSegment")).getNumDocsIndexed();
  }

  private static Object field(StatelessRealtimeSegmentWriter writer, String name)
      throws Exception {
    Field field = StatelessRealtimeSegmentWriter.class.getDeclaredField(name);
    field.setAccessible(true);
    return field.get(writer);
  }

  private static MessageBatch<byte[]> records(int start, int end) {
    List<StreamMessage<byte[]>> messages = new ArrayList<>();
    for (int offset = start; offset < end; offset++) {
      messages.add(new BytesStreamMessage(Integer.toString(offset).getBytes(StandardCharsets.UTF_8),
          new StreamMessageMetadata.Builder()
              .setOffset(new LongMsgOffset(offset), new LongMsgOffset(offset + 1)).build()));
    }
    return new MessageBatch<>() {
      @Override
      public int getMessageCount() {
        return messages.size();
      }

      @Override
      public StreamMessage<byte[]> getStreamMessage(int index) {
        return messages.get(index);
      }

      @Override
      public StreamPartitionMsgOffset getOffsetOfNextBatch() {
        return new LongMsgOffset(end);
      }

      @Override
      public long getSizeInBytes() {
        return 0;
      }
    };
  }

  private static MessageBatch<byte[]> empty(int nextOffset, int unfiltered, boolean hasDataLoss) {
    return new EmptyMessageBatch(new LongMsgOffset(nextOffset)) {
      @Override
      public int getUnfilteredMessageCount() {
        return unfiltered;
      }

      @Override
      public boolean hasDataLoss() {
        return hasDataLoss;
      }
    };
  }

  private record Replay(StatelessRealtimeSegmentWriter writer, PartitionGroupConsumer consumer,
      List<Long> requestedOffsets, File root) implements AutoCloseable {
    @Override
    public void close()
        throws Exception {
      try {
        writer.close();
      } finally {
        FileUtils.deleteDirectory(root);
      }
    }
  }

  /// Supplies a consumer from the constructing test thread; ThreadLocal isolates parallel constructor calls.
  public static class ScriptedConsumerFactory extends InterruptIgnoringConsumerFactory {
    private static final ThreadLocal<PartitionGroupConsumer> CONSUMER = new ThreadLocal<>();

    @Override
    public PartitionGroupConsumer createPartitionGroupConsumer(String clientId,
        PartitionGroupConsumptionStatus partitionGroupConsumptionStatus) {
      return CONSUMER.get();
    }
  }

  /// Stateless integer decoder, exercised through the writer's real StreamDataDecoderImpl and indexing pipeline.
  public static class IntegerDecoder extends NoOpDecoder {
    @Override
    public GenericRow decode(byte[] payload, GenericRow destination) {
      return decode(payload, 0, payload.length, destination);
    }

    @Override
    public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
      int value = Integer.parseInt(new String(payload, offset, length, StandardCharsets.UTF_8));
      destination.putValue("value", value);
      destination.putValue("ts", 1_700_000_000_000L + value);
      return destination;
    }
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
