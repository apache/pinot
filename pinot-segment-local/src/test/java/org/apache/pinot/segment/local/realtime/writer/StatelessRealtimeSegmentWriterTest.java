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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import javax.annotation.Nullable;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
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
import org.apache.pinot.spi.stream.RetriableDecodeException;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamMessage;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;


/// Tests how [StatelessRealtimeSegmentWriter] consumes and stops consuming.
public class StatelessRealtimeSegmentWriterTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String STREAM_TYPE = "test";
  // Counted down when the stream is fetched, so that consumption can be stopped while it is in progress
  private static final CountDownLatch FETCHED = new CountDownLatch(1);
  private static final int OFFSET_STREAM_BATCH_SIZE = 10;
  private static final long FAILING_OFFSET = 5;

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

  @DataProvider
  public Object[][] fetchAfterStartOffset() {
    return new Object[][]{{false}, {true}};
  }

  /// Also runs against a stream that, like Kinesis, fetches from after the offset it is given.
  @Test(timeOut = 30_000, dataProvider = "fetchAfterStartOffset")
  public void testRetriableDecodeErrorFetchesTheMessageAgainInsteadOfDroppingIt(boolean fetchAfterStartOffset)
      throws Exception {
    OffsetPayloadDecoder.reset(3);
    OffsetPayloadConsumerFactory.FETCH_AFTER_START_OFFSET.set(fetchAfterStartOffset);
    List<Long> sleeps = new CopyOnWriteArrayList<>();
    try (StatelessRealtimeSegmentWriter writer = createOffsetPayloadWriter("250", sleeps, null)) {
      writer.startConsumption();
      TestUtils.waitForCondition(aVoid -> writer.isDoneConsuming(), 10_000L, "Consumption did not finish");
      writer.stopConsumption();

      assertThat(writer.isSuccess()).isTrue();
      assertThat(OffsetPayloadDecoder.RETRIABLE_FAILURE_OFFSETS).containsExactly(FAILING_OFFSET, FAILING_OFFSET,
          FAILING_OFFSET);
      // Every message of the segment was decoded once, including the one that failed first, and the failing message was
      // the first one decoded after each fetch
      long firstOffset = fetchAfterStartOffset ? 1 : 0;
      assertThat(OffsetPayloadDecoder.DECODED_OFFSETS).containsExactlyElementsOf(
          LongStream.range(firstOffset, 20).boxed().collect(Collectors.toList()));
      long failingMessageFetchOffset = fetchAfterStartOffset ? FAILING_OFFSET - 1 : FAILING_OFFSET;
      assertThat(OffsetPayloadConsumerFactory.FETCH_START_OFFSETS).startsWith(0L, failingMessageFetchOffset,
          failingMessageFetchOffset, failingMessageFetchOffset);
      // The backoff doubled up to the cap
      assertThat(sleeps).containsExactly(100L, 200L, 250L);
    }
  }

  @Test(timeOut = 30_000)
  public void testStopConsumptionInterruptsTheDecodeRetryBackoff()
      throws Exception {
    OffsetPayloadDecoder.reset(Integer.MAX_VALUE);
    // The backoff sleeps until interrupted
    CountDownLatch backoffStarted = new CountDownLatch(1);
    try (StatelessRealtimeSegmentWriter writer = createOffsetPayloadWriter("250", new CopyOnWriteArrayList<>(),
        backoffStarted)) {
      writer.startConsumption();
      assertThat(backoffStarted.await(10, TimeUnit.SECONDS)).isTrue();
      writer.stopConsumption();

      assertThat(writer.isSuccess()).isFalse();
      assertThat(writer.getConsumptionException()).hasMessage(
          "Consumption stopped at offset: " + FAILING_OFFSET + " before reaching end offset: 20");
      assertThat(OffsetPayloadDecoder.DECODED_OFFSETS).containsExactly(0L, 1L, 2L, 3L, 4L);
    }
  }

  @Test(timeOut = 30_000)
  public void testDecodeRetryMaxDurationBoundsAnOutageOfInterleavedFailures()
      throws Exception {
    // Failures less than the limit apart, with messages decoding in between, then one after a quiet period
    OffsetPayloadDecoder.reset(Integer.MAX_VALUE);
    OffsetPayloadDecoder.RETRIABLE_FAILURES.put(FAILING_OFFSET + 2, new AtomicInteger(Integer.MAX_VALUE));
    OffsetPayloadDecoder.RETRIABLE_FAILURES.put(FAILING_OFFSET + 4, new AtomicInteger(Integer.MAX_VALUE));
    long nextOutageOffset = 15;
    OffsetPayloadDecoder.RETRIABLE_FAILURES.put(nextOutageOffset, new AtomicInteger(1));
    // Failures happen 600ms apart, except the last one, 2s after the previous one
    AtomicLong clock = new AtomicLong();
    OffsetPayloadDecoder.ON_RETRIABLE_FAILURE.set(() -> clock.addAndGet(
        OffsetPayloadDecoder.RETRIABLE_FAILURE_OFFSETS.get(OffsetPayloadDecoder.RETRIABLE_FAILURE_OFFSETS.size() - 1)
            == nextOutageOffset ? 2000 : 600));
    List<Long> sleeps = new CopyOnWriteArrayList<>();
    try (StatelessRealtimeSegmentWriter writer = createOffsetPayloadWriter("250", sleeps, null,
        Map.of(StreamConfigProperties.DECODE_RETRY_MAX_DURATION_MILLIS, "1000"), clock)) {
      writer.startConsumption();
      TestUtils.waitForCondition(aVoid -> writer.isDoneConsuming(), 10_000L, "Consumption did not finish");
      writer.stopConsumption();

      assertThat(writer.isSuccess()).isTrue();
      // The first failing message is retried until the outage has lasted 1s, then dropped, and the later failing
      // messages of the outage are dropped on their first failure. After 2s without retriable failure, a new outage
      // starts and the failing message is retried again.
      assertThat(OffsetPayloadDecoder.RETRIABLE_FAILURE_OFFSETS).containsExactly(FAILING_OFFSET, FAILING_OFFSET,
          FAILING_OFFSET, FAILING_OFFSET + 2, FAILING_OFFSET + 4, nextOutageOffset);
      assertThat(OffsetPayloadConsumerFactory.FETCH_START_OFFSETS).containsExactly(0L, FAILING_OFFSET, FAILING_OFFSET,
          nextOutageOffset, nextOutageOffset);
      assertThat(sleeps).containsExactly(100L, 200L, 100L);
      assertThat(OffsetPayloadDecoder.DECODED_OFFSETS).containsExactlyElementsOf(LongStream.range(0, 20)
          .filter(offset -> offset != FAILING_OFFSET && offset != FAILING_OFFSET + 2 && offset != FAILING_OFFSET + 4)
          .boxed().collect(Collectors.toList()));
    }
  }

  /// Creates a writer re-ingesting offsets [0, 20) of [OffsetPayloadConsumerFactory] with [OffsetPayloadDecoder]. It
  /// records the decode retry backoffs instead of sleeping them, unless `backoffStarted` is given: then the first
  /// backoff counts it down and sleeps until interrupted.
  private static StatelessRealtimeSegmentWriter createOffsetPayloadWriter(String decodeRetryMaxBackoffMillis,
      List<Long> sleeps, @Nullable CountDownLatch backoffStarted)
      throws Exception {
    return createOffsetPayloadWriter(decodeRetryMaxBackoffMillis, sleeps, backoffStarted, Map.of(), null);
  }

  /// Like above, with extra stream configs, and a clock to use instead of the system one.
  private static StatelessRealtimeSegmentWriter createOffsetPayloadWriter(String decodeRetryMaxBackoffMillis,
      List<Long> sleeps, @Nullable CountDownLatch backoffStarted, Map<String, String> streamConfigs,
      @Nullable AtomicLong clock)
      throws Exception {
    Map<String, String> streamConfigMap = new HashMap<>(Map.of(StreamConfigProperties.STREAM_TYPE, STREAM_TYPE,
        StreamConfigProperties.constructStreamProperty(STREAM_TYPE, StreamConfigProperties.STREAM_TOPIC_NAME),
        "testTopic",
        StreamConfigProperties.constructStreamProperty(STREAM_TYPE,
            StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS), OffsetPayloadConsumerFactory.class.getName(),
        StreamConfigProperties.constructStreamProperty(STREAM_TYPE, StreamConfigProperties.STREAM_DECODER_CLASS),
        OffsetPayloadDecoder.class.getName(),
        StreamConfigProperties.DECODE_RETRY_MAX_BACKOFF_MILLIS, decodeRetryMaxBackoffMillis));
    streamConfigMap.putAll(streamConfigs);
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension("col", DataType.STRING)
        .build();
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setStreamConfigs(streamConfigMap)
            .build();
    SegmentZKMetadata segmentZKMetadata =
        new SegmentZKMetadata(new LLCSegmentName(RAW_TABLE_NAME, 0, 0, System.currentTimeMillis()).getSegmentName());
    segmentZKMetadata.setStartOffset("0");
    segmentZKMetadata.setEndOffset("20");
    return new StatelessRealtimeSegmentWriter(segmentZKMetadata, new IndexLoadingConfig(tableConfig, schema), null) {
      @Override
      protected long now() {
        return clock != null ? clock.get() : super.now();
      }

      @Override
      void sleepBeforeDecodeRetry(long backoffMs)
          throws InterruptedException {
        sleeps.add(backoffMs);
        if (backoffStarted != null) {
          backoffStarted.countDown();
          super.sleepBeforeDecodeRetry(Long.MAX_VALUE);
        }
      }
    };
  }

  /// Endless stream whose message at offset `n` carries `n` as its payload. Records the start offset of every fetch.
  public static class OffsetPayloadConsumerFactory extends StreamConsumerFactory {
    static final List<Long> FETCH_START_OFFSETS = new CopyOnWriteArrayList<>();
    // Like Kinesis, fetch from after the given offset, and give each message its own offset as next offset
    static final AtomicBoolean FETCH_AFTER_START_OFFSET = new AtomicBoolean();

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
          long start = ((LongMsgOffset) startOffset).getOffset();
          FETCH_START_OFFSETS.add(start);
          boolean fetchAfterStartOffset = FETCH_AFTER_START_OFFSET.get();
          long firstOffset = fetchAfterStartOffset ? start + 1 : start;
          List<BytesStreamMessage> messages = new ArrayList<>(OFFSET_STREAM_BATCH_SIZE);
          for (long offset = firstOffset; offset < firstOffset + OFFSET_STREAM_BATCH_SIZE; offset++) {
            long nextOffset = fetchAfterStartOffset ? offset : offset + 1;
            messages.add(new BytesStreamMessage(Long.toString(offset).getBytes(StandardCharsets.UTF_8),
                new StreamMessageMetadata.Builder().setOffset(new LongMsgOffset(offset), new LongMsgOffset(nextOffset))
                    .build()));
          }
          return new MessageBatch<>() {
            @Override
            public int getMessageCount() {
              return messages.size();
            }

            @Override
            public BytesStreamMessage getStreamMessage(int index) {
              return messages.get(index);
            }

            @Override
            public StreamPartitionMsgOffset getOffsetOfNextBatch() {
              return messages.get(messages.size() - 1).getMetadata().getNextOffset();
            }

            @Override
            public long getSizeInBytes() {
              return 0;
            }
          };
        }

        @Override
        public void close() {
        }
      };
    }
  }

  /// Decodes the payloads of [OffsetPayloadConsumerFactory], first throwing [RetriableDecodeException] for
  /// [#FAILING_OFFSET] the number of times passed to [#reset(int)], or for the offsets in [#RETRIABLE_FAILURES].
  public static class OffsetPayloadDecoder implements StreamMessageDecoder<byte[]> {
    // Offset -> number of decode attempts that throw RetriableDecodeException before the message decodes
    static final Map<Long, AtomicInteger> RETRIABLE_FAILURES = new ConcurrentHashMap<>();
    static final List<Long> RETRIABLE_FAILURE_OFFSETS = new CopyOnWriteArrayList<>();
    static final List<Long> DECODED_OFFSETS = new CopyOnWriteArrayList<>();
    // Run before each RetriableDecodeException is thrown
    static final AtomicReference<Runnable> ON_RETRIABLE_FAILURE = new AtomicReference<>();

    static void reset(int retriableFailures) {
      RETRIABLE_FAILURES.clear();
      RETRIABLE_FAILURES.put(FAILING_OFFSET, new AtomicInteger(retriableFailures));
      RETRIABLE_FAILURE_OFFSETS.clear();
      DECODED_OFFSETS.clear();
      ON_RETRIABLE_FAILURE.set(() -> {
      });
      OffsetPayloadConsumerFactory.FETCH_START_OFFSETS.clear();
      OffsetPayloadConsumerFactory.FETCH_AFTER_START_OFFSET.set(false);
    }

    @Override
    public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName) {
    }

    @Nullable
    @Override
    public GenericRow decode(byte[] payload, GenericRow destination) {
      String value = new String(payload, StandardCharsets.UTF_8);
      long offset = Long.parseLong(value);
      AtomicInteger retriableFailures = RETRIABLE_FAILURES.get(offset);
      if (retriableFailures != null && retriableFailures.getAndDecrement() > 0) {
        RETRIABLE_FAILURE_OFFSETS.add(offset);
        ON_RETRIABLE_FAILURE.get().run();
        throw new RetriableDecodeException("Schema registry unavailable");
      }
      DECODED_OFFSETS.add(offset);
      destination.putValue("col", value);
      return destination;
    }

    @Nullable
    @Override
    public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
      return decode(Arrays.copyOfRange(payload, offset, offset + length), destination);
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
