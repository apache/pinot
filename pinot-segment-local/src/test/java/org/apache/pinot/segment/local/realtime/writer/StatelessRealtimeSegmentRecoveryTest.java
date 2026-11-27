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
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.realtime.converter.RealtimeSegmentConverter;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentStatsHistory;
import org.apache.pinot.segment.local.segment.creator.TransformPipeline;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.local.utils.SchemaUtils;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.SegmentZKPropsConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.AggregationConfig;
import org.apache.pinot.spi.config.table.ingestion.FilterConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.LongMsgOffsetFactory;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamConsumerFactoryProvider;
import org.apache.pinot.spi.stream.StreamMessage;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.mockito.MockedStatic;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests native re-ingestion semantics and replicate-mode recovery using isolated mocked streams.
public class StatelessRealtimeSegmentRecoveryTest {
  @DataProvider
  public Object[][] nullHandlingModes() {
    return new Object[][]{{false, false}, {true, false}, {false, true}, {true, true}};
  }

  @Test(dataProvider = "nullHandlingModes")
  public void testReplayPreservesNormalNullSemantics(boolean tableNullHandling, boolean columnNullHandling)
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("replay")
        .setEnableColumnBasedNullHandling(columnNullHandling)
        .addSingleValueDimension("key", DataType.STRING)
        .addMetric("nullableValue", DataType.LONG)
        .build();
    ReplayFixture fixture = semanticFixture(schema);
    fixture._tableConfig.getIndexingConfig().setNullHandlingEnabled(tableNullHandling);

    List<GenericRow> rows = replayAndCompareWithNormalConsumption(fixture);

    assertEquals(rows.size(), 2);
    assertEquals(rows.get(0).isNullValue("nullableValue"), tableNullHandling || columnNullHandling);
    assertEquals(rows.get(0).getValue("nullableValue"), schema.getFieldSpecFor("nullableValue").getDefaultNullValue());
    assertFalse(rows.get(1).isNullValue("nullableValue"));
    assertEquals(rows.get(1).getValue("nullableValue"), 10L);
  }

  @Test
  public void testReplayPreservesLegacyMetricAggregation()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("replay")
        .addSingleValueDimension("key", DataType.STRING)
        .addMetric("amount", DataType.LONG)
        .build();
    ReplayFixture fixture = semanticFixture(schema);
    fixture._tableConfig.getIndexingConfig().setAggregateMetrics(true);
    fixture._tableConfig.getIndexingConfig().setNoDictionaryColumns(List.of("amount"));

    List<GenericRow> rows = replayAndCompareWithNormalConsumption(fixture);

    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0).getValue("amount"), 30L);
  }

  @Test
  public void testReplayPreservesAggregationSourceAndCountInitialization()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("replay")
        .addSingleValueDimension("key", DataType.STRING)
        .addMetric("totalAmount", DataType.LONG)
        .addMetric("eventCount", DataType.LONG)
        .build();
    ReplayFixture fixture = semanticFixture(schema);
    fixture._tableConfig.getIndexingConfig().setNoDictionaryColumns(List.of("totalAmount", "eventCount"));
    fixture._tableConfig.getIngestionConfig().setAggregationConfigs(List.of(
        new AggregationConfig("totalAmount", "SUM(sourceAmount)"), new AggregationConfig("eventCount", "COUNT(*)")));

    List<GenericRow> rows = replayAndCompareWithNormalConsumption(fixture);

    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0).getValue("totalAmount"), 30L);
    assertEquals(rows.get(0).getValue("eventCount"), 2L);
  }

  @Test(dataProvider = "nullHandlingModes")
  public void testReplayUsesOriginalCreationTimeForMissingTimestamp(boolean tableNullHandling,
      boolean columnNullHandling)
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("replay")
        .setEnableColumnBasedNullHandling(columnNullHandling)
        .addSingleValueDimension("key", DataType.STRING)
        .addDateTime("ts", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
    Object originalDefault = schema.getFieldSpecFor("ts").getDefaultNullValue();
    ReplayFixture fixture = semanticFixture(schema);
    fixture._tableConfig.getValidationConfig().setTimeColumnName("ts");
    fixture._tableConfig.getIndexingConfig().setNullHandlingEnabled(tableNullHandling);
    long creationTime = 1_700_000_000_000L;
    fixture._metadata.setCreationTime(creationTime);

    List<GenericRow> rows = replayAndCompareWithNormalConsumption(fixture);

    assertEquals(rows.size(), 2);
    for (GenericRow row : rows) {
      assertEquals(row.getValue("ts"), creationTime);
      assertEquals(row.isNullValue("ts"), tableNullHandling || columnNullHandling);
    }
    assertEquals(schema.getFieldSpecFor("ts").getDefaultNullValue(), originalDefault);
  }

  @Test
  public void testReplaySkipsOversizedMultiValueRecordAndRestoresFollowingRecord()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("replay")
        .addSingleValueDimension("key", DataType.STRING)
        .addMetric("amount", DataType.LONG)
        .addMultiValueDimension("tags", DataType.INT)
        .build();
    Map<String, String> stream = new HashMap<>(stream("first"));
    stream.put("stream.kafka.decoder.class.name", OversizedMultiValueDecoder.class.getName());
    ReplayFixture fixture = new ReplayFixture(0, List.of(stream), schema);
    fixture._metadata.setEndOffset("13");
    MessageBatch<byte[]> messages = batch(false, 13, 10, 11, 12);
    when(fixture._consumer.fetchMessages(any(), anyInt())).thenReturn(messages);
    File extracted = Files.createTempDirectory("replayed-multi-values").toFile();
    try (MockedStatic<StreamConsumerFactoryProvider> ignored = fixture.mockFactory();
        StatelessRealtimeSegmentWriter writer = fixture.writer()) {
      consume(writer);
      assertTrue(writer.isSuccess(), String.valueOf(writer.getConsumptionException()));
      assertNull(writer.getConsumptionException());
      assertEquals(writer.getIngestionStats(),
          new StatelessRealtimeSegmentWriter.IngestionStats(2, 0, 0, 1, 0, 0, 0));
      TarCompressionUtils.untar(writer.buildSegment(), extracted);
      File indexDirectory = new File(extracted, fixture._metadata.getSegmentName());
      SegmentMetadataImpl rebuilt = new SegmentMetadataImpl(indexDirectory);
      assertEquals(rebuilt.getStartOffset(), "10");
      assertEquals(rebuilt.getEndOffset(), "13");
      List<GenericRow> rows = readRows(indexDirectory);
      assertEquals(rows.size(), 2);
      assertEquals(rows.get(0).getValue("amount"), 10L);
      assertEquals(rows.get(1).getValue("amount"), 30L);
      assertEquals(rows.get(0).getValue("tags"), new Object[]{10, 10});
      assertEquals(rows.get(1).getValue("tags"), new Object[]{12, 12});
    } finally {
      FileUtils.deleteDirectory(extracted);
    }
  }

  @DataProvider
  public Object[][] transformErrorModes() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "transformErrorModes")
  public void testReplayTracksTransformErrorsAndContinues(boolean continueOnError)
      throws Exception {
    Map<String, String> stream = new HashMap<>(stream("first"));
    stream.put("stream.kafka.decoder.class.name", InvalidTransformDecoder.class.getName());
    ReplayFixture fixture = new ReplayFixture(0, List.of(stream));
    fixture._tableConfig.getIngestionConfig().setContinueOnError(continueOnError);
    fixture._tableConfig.getIngestionConfig()
        .setFilterConfig(new FilterConfig("Groovy({ Integer.parseInt(col) > 100 }, col)"));
    fixture._metadata.setEndOffset("13");
    MessageBatch<byte[]> messages = batch(false, 13, 10, 11, 12);
    when(fixture._consumer.fetchMessages(any(), anyInt())).thenReturn(messages);
    File extracted = Files.createTempDirectory("replayed-transform-error").toFile();
    try (MockedStatic<StreamConsumerFactoryProvider> ignored = fixture.mockFactory();
        StatelessRealtimeSegmentWriter writer = fixture.writer()) {
      consume(writer);
      assertTrue(writer.isSuccess(), String.valueOf(writer.getConsumptionException()));
      assertNull(writer.getConsumptionException());
      assertEquals(writer.getIngestionStats(),
          new StatelessRealtimeSegmentWriter.IngestionStats(continueOnError ? 3 : 2, 0,
              continueOnError ? 0 : 1, 0, 0, continueOnError ? 1 : 0, 0));
      TarCompressionUtils.untar(writer.buildSegment(), extracted);
      List<GenericRow> rows = readRows(new File(extracted, fixture._metadata.getSegmentName()));
      assertEquals(rows.size(), continueOnError ? 3 : 2);
      assertEquals(rows.get(0).getValue("col"), "10");
      assertEquals(rows.get(rows.size() - 1).getValue("col"), "12");
    } finally {
      FileUtils.deleteDirectory(extracted);
    }
  }

  @Test
  public void testReplayCountsFilteredRows()
      throws Exception {
    ReplayFixture fixture = new ReplayFixture(0, List.of(stream("first")));
    fixture._tableConfig.getIngestionConfig().setFilterConfig(new FilterConfig("col = '11'"));
    fixture._metadata.setEndOffset("13");
    MessageBatch<byte[]> messages = batch(false, 13, 10, 11, 12);
    when(fixture._consumer.fetchMessages(any(), anyInt())).thenReturn(messages);
    try (MockedStatic<StreamConsumerFactoryProvider> ignored = fixture.mockFactory();
        StatelessRealtimeSegmentWriter writer = fixture.writer()) {
      consume(writer);
      assertTrue(writer.isSuccess(), String.valueOf(writer.getConsumptionException()));
      assertEquals(writer.getIngestionStats(),
          new StatelessRealtimeSegmentWriter.IngestionStats(2, 0, 0, 0, 1, 0, 0));
    }
  }

  @DataProvider
  public Object[][] decodeErrorModes() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "decodeErrorModes")
  public void testReplayHonorsStopOnDecodeError(boolean stopOnDecodeError)
      throws Exception {
    Map<String, String> stream = new HashMap<>(stream("first"));
    stream.put("stream.kafka.decoder.class.name", FailingDecoder.class.getName());
    stream.put("stopOnDecodeError", Boolean.toString(stopOnDecodeError));
    ReplayFixture fixture = new ReplayFixture(0, List.of(stream));
    fixture._metadata.setEndOffset("13");
    MessageBatch<byte[]> messages = batch(false, 13, 10, 11, 12);
    when(fixture._consumer.fetchMessages(any(), anyInt())).thenReturn(messages);
    File extracted = Files.createTempDirectory("replayed-decode-error").toFile();
    try (MockedStatic<StreamConsumerFactoryProvider> ignored = fixture.mockFactory();
        StatelessRealtimeSegmentWriter writer = fixture.writer()) {
      consume(writer);
      assertEquals(writer.isSuccess(), !stopOnDecodeError);
      assertEquals(writer.getIngestionStats(),
          new StatelessRealtimeSegmentWriter.IngestionStats(stopOnDecodeError ? 1 : 2, 1, 0, 0, 0, 0, 0));
      if (stopOnDecodeError) {
        assertTrue(writer.getConsumptionException().getMessage().contains("decode error at offset: 11"));
      } else {
        assertNull(writer.getConsumptionException());
        TarCompressionUtils.untar(writer.buildSegment(), extracted);
        List<GenericRow> rows = readRows(new File(extracted, fixture._metadata.getSegmentName()));
        assertEquals(rows.size(), 2);
        assertEquals(rows.get(0).getValue("col"), "10");
        assertEquals(rows.get(1).getValue("col"), "12");
      }
    } finally {
      FileUtils.deleteDirectory(extracted);
    }
  }

  @DataProvider
  public Object[][] replayModes() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "replayModes")
  public void testMultiStreamReplayUsesSelectedTopicAndRawPartition(boolean replicateMode)
      throws Exception {
    ReplayFixture fixture = new ReplayFixture(10007, List.of(stream("first"), stream("second")), replicateMode);
    try (MockedStatic<StreamConsumerFactoryProvider> ignored = fixture.mockFactory();
        StatelessRealtimeSegmentWriter writer = fixture.writer()) {
      assertEquals(fixture._config.get().getTopicName(), "second");
      assertEquals(fixture._config.get().getStreamConfigsMap().get("auto.offset.reset"),
          replicateMode ? "none" : "latest");
      assertEquals(fixture._status.get().getPartitionGroupId(), 10007);
      assertEquals(fixture._status.get().getStreamPartitionGroupId(), 7);
      assertEquals(fixture._status.get().getSequenceNumber(), 2);
      assertEquals(fixture._status.get().getStartOffset().toString(), "10");
      assertEquals(fixture._status.get().getEndOffset().toString(), "12");
      verify(fixture._factory).createPartitionMetadataProvider(anyString(), eq(7));
    }
  }

  @Test(dataProvider = "replayModes")
  public void testReplayBuildContainsOnlySavedHalfOpenRange(boolean replicateMode)
      throws Exception {
    ReplayFixture fixture = new ReplayFixture(0, List.of(stream("first")), replicateMode);
    MessageBatch<byte[]> messages = batch(false, 13, 10, 11, 12);
    when(fixture._consumer.fetchMessages(any(), anyInt())).thenReturn(messages);
    File extracted = Files.createTempDirectory("replayed-segment").toFile();
    try (MockedStatic<StreamConsumerFactoryProvider> ignored = fixture.mockFactory();
        StatelessRealtimeSegmentWriter writer = fixture.writer()) {
      consume(writer);
      assertTrue(writer.isSuccess());
      TarCompressionUtils.untar(writer.buildSegment(), extracted);
      SegmentMetadataImpl rebuilt = new SegmentMetadataImpl(new File(extracted, fixture._metadata.getSegmentName()));
      assertEquals(rebuilt.getTotalDocs(), 2);
      assertEquals(rebuilt.getStartOffset(), "10");
      assertEquals(rebuilt.getEndOffset(), "12");
    } finally {
      FileUtils.deleteDirectory(extracted);
    }
  }

  @Test
  public void testExpiredReplayStartDoesNotPublishTruncatedArtifact()
      throws Exception {
    ReplayFixture fixture = new ReplayFixture(0, List.of(stream("first")), true);
    when(fixture._provider.fetchStreamPartitionOffset(any(), anyLong())).thenReturn(new LongMsgOffset(11));
    try (MockedStatic<StreamConsumerFactoryProvider> ignored = fixture.mockFactory();
        StatelessRealtimeSegmentWriter writer = fixture.writer()) {
      consume(writer);
      assertFalse(writer.isSuccess());
      assertTrue(writer.getConsumptionException().getMessage().contains("no longer retained"));
      expectThrows(IllegalStateException.class, writer::buildSegment);
      verify(fixture._consumer, never()).fetchMessages(any(), anyInt());
    }
  }

  @Test
  public void testDataLossAfterInitialRetentionCheckBlocksReplay()
      throws Exception {
    ReplayFixture fixture = new ReplayFixture(0, List.of(stream("first")), true);
    MessageBatch<byte[]> messages = batch(true, 20);
    when(fixture._consumer.fetchMessages(any(), anyInt())).thenReturn(messages);
    try (MockedStatic<StreamConsumerFactoryProvider> ignored = fixture.mockFactory();
        StatelessRealtimeSegmentWriter writer = fixture.writer()) {
      consume(writer);
      assertFalse(writer.isSuccess());
      assertTrue(writer.getConsumptionException().getMessage().contains("Stream data loss"));
      expectThrows(IllegalStateException.class, writer::buildSegment);
    }
  }

  @Test
  public void testEmptyEndOfStreamCannotCompleteFixedRangeReplay()
      throws Exception {
    ReplayFixture fixture = new ReplayFixture(0, List.of(stream("first")), true);
    MessageBatch<byte[]> messages = batch(false, 11);
    when(messages.isEndOfPartitionGroup()).thenReturn(true);
    when(fixture._consumer.fetchMessages(any(), anyInt())).thenReturn(messages);
    try (MockedStatic<StreamConsumerFactoryProvider> ignored = fixture.mockFactory();
        StatelessRealtimeSegmentWriter writer = fixture.writer()) {
      consume(writer);
      assertFalse(writer.isSuccess());
      assertTrue(writer.getConsumptionException().getMessage().contains("Stream ended before replay reached"));
      expectThrows(IllegalStateException.class, writer::buildSegment);
    }
  }

  @Test
  public void testReplicateReplayRejectsBuildAfterFatalDecodeError()
      throws Exception {
    Map<String, String> stream = new HashMap<>(stream("first"));
    stream.put("stream.kafka.decoder.class.name", FailingDecoder.class.getName());
    stream.put("stopOnDecodeError", "true");
    ReplayFixture fixture = new ReplayFixture(0, List.of(stream), true);
    fixture._metadata.setEndOffset("13");
    MessageBatch<byte[]> messages = batch(false, 13, 10, 11, 12);
    when(fixture._consumer.fetchMessages(any(), anyInt())).thenReturn(messages);
    try (MockedStatic<StreamConsumerFactoryProvider> ignored = fixture.mockFactory();
        StatelessRealtimeSegmentWriter writer = fixture.writer()) {
      consume(writer);
      assertFalse(writer.isSuccess());
      assertTrue(writer.getConsumptionException().getMessage().contains("decode error at offset: 11"));
      assertEquals(writer.getIngestionStats(),
          new StatelessRealtimeSegmentWriter.IngestionStats(1, 1, 0, 0, 0, 0, 0));
      expectThrows(IllegalStateException.class, writer::buildSegment);
    }
  }

  private static void consume(StatelessRealtimeSegmentWriter writer) {
    writer.startConsumption();
    TestUtils.waitForCondition(ignored -> writer.isDoneConsuming(), 10_000, "Replay did not finish");
    writer.stopConsumption();
  }

  private static ReplayFixture semanticFixture(Schema schema)
      throws Exception {
    Map<String, String> stream = new HashMap<>(stream("first"));
    stream.put("stream.kafka.decoder.class.name", IngestionDecoder.class.getName());
    ReplayFixture fixture = new ReplayFixture(0, List.of(stream), schema);
    MessageBatch<byte[]> messages = batch(false, 12, 10, 11);
    when(fixture._consumer.fetchMessages(any(), anyInt())).thenReturn(messages);
    return fixture;
  }

  /// Compares real immutable rows, including NULL markings, against the mutable configuration used by normal ingestion.
  private static List<GenericRow> replayAndCompareWithNormalConsumption(ReplayFixture fixture)
      throws Exception {
    File directory = Files.createTempDirectory("replayed-semantics").toFile();
    MutableSegmentImpl normal = null;
    try (MockedStatic<StreamConsumerFactoryProvider> ignored = fixture.mockFactory();
        StatelessRealtimeSegmentWriter writer = fixture.writer()) {
      consume(writer);
      assertTrue(writer.isSuccess(), String.valueOf(writer.getConsumptionException()));
      File replayOutput = new File(directory, "replay");
      TarCompressionUtils.untar(writer.buildSegment(), replayOutput);
      List<GenericRow> replayRows = readRows(new File(replayOutput, fixture._metadata.getSegmentName()));

      TableConfig tableConfig = fixture._tableConfig;
      Schema normalSchema = SchemaUtils.prepareRealtimeSegmentSchema(tableConfig, fixture._schema, fixture._metadata);
      File normalDirectory = new File(directory, "normal");
      FileUtils.forceMkdir(normalDirectory);
      IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(tableConfig, normalSchema);
      RealtimeSegmentConfig config = new RealtimeSegmentConfig.Builder(indexLoadingConfig)
          .setTableNameWithType(tableConfig.getTableName())
          .setSegmentName(fixture._metadata.getSegmentName())
          .setStreamName("first")
          .setSchema(normalSchema)
          .setCapacity(100)
          .setAvgNumMultiValues(2)
          .setSegmentZKMetadata(fixture._metadata)
          .setMemoryManager(new DirectMemoryManager(fixture._metadata.getSegmentName()))
          .setStatsHistory(RealtimeSegmentStatsHistory.deserializeFrom(new File(normalDirectory, "stats")))
          .setAggregateMetrics(tableConfig.getIndexingConfig().isAggregateMetrics())
          .setIngestionAggregationConfigs(IngestionConfigUtils.getAggregationConfigs(tableConfig))
          .setDefaultNullHandlingEnabled(tableConfig.getIndexingConfig().isNullHandlingEnabled())
          .setConsumerDir(normalDirectory.getAbsolutePath())
          .build();
      normal = new MutableSegmentImpl(config, null);
      TransformPipeline transform = new TransformPipeline(tableConfig, normalSchema);
      for (int offset = 10; offset < 12; offset++) {
        for (GenericRow row : transform.processRow(ingestionRow(offset)).getTransformedRows()) {
          normal.index(row, null);
        }
      }
      SegmentZKPropsConfig properties = new SegmentZKPropsConfig();
      properties.setStartOffset(fixture._metadata.getStartOffset());
      properties.setEndOffset(fixture._metadata.getEndOffset());
      new RealtimeSegmentConverter(normal, properties, normalDirectory.getAbsolutePath(), normalSchema,
          tableConfig.getTableName(), tableConfig, fixture._metadata.getSegmentName(),
          tableConfig.getIndexingConfig().isNullHandlingEnabled()).build(null);
      assertEquals(replayRows, readRows(new File(normalDirectory, fixture._metadata.getSegmentName())));
      return replayRows;
    } finally {
      if (normal != null) {
        normal.destroy();
      }
      FileUtils.deleteDirectory(directory);
    }
  }

  private static List<GenericRow> readRows(File indexDirectory)
      throws Exception {
    List<GenericRow> rows = new ArrayList<>();
    try (PinotSegmentRecordReader reader = new PinotSegmentRecordReader()) {
      reader.init(indexDirectory, null, null, false, null);
      while (reader.hasNext()) {
        rows.add(reader.next(new GenericRow()));
      }
    }
    return rows;
  }

  private static GenericRow ingestionRow(int offset) {
    GenericRow row = new GenericRow();
    row.putValue("key", "same");
    row.putValue("amount", (offset - 9L) * 10);
    row.putValue("sourceAmount", (offset - 9L) * 10);
    row.putValue("nullableValue", offset == 10 ? null : 10L);
    return row;
  }

  private static Map<String, String> stream(String topic) {
    return Map.of("streamType", "kafka", "stream.kafka.topic.name", topic,
        "stream.kafka.consumer.factory.class.name", "unused.Factory",
        "stream.kafka.decoder.class.name", ColumnDecoder.class.getName(), "auto.offset.reset", "latest");
  }

  @SuppressWarnings("unchecked")
  private static MessageBatch<byte[]> batch(boolean dataLoss, long next, long... offsets) {
    MessageBatch<byte[]> batch = mock(MessageBatch.class);
    List<StreamMessage<byte[]>> messages = new ArrayList<>();
    for (long offset : offsets) {
      byte[] value = {(byte) offset};
      StreamMessageMetadata metadata = new StreamMessageMetadata.Builder()
          .setOffset(new LongMsgOffset(offset), new LongMsgOffset(offset + 1)).build();
      messages.add(new StreamMessage<>(null, value, value.length, metadata));
    }
    when(batch.getMessageCount()).thenReturn(messages.size());
    when(batch.getUnfilteredMessageCount()).thenReturn(messages.size());
    when(batch.getStreamMessage(anyInt())).thenAnswer(invocation -> messages.get(invocation.<Integer>getArgument(0)));
    when(batch.getOffsetOfNextBatch()).thenReturn(new LongMsgOffset(next));
    when(batch.hasDataLoss()).thenReturn(dataLoss);
    return batch;
  }

  /// A fixture owns every mocked stream dependency; factory interception only covers synchronous writer construction.
  private static class ReplayFixture {
    final StreamConsumerFactory _factory = mock(StreamConsumerFactory.class);
    final StreamMetadataProvider _provider = mock(StreamMetadataProvider.class);
    final PartitionGroupConsumer _consumer = mock(PartitionGroupConsumer.class);
    final AtomicReference<StreamConfig> _config = new AtomicReference<>();
    final AtomicReference<PartitionGroupConsumptionStatus> _status = new AtomicReference<>();
    final SegmentZKMetadata _metadata;
    final TableConfig _tableConfig;
    final Schema _schema;

    ReplayFixture(int partitionId, List<Map<String, String>> streams)
        throws Exception {
      this(partitionId, streams, false);
    }

    ReplayFixture(int partitionId, List<Map<String, String>> streams, boolean replicateMode)
        throws Exception {
      this(partitionId, streams, new Schema.SchemaBuilder().setSchemaName("replay")
          .addSingleValueDimension("col", DataType.STRING).build(), replicateMode);
    }

    ReplayFixture(int partitionId, List<Map<String, String>> streams, Schema schema)
        throws Exception {
      this(partitionId, streams, schema, false);
    }

    ReplayFixture(int partitionId, List<Map<String, String>> streams, Schema schema, boolean replicateMode)
        throws Exception {
      _schema = schema;
      IngestionConfig ingestion = new IngestionConfig();
      ingestion.setStreamIngestionConfig(new StreamIngestionConfig(streams));
      _tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("replay")
          .setIngestionConfig(ingestion).build();
      _metadata = new SegmentZKMetadata(new LLCSegmentName("replay", partitionId, 2, 0L).getSegmentName());
      _metadata.setReplicateMode(replicateMode);
      _metadata.setStatus(Status.COMMITTING);
      _metadata.setStartOffset("10");
      _metadata.setEndOffset("12");
      when(_factory.createStreamMsgOffsetFactory()).thenReturn(new LongMsgOffsetFactory());
      when(_factory.createPartitionMetadataProvider(anyString(), anyInt())).thenReturn(_provider);
      when(_provider.fetchStreamPartitionOffset(any(), anyLong())).thenReturn(new LongMsgOffset(0));
      when(_factory.createPartitionGroupConsumer(anyString(), any())).thenAnswer(invocation -> {
        _status.set(invocation.getArgument(1));
        return _consumer;
      });
    }

    MockedStatic<StreamConsumerFactoryProvider> mockFactory() {
      MockedStatic<StreamConsumerFactoryProvider> mock = mockStatic(StreamConsumerFactoryProvider.class);
      mock.when(() -> StreamConsumerFactoryProvider.create(any())).thenAnswer(invocation -> {
        _config.set(invocation.getArgument(0));
        return _factory;
      });
      return mock;
    }

    StatelessRealtimeSegmentWriter writer()
        throws Exception {
      return new StatelessRealtimeSegmentWriter(_metadata, new IndexLoadingConfig(_tableConfig, _schema), null);
    }
  }

  /// Supplies raw source fields so replay exercises the same transform and mutable ingestion paths as normal input.
  public static class IngestionDecoder implements StreamMessageDecoder<byte[]> {
    @Override
    public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName) {
    }

    @Override
    public GenericRow decode(byte[] payload, GenericRow destination) {
      return decode(payload, 0, payload.length, destination);
    }

    @Override
    public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
      destination.init(ingestionRow(payload[offset]));
      return destination;
    }
  }

  /// Includes an oversized middle row followed by a valid row in the saved replay range.
  public static class OversizedMultiValueDecoder extends IngestionDecoder {
    @Override
    public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
      super.decode(payload, offset, length, destination);
      int value = payload[offset];
      Integer[] tags = new Integer[value == 11 ? 1001 : 2];
      Arrays.fill(tags, value);
      destination.putValue("tags", tags);
      return destination;
    }
  }

  /// Produces an invalid filter input in the middle of the range while keeping decoded rows valid for indexing.
  public static class InvalidTransformDecoder extends ColumnDecoder {
    @Override
    public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
      super.decode(payload, offset, length, destination);
      if (payload[offset] == 11) {
        destination.putValue("col", "invalid");
      }
      return destination;
    }
  }

  /// Fails on the middle message to exercise the configured terminal decode-error policy.
  public static class FailingDecoder extends ColumnDecoder {
    @Override
    public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
      if (payload[offset] == 11) {
        throw new IllegalArgumentException("Invalid message");
      }
      return super.decode(payload, offset, length, destination);
    }
  }

  /// Decoder used by the real mutable segment while testing the replay interval.
  public static class ColumnDecoder implements StreamMessageDecoder<byte[]> {
    @Override
    public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName) {
    }

    @Override
    public GenericRow decode(byte[] payload, GenericRow destination) {
      destination.putValue("col", Byte.toString(payload[0]));
      return destination;
    }

    @Override
    public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
      destination.putValue("col", Byte.toString(payload[offset]));
      return destination;
    }
  }
}
