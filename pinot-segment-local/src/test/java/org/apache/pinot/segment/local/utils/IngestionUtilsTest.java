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
package org.apache.pinot.segment.local.utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.segment.local.utils.IngestionUtils.PartitioningListener;
import org.apache.pinot.segment.local.utils.IngestionUtils.StreamDecoder;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.AggregationConfig;
import org.apache.pinot.spi.config.table.ingestion.ComplexTypeConfig;
import org.apache.pinot.spi.config.table.ingestion.FilterConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamDataDecoderResult;
import org.apache.pinot.spi.stream.StreamMessage;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.slf4j.Logger;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests ingestion field extraction and shared stream initialization.
public class IngestionUtilsTest {
  private static final Logger LOGGER = mock(Logger.class);
  private static final ThreadLocal<DecoderState> DECODER_STATE = ThreadLocal.withInitial(DecoderState::new);

  /// Transform functions have moved to table config. This test remains for backward compatible handling testing
  @Test
  public void testExtractFieldsSchema() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    Schema schema;

    // from groovy function
    schema = new Schema();
    DimensionFieldSpec dimensionFieldSpec = new DimensionFieldSpec("d1", FieldSpec.DataType.STRING, true);
    dimensionFieldSpec.setTransformFunction("Groovy({function}, argument1, argument2)");
    schema.addField(dimensionFieldSpec);
    List<String> extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(tableConfig, schema));
    Assert.assertEquals(extract.size(), 3);
    Assert.assertTrue(extract.containsAll(Arrays.asList("d1", "argument1", "argument2")));

    // groovy function, no arguments
    schema = new Schema();
    dimensionFieldSpec = new DimensionFieldSpec("d1", FieldSpec.DataType.STRING, true);
    dimensionFieldSpec.setTransformFunction("Groovy({function})");
    schema.addField(dimensionFieldSpec);
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(tableConfig, schema));
    Assert.assertEquals(extract.size(), 1);
    Assert.assertTrue(extract.contains("d1"));

    // Map implementation for Avro - map__KEYS indicates map is source column
    schema = new Schema();
    dimensionFieldSpec = new DimensionFieldSpec("map__KEYS", FieldSpec.DataType.INT, false);
    schema.addField(dimensionFieldSpec);
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(tableConfig, schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Arrays.asList("map", "map__KEYS")));

    // Map implementation for Avro - map__VALUES indicates map is source column
    schema = new Schema();
    dimensionFieldSpec = new DimensionFieldSpec("map__VALUES", FieldSpec.DataType.LONG, false);
    schema.addField(dimensionFieldSpec);
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(tableConfig, schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Arrays.asList("map", "map__VALUES")));

    // Time field spec
    // only incoming
    schema = new Schema.SchemaBuilder().addTime(
        new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "time"), null).build();
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(tableConfig, schema));
    Assert.assertEquals(extract.size(), 1);
    Assert.assertTrue(extract.contains("time"));

    // incoming and outgoing different column name
    schema = new Schema.SchemaBuilder().addTime(
        new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "in"),
        new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "out")).build();
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(tableConfig, schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Arrays.asList("in", "out")));

    // inbuilt functions
    schema = new Schema();
    dimensionFieldSpec = new DimensionFieldSpec("hoursSinceEpoch", FieldSpec.DataType.LONG, true);
    dimensionFieldSpec.setTransformFunction("toEpochHours(\"timestamp\")");
    schema.addField(dimensionFieldSpec);
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(tableConfig, schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Arrays.asList("timestamp", "hoursSinceEpoch")));

    // inbuilt functions with literal
    schema = new Schema();
    dimensionFieldSpec = new DimensionFieldSpec("tenMinutesSinceEpoch", FieldSpec.DataType.LONG, true);
    dimensionFieldSpec.setTransformFunction("toEpochMinutesBucket(\"timestamp\", 10)");
    schema.addField(dimensionFieldSpec);
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(tableConfig, schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Lists.newArrayList("tenMinutesSinceEpoch", "timestamp")));

    // inbuilt functions on DateTimeFieldSpec
    schema = new Schema();
    DateTimeFieldSpec dateTimeFieldSpec =
        new DateTimeFieldSpec("date", FieldSpec.DataType.STRING, "1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd", "1:DAYS");
    dateTimeFieldSpec.setTransformFunction("toDateTime(\"timestamp\", 'yyyy-MM-dd')");
    schema.addField(dateTimeFieldSpec);
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(tableConfig, schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Lists.newArrayList("date", "timestamp")));
  }

  @Test
  public void testExtractFieldsIngestionConfig() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    Schema schema = new Schema();

    // filter config
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setFilterConfig(new FilterConfig("Groovy({x > 100}, x)"));
    tableConfig.setIngestionConfig(ingestionConfig);
    Set<String> fields = IngestionUtils.getFieldsForRecordExtractor(tableConfig, schema);
    Assert.assertEquals(fields.size(), 1);
    Assert.assertTrue(fields.containsAll(Sets.newHashSet("x")));

    schema.addField(new DimensionFieldSpec("y", FieldSpec.DataType.STRING, true));
    fields = IngestionUtils.getFieldsForRecordExtractor(tableConfig, schema);
    Assert.assertEquals(fields.size(), 2);
    Assert.assertTrue(fields.containsAll(Sets.newHashSet("x", "y")));

    // transform configs
    schema = new Schema.SchemaBuilder().addSingleValueDimension("d1", FieldSpec.DataType.STRING).build();
    ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(
        List.of(new TransformConfig("d1", "Groovy({function}, argument1, argument2)")));
    tableConfig.setIngestionConfig(ingestionConfig);
    List<String> extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(tableConfig, schema));
    Assert.assertEquals(extract.size(), 3);
    Assert.assertTrue(extract.containsAll(Arrays.asList("d1", "argument1", "argument2")));

    // groovy function, no arguments
    ingestionConfig.setTransformConfigs(List.of(new TransformConfig("d1", "Groovy({function})")));
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(tableConfig, schema));
    Assert.assertEquals(extract.size(), 1);
    Assert.assertTrue(extract.contains("d1"));

    // inbuilt functions
    schema = new Schema.SchemaBuilder().addSingleValueDimension("hoursSinceEpoch", FieldSpec.DataType.LONG).build();
    ingestionConfig.setTransformConfigs(
        List.of(new TransformConfig("hoursSinceEpoch", "toEpochHours(timestampColumn)")));
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(tableConfig, schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Arrays.asList("timestampColumn", "hoursSinceEpoch")));

    // inbuilt functions with literal
    schema =
        new Schema.SchemaBuilder().addSingleValueDimension("tenMinutesSinceEpoch", FieldSpec.DataType.LONG).build();
    ingestionConfig.setTransformConfigs(List.of(
        new TransformConfig("tenMinutesSinceEpoch", "toEpochMinutesBucket(timestampColumn, 10)")));
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(tableConfig, schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Lists.newArrayList("tenMinutesSinceEpoch", "timestampColumn")));

    // inbuilt functions on DateTimeFieldSpec
    schema = new Schema.SchemaBuilder().addDateTime("dateColumn", FieldSpec.DataType.STRING,
        "1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd", "1:DAYS").build();
    ingestionConfig.setTransformConfigs(
        List.of(new TransformConfig("dateColumn", "toDateTime(timestampColumn, 'yyyy-MM-dd')")));
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(tableConfig, schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Lists.newArrayList("dateColumn", "timestampColumn")));

    // filter + transform configs + schema fields  + schema transform
    schema = new Schema.SchemaBuilder().addSingleValueDimension("d1", FieldSpec.DataType.STRING)
        .addSingleValueDimension("d2", FieldSpec.DataType.STRING).addMetric("m1", FieldSpec.DataType.INT)
        .addDateTime("dateColumn", FieldSpec.DataType.STRING, "1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd", "1:DAYS").build();
    schema.getFieldSpecFor("d2").setTransformFunction("reverse(xy)");
    ingestionConfig.setFilterConfig(new FilterConfig("Groovy({d1 == \"10\"}, d1)"));
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(tableConfig, schema));
    Assert.assertEquals(extract.size(), 6);
    Assert.assertTrue(extract.containsAll(Lists.newArrayList("d1", "d2", "m1", "dateColumn", "xy", "timestampColumn")));

    // filter + transform configs + schema fields  + schema transform + complex type configs
    schema = new Schema.SchemaBuilder().addSingleValueDimension("d1", FieldSpec.DataType.STRING)
        .addSingleValueDimension("d2", FieldSpec.DataType.STRING).addMetric("m1", FieldSpec.DataType.INT)
        .addDateTime("dateColumn", FieldSpec.DataType.STRING, "1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd", "1:DAYS").build();
    schema.getFieldSpecFor("d2").setTransformFunction("reverse(xy)");
    ingestionConfig.setComplexTypeConfig(new ComplexTypeConfig(Arrays.asList("before.test", "after.test"), ".",
        ComplexTypeConfig.CollectionNotUnnestedToJson.NON_PRIMITIVE, Map.of("before", "after")));
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(tableConfig, schema));
    Assert.assertEquals(extract.size(), 8);
    List<String> expectedColumns =
        Arrays.asList("d1", "d2", "m1", "dateColumn", "xy", "timestampColumn", "before", "after");
    Assert.assertTrue(extract.containsAll(expectedColumns));
  }

  @Test
  public void testExtractFieldsAggregationConfig() {
    IngestionConfig ingestionConfig = new IngestionConfig();
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setIngestionConfig(ingestionConfig).build();
    Schema schema = new Schema();

    ingestionConfig.setAggregationConfigs(List.of(new AggregationConfig("d1", "SUM(s1)")));
    Set<String> fields = IngestionUtils.getFieldsForRecordExtractor(tableConfig, schema);
    Assert.assertEquals(fields.size(), 1);
    Assert.assertTrue(fields.containsAll(Sets.newHashSet("s1")));

    ingestionConfig.setAggregationConfigs(List.of(new AggregationConfig("d1", "MIN(s1)")));
    fields = IngestionUtils.getFieldsForRecordExtractor(tableConfig, schema);
    Assert.assertEquals(fields.size(), 1);
    Assert.assertTrue(fields.containsAll(Sets.newHashSet("s1")));

    ingestionConfig.setAggregationConfigs(List.of(new AggregationConfig("d1", "MAX(s1)")));
    fields = IngestionUtils.getFieldsForRecordExtractor(tableConfig, schema);
    Assert.assertEquals(fields.size(), 1);
    Assert.assertTrue(fields.containsAll(Sets.newHashSet("s1")));
  }

  @Test
  public void testComplexTypeConfig() {
    IngestionConfig ingestionConfig = new IngestionConfig();
    ComplexTypeConfig complexTypeConfig =
        new ComplexTypeConfig(null, "__", ComplexTypeConfig.CollectionNotUnnestedToJson.NON_PRIMITIVE, null);
    ingestionConfig.setComplexTypeConfig(complexTypeConfig);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setIngestionConfig(ingestionConfig).build();
    Schema schema = new Schema();
    schema.addField(new DimensionFieldSpec("a_b__c_d", FieldSpec.DataType.STRING, true));
    schema.addField(new DimensionFieldSpec("f_d", FieldSpec.DataType.STRING, false));
    schema.addField(new DimensionFieldSpec("ab__cd", FieldSpec.DataType.STRING, true));
    Set<String> fields = IngestionUtils.getFieldsForRecordExtractor(tableConfig, schema);
    Assert.assertEquals(fields.size(), 3);
    Assert.assertTrue(fields.containsAll(Sets.newHashSet("a_b", "f_d", "ab")));
  }

  @DataProvider
  public Object[][] consumptionModes() {
    return new Object[][]{{"kafka", false}, {"kafka", true}, {"test", false}, {"test", true}};
  }

  @Test(dataProvider = "consumptionModes")
  public void testConsumptionConfigPreservesSourceSettings(String streamType, boolean replicateMode) {
    Map<String, String> source = streamMap(streamType);
    source.put("auto.offset.reset", "latest");
    Map<String, String> original = Map.copyOf(source);

    StreamConfig result = IngestionUtils.createConsumptionStreamConfig("table_REALTIME", source, replicateMode);

    assertEquals(result.getStreamConfigsMap().get("auto.offset.reset"),
        replicateMode && "kafka".equals(streamType) ? "none" : "latest");
    assertEquals(source, original);
    assertEquals(result.getTopicName(), "topic");
  }

  @Test
  public void testPartitionFunctionUsesFullCountAndRetainsOriginalId() {
    StreamMetadataProvider provider = mock(StreamMetadataProvider.class);
    when(provider.fetchPartitionCount(5000L)).thenReturn(4);
    PartitioningListener listener = mock(PartitioningListener.class);
    RealtimeSegmentConfig.Builder builder = new RealtimeSegmentConfig.Builder();

    IngestionUtils.configureRealtimePartitioning(builder, partitionConfig(2), 3, provider, 5000L, LOGGER, listener);

    RealtimeSegmentConfig config = builder.build();
    assertEquals(config.getPartitionColumn(), "key");
    assertEquals(config.getPartitionId(), 3);
    assertEquals(config.getPartitionFunction().getNumPartitions(), 4);
    assertEquals(config.getPartitionFunction().getPartition("7"), 3);
    verify(listener).onPartitionCountMismatch(2, 4);
    verify(listener, never()).onPartitionCountFetchFailure(anyInt(), any());
  }

  @Test
  public void testPartitionCountLookupFailureUsesConfiguredCount() {
    StreamMetadataProvider provider = mock(StreamMetadataProvider.class);
    IllegalStateException failure = new IllegalStateException("Metadata unavailable");
    when(provider.fetchPartitionCount(10_000L)).thenThrow(failure);
    PartitioningListener listener = mock(PartitioningListener.class);
    RealtimeSegmentConfig.Builder builder = new RealtimeSegmentConfig.Builder();

    IngestionUtils.configureRealtimePartitioning(builder, partitionConfig(4), 3, provider, 10_000L, LOGGER, listener);

    RealtimeSegmentConfig config = builder.build();
    assertEquals(config.getPartitionFunction().getNumPartitions(), 4);
    assertEquals(config.getPartitionId(), 3);
    verify(listener).onPartitionCountFetchFailure(4, failure);
    verify(listener, never()).onPartitionCountMismatch(anyInt(), anyInt());
  }

  @DataProvider
  public Object[][] absentOrUnsupportedPartitionConfig() {
    return new Object[][]{
        {null},
        {new SegmentPartitionConfig(Map.of("a", new ColumnPartitionConfig("Modulo", 4),
            "b", new ColumnPartitionConfig("Modulo", 4)))}
    };
  }

  @Test(dataProvider = "absentOrUnsupportedPartitionConfig")
  public void testAbsentOrUnsupportedPartitionConfigDoesNotQueryMetadata(
      @Nullable SegmentPartitionConfig partitionConfig) {
    StreamMetadataProvider provider = mock(StreamMetadataProvider.class);
    PartitioningListener listener = mock(PartitioningListener.class);
    RealtimeSegmentConfig.Builder builder = new RealtimeSegmentConfig.Builder().setPartitionId(77);

    IngestionUtils.configureRealtimePartitioning(builder, partitionConfig, 3, provider, 5000L, LOGGER, listener);

    RealtimeSegmentConfig config = builder.build();
    assertNull(config.getPartitionColumn());
    assertNull(config.getPartitionFunction());
    assertEquals(config.getPartitionId(), 77);
    verifyNoInteractions(provider, listener);
  }

  @DataProvider
  public Object[][] keyTypes() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "keyTypes")
  public void testDecoderReceivesProjectedFieldsAndPreservesKeyType(boolean bytesKey)
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("table")
        .addSingleValueDimension("value", DataType.STRING)
        .addSingleValueDimension("__key", bytesKey ? DataType.BYTES : DataType.STRING)
        .build();
    StreamConfig streamConfig = new StreamConfig("table_REALTIME", streamMap("kafka"));
    TableConfig table = new TableConfigBuilder(TableType.REALTIME).setTableName("table").build();
    try {
      StreamDecoder decoder = IngestionUtils.createStreamDecoder(streamConfig, table, schema, LOGGER,
          RetryPolicies.noDelayRetryPolicy(1));
      assertEquals(decoder.decoderClassName(), RecordingDecoder.class.getName());
      assertTrue(DECODER_STATE.get()._fields.contains("value"));
      assertEquals(DECODER_STATE.get()._topic, "topic");
      byte[] key = "key".getBytes(StandardCharsets.UTF_8);
      byte[] value = "payload".getBytes(StandardCharsets.UTF_8);
      StreamMessageMetadata metadata = new StreamMessageMetadata.Builder()
          .setOffset(new LongMsgOffset(0), new LongMsgOffset(1)).build();
      StreamDataDecoderResult result =
          decoder.decoder().decode(new StreamMessage<>(key, value, value.length, metadata));
      assertNull(result.getException());
      assertNotNull(result.getResult());
      assertEquals(result.getResult().getValue("value"), "payload");
      assertEquals(result.getResult().getValue("__key"), bytesKey ? key : "key");
    } finally {
      DECODER_STATE.remove();
    }
  }

  @Test
  public void testDecoderInitializationRetriesBeforeReturningUsableDecoder()
      throws Exception {
    DECODER_STATE.get()._failuresBeforeSuccess = 2;
    try {
      StreamDecoder decoder = createDecoderWithRetryLimit(3);
      assertNotNull(decoder.decoder());
      assertEquals(DECODER_STATE.get()._attempts, 3);
    } finally {
      DECODER_STATE.remove();
    }
  }

  @Test
  public void testDecoderInitializationDoesNotReturnAfterRetryExhaustion() {
    DECODER_STATE.get()._failuresBeforeSuccess = 3;
    try {
      expectThrows(AttemptsExceededException.class, () -> createDecoderWithRetryLimit(2));
      assertEquals(DECODER_STATE.get()._attempts, 2);
    } finally {
      DECODER_STATE.remove();
    }
  }

  private static StreamDecoder createDecoderWithRetryLimit(int attempts)
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("table")
        .addSingleValueDimension("value", DataType.STRING).build();
    return IngestionUtils.createStreamDecoder(new StreamConfig("table_REALTIME", streamMap("kafka")),
        new TableConfigBuilder(TableType.REALTIME).setTableName("table").build(), schema, LOGGER,
        RetryPolicies.noDelayRetryPolicy(attempts));
  }

  private static SegmentPartitionConfig partitionConfig(int numPartitions) {
    return new SegmentPartitionConfig(Map.of("key", new ColumnPartitionConfig("Modulo", numPartitions)));
  }

  private static Map<String, String> streamMap(String streamType) {
    Map<String, String> stream = new HashMap<>();
    stream.put("streamType", streamType);
    stream.put("stream." + streamType + ".topic.name", "topic");
    stream.put("stream." + streamType + ".decoder.class.name", RecordingDecoder.class.getName());
    return stream;
  }

  /// Mutable initialization observations owned by the thread constructing the plugin decoder.
  private static final class DecoderState {
    private int _attempts;
    private int _failuresBeforeSuccess;
    private Set<String> _fields;
    private String _topic;
  }

  /// A real plugin decoder whose controlled initialization failures exercise the factory retry boundary.
  public static class RecordingDecoder implements StreamMessageDecoder<byte[]> {
    @Override
    public void init(Map<String, String> properties, Set<String> fieldsToRead, String topicName) {
      DecoderState state = DECODER_STATE.get();
      if (state._attempts++ < state._failuresBeforeSuccess) {
        throw new IllegalStateException("Initialization is temporarily unavailable");
      }
      state._fields = Set.copyOf(fieldsToRead);
      state._topic = topicName;
    }

    @Override
    public GenericRow decode(byte[] payload, GenericRow destination) {
      return decode(payload, 0, payload.length, destination);
    }

    @Override
    public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
      destination.putValue("value", new String(payload, offset, length, StandardCharsets.UTF_8));
      return destination;
    }
  }
}
