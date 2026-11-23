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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.AggregationConfig;
import org.apache.pinot.spi.config.table.ingestion.ComplexTypeConfig;
import org.apache.pinot.spi.config.table.ingestion.FilterConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.SchemaConformingTransformerConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests for TableConfigUtils#validateStreamValueField.
public class TableConfigUtilsStreamValueFieldTest {
  private static final String JSON_DECODER = "org.apache.pinot.plugin.inputformat.json.JSONMessageDecoder";
  private static final TransformConfig STATE = new TransformConfig("state", "\"__value\"");
  private static final TransformConfig SCOPE = new TransformConfig("scope", "fromUtf8(\"__header$subject_prefix\")");
  private static final TransformConfig TS = new TransformConfig("ts", "\"__metadata$recordTimestamp\"");
  private static final TransformConfig TYPE = new TransformConfig("type", "jsonPathString(state, '$.type', null)");

  @Test
  public void testValidConfigs() {
    // Whole value stored in a column, header column, column derived from the stored value
    validate(tableConfig(List.of(STATE, SCOPE, TS, TYPE)), schema(true));
    // Columns derived directly from the value, without storing it
    TransformConfig typeFromValue = new TransformConfig("type", "jsonPathString(\"__value\", '$.type', null)");
    validate(tableConfig(List.of(SCOPE, TS, typeFromValue)), schema(false));
    // Filter and aggregation over derived columns
    IngestionConfig ingestionConfig = ingestionConfig(List.of(STATE, SCOPE, TS, TYPE), Map.of());
    ingestionConfig.setFilterConfig(new FilterConfig("strcmp(type, 'TEST') = 0"));
    validate(tableConfig(ingestionConfig), schema(true));
  }

  @Test
  public void testTablesNotReadingTheValueAreUnaffected() {
    // A table that splits the message into fields (no __value), even with a SchemaConformingTransformer
    IngestionConfig ingestionConfig = ingestionConfig(List.of(SCOPE, TS), Map.of());
    ingestionConfig.setSchemaConformingTransformerConfig(schemaConformingTransformerConfig());
    validate(tableConfig(ingestionConfig), schema(true));
  }

  @Test
  public void testReservedName() {
    Schema schema = schema(true);
    schema.addField(new DimensionFieldSpec("__value", DataType.JSON, true));
    assertRejected(tableConfig(List.of(SCOPE, TS, TYPE)), schema, "reserved");
  }

  @Test
  public void testIncompatibleConfigs() {
    TableConfig offline = new TableConfigBuilder(TableType.OFFLINE).setTableName("t").setTimeColumnName("ts")
        .setIngestionConfig(ingestionConfig(List.of(STATE, SCOPE, TS, TYPE), null)).build();
    assertRejected(offline, schema(true), "REALTIME");

    IngestionConfig sct = ingestionConfig(List.of(STATE, SCOPE, TS, TYPE), Map.of());
    sct.setSchemaConformingTransformerConfig(schemaConformingTransformerConfig());
    assertRejected(tableConfig(sct), schema(true), "schemaConformingTransformerConfig");

    IngestionConfig complexType = ingestionConfig(List.of(STATE, SCOPE, TS, TYPE), Map.of());
    complexType.setComplexTypeConfig(new ComplexTypeConfig(null, null, null, null));
    assertRejected(tableConfig(complexType), schema(true), "complexTypeConfig");

    assertRejected(tableConfig(ingestionConfig(List.of(STATE, SCOPE, TS, TYPE),
            Map.of("stream.kafka.decoder.class.name", "org.apache.pinot.plugin.inputformat.csv.CSVMessageDecoder"))),
        schema(true), "only populated by");
    assertRejected(tableConfig(ingestionConfig(List.of(STATE, SCOPE, TS, TYPE),
        Map.of("stream.kafka.decoder.prop.recordExtractorClass", "com.example.Extractor"))), schema(true), "extractor");
    assertRejected(tableConfig(ingestionConfig(List.of(STATE, SCOPE, TS, TYPE),
        Map.of("stream.kafka.decoder.prop.jsonFormat", "SQLITE_JSONB"))), schema(true), "SQLITE_JSONB");
  }

  @Test
  public void testColumnsThatCannotBePopulated() {
    // Transform reading a top-level key directly
    assertRejected(tableConfig(List.of(STATE, SCOPE, TS, new TransformConfig("type", "upper(kind)"))), schema(true),
        "reads 'kind'");
    // Filter reading a top-level key directly
    IngestionConfig filter = ingestionConfig(List.of(STATE, SCOPE, TS, TYPE), Map.of());
    filter.setFilterConfig(new FilterConfig("strcmp(model, 'TEST') = 0"));
    assertRejected(tableConfig(filter), schema(true), "reads 'model'");
    // Aggregation reading a top-level key directly
    IngestionConfig aggregation = ingestionConfig(List.of(STATE, SCOPE, TS, TYPE), Map.of());
    aggregation.setAggregationConfigs(List.of(new AggregationConfig("total", "SUM(amount)")));
    Schema withMetric = schema(true);
    withMetric.addField(new MetricFieldSpec("total", DataType.LONG));
    assertRejected(tableConfig(aggregation), withMetric, "reads 'amount'");
    // Schema column without any transform
    Schema extraColumn = schema(true);
    extraColumn.addField(new DimensionFieldSpec("temp", DataType.DOUBLE, true));
    assertRejected(tableConfig(List.of(STATE, SCOPE, TS, TYPE)), extraColumn, "Column 'temp'");
    // Schema-level transform reading a top-level key directly
    Schema schemaTransform = schema(true);
    DimensionFieldSpec region = new DimensionFieldSpec("region", DataType.STRING, true);
    region.setTransformFunction("upper(country)");
    schemaTransform.addField(region);
    assertRejected(tableConfig(List.of(STATE, SCOPE, TS, TYPE)), schemaTransform, "reads 'country'");
    // Schema-level transform derived from the value is accepted
    region.setTransformFunction("jsonPathString(state, '$.region', null)");
    validate(tableConfig(List.of(STATE, SCOPE, TS, TYPE)), schemaTransform);
  }

  @Test
  public void testRunsAsPartOfTableConfigValidation() {
    TableConfig tableConfig = tableConfig(List.of(STATE, SCOPE, TS, TYPE));
    TableConfigUtils.validate(tableConfig, schema(true));
    tableConfig.getIngestionConfig().setTransformConfigs(
        List.of(STATE, SCOPE, TS, new TransformConfig("type", "upper(kind)")));
    IllegalStateException e =
        expectThrows(IllegalStateException.class, () -> TableConfigUtils.validate(tableConfig, schema(true)));
    assertTrue(e.getMessage().contains("reads 'kind'"), e.getMessage());
  }

  private static SchemaConformingTransformerConfig schemaConformingTransformerConfig() {
    try {
      return JsonUtils.stringToObject("{\"indexableExtrasField\": \"state\"}",
          SchemaConformingTransformerConfig.class);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void validate(TableConfig tableConfig, Schema schema) {
    TableConfigUtils.validateStreamValueField(tableConfig, schema);
  }

  private static void assertRejected(TableConfig tableConfig, Schema schema, String expectedMessage) {
    IllegalStateException e = expectThrows(IllegalStateException.class, () -> validate(tableConfig, schema));
    assertTrue(e.getMessage().contains(expectedMessage), e.getMessage());
  }

  private static Schema schema(boolean withState) {
    Schema.SchemaBuilder builder = new Schema.SchemaBuilder().setSchemaName("t")
        .addSingleValueDimension("scope", DataType.STRING)
        .addSingleValueDimension("type", DataType.STRING)
        .addDateTime("ts", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS");
    if (withState) {
      builder.addSingleValueDimension("state", DataType.JSON);
    }
    return builder.build();
  }

  private static TableConfig tableConfig(List<TransformConfig> transformConfigs) {
    return tableConfig(ingestionConfig(transformConfigs, Map.of()));
  }

  private static TableConfig tableConfig(IngestionConfig ingestionConfig) {
    return new TableConfigBuilder(TableType.REALTIME).setTableName("t").setTimeColumnName("ts")
        .setIngestionConfig(ingestionConfig).build();
  }

  private static IngestionConfig ingestionConfig(List<TransformConfig> transformConfigs,
      @Nullable Map<String, String> streamConfigOverrides) {
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(new ArrayList<>(transformConfigs));
    if (streamConfigOverrides != null) {
      Map<String, String> streamConfig = new HashMap<>(Map.of("streamType", "kafka",
          "stream.kafka.topic.name", "topic", "stream.kafka.broker.list", "localhost:9092",
          "stream.kafka.consumer.factory.class.name", "org.apache.pinot.plugin.stream.kafka40.KafkaConsumerFactory",
          "stream.kafka.decoder.class.name", JSON_DECODER));
      streamConfig.putAll(streamConfigOverrides);
      ingestionConfig.setStreamIngestionConfig(new StreamIngestionConfig(List.of(streamConfig)));
    }
    return ingestionConfig;
  }
}
