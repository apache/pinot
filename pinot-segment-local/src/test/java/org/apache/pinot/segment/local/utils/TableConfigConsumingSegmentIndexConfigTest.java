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

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.ConsumingSegmentFieldConfig;
import org.apache.pinot.spi.config.table.ConsumingSegmentIndexConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.RealtimeConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/// Tests for [TableConfigUtils#applyConsumingSegmentIndexConfig(TableConfig)] and the validation rules around
/// `realtimeConfig.consumingSegmentIndexConfig`. The profile is mutable-consuming-only: committed immutable
/// segments continue to use the persisted table config.
public class TableConfigConsumingSegmentIndexConfigTest {
  private static final String TABLE_NAME = "consumingProfileTable";
  private static final String TIME_COLUMN = "ts";

  private static Schema buildSchema() {
    return new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension("colA", org.apache.pinot.spi.data.FieldSpec.DataType.STRING)
        .addSingleValueDimension("colB", org.apache.pinot.spi.data.FieldSpec.DataType.STRING)
        .addDateTimeField(TIME_COLUMN, org.apache.pinot.spi.data.FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH",
            "1:MILLISECONDS")
        .build();
  }

  private static Map<String, String> streamConfigs() {
    return Map.of(
        "streamType", "kafka",
        "stream.kafka.topic.name", TABLE_NAME,
        "stream.kafka.consumer.type", "lowlevel",
        "stream.kafka.decoder.class.name", "org.apache.pinot.plugin.stream.kafka.KafkaJSONMessageDecoder",
        "stream.kafka.consumer.factory.class.name",
        "org.apache.pinot.plugin.stream.kafka30.KafkaConsumerFactory",
        "stream.kafka.broker.list", "localhost:9092",
        "realtime.segment.flush.threshold.rows", "1000");
  }

  private static ObjectNode invertedIndexConfig() {
    ObjectNode indexes = JsonUtils.newObjectNode();
    indexes.set("inverted", JsonUtils.newObjectNode().put("enabled", true));
    return indexes;
  }

  private static RealtimeConfig consumingProfile(String column, FieldConfig.EncodingType encodingType,
      ObjectNode indexes) {
    return new RealtimeConfig(new ConsumingSegmentIndexConfig(List.of(
        new ConsumingSegmentFieldConfig(column, encodingType, indexes))));
  }

  private static TableConfig baseRealtimeTable(RealtimeConfig realtimeConfig) {
    FieldConfig persistedRaw = new FieldConfig.Builder("colA")
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .build();
    return new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setStreamConfigs(streamConfigs())
        .setFieldConfigList(List.of(persistedRaw))
        .setNoDictionaryColumns(List.of("colA"))
        .setRealtimeConfig(realtimeConfig)
        .build();
  }

  @Test
  public void applyConsumingSegmentIndexConfigUpgradesMutableShapeOnly() {
    TableConfig tableConfig =
        baseRealtimeTable(consumingProfile("colA", FieldConfig.EncodingType.DICTIONARY, invertedIndexConfig()));

    TableConfig consumingView = TableConfigUtils.applyConsumingSegmentIndexConfig(tableConfig);

    assertNotSame(consumingView, tableConfig);
    FieldConfig consumingCol = consumingView.getFieldConfigList().get(0);
    assertEquals(consumingCol.getEncodingType(), FieldConfig.EncodingType.DICTIONARY);
    assertTrue(consumingCol.getIndexes().path("inverted").path("enabled").asBoolean(false));
    assertFalse(consumingView.getIndexingConfig().getNoDictionaryColumns().contains("colA"));

    FieldConfig persistedCol = tableConfig.getFieldConfigList().get(0);
    assertEquals(persistedCol.getEncodingType(), FieldConfig.EncodingType.RAW);
    assertTrue(tableConfig.getIndexingConfig().getNoDictionaryColumns().contains("colA"));
    assertTrue(persistedCol.getIndexes().isNull(), "apply must not mutate the persisted table config");
  }

  @Test
  public void applyConsumingSegmentIndexConfigScrubsNoDictionaryConfig() {
    TableConfig tableConfig =
        baseRealtimeTable(consumingProfile("colA", FieldConfig.EncodingType.DICTIONARY, invertedIndexConfig()));
    tableConfig.getIndexingConfig().setNoDictionaryConfig(Map.of("colA", "RAW", "colB", "RAW"));

    TableConfig consumingView = TableConfigUtils.applyConsumingSegmentIndexConfig(tableConfig);

    assertFalse(consumingView.getIndexingConfig().getNoDictionaryConfig().containsKey("colA"));
    assertEquals(consumingView.getIndexingConfig().getNoDictionaryConfig().get("colB"), "RAW");
    assertTrue(tableConfig.getIndexingConfig().getNoDictionaryConfig().containsKey("colA"),
        "apply must not mutate noDictionaryConfig on the persisted table config");
  }

  @Test
  public void applyEncodingOnlyProfilePreservesTopLevelIndexLists() {
    TableConfig tableConfig = baseRealtimeTable(consumingProfile("colA", FieldConfig.EncodingType.DICTIONARY, null));
    tableConfig.getIndexingConfig().setInvertedIndexColumns(List.of("colA", "colB"));

    TableConfig consumingView = TableConfigUtils.applyConsumingSegmentIndexConfig(tableConfig);

    assertTrue(consumingView.getIndexingConfig().getInvertedIndexColumns().contains("colA"),
        "Encoding-only profiles must not clear existing top-level index lists");
    assertTrue(consumingView.getIndexingConfig().getInvertedIndexColumns().contains("colB"));
    assertFalse(consumingView.getIndexingConfig().getNoDictionaryColumns().contains("colA"),
        "DICTIONARY encoding override must clear legacy no-dictionary config");
    assertTrue(tableConfig.getIndexingConfig().getNoDictionaryColumns().contains("colA"),
        "apply must not mutate noDictionaryColumns on the persisted table config");
  }

  @Test
  public void applyIndexOverrideProfileScrubsOnlyIndexConfigs() {
    TableConfig tableConfig = baseRealtimeTable(consumingProfile("colA", null, JsonUtils.newObjectNode()));
    tableConfig.getIndexingConfig().setInvertedIndexColumns(List.of("colA", "colB"));
    tableConfig.getIndexingConfig().setOnHeapDictionaryColumns(List.of("colA", "colB"));
    tableConfig.getIndexingConfig().setVarLengthDictionaryColumns(List.of("colA", "colB"));

    TableConfig consumingView = TableConfigUtils.applyConsumingSegmentIndexConfig(tableConfig);

    assertFalse(consumingView.getIndexingConfig().getInvertedIndexColumns().contains("colA"));
    assertTrue(consumingView.getIndexingConfig().getInvertedIndexColumns().contains("colB"));
    assertTrue(consumingView.getIndexingConfig().getOnHeapDictionaryColumns().contains("colA"),
        "Index-only profiles must not clear dictionary implementation config");
    assertTrue(consumingView.getIndexingConfig().getVarLengthDictionaryColumns().contains("colA"),
        "Index-only profiles must not clear dictionary implementation config");
    assertTrue(consumingView.getIndexingConfig().getNoDictionaryColumns().contains("colA"),
        "Index-only profiles must not clear no-dictionary config unless encoding is overridden");
    assertTrue(consumingView.getFieldConfigList().get(0).getIndexes().isObject());
    assertTrue(consumingView.getFieldConfigList().get(0).getIndexes().isEmpty());
  }

  @Test
  public void applyIndexOverrideProfileClearsLegacyFieldIndexTypes() {
    FieldConfig legacyInverted = new FieldConfig.Builder("colA")
        .withEncodingType(FieldConfig.EncodingType.DICTIONARY)
        .withIndexTypes(List.of(FieldConfig.IndexType.INVERTED))
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setStreamConfigs(streamConfigs())
        .setFieldConfigList(List.of(legacyInverted))
        .setRealtimeConfig(consumingProfile("colA", null, JsonUtils.newObjectNode()))
        .build();

    TableConfig consumingView = TableConfigUtils.applyConsumingSegmentIndexConfig(tableConfig);

    assertTrue(consumingView.getFieldConfigList().get(0).getIndexTypes().isEmpty());
    assertTrue(tableConfig.getFieldConfigList().get(0).getIndexTypes().contains(FieldConfig.IndexType.INVERTED),
        "apply must not mutate legacy indexTypes on the persisted table config");
  }

  @Test
  public void applyConsumingSegmentIndexConfigReturnsSameTableWhenAbsent() {
    TableConfig tableConfig = baseRealtimeTable(null);

    assertSame(TableConfigUtils.applyConsumingSegmentIndexConfig(tableConfig), tableConfig);
  }

  @Test
  public void validateRejectsOfflineTable() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setRealtimeConfig(consumingProfile("colA", FieldConfig.EncodingType.DICTIONARY, invertedIndexConfig()))
        .build();

    assertValidationFails(tableConfig, "only supported on REALTIME tables");
  }

  @Test
  public void validateRejectsUnsupportedIndex() {
    ObjectNode indexes = JsonUtils.newObjectNode();
    indexes.set("text", JsonUtils.newObjectNode().put("enabled", true));
    TableConfig tableConfig = baseRealtimeTable(consumingProfile("colA", FieldConfig.EncodingType.DICTIONARY, indexes));

    assertValidationFails(tableConfig, "Unsupported consuming segment index");
  }

  @Test
  public void validateRejectsUnknownFieldConfigKey() {
    try {
      ObjectNode tableConfigJson = (ObjectNode) baseRealtimeTable(
          consumingProfile("colA", FieldConfig.EncodingType.DICTIONARY, invertedIndexConfig())).toJsonNode();
      ObjectNode profileFieldJson = (ObjectNode) tableConfigJson.path("realtimeConfig")
          .path("consumingSegmentIndexConfig").path("fieldConfigList").get(0);
      profileFieldJson.putArray("indexTypes").add("INVERTED");
      TableConfigUtils.validate(JsonUtils.jsonNodeToObject(tableConfigJson, TableConfig.class), buildSchema());
      fail("Expected validation failure for unknown consuming segment field config key");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Unknown consuming segment field config keys"),
          "Expected unknown-key message, got: " + e.getMessage());
    }
  }

  @Test
  public void validateRejectsUnknownRealtimeConfigKey() {
    try {
      ObjectNode tableConfigJson = (ObjectNode) baseRealtimeTable(null).toJsonNode();
      ObjectNode realtimeConfigJson = JsonUtils.newObjectNode();
      realtimeConfigJson.set("consumingSegmentIndexConfigs", JsonUtils.newObjectNode());
      tableConfigJson.set("realtimeConfig", realtimeConfigJson);
      TableConfigUtils.validate(JsonUtils.jsonNodeToObject(tableConfigJson, TableConfig.class), buildSchema());
      fail("Expected validation failure for unknown realtime config key");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Unknown realtimeConfig keys"),
          "Expected unknown-key message, got: " + e.getMessage());
    }
  }

  @Test
  public void validateRejectsSortedColumn() {
    TableConfig tableConfig = baseRealtimeTable(
        consumingProfile("colA", FieldConfig.EncodingType.DICTIONARY, invertedIndexConfig()));
    tableConfig.getIndexingConfig().setSortedColumn(List.of("colA"));

    assertValidationFails(tableConfig, "sorted column");
  }

  @Test
  public void validateRejectsPartitionColumn() {
    TableConfig tableConfig = baseRealtimeTable(
        consumingProfile("colA", FieldConfig.EncodingType.DICTIONARY, invertedIndexConfig()));
    tableConfig.getIndexingConfig().setSegmentPartitionConfig(
        new SegmentPartitionConfig(Map.of("colA", new ColumnPartitionConfig("Murmur", 4))));

    assertValidationFails(tableConfig, "partition column");
  }

  @Test
  public void buildConsumingSegmentConfigBuilderAppliesProfile() {
    TableConfig tableConfig =
        baseRealtimeTable(consumingProfile("colA", FieldConfig.EncodingType.DICTIONARY, invertedIndexConfig()));
    IndexLoadingConfig ilc = new IndexLoadingConfig(tableConfig, buildSchema());

    RealtimeSegmentConfig built = TableConfigUtils.buildConsumingSegmentConfigBuilder(
            tableConfig, buildSchema(), ilc, LoggerFactory.getLogger(TableConfigConsumingSegmentIndexConfigTest.class),
            null)
        .build();

    FieldIndexConfigs colA = built.getIndexConfigByCol().get("colA");
    assertTrue(colA.getConfig(StandardIndexes.dictionary()).isEnabled());
    assertTrue(colA.getConfig(StandardIndexes.inverted()).isEnabled());
  }

  @Test
  public void buildConsumingSegmentConfigBuilderFallsBackOnInvalidProfile() {
    ObjectNode indexes = JsonUtils.newObjectNode();
    indexes.set("text", JsonUtils.newObjectNode().put("enabled", true));
    TableConfig tableConfig = baseRealtimeTable(consumingProfile("colA", FieldConfig.EncodingType.DICTIONARY, indexes));
    IndexLoadingConfig ilc = new IndexLoadingConfig(tableConfig, buildSchema());
    AtomicInteger fallbackInvocations = new AtomicInteger();

    RealtimeSegmentConfig built = TableConfigUtils.buildConsumingSegmentConfigBuilder(
            tableConfig, buildSchema(), ilc, LoggerFactory.getLogger(TableConfigConsumingSegmentIndexConfigTest.class),
            fallbackInvocations::incrementAndGet)
        .build();

    FieldIndexConfigs colA = built.getIndexConfigByCol().get("colA");
    assertFalse(colA.getConfig(StandardIndexes.dictionary()).isEnabled(),
        "Fallback must use the persisted RAW/no-dictionary shape");
    assertEquals(fallbackInvocations.get(), 1);
  }

  @Test
  public void consumingProfileDoesNotApplyStorageTierOverwrites()
      throws Exception {
    FieldConfig persistedRawWithTierOverwrite = new FieldConfig.Builder("colA")
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withTierOverwrites(JsonUtils.stringToJsonNode("{\"coldTier\":{\"encodingType\":\"RAW\",\"indexes\":{}}}"))
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setStreamConfigs(streamConfigs())
        .setFieldConfigList(List.of(persistedRawWithTierOverwrite))
        .setNoDictionaryColumns(List.of("colA"))
        .setRealtimeConfig(consumingProfile("colA", FieldConfig.EncodingType.DICTIONARY, invertedIndexConfig()))
        .build();
    IndexLoadingConfig ilc = new IndexLoadingConfig(tableConfig, buildSchema());
    ilc.setSegmentTier("coldTier");

    RealtimeSegmentConfig built = TableConfigUtils.buildConsumingSegmentConfigBuilder(
            tableConfig, buildSchema(), ilc, LoggerFactory.getLogger(TableConfigConsumingSegmentIndexConfigTest.class),
            null)
        .build();

    FieldIndexConfigs colA = built.getIndexConfigByCol().get("colA");
    assertTrue(colA.getConfig(StandardIndexes.dictionary()).isEnabled(),
        "Consuming profile is lifecycle-scoped and must not be overwritten by storage-tier config");
    assertTrue(colA.getConfig(StandardIndexes.inverted()).isEnabled());
  }

  private static void assertValidationFails(TableConfig tableConfig, String expectedMessagePart) {
    try {
      TableConfigUtils.validate(tableConfig, buildSchema());
      fail("Expected validation failure containing: " + expectedMessagePart);
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains(expectedMessagePart),
          "Expected message containing '" + expectedMessagePart + "', got: " + e.getMessage());
    }
  }
}
