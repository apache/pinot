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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/// Tests for [TableConfigUtils#applyConsumingOverrides(TableConfig)] and the validation rules around
/// [FieldConfig#getConsumingOverride()]. The override is consuming-segment-only — it merges into the [FieldConfig]
/// that drives the mutable consuming segment, while the persisted/immutable segment shape comes from the
/// un-overridden table config.
public class TableConfigConsumingOverrideTest {
  private static final String TABLE_NAME = "overrideTable";
  private static final String TIME_COLUMN = "ts";

  private static Schema buildSchema() {
    return new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension("colA", FieldSpec.DataType.STRING)
        .addSingleValueDimension("colB", FieldSpec.DataType.LONG)
        .addSingleValueDimension("colC", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
  }

  private static Map<String, String> streamConfigs() {
    return Map.of(
        "streamType", "kafka",
        "stream.kafka.consumer.type", "lowlevel",
        "stream.kafka.topic.name", "test",
        "stream.kafka.decoder.class.name",
            "org.apache.pinot.plugin.stream.kafka.KafkaJSONMessageDecoder",
        "stream.kafka.consumer.factory.class.name",
            "org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory",
        "stream.kafka.broker.list", "localhost:9092",
        "realtime.segment.flush.threshold.rows", "1000");
  }

  /// Builds a [FieldConfig] that on the persisted/immutable side is RAW with no indexes, but with a
  /// `consumingOverride` that lifts it to DICTIONARY + INVERTED for the mutable consuming segment.
  private static FieldConfig buildRawColumnWithRichConsumingOverride(String name, ObjectNode override) {
    return new FieldConfig.Builder(name)
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withConsumingOverride(override)
        .build();
  }

  @Test
  public void applyConsumingOverridesIsNoOpWhenAbsent() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setStreamConfigs(streamConfigs())
        .setNoDictionaryColumns(List.of("colA"))
        .build();
    assertSame(TableConfigUtils.applyConsumingOverrides(tableConfig), tableConfig);
  }

  @Test
  public void applyConsumingOverridesUpgradesEncodingAndIndexes() {
    /// Persisted: RAW, no indexes. Consuming: DICTIONARY + INVERTED for fast filtering.
    ObjectNode override = JsonUtils.newObjectNode();
    override.put("encodingType", FieldConfig.EncodingType.DICTIONARY.name());
    override.set("indexes",
        JsonUtils.newObjectNode().set("inverted", JsonUtils.newObjectNode().put("enabled", true)));
    FieldConfig overridden = buildRawColumnWithRichConsumingOverride("colA", override);

    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setStreamConfigs(streamConfigs())
        .setFieldConfigList(List.of(overridden))
        /// Persisted shape says noDictionary on colA — the consuming-side merge must scrub this so the consuming
        /// segment can actually have a dictionary.
        .setNoDictionaryColumns(List.of("colA"))
        .build();

    TableConfig consumingView = TableConfigUtils.applyConsumingOverrides(tableConfig);
    assertNotSame(consumingView, tableConfig);

    FieldConfig consumingCol = consumingView.getFieldConfigList().get(0);
    assertEquals(consumingCol.getName(), "colA");
    assertEquals(consumingCol.getEncodingType(), FieldConfig.EncodingType.DICTIONARY,
        "Consuming-side encoding should be lifted to DICTIONARY by the override");
    JsonNode mergedIndexes = consumingCol.getIndexes();
    assertTrue(mergedIndexes.has("inverted"),
        "Consuming-side indexes should contain the inverted entry from the override");
    assertTrue(mergedIndexes.path("inverted").path("enabled").asBoolean(false),
        "Consuming-side inverted index should be enabled");
    JsonNode overrideMarker = consumingCol.getConsumingOverride();
    assertTrue(overrideMarker == null || overrideMarker.isNull() || overrideMarker.isEmpty(),
        "consumingOverride marker should be cleared after merge, got: " + overrideMarker);

    /// colA must be scrubbed from noDictionaryColumns in the consuming view so the dictionary actually applies.
    IndexingConfig indexingConfig = consumingView.getIndexingConfig();
    List<String> noDictionaryColumns = indexingConfig.getNoDictionaryColumns();
    assertFalse(noDictionaryColumns != null && noDictionaryColumns.contains("colA"),
        "colA should be scrubbed from noDictionaryColumns on the consuming view");

    /// Original tableConfig must not have been mutated — persisted/immutable side stays RAW.
    assertTrue(tableConfig.getIndexingConfig().getNoDictionaryColumns().contains("colA"),
        "applyConsumingOverrides must not mutate the input table config");
  }

  @Test
  public void applyConsumingOverridesScrubsNoDictionaryConfig() {
    ObjectNode override = JsonUtils.newObjectNode();
    override.put("encodingType", FieldConfig.EncodingType.DICTIONARY.name());
    FieldConfig overridden = buildRawColumnWithRichConsumingOverride("colA", override);

    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setStreamConfigs(streamConfigs())
        .setFieldConfigList(List.of(overridden))
        .build();
    tableConfig.getIndexingConfig().setNoDictionaryConfig(Map.of("colA", "RAW", "colB", "RAW"));

    TableConfig consumingView = TableConfigUtils.applyConsumingOverrides(tableConfig);
    Map<String, String> noDictionaryConfig = consumingView.getIndexingConfig().getNoDictionaryConfig();
    assertFalse(noDictionaryConfig.containsKey("colA"),
        "colA should be scrubbed from noDictionaryConfig on the consuming view");
    assertEquals(noDictionaryConfig.get("colB"), "RAW",
        "unrelated noDictionaryConfig entries must be preserved");
    assertTrue(tableConfig.getIndexingConfig().getNoDictionaryConfig().containsKey("colA"),
        "applyConsumingOverrides must not mutate the input noDictionaryConfig");
  }

  @Test
  public void applyConsumingOverridesPreservesUnrelatedColumns() {
    ObjectNode override = JsonUtils.newObjectNode();
    override.put("encodingType", FieldConfig.EncodingType.DICTIONARY.name());
    FieldConfig overridden = buildRawColumnWithRichConsumingOverride("colA", override);
    FieldConfig regular = new FieldConfig.Builder("colB")
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .build();

    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setStreamConfigs(streamConfigs())
        .setFieldConfigList(List.of(overridden, regular))
        .build();

    TableConfig consumingView = TableConfigUtils.applyConsumingOverrides(tableConfig);
    FieldConfig consumingB = consumingView.getFieldConfigList().stream()
        .filter(fc -> "colB".equals(fc.getName())).findFirst().orElseThrow();
    assertEquals(consumingB.getEncodingType(), FieldConfig.EncodingType.RAW);
  }

  @Test
  public void applyConsumingOverridesIgnoresEmptyOverride() {
    FieldConfig empty = new FieldConfig.Builder("colA")
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withConsumingOverride(JsonUtils.newObjectNode())
        .build();

    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setStreamConfigs(streamConfigs())
        .setFieldConfigList(List.of(empty))
        .build();

    assertSame(TableConfigUtils.applyConsumingOverrides(tableConfig), tableConfig,
        "Empty override object must be treated as no-op");
  }

  @Test
  public void validateRejectsConsumingOverrideOnOfflineTable() {
    ObjectNode override = JsonUtils.newObjectNode();
    override.put("encodingType", FieldConfig.EncodingType.DICTIONARY.name());
    FieldConfig overridden = buildRawColumnWithRichConsumingOverride("colA", override);

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setFieldConfigList(List.of(overridden))
        .build();

    try {
      TableConfigUtils.validate(tableConfig, buildSchema());
      fail("Expected validation failure for consuming override on offline table");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("REALTIME"),
          "Expected message to mention REALTIME requirement, got: " + e.getMessage());
    }
  }

  @Test
  public void validateRejectsTypoedOverrideKey() {
    // Jackson's @JsonIgnoreProperties(ignoreUnknown = true) on BaseJsonConfig would otherwise drop a typo'd key
    // silently. validateConsumingOverrides enforces an explicit allowlist so the user gets a clear error before the
    // table config is persisted.
    ObjectNode override = JsonUtils.newObjectNode();
    override.put("encodingTpye", FieldConfig.EncodingType.DICTIONARY.name());
    FieldConfig overridden = buildRawColumnWithRichConsumingOverride("colA", override);

    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setStreamConfigs(streamConfigs())
        .setFieldConfigList(List.of(overridden))
        .build();

    try {
      TableConfigUtils.validate(tableConfig, buildSchema());
      fail("Expected validation failure for typo'd override key");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Unknown FieldConfig.consumingOverride key"),
          "Expected unknown-key message, got: " + e.getMessage());
      assertTrue(e.getMessage().contains("encodingTpye"),
          "Expected message to surface the bad key, got: " + e.getMessage());
    }
  }

  @Test
  public void validateRejectsNonObjectConsumingOverride() {
    FieldConfig overridden = new FieldConfig.Builder("colA")
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withConsumingOverride(JsonUtils.newArrayNode())
        .build();

    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setStreamConfigs(streamConfigs())
        .setFieldConfigList(List.of(overridden))
        .build();

    try {
      TableConfigUtils.validate(tableConfig, buildSchema());
      fail("Expected validation failure for non-object consumingOverride");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("must be a JSON object"),
          "Expected object-shape message, got: " + e.getMessage());
    }
  }

  @Test
  public void validateAcceptsValidConsumingOverride() {
    ObjectNode override = JsonUtils.newObjectNode();
    override.put("encodingType", FieldConfig.EncodingType.DICTIONARY.name());
    override.set("indexes",
        JsonUtils.newObjectNode().set("inverted", JsonUtils.newObjectNode().put("enabled", true)));
    FieldConfig overridden = buildRawColumnWithRichConsumingOverride("colA", override);

    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setStreamConfigs(streamConfigs())
        .setFieldConfigList(List.of(overridden))
        .setNoDictionaryColumns(List.of("colA"))
        .build();

    // Should not throw — colA is RAW persisted, DICTIONARY + INVERTED on consuming.
    TableConfigUtils.validate(tableConfig, buildSchema());
  }

  @Test
  public void validateAcceptsValidConsumingOverrideWithNoDictionaryConfig() {
    ObjectNode override = JsonUtils.newObjectNode();
    override.put("encodingType", FieldConfig.EncodingType.DICTIONARY.name());
    override.set("indexes",
        JsonUtils.newObjectNode().set("inverted", JsonUtils.newObjectNode().put("enabled", true)));
    FieldConfig overridden = buildRawColumnWithRichConsumingOverride("colA", override);

    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setStreamConfigs(streamConfigs())
        .setFieldConfigList(List.of(overridden))
        .build();
    tableConfig.getIndexingConfig().setNoDictionaryConfig(Map.of("colA", "RAW"));

    TableConfigUtils.validate(tableConfig, buildSchema());
  }

  @Test
  public void applyConsumingOverridesIsIdempotent() {
    // Calling applyConsumingOverrides twice on the same input should not corrupt the merged shape: the second call
    // sees a config without overrides (because the override marker is stripped on first merge) and returns it
    // unchanged. This guards against accidental double-application during tests or future refactors.
    ObjectNode override = JsonUtils.newObjectNode();
    override.put("encodingType", FieldConfig.EncodingType.DICTIONARY.name());
    FieldConfig overridden = buildRawColumnWithRichConsumingOverride("colA", override);

    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setStreamConfigs(streamConfigs())
        .setFieldConfigList(List.of(overridden))
        .build();

    TableConfig once = TableConfigUtils.applyConsumingOverrides(tableConfig);
    TableConfig twice = TableConfigUtils.applyConsumingOverrides(once);
    assertSame(twice, once,
        "Second applyConsumingOverrides on a merged config must be a no-op (override marker already cleared)");
  }

  @Test
  public void applyConsumingOverridesRejectsTypoedKeyOnStaleConfig() {
    // Simulates a hand-edited config that bypassed validate(): the merge utility itself must reject typo'd keys so
    // the consuming segment never gets built with a silently-dropped override.
    ObjectNode override = JsonUtils.newObjectNode();
    override.put("encodingTpye", FieldConfig.EncodingType.DICTIONARY.name());
    FieldConfig overridden = buildRawColumnWithRichConsumingOverride("colA", override);

    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setStreamConfigs(streamConfigs())
        .setFieldConfigList(List.of(overridden))
        .build();

    try {
      TableConfigUtils.applyConsumingOverrides(tableConfig);
      fail("Expected applyConsumingOverrides to reject a typo'd override key");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Unknown FieldConfig.consumingOverride key"),
          "Expected unknown-key message, got: " + e.getMessage());
    }
  }

  @Test
  public void applyConsumingOverridesRejectsNonObjectOverrideOnStaleConfig() {
    FieldConfig overridden = new FieldConfig.Builder("colA")
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withConsumingOverride(JsonUtils.newArrayNode())
        .build();

    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setStreamConfigs(streamConfigs())
        .setFieldConfigList(List.of(overridden))
        .build();

    try {
      TableConfigUtils.applyConsumingOverrides(tableConfig);
      fail("Expected applyConsumingOverrides to reject a non-object override");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("must be a JSON object"),
          "Expected object-shape message, got: " + e.getMessage());
    }
  }

  @Test
  public void applyConsumingOverridesRejectsOnOfflineTable() {
    // Same defense-in-depth: an offline table that somehow carries a consumingOverride must be rejected at apply
    // time, not silently treated as if it were realtime.
    ObjectNode override = JsonUtils.newObjectNode();
    override.put("encodingType", FieldConfig.EncodingType.DICTIONARY.name());
    FieldConfig overridden = buildRawColumnWithRichConsumingOverride("colA", override);

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setFieldConfigList(List.of(overridden))
        .build();

    try {
      TableConfigUtils.applyConsumingOverrides(tableConfig);
      fail("Expected applyConsumingOverrides to reject offline table with consumingOverride");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("REALTIME"),
          "Expected REALTIME message, got: " + e.getMessage());
    }
  }

  @Test
  public void validateRejectsPropertiesKeyOutsideAllowlist() {
    // 'properties' is intentionally NOT in the allowlist because TransformPipeline / aggregation read FieldConfig
    // properties from the un-overridden table config, which would create silent inconsistency. Confirm the user
    // gets an explicit error rather than silently-applied-then-ignored behavior.
    ObjectNode override = JsonUtils.newObjectNode();
    override.put("encodingType", FieldConfig.EncodingType.DICTIONARY.name());
    override.set("properties", JsonUtils.newObjectNode().put("foo", "bar"));
    FieldConfig overridden = buildRawColumnWithRichConsumingOverride("colA", override);

    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setStreamConfigs(streamConfigs())
        .setFieldConfigList(List.of(overridden))
        .build();

    try {
      TableConfigUtils.validate(tableConfig, buildSchema());
      fail("Expected validation failure for unsupported override key 'properties'");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Unknown FieldConfig.consumingOverride key"),
          "Expected unknown-key message, got: " + e.getMessage());
      assertTrue(e.getMessage().contains("properties"),
          "Expected message to surface the bad key, got: " + e.getMessage());
    }
  }

  @Test
  public void validateRejectsIndexTypesKeyOutsideAllowlist() {
    /// `indexTypes` is the legacy flat-list API; the consumingOverride allowlist is the modern typed `indexes`
    /// JSON tree. Confirm the user gets an explicit error rather than a silently-honored override.
    ObjectNode override = JsonUtils.newObjectNode();
    override.put("encodingType", FieldConfig.EncodingType.DICTIONARY.name());
    override.set("indexTypes", JsonUtils.newArrayNode().add(FieldConfig.IndexType.INVERTED.name()));
    FieldConfig overridden = buildRawColumnWithRichConsumingOverride("colA", override);

    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setStreamConfigs(streamConfigs())
        .setFieldConfigList(List.of(overridden))
        .build();

    try {
      TableConfigUtils.validate(tableConfig, buildSchema());
      fail("Expected validation failure for unsupported override key 'indexTypes'");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Unknown FieldConfig.consumingOverride key"),
          "Expected unknown-key message, got: " + e.getMessage());
      assertTrue(e.getMessage().contains("indexTypes"),
          "Expected message to surface the bad key, got: " + e.getMessage());
    }
  }

  @Test
  public void applyConsumingOverridesClearsLegacyIndexTypesWhenIndexesOverridePresent() {
    ObjectNode override = JsonUtils.newObjectNode();
    override.set("indexes",
        JsonUtils.newObjectNode().set("inverted", JsonUtils.newObjectNode().put("enabled", true)));
    FieldConfig overridden = new FieldConfig.Builder("colA")
        .withEncodingType(FieldConfig.EncodingType.DICTIONARY)
        .withIndexTypes(List.of(FieldConfig.IndexType.INVERTED))
        .withConsumingOverride(override)
        .build();

    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setStreamConfigs(streamConfigs())
        .setFieldConfigList(List.of(overridden))
        .build();

    TableConfig consumingView = TableConfigUtils.applyConsumingOverrides(tableConfig);
    FieldConfig consumingCol = consumingView.getFieldConfigList().get(0);
    assertTrue(consumingCol.getIndexTypes().isEmpty(),
        "consumingOverride.indexes must replace and clear legacy indexTypes");
    assertTrue(consumingCol.getIndexes().path("inverted").path("enabled").asBoolean(false),
        "modern indexes override should be preserved");
    assertEquals(tableConfig.getFieldConfigList().get(0).getIndexTypes(), List.of(FieldConfig.IndexType.INVERTED),
        "applyConsumingOverrides must not mutate legacy indexTypes on the input");
  }

  @Test
  public void applyConsumingOverridesScrubsAllPerColumnLists() {
    // Confirm the deny-list scrub touches every per-column list/map in tableIndexConfig, not just a hard-coded
    // subset. Colour the table with several per-column collections that all reference colA.
    ObjectNode override = JsonUtils.newObjectNode();
    override.put("encodingType", FieldConfig.EncodingType.DICTIONARY.name());
    FieldConfig overridden = buildRawColumnWithRichConsumingOverride("colA", override);

    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setStreamConfigs(streamConfigs())
        .setFieldConfigList(List.of(overridden))
        .setNoDictionaryColumns(List.of("colA", "colB"))
        .setInvertedIndexColumns(List.of("colA", "colB"))
        .setRangeIndexColumns(List.of("colA"))
        .setBloomFilterColumns(List.of("colA"))
        .setVarLengthDictionaryColumns(List.of("colA"))
        .build();

    TableConfig consumingView = TableConfigUtils.applyConsumingOverrides(tableConfig);
    IndexingConfig indexingConfig = consumingView.getIndexingConfig();
    assertFalse(indexingConfig.getNoDictionaryColumns().contains("colA"),
        "colA must be scrubbed from noDictionaryColumns");
    assertTrue(indexingConfig.getNoDictionaryColumns().contains("colB"),
        "colB must remain in noDictionaryColumns");
    assertFalse(indexingConfig.getInvertedIndexColumns().contains("colA"),
        "colA must be scrubbed from invertedIndexColumns");
    assertFalse(indexingConfig.getRangeIndexColumns() != null
            && indexingConfig.getRangeIndexColumns().contains("colA"),
        "colA must be scrubbed from rangeIndexColumns");
    assertFalse(indexingConfig.getBloomFilterColumns() != null
            && indexingConfig.getBloomFilterColumns().contains("colA"),
        "colA must be scrubbed from bloomFilterColumns");
    assertFalse(indexingConfig.getVarLengthDictionaryColumns() != null
            && indexingConfig.getVarLengthDictionaryColumns().contains("colA"),
        "colA must be scrubbed from varLengthDictionaryColumns");
  }

  @Test
  public void buildConsumingSegmentConfigBuilderUsesIndexLoadingConfigWhenNoOverride() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setStreamConfigs(streamConfigs())
        .setInvertedIndexColumns(List.of("colA"))
        .build();
    IndexLoadingConfig ilc = new IndexLoadingConfig(tableConfig, buildSchema());
    RealtimeSegmentConfig.Builder builder = TableConfigUtils.buildConsumingSegmentConfigBuilder(
        tableConfig, buildSchema(), ilc, LoggerFactory.getLogger(TableConfigConsumingOverrideTest.class), null);
    RealtimeSegmentConfig built = builder.build();
    FieldIndexConfigs colA = built.getIndexConfigByCol().get("colA");
    assertTrue(colA != null && colA.getConfig(StandardIndexes.inverted()).isEnabled(),
        "no-override path must yield IndexLoadingConfig-driven shape (inverted index enabled on colA)");
  }

  @Test
  public void buildConsumingSegmentConfigBuilderAppliesOverrideWhenPresent() {
    ObjectNode override = JsonUtils.newObjectNode();
    override.put("encodingType", FieldConfig.EncodingType.DICTIONARY.name());
    override.set("indexes",
        JsonUtils.newObjectNode().set("inverted", JsonUtils.newObjectNode().put("enabled", true)));
    FieldConfig overridden = buildRawColumnWithRichConsumingOverride("colA", override);

    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setStreamConfigs(streamConfigs())
        .setFieldConfigList(List.of(overridden))
        .setNoDictionaryColumns(List.of("colA"))
        .build();
    IndexLoadingConfig ilc = new IndexLoadingConfig(tableConfig, buildSchema());
    RealtimeSegmentConfig.Builder builder = TableConfigUtils.buildConsumingSegmentConfigBuilder(
        tableConfig, buildSchema(), ilc, LoggerFactory.getLogger(TableConfigConsumingOverrideTest.class), null);
    RealtimeSegmentConfig built = builder.build();
    FieldIndexConfigs colA = built.getIndexConfigByCol().get("colA");
    assertTrue(colA.getConfig(StandardIndexes.dictionary()).isEnabled(),
        "override must lift colA to dictionary on the consuming-side builder");
    assertTrue(colA.getConfig(StandardIndexes.inverted()).isEnabled(),
        "override must add inverted on colA on the consuming-side builder");
  }

  @Test
  public void applyConsumingOverridesDoesNotMutateInput() {
    /// The input TableConfig is shared across server threads (cached on the data manager). A successful merge must
    /// not mutate any of its sub-objects — particularly the JsonNode-typed fields on FieldConfig (consumingOverride,
    /// indexes, tierOverwrites) which are at risk of aliasing through tableConfig.toJsonNode().
    ObjectNode override = JsonUtils.newObjectNode();
    override.put("encodingType", FieldConfig.EncodingType.DICTIONARY.name());
    override.set("indexes",
        JsonUtils.newObjectNode().set("inverted", JsonUtils.newObjectNode().put("enabled", true)));
    FieldConfig overridden = buildRawColumnWithRichConsumingOverride("colA", override);

    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setStreamConfigs(streamConfigs())
        .setFieldConfigList(List.of(overridden))
        .setNoDictionaryColumns(List.of("colA"))
        .build();

    // Snapshot the input shape.
    String beforeJson = tableConfig.toJsonString();
    JsonNode beforeOverride = tableConfig.getFieldConfigList().get(0).getConsumingOverride();
    String beforeOverrideJson = beforeOverride.toString();

    TableConfigUtils.applyConsumingOverrides(tableConfig);

    // Source TableConfig must be byte-for-byte identical, AND its consumingOverride sub-tree must still hold the
    // original override (i.e. not stripped of encodingType / indexTypes by the in-place merge).
    assertEquals(tableConfig.toJsonString(), beforeJson,
        "applyConsumingOverrides must not mutate the input TableConfig");
    assertEquals(tableConfig.getFieldConfigList().get(0).getConsumingOverride().toString(), beforeOverrideJson,
        "applyConsumingOverrides must not mutate the input FieldConfig.consumingOverride sub-tree");
    assertTrue(tableConfig.getIndexingConfig().getNoDictionaryColumns().contains("colA"),
        "applyConsumingOverrides must not mutate the input IndexingConfig per-column lists");
  }

  @Test
  public void fieldConfigJsonWithoutConsumingOverrideStillDeserializes()
      throws Exception {
    /// Backward-compat: any FieldConfig JSON written by an older controller (no consumingOverride field) must
    /// deserialize cleanly; getConsumingOverride() returns a NullNode (matching sibling getIndexes()
    /// / getTierOverwrites() contract).
    String legacyJson = "{"
        + "\"name\":\"colA\","
        + "\"encodingType\":\"RAW\","
        + "\"indexTypes\":[]"
        + "}";
    FieldConfig fc = JsonUtils.stringToObject(legacyJson, FieldConfig.class);
    assertEquals(fc.getName(), "colA");
    assertEquals(fc.getEncodingType(), FieldConfig.EncodingType.RAW);
    JsonNode override = fc.getConsumingOverride();
    assertNotNull(override, "getConsumingOverride() must never return null");
    assertTrue(override.isNull(), "Absent override must be a NullNode, got: " + override);
  }

  @Test
  public void validateRejectsConsumingOverrideOnSortedColumn() {
    /// sortedColumn is structural for the consuming segment — Builder(IndexLoadingConfig) auto-adds an inverted
    /// index on dictionary-enabled sorted columns. Allowing the override would create a contradiction with that
    /// auto-handling.
    ObjectNode override = JsonUtils.newObjectNode();
    override.put("encodingType", FieldConfig.EncodingType.DICTIONARY.name());
    FieldConfig overridden = buildRawColumnWithRichConsumingOverride("colA", override);

    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setStreamConfigs(streamConfigs())
        .setFieldConfigList(List.of(overridden))
        .setSortedColumn("colA")
        .build();

    try {
      TableConfigUtils.validate(tableConfig, buildSchema());
      fail("Expected validation failure for consuming override on sorted column");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("sorted column"),
          "Expected sorted-column message, got: " + e.getMessage());
    }
  }

  @Test
  public void applyConsumingOverridesRejectsSortedColumnOnStaleConfig() {
    ObjectNode override = JsonUtils.newObjectNode();
    override.put("encodingType", FieldConfig.EncodingType.DICTIONARY.name());
    FieldConfig overridden = buildRawColumnWithRichConsumingOverride("colA", override);

    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setStreamConfigs(streamConfigs())
        .setFieldConfigList(List.of(overridden))
        .setSortedColumn("colA")
        .build();

    try {
      TableConfigUtils.applyConsumingOverrides(tableConfig);
      fail("Expected applyConsumingOverrides to reject sorted column override");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("sorted column"),
          "Expected sorted-column message, got: " + e.getMessage());
    }
  }

  @Test
  public void validateRejectsCompressionCodecKeyOutsideAllowlist() {
    /// `compressionCodec` is intentionally NOT in the allowlist — only `encodingType` + `indexes` are allowed.
    ObjectNode override = JsonUtils.newObjectNode();
    override.put("encodingType", FieldConfig.EncodingType.RAW.name());
    override.put("compressionCodec", FieldConfig.CompressionCodec.LZ4.name());
    FieldConfig overridden = buildRawColumnWithRichConsumingOverride("colA", override);

    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setStreamConfigs(streamConfigs())
        .setFieldConfigList(List.of(overridden))
        .build();

    try {
      TableConfigUtils.validate(tableConfig, buildSchema());
      fail("Expected validation failure for unsupported override key 'compressionCodec'");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Unknown FieldConfig.consumingOverride key"),
          "Expected unknown-key message, got: " + e.getMessage());
      assertTrue(e.getMessage().contains("compressionCodec"),
          "Expected message to surface the bad key, got: " + e.getMessage());
    }
  }

  @Test
  public void buildConsumingSegmentConfigBuilderFallsBackOnInvariantFailure() {
    /// Simulates a stale config that bypassed validate: the override carries a typo'd key. The dispatch helper
    /// must catch the resulting RuntimeException, log, and fall back to the IndexLoadingConfig-driven Builder so
    /// consumption keeps making forward progress on the persisted shape.
    ObjectNode override = JsonUtils.newObjectNode();
    override.put("encodingTpye", FieldConfig.EncodingType.DICTIONARY.name());
    FieldConfig overridden = buildRawColumnWithRichConsumingOverride("colA", override);

    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setStreamConfigs(streamConfigs())
        .setFieldConfigList(List.of(overridden))
        .setNoDictionaryColumns(List.of("colA"))
        .build();
    IndexLoadingConfig ilc = new IndexLoadingConfig(tableConfig, buildSchema());
    /// Track that the onFallback callback fired — this is the hook RealtimeSegmentDataManager uses to bump the
    /// CONSUMING_OVERRIDE_FALLBACK metric, so the test asserts the contract the production caller relies on.
    AtomicInteger fallbackInvocations = new AtomicInteger();
    /// Should NOT throw — the helper catches and logs.
    RealtimeSegmentConfig.Builder builder = TableConfigUtils.buildConsumingSegmentConfigBuilder(
        tableConfig, buildSchema(), ilc, LoggerFactory.getLogger(TableConfigConsumingOverrideTest.class),
        fallbackInvocations::incrementAndGet);
    RealtimeSegmentConfig built = builder.build();
    FieldIndexConfigs colA = built.getIndexConfigByCol().get("colA");
    /// Persisted shape on colA is RAW (no dictionary); the fallback must reflect that, NOT the override.
    assertFalse(colA.getConfig(StandardIndexes.dictionary()).isEnabled(),
        "Fallback path must use the persisted RAW shape, not the override-attempted dictionary shape");
    assertEquals(fallbackInvocations.get(), 1,
        "onFallback callback must be invoked exactly once when the override merge fails");
  }

  @Test
  public void validateRejectsConsumingOverrideOnPartitionColumn() {
    ObjectNode override = JsonUtils.newObjectNode();
    override.put("encodingType", FieldConfig.EncodingType.DICTIONARY.name());
    FieldConfig overridden = buildRawColumnWithRichConsumingOverride("colA", override);

    ColumnPartitionConfig partitionCfg = new ColumnPartitionConfig("Murmur", 4);
    SegmentPartitionConfig segmentPartitionConfig = new SegmentPartitionConfig(Map.of("colA", partitionCfg));

    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setStreamConfigs(streamConfigs())
        .setFieldConfigList(List.of(overridden))
        .setSegmentPartitionConfig(segmentPartitionConfig)
        .build();

    try {
      TableConfigUtils.validate(tableConfig, buildSchema());
      fail("Expected validation failure for consuming override on partition column");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("partition column"),
          "Expected partition-column message, got: " + e.getMessage());
    }
  }

  @Test
  public void applyConsumingOverridesRejectsPartitionColumnOnStaleConfig() {
    ObjectNode override = JsonUtils.newObjectNode();
    override.put("encodingType", FieldConfig.EncodingType.DICTIONARY.name());
    FieldConfig overridden = buildRawColumnWithRichConsumingOverride("colA", override);

    ColumnPartitionConfig partitionCfg = new ColumnPartitionConfig("Murmur", 4);
    SegmentPartitionConfig segmentPartitionConfig = new SegmentPartitionConfig(Map.of("colA", partitionCfg));

    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setStreamConfigs(streamConfigs())
        .setFieldConfigList(List.of(overridden))
        .setSegmentPartitionConfig(segmentPartitionConfig)
        .build();

    try {
      TableConfigUtils.applyConsumingOverrides(tableConfig);
      fail("Expected applyConsumingOverrides to reject partition column override");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("partition column"),
          "Expected partition-column message, got: " + e.getMessage());
    }
  }

  @Test
  public void tableConfigJsonRoundTripPreservesConsumingOverride()
      throws Exception {
    /// Mirrors the production code path in applyConsumingOverrides which uses tableConfig.toJsonNode() — guarantees
    /// the override survives the same serialization/deserialization that the merge utility relies on.
    ObjectNode override = JsonUtils.newObjectNode();
    override.put("encodingType", FieldConfig.EncodingType.DICTIONARY.name());
    override.set("indexes",
        JsonUtils.newObjectNode().set("inverted", JsonUtils.newObjectNode().put("enabled", true)));
    FieldConfig overridden = buildRawColumnWithRichConsumingOverride("colA", override);

    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setStreamConfigs(streamConfigs())
        .setFieldConfigList(List.of(overridden))
        .build();

    JsonNode tcJson = tableConfig.toJsonNode();
    TableConfig roundTripped = JsonUtils.jsonNodeToObject(tcJson, TableConfig.class);
    FieldConfig fc = roundTripped.getFieldConfigList().get(0);
    JsonNode overrideAfter = fc.getConsumingOverride();
    assertTrue(overrideAfter.isObject() && !overrideAfter.isEmpty(),
        "consumingOverride must survive a TableConfig JSON round-trip; got: " + overrideAfter);
    assertEquals(overrideAfter.get("encodingType").asText(), "DICTIONARY");
    assertTrue(overrideAfter.path("indexes").path("inverted").path("enabled").asBoolean(false),
        "inverted index entry must survive a TableConfig JSON round-trip");
  }

  @Test
  public void fieldConfigSerdePreservesConsumingOverride()
      throws Exception {
    ObjectNode override = JsonUtils.newObjectNode();
    override.put("encodingType", FieldConfig.EncodingType.DICTIONARY.name());
    override.set("indexes",
        JsonUtils.newObjectNode().set("inverted", JsonUtils.newObjectNode().put("enabled", true)));
    FieldConfig fieldConfig = new FieldConfig.Builder("colA")
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withConsumingOverride(override)
        .build();

    String json = fieldConfig.toJsonString();
    FieldConfig roundTripped = JsonUtils.stringToObject(json, FieldConfig.class);
    JsonNode consumingOverride = roundTripped.getConsumingOverride();
    assertEquals(consumingOverride.get("encodingType").asText(), "DICTIONARY");
    assertTrue(consumingOverride.path("indexes").path("inverted").path("enabled").asBoolean(false),
        "inverted index entry must survive FieldConfig serde round-trip");
  }
}
