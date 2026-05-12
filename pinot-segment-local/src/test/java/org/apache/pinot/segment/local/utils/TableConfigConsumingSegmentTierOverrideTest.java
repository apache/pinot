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

import java.util.List;
import java.util.Map;
import org.apache.pinot.common.tier.TierFactory;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/// Tests for the synthetic `consuming` tier override used while constructing mutable realtime consuming segments.
public class TableConfigConsumingSegmentTierOverrideTest {
  private static final String TABLE_NAME = "consumingTierOverrideTest";
  private static final String PROFILED_COLUMN = "profiledString";
  private static final String CONTROL_COLUMN = "controlString";
  private static final String TIME_COLUMN = "tsMillis";
  private static final String CONSUMING_TIER = "consuming";
  private static final String COLD_TIER = "coldTier";

  @Test
  public void consumingTierOverrideAppliesToMutableBuilderOnly()
      throws Exception {
    Schema schema = schema();
    TableConfig tableConfig = tableWithConsumingOverride();
    IndexLoadingConfig persistedConfig = new IndexLoadingConfig(tableConfig, schema);

    FieldIndexConfigs persistedColumnConfig = persistedConfig.getFieldIndexConfig(PROFILED_COLUMN);
    assertFalse(persistedColumnConfig.getConfig(StandardIndexes.dictionary()).isEnabled());
    assertFalse(persistedColumnConfig.getConfig(StandardIndexes.inverted()).isEnabled());

    RealtimeSegmentConfig consumingConfig =
        TableConfigUtils.buildConsumingSegmentConfigBuilder(tableConfig, schema, persistedConfig).build();
    FieldIndexConfigs consumingColumnConfig = consumingConfig.getIndexConfigByCol().get(PROFILED_COLUMN);
    assertTrue(consumingColumnConfig.getConfig(StandardIndexes.dictionary()).isEnabled());
    assertTrue(consumingColumnConfig.getConfig(StandardIndexes.inverted()).isEnabled());

    FieldIndexConfigs controlColumnConfig = consumingConfig.getIndexConfigByCol().get(CONTROL_COLUMN);
    assertTrue(controlColumnConfig.getConfig(StandardIndexes.dictionary()).isEnabled());
    assertFalse(controlColumnConfig.getConfig(StandardIndexes.inverted()).isEnabled());

    assertSame(persistedConfig.getTableConfig(), tableConfig);
    assertEquals(tableConfig.getIndexingConfig().getNoDictionaryColumns(), List.of(PROFILED_COLUMN));
    assertEquals(tableConfig.getFieldConfigList().get(0).getEncodingType(), FieldConfig.EncodingType.RAW);
  }

  @Test
  public void storageTierOverridesDoNotApplyToConsumingBuilder()
      throws Exception {
    Schema schema = schema();
    TableConfig tableConfig = tableWithOnlyColdTierOverride();
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(tableConfig, schema);

    RealtimeSegmentConfig consumingConfig =
        TableConfigUtils.buildConsumingSegmentConfigBuilder(tableConfig, schema, indexLoadingConfig).build();

    FieldIndexConfigs profiledColumnConfig = consumingConfig.getIndexConfigByCol().get(PROFILED_COLUMN);
    assertFalse(profiledColumnConfig.getConfig(StandardIndexes.dictionary()).isEnabled());
    assertFalse(profiledColumnConfig.getConfig(StandardIndexes.inverted()).isEnabled());
  }

  @Test
  public void realConsumingStorageTierKeepsStorageTierSemantics()
      throws Exception {
    Schema schema = schema();
    TableConfig tableConfig = tableWithRealConsumingTierOverride();
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(tableConfig, schema);

    RealtimeSegmentConfig consumingConfig =
        TableConfigUtils.buildConsumingSegmentConfigBuilder(tableConfig, schema, indexLoadingConfig).build();
    FieldIndexConfigs mutableColumnConfig = consumingConfig.getIndexConfigByCol().get(PROFILED_COLUMN);
    assertFalse(mutableColumnConfig.getConfig(StandardIndexes.dictionary()).isEnabled());
    assertFalse(mutableColumnConfig.getConfig(StandardIndexes.inverted()).isEnabled());

    indexLoadingConfig.setSegmentTier(CONSUMING_TIER);
    FieldIndexConfigs storageTierColumnConfig = indexLoadingConfig.getFieldIndexConfig(PROFILED_COLUMN);
    assertTrue(storageTierColumnConfig.getConfig(StandardIndexes.dictionary()).isEnabled());
    assertTrue(storageTierColumnConfig.getConfig(StandardIndexes.inverted()).isEnabled());
  }

  @Test
  public void existingTierOverrideHelperBuildsTheConsumingView()
      throws Exception {
    TableConfig tableConfig = tableWithConsumingOverride();
    TableConfig consumingView = TableConfigUtils.overwriteTableConfigForTier(tableConfig, CONSUMING_TIER);

    assertEquals(consumingView.getIndexingConfig().getNoDictionaryColumns(), List.of());
    FieldConfig consumingFieldConfig = consumingView.getFieldConfigList().get(0);
    assertEquals(consumingFieldConfig.getEncodingType(), FieldConfig.EncodingType.DICTIONARY);
    assertTrue(consumingFieldConfig.getIndexes().has("inverted"));

    TableConfig coldView = TableConfigUtils.overwriteTableConfigForTier(tableConfig, COLD_TIER);
    assertSame(coldView, tableConfig);
  }

  @Test
  public void validationRejectsInvalidConsumingView()
      throws Exception {
    FieldConfig profiledFieldConfig = new FieldConfig.Builder(PROFILED_COLUMN)
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withTierOverwrites(JsonUtils.stringToJsonNode("{\"consuming\":{\"encodingType\":\"DICTIONARY\","
            + "\"indexes\":{\"inverted\":{\"enabled\":true}}}}"))
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setStreamConfigs(streamConfigs())
        .setNoDictionaryColumns(List.of(PROFILED_COLUMN))
        .setFieldConfigList(List.of(profiledFieldConfig))
        .build();

    try {
      TableConfigUtils.validate(tableConfig, schemaWithTime());
      fail("Should reject consuming override that does not clear persisted noDictionaryColumns");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("tierOverwrites.consuming"),
          "Expected consuming override validation error, got: " + e.getMessage());
    }
  }

  @Test
  public void validationRejectsUnsupportedConsumingTableIndexOverwrite()
      throws Exception {
    FieldConfig profiledFieldConfig = new FieldConfig.Builder(PROFILED_COLUMN)
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withTierOverwrites(JsonUtils.stringToJsonNode("{\"consuming\":{\"encodingType\":\"DICTIONARY\"}}"))
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setStreamConfigs(streamConfigs())
        .setNoDictionaryColumns(List.of(PROFILED_COLUMN))
        .setTierOverwrites(JsonUtils.stringToJsonNode("{\"consuming\":{\"noDictionaryColumns\":[],"
            + "\"aggregateMetrics\":true}}"))
        .setFieldConfigList(List.of(profiledFieldConfig))
        .build();

    try {
      TableConfigUtils.validate(tableConfig, schemaWithTime());
      fail("Should reject consuming override for tableIndexConfig keys not used by mutable index loading");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("aggregateMetrics"),
          "Expected unsupported consuming tableIndexConfig key validation error, got: " + e.getMessage());
    }
  }

  private static Schema schema() {
    return new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(PROFILED_COLUMN, FieldSpec.DataType.STRING)
        .addSingleValueDimension(CONTROL_COLUMN, FieldSpec.DataType.STRING)
        .build();
  }

  private static TableConfig tableWithConsumingOverride()
      throws Exception {
    FieldConfig profiledFieldConfig = new FieldConfig.Builder(PROFILED_COLUMN)
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withTierOverwrites(JsonUtils.stringToJsonNode("{\"consuming\":{\"encodingType\":\"DICTIONARY\","
            + "\"indexes\":{\"inverted\":{\"enabled\":true}}}}"))
        .build();
    return new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(List.of(PROFILED_COLUMN))
        .setTierOverwrites(JsonUtils.stringToJsonNode("{\"consuming\":{\"noDictionaryColumns\":[]}}"))
        .setFieldConfigList(List.of(profiledFieldConfig))
        .build();
  }

  private static TableConfig tableWithOnlyColdTierOverride()
      throws Exception {
    FieldConfig profiledFieldConfig = new FieldConfig.Builder(PROFILED_COLUMN)
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withTierOverwrites(JsonUtils.stringToJsonNode("{\"coldTier\":{\"encodingType\":\"DICTIONARY\","
            + "\"indexes\":{\"inverted\":{\"enabled\":true}}}}"))
        .build();
    return new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(List.of(PROFILED_COLUMN))
        .setTierOverwrites(JsonUtils.stringToJsonNode("{\"coldTier\":{\"noDictionaryColumns\":[]}}"))
        .setFieldConfigList(List.of(profiledFieldConfig))
        .build();
  }

  private static TableConfig tableWithRealConsumingTierOverride()
      throws Exception {
    FieldConfig profiledFieldConfig = new FieldConfig.Builder(PROFILED_COLUMN)
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withTierOverwrites(JsonUtils.stringToJsonNode("{\"consuming\":{\"encodingType\":\"DICTIONARY\","
            + "\"indexes\":{\"inverted\":{\"enabled\":true}}}}"))
        .build();
    return new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(List.of(PROFILED_COLUMN))
        .setTierOverwrites(JsonUtils.stringToJsonNode("{\"consuming\":{\"noDictionaryColumns\":[]}}"))
        .setFieldConfigList(List.of(profiledFieldConfig))
        .setTierConfigList(List.of(new TierConfig(CONSUMING_TIER, TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "30d", null,
            TierFactory.PINOT_SERVER_STORAGE_TYPE, "consuming_tag_REALTIME", null, null)))
        .build();
  }

  private static Schema schemaWithTime() {
    return new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(PROFILED_COLUMN, FieldSpec.DataType.STRING)
        .addSingleValueDimension(CONTROL_COLUMN, FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
  }

  private static Map<String, String> streamConfigs() {
    return Map.of("streamType", "kafka", "stream.kafka.topic.name", "test", "stream.kafka.decoder.class.name",
        "org.apache.pinot.plugin.stream.kafka.KafkaJSONMessageDecoder");
  }
}
