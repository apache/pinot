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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests for `tierOverwrites.consuming` while constructing mutable realtime consuming segments.
public class TableConfigConsumingSegmentTierOverrideTest {
  private static final String TABLE_NAME = "consumingTierOverrideTest";
  private static final String PROFILED_COLUMN = "profiledString";
  private static final String CONTROL_COLUMN = "controlString";
  private static final String TIME_COLUMN = "tsMillis";
  private static final String CONSUMING_TIER = "consuming";

  @Test
  public void consumingTierOverrideBuildsMutableViewOnly()
      throws Exception {
    Schema schema = schemaWithTime();
    TableConfig tableConfig = tableWithConsumingOverride();
    IndexLoadingConfig persistedConfig = new IndexLoadingConfig(tableConfig, schema);

    FieldIndexConfigs persistedColumnConfig = persistedConfig.getFieldIndexConfig(PROFILED_COLUMN);
    assertColumnIndexes(persistedColumnConfig, false, false);
    assertNull(tableConfig.getIndexingConfig().getMultiColumnTextIndexConfig());

    IndexLoadingConfig consumingIndexLoadingConfig =
        TableConfigUtils.buildConsumingSegmentIndexLoadingConfig(tableConfig, schema, persistedConfig);
    RealtimeSegmentConfig consumingConfig = new RealtimeSegmentConfig.Builder(consumingIndexLoadingConfig)
        .setTextIndexConfig(consumingIndexLoadingConfig.getMultiColTextIndexConfig())
        .build();
    FieldIndexConfigs consumingColumnConfig = consumingConfig.getIndexConfigByCol().get(PROFILED_COLUMN);
    assertColumnIndexes(consumingColumnConfig, true, true);

    FieldIndexConfigs controlColumnConfig = consumingConfig.getIndexConfigByCol().get(CONTROL_COLUMN);
    assertColumnIndexes(controlColumnConfig, true, false);
    assertEquals(consumingConfig.getMultiColIndexConfig().getColumns(), List.of(PROFILED_COLUMN));

    assertEquals(tableConfig.getIndexingConfig().getNoDictionaryColumns(), List.of(PROFILED_COLUMN));
    assertEquals(tableConfig.getFieldConfigList().get(0).getIndexes().get("forward").get("encodingType").asText(),
        FieldConfig.EncodingType.RAW.name());
  }

  @Test
  public void realConsumingStorageTierDoesNotApplyMutableConsumingOverride()
      throws Exception {
    Schema schema = schemaWithTime();
    TableConfig tableConfig = tableWithRealConsumingStorageTierOverride();
    TableConfigUtils.validate(tableConfig, schema);
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(tableConfig, schema);

    IndexLoadingConfig consumingIndexLoadingConfig =
        TableConfigUtils.buildConsumingSegmentIndexLoadingConfig(tableConfig, schema, indexLoadingConfig);
    RealtimeSegmentConfig consumingConfig = new RealtimeSegmentConfig.Builder(consumingIndexLoadingConfig)
        .setTextIndexConfig(consumingIndexLoadingConfig.getMultiColTextIndexConfig())
        .build();

    FieldIndexConfigs profiledColumnConfig = consumingConfig.getIndexConfigByCol().get(PROFILED_COLUMN);
    assertColumnIndexes(profiledColumnConfig, false, false);

    IndexLoadingConfig consumingStorageConfig = new IndexLoadingConfig(tableConfig, schema);
    consumingStorageConfig.setSegmentTier(CONSUMING_TIER);
    FieldIndexConfigs storageTierColumnConfig = consumingStorageConfig.getFieldIndexConfig(PROFILED_COLUMN);
    assertColumnIndexes(storageTierColumnConfig, true, true);
  }

  @Test
  public void validationRejectsConsumingTierOverwriteViewThatFailsNormalIndexValidation()
      throws Exception {
    assertInvalidTierOverwriteView(CONSUMING_TIER, invalidRealtimeConsumingOverride(), schemaWithTime());
  }

  private static void assertColumnIndexes(FieldIndexConfigs fieldIndexConfigs, boolean dictionaryEnabled,
      boolean invertedEnabled) {
    assertEquals(fieldIndexConfigs.getConfig(StandardIndexes.dictionary()).isEnabled(), dictionaryEnabled);
    assertEquals(fieldIndexConfigs.getConfig(StandardIndexes.inverted()).isEnabled(), invertedEnabled);
  }

  private static void assertInvalidTierOverwriteView(String tier, TableConfig tableConfig, Schema schema) {
    IllegalStateException e = expectThrows(IllegalStateException.class, () -> TableConfigUtils.validate(tableConfig,
        schema));
    assertTrue(e.getMessage().contains("tierOverwrites." + tier),
        "Expected tier override validation error, got: " + e.getMessage());
  }

  private static TableConfig tableWithConsumingOverride()
      throws Exception {
    FieldConfig profiledFieldConfig = profiledFieldConfigWithTierOverwrite(CONSUMING_TIER);
    return new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(List.of(PROFILED_COLUMN))
        .setTierOverwrites(JsonUtils.stringToJsonNode("{\"consuming\":{\"noDictionaryColumns\":[],"
            + "\"multiColumnTextIndexConfig\":{\"columns\":[\"" + PROFILED_COLUMN + "\"]}}}"))
        .setFieldConfigList(List.of(profiledFieldConfig))
        .build();
  }

  private static TableConfig tableWithRealConsumingStorageTierOverride()
      throws Exception {
    FieldConfig profiledFieldConfig = profiledFieldConfigWithTierOverwrite(CONSUMING_TIER);
    return new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setStreamConfigs(streamConfigs())
        .setNoDictionaryColumns(List.of(PROFILED_COLUMN))
        .setTierConfigList(List.of(consumingTierConfig()))
        .setTierOverwrites(JsonUtils.stringToJsonNode("{\"consuming\":{\"noDictionaryColumns\":[]}}"))
        .setFieldConfigList(List.of(profiledFieldConfig))
        .build();
  }

  private static Schema schemaWithTime() {
    return new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(PROFILED_COLUMN, FieldSpec.DataType.STRING)
        .addSingleValueDimension(CONTROL_COLUMN, FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
  }

  private static TableConfig invalidRealtimeConsumingOverride()
      throws Exception {
    return new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setStreamConfigs(streamConfigs())
        .setNoDictionaryColumns(List.of(PROFILED_COLUMN))
        .setFieldConfigList(List.of(profiledFieldConfigWithTierOverwrite(CONSUMING_TIER)))
        .build();
  }

  private static TierConfig consumingTierConfig() {
    return new TierConfig(CONSUMING_TIER, TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "30d", null,
        TierFactory.PINOT_SERVER_STORAGE_TYPE, "consuming_tag_REALTIME", null, null);
  }

  private static FieldConfig profiledFieldConfigWithTierOverwrite(String tier)
      throws Exception {
    return new FieldConfig.Builder(PROFILED_COLUMN)
        .withIndexes(JsonUtils.stringToJsonNode("{\"forward\":{\"encodingType\":\"RAW\"}}"))
        .withTierOverwrites(JsonUtils.stringToJsonNode("{\"" + tier
            + "\":{\"indexes\":{\"forward\":{\"encodingType\":\"DICTIONARY\"},\"inverted\":{\"disabled\":false}}}}"))
        .build();
  }

  private static Map<String, String> streamConfigs() {
    return Map.of("streamType", "kafka", "stream.kafka.topic.name", "test", "stream.kafka.decoder.class.name",
        "org.apache.pinot.plugin.stream.kafka.KafkaJSONMessageDecoder");
  }
}
