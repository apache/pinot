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
package org.apache.pinot.segment.local.segment.index.loader;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


public class IndexLoadingConfigTest {
  private static final String TABLE_NAME = "table01";

  @Test
  public void testCalculateIndexConfigsWithoutTierOverwrites()
      throws IOException {
    InstanceDataManagerConfig idmCfg = mock(InstanceDataManagerConfig.class);
    when(idmCfg.getConfig()).thenReturn(new PinotConfiguration());
    // Schema has two string columns: col1 and col2.
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addSingleValueDimension("col1", FieldSpec.DataType.INT)
            .addSingleValueDimension("col2", FieldSpec.DataType.STRING).build();
    // On the default tier, both are dict-encoded. col1 has inverted index as set in `tableIndexConfig` and col2 has
    // bloom filter as configured with `fieldConfigList`; and there is one ST index built with col1.
    //@formatter:off
    String col2CfgStr = "{"
        + "  \"name\": \"col2\","
        + "  \"indexes\": {"
        + "    \"bloom\": {\"enabled\": \"true\"}"
        + "  }"
        + "}";
    FieldConfig col2Cfg = JsonUtils.stringToObject(col2CfgStr, FieldConfig.class);
    String stIdxCfgStr = "{"
        + "  \"dimensionsSplitOrder\": [\"col1\"],"
        + "  \"functionColumnPairs\": [\"MAX__col1\"],"
        + "  \"maxLeafRecords\": 10"
        + "}";
    //@formatter:on
    StarTreeIndexConfig stIdxCfg = JsonUtils.stringToObject(stIdxCfgStr, StarTreeIndexConfig.class);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(Collections.singletonList("col1"))
        .setStarTreeIndexConfigs(Collections.singletonList(stIdxCfg))
        .setFieldConfigList(Collections.singletonList(col2Cfg)).build();
    IndexLoadingConfig ilc = new IndexLoadingConfig(idmCfg, tableConfig, schema);
    // Check index configs for default tier
    assertEquals(ilc.getStarTreeIndexConfigs().size(), 1);
    Map<String, FieldIndexConfigs> allFieldCfgs = ilc.getFieldIndexConfigByColName();
    FieldIndexConfigs fieldCfgs = allFieldCfgs.get("col1");
    assertTrue(fieldCfgs.getConfig(StandardIndexes.inverted()).isEnabled());
    assertFalse(fieldCfgs.getConfig(StandardIndexes.bloomFilter()).isEnabled());
    assertTrue(fieldCfgs.getConfig(StandardIndexes.dictionary()).isEnabled());
    fieldCfgs = allFieldCfgs.get("col2");
    assertFalse(fieldCfgs.getConfig(StandardIndexes.inverted()).isEnabled());
    assertTrue(fieldCfgs.getConfig(StandardIndexes.bloomFilter()).isEnabled());
    assertTrue(fieldCfgs.getConfig(StandardIndexes.dictionary()).isEnabled());
  }

  @Test
  public void testCalculateIndexConfigsWithTierOverwrites()
      throws IOException {
    InstanceDataManagerConfig idmCfg = mock(InstanceDataManagerConfig.class);
    when(idmCfg.getConfig()).thenReturn(new PinotConfiguration());
    // Schema has two string columns: col1 and col2.
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addSingleValueDimension("col1", FieldSpec.DataType.INT)
            .addSingleValueDimension("col2", FieldSpec.DataType.STRING).build();
    // On the default tier, both are dict-encoded. col1 has inverted index as set in `tableIndexConfig` and col2 has
    // bloom filter as configured with `fieldConfigList`; and there is one ST index built with col1.
    // On the coldTier, we overwrite col1 to use bloom filter only and col2 to use raw encoding w/o any index.
    //@formatter:off
    String col1CfgStr = "{"
        + "  \"name\": \"col1\","
        + "  \"indexes\": {"
        + "    \"inverted\": {\"enabled\": \"true\"}"
        + "  },"
        + "  \"tierOverwrites\": {"
        + "    \"coldTier\": {"
        + "      \"indexes\": {"
        + "        \"bloom\": {\"enabled\": \"true\"}"
        + "      }"
        + "    }"
        + "  }"
        + "}";
    FieldConfig col1Cfg = JsonUtils.stringToObject(col1CfgStr, FieldConfig.class);
    String col2CfgStr = "{"
        + "  \"name\": \"col2\","
        + "  \"indexes\": {"
        + "    \"bloom\": {\"enabled\": \"true\"}"
        + "  },"
        + "  \"tierOverwrites\": {"
        + "    \"coldTier\": {"
        + "      \"encodingType\": \"RAW\","
        + "      \"indexes\": {}"
        + "    }"
        + "  }"
        + "}";
    FieldConfig col2Cfg = JsonUtils.stringToObject(col2CfgStr, FieldConfig.class);
    String stIdxCfgStr = "{"
        + "  \"dimensionsSplitOrder\": [\"col1\"],"
        + "  \"functionColumnPairs\": [\"MAX__col1\"],"
        + "  \"maxLeafRecords\": 10"
        + "}";
    //@formatter:on
    StarTreeIndexConfig stIdxCfg = JsonUtils.stringToObject(stIdxCfgStr, StarTreeIndexConfig.class);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setStarTreeIndexConfigs(Collections.singletonList(stIdxCfg))
        .setTierOverwrites(JsonUtils.stringToJsonNode("{\"coldTier\": {\"starTreeIndexConfigs\": []}}"))
        .setFieldConfigList(Arrays.asList(col1Cfg, col2Cfg)).build();
    IndexLoadingConfig ilc = new IndexLoadingConfig(idmCfg, tableConfig, schema);
    ilc.setSegmentTier("coldTier");
    // Check index configs for coldTier
    assertEquals(ilc.getStarTreeIndexConfigs().size(), 0);
    Map<String, FieldIndexConfigs> allFieldCfgs = ilc.getFieldIndexConfigByColName();
    FieldIndexConfigs fieldCfgs = allFieldCfgs.get("col1");
    assertFalse(fieldCfgs.getConfig(StandardIndexes.inverted()).isEnabled());
    assertTrue(fieldCfgs.getConfig(StandardIndexes.bloomFilter()).isEnabled());
    assertTrue(fieldCfgs.getConfig(StandardIndexes.dictionary()).isEnabled());
    fieldCfgs = allFieldCfgs.get("col2");
    assertFalse(fieldCfgs.getConfig(StandardIndexes.inverted()).isEnabled());
    assertFalse(fieldCfgs.getConfig(StandardIndexes.bloomFilter()).isEnabled());
    assertFalse(fieldCfgs.getConfig(StandardIndexes.dictionary()).isEnabled());
  }

  @Test
  public void testCalculateForwardIndexConfig()
      throws JsonProcessingException {
    // Check default settings
    //@formatter:off
    String col1CfgStr = "{"
        + "  \"name\": \"col1\","
        + "  \"encodingType\": \"RAW\","
        + "  \"indexes\": {"
        + "    \"forward\": {}"
        + "  }"
        + "}";
    //@formatter:on
    FieldConfig col1Cfg = JsonUtils.stringToObject(col1CfgStr, FieldConfig.class);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setFieldConfigList(List.of(col1Cfg)).build();
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addSingleValueDimension("col1", FieldSpec.DataType.INT)
            .build();
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(tableConfig, schema);
    FieldIndexConfigs indexConfigs = indexLoadingConfig.getFieldIndexConfig("col1");
    assertNotNull(indexConfigs);
    ForwardIndexConfig forwardIndexConfig = indexConfigs.getConfig(StandardIndexes.forward());
    assertTrue(forwardIndexConfig.isEnabled());
    assertNull(forwardIndexConfig.getCompressionCodec());
    assertFalse(forwardIndexConfig.isDeriveNumDocsPerChunk());
    assertEquals(forwardIndexConfig.getRawIndexWriterVersion(), ForwardIndexConfig.getDefaultRawWriterVersion());
    assertEquals(forwardIndexConfig.getTargetMaxChunkSize(), ForwardIndexConfig.getDefaultTargetMaxChunkSize());
    assertEquals(forwardIndexConfig.getTargetDocsPerChunk(), ForwardIndexConfig.getDefaultTargetDocsPerChunk());

    // Check custom settings
    //@formatter:off
    col1CfgStr = "{"
        + "  \"name\": \"col1\","
        + "  \"encodingType\": \"RAW\","
        + "  \"indexes\": {"
        + "    \"forward\": {"
        + "      \"compressionCodec\": \"SNAPPY\","
        + "      \"deriveNumDocsPerChunk\": true,"
        + "      \"rawIndexWriterVersion\": 4,"
        + "      \"targetMaxChunkSize\": \"100K\","
        + "      \"targetDocsPerChunk\": 100"
        + "    }"
        + "  }"
        + "}";
    //@formatter:on
    col1Cfg = JsonUtils.stringToObject(col1CfgStr, FieldConfig.class);
    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setFieldConfigList(List.of(col1Cfg)).build();
    schema =
        new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addSingleValueDimension("col1", FieldSpec.DataType.INT)
            .build();
    indexLoadingConfig = new IndexLoadingConfig(tableConfig, schema);
    indexConfigs = indexLoadingConfig.getFieldIndexConfig("col1");
    assertNotNull(indexConfigs);
    forwardIndexConfig = indexConfigs.getConfig(StandardIndexes.forward());
    assertTrue(forwardIndexConfig.isEnabled());
    assertEquals(forwardIndexConfig.getCompressionCodec(), FieldConfig.CompressionCodec.SNAPPY);
    assertTrue(forwardIndexConfig.isDeriveNumDocsPerChunk());
    assertEquals(forwardIndexConfig.getRawIndexWriterVersion(), 4);
    assertEquals(forwardIndexConfig.getTargetMaxChunkSize(), "100K");
    assertEquals(forwardIndexConfig.getTargetDocsPerChunk(), 100);

    // Check disabled settings
    //@formatter:off
    col1CfgStr = "{"
        + "  \"name\": \"col1\","
        + "  \"encodingType\": \"RAW\","
        + "  \"indexes\": {"
        + "    \"forward\": {"
        + "      \"disabled\": true"
        + "    }"
        + "  }"
        + "}";
    //@formatter:on
    col1Cfg = JsonUtils.stringToObject(col1CfgStr, FieldConfig.class);
    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setFieldConfigList(List.of(col1Cfg)).build();
    indexLoadingConfig = new IndexLoadingConfig(tableConfig, schema);
    indexConfigs = indexLoadingConfig.getFieldIndexConfig("col1");
    assertNotNull(indexConfigs);
    forwardIndexConfig = indexConfigs.getConfig(StandardIndexes.forward());
    assertFalse(forwardIndexConfig.isEnabled());
    assertNull(forwardIndexConfig.getCompressionCodec());
    assertFalse(forwardIndexConfig.isDeriveNumDocsPerChunk());
    assertEquals(forwardIndexConfig.getRawIndexWriterVersion(), ForwardIndexConfig.getDefaultRawWriterVersion());
    assertEquals(forwardIndexConfig.getTargetMaxChunkSize(), ForwardIndexConfig.getDefaultTargetMaxChunkSize());
    assertEquals(forwardIndexConfig.getTargetDocsPerChunk(), ForwardIndexConfig.getDefaultTargetDocsPerChunk());
  }
}
