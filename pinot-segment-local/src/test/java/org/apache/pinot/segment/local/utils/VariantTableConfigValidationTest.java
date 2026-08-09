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
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;
import org.apache.pinot.spi.config.table.FieldConfig.EncodingType;
import org.apache.pinot.spi.config.table.FieldConfig.IndexType;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class VariantTableConfigValidationTest {
  private static final String TABLE_NAME = "variantTable";
  private static final String VARIANT_COLUMN = "payload";
  private static final String ID_COLUMN = "id";

  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .setSchemaName(TABLE_NAME)
      .addSingleValueDimension(ID_COLUMN, DataType.STRING)
      .addSingleValueDimension(VARIANT_COLUMN, DataType.VARIANT)
      .build();

  private static final Schema UPSERT_SCHEMA = new Schema.SchemaBuilder()
      .setSchemaName(TABLE_NAME)
      .addSingleValueDimension(ID_COLUMN, DataType.STRING)
      .addSingleValueDimension(VARIANT_COLUMN, DataType.VARIANT)
      .setPrimaryKeyColumns(List.of(ID_COLUMN))
      .build();

  @Test
  public void testRawForwardIndexWithCompressionAndNullVectorIsValid() {
    TableConfig tableConfig = tableWith(rawVariantFieldConfig());
    assertValid(tableConfig, SCHEMA);
  }

  @Test
  public void testVariantRequiresEffectiveStorageNullHandling() {
    TableConfig nullHandlingDisabled = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(TABLE_NAME)
        .setFieldConfigList(List.of(rawVariantFieldConfig()))
        .build();
    assertInvalid(nullHandlingDisabled, SCHEMA, "Null handling must be enabled for tables containing VARIANT columns");

    Schema columnBasedNullHandlingSchema = new Schema.SchemaBuilder()
        .setSchemaName(TABLE_NAME)
        .setEnableColumnBasedNullHandling(true)
        .addSingleValueDimension(ID_COLUMN, DataType.STRING)
        .addSingleValueDimension(VARIANT_COLUMN, DataType.VARIANT)
        .build();
    TableConfig columnBasedNullHandlingTable = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(TABLE_NAME)
        .setFieldConfigList(List.of(rawVariantFieldConfig()))
        .build();
    assertValid(columnBasedNullHandlingTable, columnBasedNullHandlingSchema);

    Schema nonVariantSchema = new Schema.SchemaBuilder()
        .setSchemaName(TABLE_NAME)
        .addSingleValueDimension(ID_COLUMN, DataType.STRING)
        .build();
    TableConfig nonVariantTable =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    assertValid(nonVariantTable, nonVariantSchema);
  }

  @Test
  public void testDictionaryEncodingIsRejected() {
    FieldConfig fieldConfig = new FieldConfig.Builder(VARIANT_COLUMN)
        .withEncodingType(EncodingType.DICTIONARY)
        .build();
    assertInvalid(tableWith(fieldConfig), SCHEMA, "RAW forward index encoding");
  }

  @Test
  public void testImplicitDictionaryEncodingIsRejected() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(TABLE_NAME)
        .setNullHandlingEnabled(true)
        .build();
    assertInvalid(tableConfig, SCHEMA, "RAW forward index encoding");
  }

  @Test
  public void testExplicitDictionaryWithRawForwardIndexIsRejected() {
    ObjectNode indexes = JsonUtils.newObjectNode();
    indexes.set("dictionary", JsonUtils.newObjectNode());
    FieldConfig fieldConfig = new FieldConfig.Builder(VARIANT_COLUMN)
        .withEncodingType(EncodingType.RAW)
        .withIndexes(indexes)
        .build();
    assertInvalid(tableWith(fieldConfig), SCHEMA, "cannot use a dictionary");
  }

  @Test
  public void testDisabledForwardIndexIsRejected() {
    FieldConfig fieldConfig = new FieldConfig.Builder(VARIANT_COLUMN)
        .withEncodingType(EncodingType.RAW)
        .withProperties(Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true"))
        .build();
    assertInvalid(tableWith(fieldConfig), SCHEMA, "enabled forward index");
  }

  @Test
  public void testSecondaryIndexesAreRejected() {
    for (IndexType indexType
        : new IndexType[]{IndexType.INVERTED, IndexType.FST, IndexType.IFST, IndexType.TEXT, IndexType.JSON,
            IndexType.RANGE}) {
      FieldConfig fieldConfig = new FieldConfig.Builder(VARIANT_COLUMN)
          .withEncodingType(EncodingType.RAW)
          .withIndexTypes(List.of(indexType))
          .build();
      // Dictionary-backed index types can make the effective dictionary configuration fail first.
      assertInvalid(tableWith(fieldConfig), SCHEMA, "VARIANT column");
    }

    TableConfig bloomTable = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(TABLE_NAME)
        .setNullHandlingEnabled(true)
        .setFieldConfigList(List.of(rawVariantFieldConfig()))
        .setBloomFilterColumns(List.of(VARIANT_COLUMN))
        .build();
    assertInvalid(bloomTable, SCHEMA, "supports only a RAW forward index");
  }

  @Test
  public void testSortedAndPartitionKeysAreRejected() {
    TableConfig sortedTable = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(TABLE_NAME)
        .setNullHandlingEnabled(true)
        .setFieldConfigList(List.of(rawVariantFieldConfig()))
        .setSortedColumn(VARIANT_COLUMN)
        .build();
    assertInvalid(sortedTable, SCHEMA, "Cannot sort on VARIANT column");

    SegmentPartitionConfig partitionConfig = new SegmentPartitionConfig(
        Map.of(VARIANT_COLUMN, new ColumnPartitionConfig("Murmur", 4)));
    TableConfig partitionedTable = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(TABLE_NAME)
        .setNullHandlingEnabled(true)
        .setFieldConfigList(List.of(rawVariantFieldConfig()))
        .setSegmentPartitionConfig(partitionConfig)
        .build();
    assertInvalid(partitionedTable, SCHEMA, "Cannot partition on VARIANT column");
  }

  @Test
  public void testPrimaryKeyIsRejected() {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName(TABLE_NAME)
        .addSingleValueDimension(ID_COLUMN, DataType.STRING)
        .addSingleValueDimension(VARIANT_COLUMN, DataType.VARIANT)
        .setPrimaryKeyColumns(List.of(VARIANT_COLUMN))
        .build();
    assertInvalid(tableWith(rawVariantFieldConfig()), schema, "cannot be used as a primary key");
  }

  @Test
  public void testUpsertComparisonColumnIsRejected() {
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setComparisonColumn(VARIANT_COLUMN);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(TABLE_NAME)
        .setUpsertConfig(upsertConfig)
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .build();

    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, UPSERT_SCHEMA);
      fail("Expected VARIANT upsert comparison column validation to fail");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage() != null
              && e.getMessage().contains("VARIANT column cannot be used as an upsert comparison column"),
          "Unexpected validation error: " + e.getMessage());
    }
  }

  @Test
  public void testNonOverwritePartialUpsertStrategyIsRejected() {
    TableConfig tableConfig = partialUpsertTable(UpsertConfig.Strategy.IGNORE);
    try {
      TableConfigUtils.validatePartialUpsertStrategies(tableConfig, UPSERT_SCHEMA);
      fail("Expected non-OVERWRITE VARIANT partial-upsert strategy validation to fail");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage() != null
              && e.getMessage().contains("VARIANT column supports only OVERWRITE partial-upsert strategy"),
          "Unexpected validation error: " + e.getMessage());
    }
  }

  @Test
  public void testOverwritePartialUpsertStrategyIsValid() {
    TableConfigUtils.validatePartialUpsertStrategies(
        partialUpsertTable(UpsertConfig.Strategy.OVERWRITE), UPSERT_SCHEMA);
  }

  @Test
  public void testNonOverwriteDefaultPartialUpsertStrategyIsRejectedForUnlistedVariant() {
    // The VARIANT column is not listed in partialUpsertStrategies, so at merge time it uses
    // defaultPartialUpsertStrategy. A non-OVERWRITE default must still be rejected.
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    upsertConfig.setComparisonColumn(ID_COLUMN);
    upsertConfig.setPartialUpsertStrategies(Map.of(ID_COLUMN, UpsertConfig.Strategy.OVERWRITE));
    upsertConfig.setDefaultPartialUpsertStrategy(UpsertConfig.Strategy.UNION);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(TABLE_NAME)
        .setNullHandlingEnabled(true)
        .setUpsertConfig(upsertConfig)
        .build();
    try {
      TableConfigUtils.validatePartialUpsertStrategies(tableConfig, UPSERT_SCHEMA);
      fail("Expected non-OVERWRITE default VARIANT partial-upsert strategy validation to fail");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage() != null
              && e.getMessage().contains("VARIANT column supports only OVERWRITE partial-upsert strategy"),
          "Unexpected validation error: " + e.getMessage());
    }
  }

  @Test
  public void testDefaultOverwritePartialUpsertStrategyIsValidForUnlistedVariant() {
    // The VARIANT column is unlisted and defaultPartialUpsertStrategy defaults to OVERWRITE, which is valid.
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    upsertConfig.setComparisonColumn(ID_COLUMN);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(TABLE_NAME)
        .setNullHandlingEnabled(true)
        .setUpsertConfig(upsertConfig)
        .build();
    TableConfigUtils.validatePartialUpsertStrategies(tableConfig, UPSERT_SCHEMA);
  }

  @Test
  public void testCustomPartialUpsertMergerIsRejectedForVariant() {
    // A custom merger class cannot be validated against the OVERWRITE-only VARIANT contract, so it must be rejected.
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    upsertConfig.setComparisonColumn(ID_COLUMN);
    upsertConfig.setPartialUpsertMergerClass("com.example.CustomMerger");
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(TABLE_NAME)
        .setNullHandlingEnabled(true)
        .setUpsertConfig(upsertConfig)
        .build();
    try {
      TableConfigUtils.validatePartialUpsertStrategies(tableConfig, UPSERT_SCHEMA);
      fail("Expected custom partialUpsertMergerClass with a VARIANT column to fail");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage() != null && e.getMessage().contains("custom")
              && e.getMessage().contains("VARIANT column supports only OVERWRITE partial-upsert strategy"),
          "Unexpected validation error: " + e.getMessage());
    }
  }

  @Test
  public void testMetricsAggregationAndDefaultStarTreeAreRejected() {
    TableConfig aggregateMetricsTable = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(TABLE_NAME)
        .setNullHandlingEnabled(true)
        .setFieldConfigList(List.of(rawVariantFieldConfig()))
        .setAggregateMetrics(true)
        .build();
    assertInvalid(aggregateMetricsTable, SCHEMA, "VARIANT dimension column as an aggregation key");

    TableConfig defaultStarTreeTable = tableWith(rawVariantFieldConfig());
    defaultStarTreeTable.getIndexingConfig().setEnableDefaultStarTree(true);
    assertInvalid(defaultStarTreeTable, SCHEMA, "Default star-tree index cannot include VARIANT column");
  }

  @Test
  public void testExplicitStarTreeIsRejected() {
    StarTreeIndexConfig starTreeIndexConfig =
        new StarTreeIndexConfig(List.of(ID_COLUMN), null, List.of("SUM__" + VARIANT_COLUMN), null, 1);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(TABLE_NAME)
        .setNullHandlingEnabled(true)
        .setFieldConfigList(List.of(rawVariantFieldConfig()))
        .setStarTreeIndexConfigs(List.of(starTreeIndexConfig))
        .build();
    assertInvalid(tableConfig, SCHEMA, "Star-tree index cannot be created on VARIANT column");
  }

  private static FieldConfig rawVariantFieldConfig() {
    return new FieldConfig.Builder(VARIANT_COLUMN)
        .withEncodingType(EncodingType.RAW)
        .withCompressionCodec(CompressionCodec.ZSTANDARD)
        .build();
  }

  private static TableConfig tableWith(FieldConfig fieldConfig) {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(TABLE_NAME)
        .setNullHandlingEnabled(true)
        .setFieldConfigList(List.of(fieldConfig))
        .build();
  }

  private static TableConfig partialUpsertTable(UpsertConfig.Strategy strategy) {
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    upsertConfig.setComparisonColumn(ID_COLUMN);
    upsertConfig.setPartialUpsertStrategies(Map.of(VARIANT_COLUMN, strategy));
    return new TableConfigBuilder(TableType.REALTIME)
        .setTableName(TABLE_NAME)
        .setNullHandlingEnabled(true)
        .setUpsertConfig(upsertConfig)
        .build();
  }

  private static void assertValid(TableConfig tableConfig, Schema schema) {
    try {
      TableConfigUtils.validate(tableConfig, schema);
    } catch (Exception e) {
      fail("Expected validation to pass, but got: " + e.getMessage(), e);
    }
  }

  private static void assertInvalid(TableConfig tableConfig, Schema schema, String messageFragment) {
    try {
      TableConfigUtils.validate(tableConfig, schema);
      fail("Expected validation failure containing: " + messageFragment);
    } catch (Exception e) {
      assertTrue(e.getMessage() != null && e.getMessage().contains(messageFragment),
          "Expected '" + messageFragment + "' in error, but got: " + e.getMessage());
    }
  }
}
