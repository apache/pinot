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
package org.apache.pinot.core.util;

import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.tier.TierFactory;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.core.startree.v2.AggregationFunctionColumnPair;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.ingestion.FilterConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for the validations in {@link TableConfigUtils}
 */
public class TableConfigUtilsTest {

  private static final String TABLE_NAME = "testTable";
  private static final String TIME_COLUMN = "timeColumn";

  @Test
  public void validateTimeColumnValidationConfig() {
    // REALTIME table

    // null timeColumnName and schema
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).build();
    try {
      TableConfigUtils.validate(tableConfig, null);
      Assert.fail("Should fail for null timeColumnName and null schema in REALTIME table");
    } catch (IllegalStateException e) {
      // expected
    }

    // null schema only
    tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).setTimeColumnName(TIME_COLUMN).build();
    try {
      TableConfigUtils.validate(tableConfig, null);
      Assert.fail("Should fail for null schema in REALTIME table");
    } catch (IllegalStateException e) {
      // expected
    }

    // null timeColumnName only
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).build();
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for null timeColumnName in REALTIME table");
    } catch (IllegalStateException e) {
      // expected
    }

    // timeColumnName not present in schema
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).build();
    tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).setTimeColumnName(TIME_COLUMN).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for timeColumnName not present in schema for REALTIME table");
    } catch (IllegalStateException e) {
      // expected
    }

    // timeColumnName not present as valid time spec schema
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(TIME_COLUMN, FieldSpec.DataType.LONG).build();
    tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).setTimeColumnName(TIME_COLUMN).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for invalid fieldSpec for timeColumnName in schema for REALTIME table");
    } catch (IllegalStateException e) {
      // expected
    }

    // valid
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();
    tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).setTimeColumnName(TIME_COLUMN).build();
    TableConfigUtils.validate(tableConfig, schema);

    // OFFLINE table
    // null timeColumnName and schema - allowed in OFFLINE
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    TableConfigUtils.validate(tableConfig, null);

    // null schema only - allowed in OFFLINE
    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTimeColumnName(TIME_COLUMN).build();
    TableConfigUtils.validate(tableConfig, null);

    // null timeColumnName only - allowed in OFFLINE
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).build();
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    TableConfigUtils.validate(tableConfig, schema);

    // non-null schema and timeColumnName, but timeColumnName not present in schema
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).build();
    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTimeColumnName(TIME_COLUMN).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for timeColumnName not present in schema for OFFLINE table");
    } catch (IllegalStateException e) {
      // expected
    }

    // non-null schema nd timeColumnName, but timeColumnName not present as a time spec in schema
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(TIME_COLUMN, FieldSpec.DataType.STRING).build();
    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTimeColumnName(TIME_COLUMN).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for timeColumnName not present in schema for OFFLINE table");
    } catch (IllegalStateException e) {
      // expected
    }

    // empty timeColumnName - valid
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).build();
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTimeColumnName("").build();
    TableConfigUtils.validate(tableConfig, schema);

    // valid
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();
    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTimeColumnName(TIME_COLUMN).build();
    TableConfigUtils.validate(tableConfig, schema);
  }

  @Test
  public void validateDimensionTableConfig() {
    // dimension table with REALTIME type (should be OFFLINE)
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(TIME_COLUMN, FieldSpec.DataType.STRING).build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(TABLE_NAME).setIsDimTable(true).setTimeColumnName(TIME_COLUMN).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail with a Dimension table of type REALTIME");
    } catch (IllegalStateException e) {
      // expected
    }

    // dimension table without a schema
    tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(TABLE_NAME).setIsDimTable(true).setTimeColumnName(TIME_COLUMN).build();
    try {
      TableConfigUtils.validate(tableConfig, null);
      Assert.fail("Should fail with a Dimension table without a schema");
    } catch (IllegalStateException e) {
      // expected
    }

    // dimension table without a Primary Key
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(TIME_COLUMN, FieldSpec.DataType.STRING).build();
    tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(TABLE_NAME).setIsDimTable(true).setTimeColumnName(TIME_COLUMN).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail with a Dimension without a primary key");
    } catch (IllegalStateException e) {
      // expected
    }

    // valid dimension table
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
        .setPrimaryKeyColumns(Lists.newArrayList("myCol")).build();
    tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(TABLE_NAME).setIsDimTable(true).build();
    TableConfigUtils.validate(tableConfig, schema);
  }

  @Test
  public void validateIngestionConfig() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).build();
    // null ingestion config
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setIngestionConfig(null).build();
    TableConfigUtils.validate(tableConfig, schema);

    // null filter config, transform config
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setIngestionConfig(new IngestionConfig(null, null, null, null)).build();
    TableConfigUtils.validate(tableConfig, schema);

    // null filter function
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setIngestionConfig(new IngestionConfig(null, null, new FilterConfig(null), null)).build();
    TableConfigUtils.validate(tableConfig, schema);

    // valid filterFunction
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setIngestionConfig(
        new IngestionConfig(null, null, new FilterConfig("startsWith(columnX, \"myPrefix\")"), null)).build();
    TableConfigUtils.validate(tableConfig, schema);

    // valid filterFunction
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setIngestionConfig(new IngestionConfig(null, null, new FilterConfig("Groovy({x == 10}, x)"), null)).build();
    TableConfigUtils.validate(tableConfig, schema);

    // invalid filter function
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setIngestionConfig(new IngestionConfig(null, null, new FilterConfig("Groovy(badExpr)"), null)).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail on invalid filter function string");
    } catch (IllegalStateException e) {
      // expected
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setIngestionConfig(new IngestionConfig(null, null, new FilterConfig("fakeFunction(xx)"), null)).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for invalid filter function");
    } catch (IllegalStateException e) {
      // expected
    }

    // empty transform configs
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setIngestionConfig(new IngestionConfig(null, null, null, Collections.emptyList())).build();
    TableConfigUtils.validate(tableConfig, schema);

    // transformed column not in schema
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setIngestionConfig(
        new IngestionConfig(null, null, null, Lists.newArrayList(new TransformConfig("myCol", "reverse(anotherCol)"))))
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for transformedColumn not present in schema");
    } catch (IllegalStateException e) {
      // expected
    }

    schema =
        new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
            .build();
    // valid transform configs
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setIngestionConfig(
        new IngestionConfig(null, null, null, Lists.newArrayList(new TransformConfig("myCol", "reverse(anotherCol)"))))
        .build();
    TableConfigUtils.validate(tableConfig, schema);

    schema =
        new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
            .addMetric("transformedCol", FieldSpec.DataType.LONG).build();
    // valid transform configs
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setIngestionConfig(
        new IngestionConfig(null, null, null, Lists.newArrayList(new TransformConfig("myCol", "reverse(anotherCol)"),
            new TransformConfig("transformedCol", "Groovy({x+y}, x, y)")))).build();
    TableConfigUtils.validate(tableConfig, schema);

    // null transform column name
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setIngestionConfig(
        new IngestionConfig(null, null, null, Lists.newArrayList(new TransformConfig(null, "reverse(anotherCol)"))))
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for null column name in transform config");
    } catch (IllegalStateException e) {
      // expected
    }

    // null transform function string
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setIngestionConfig(
        new IngestionConfig(null, null, null, Lists.newArrayList(new TransformConfig("myCol", null)))).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for null transform function in transform config");
    } catch (IllegalStateException e) {
      // expected
    }

    // invalid function
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setIngestionConfig(
        new IngestionConfig(null, null, null, Lists.newArrayList(new TransformConfig("myCol", "fakeFunction(col)"))))
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for invalid transform function in transform config");
    } catch (IllegalStateException e) {
      // expected
    }

    // invalid function
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setIngestionConfig(
        new IngestionConfig(null, null, null, Lists.newArrayList(new TransformConfig("myCol", "Groovy(badExpr)"))))
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for invalid transform function in transform config");
    } catch (IllegalStateException e) {
      // expected
    }

    // input field name used as destination field
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setIngestionConfig(
        new IngestionConfig(null, null, null, Lists.newArrayList(new TransformConfig("myCol", "reverse(myCol)"))))
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail due to use of myCol as arguments and columnName");
    } catch (IllegalStateException e) {
      // expected
    }

    // input field name used as destination field
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setIngestionConfig(
        new IngestionConfig(null, null, null,
            Lists.newArrayList(new TransformConfig("myCol", "Groovy({x + y + myCol}, x, myCol, y)")))).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail due to use of myCol as arguments and columnName");
    } catch (IllegalStateException e) {
      // expected
    }

    // duplicate transform config
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setIngestionConfig(
        new IngestionConfig(null, null, null,
            Lists.newArrayList(new TransformConfig("myCol", "reverse(x)"), new TransformConfig("myCol", "lower(y)"))))
        .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail due to duplicate transform config");
    } catch (IllegalStateException e) {
      // expected
    }

    // derived columns - should pass
    schema =
        new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addSingleValueDimension("derivedCol", FieldSpec.DataType.STRING)
            .addMetric("transformedCol", FieldSpec.DataType.LONG).build();
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setIngestionConfig(
        new IngestionConfig(null, null, null, Lists.newArrayList(new TransformConfig("transformedCol", "reverse(x)"),
            new TransformConfig("derivedCol", "lower(transformedCol)")))).build();
    TableConfigUtils.validate(tableConfig, schema);

    // derived columns - should pass
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setIngestionConfig(
        new IngestionConfig(null, null, null, Lists.newArrayList(new TransformConfig("transformedCol", "reverse(x)"),
            new TransformConfig("derivedCol", "Groovy({transformedCol + x}, transformedCol, x)")))).build();
    TableConfigUtils.validate(tableConfig, schema);

    // derived columns - should pass
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension("derivedCol", FieldSpec.DataType.STRING)
        .addMetric("transformedCol1", FieldSpec.DataType.LONG).addMetric("transformedCol2", FieldSpec.DataType.LONG)
        .build();
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setIngestionConfig(
        new IngestionConfig(null, null, null, Lists.newArrayList(new TransformConfig("transformedCol1", "reverse(x)"),
            new TransformConfig("transformedCol2", "reverse(y)"), new TransformConfig("derivedCol",
                "Groovy({transformedCol1 + transformedCol2}, transformedCol1, transformedCol2)")))).build();
    TableConfigUtils.validate(tableConfig, schema);

    // derived column on derived column - should fail
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension("derivedCol", FieldSpec.DataType.STRING)
        .addMetric("derivedColOnDerivedCol", FieldSpec.DataType.LONG).addMetric("transformedCol", FieldSpec.DataType.LONG)
        .build();
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setIngestionConfig(
        new IngestionConfig(null, null, null, Lists.newArrayList(new TransformConfig("transformedCol", "reverse(x)"),
            new TransformConfig("derivedCol", "lower(transformedCol)"),
            new TransformConfig("derivedColOnDerivedCol", "lower(derivedCol)")))).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail due to derived column on derived column");
    } catch (IllegalStateException e) {
      // expected
    }

    // derived column on derived column - should fail
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setIngestionConfig(
        new IngestionConfig(null, null, null, Lists.newArrayList(new TransformConfig("transformedCol", "reverse(x)"),
            new TransformConfig("derivedCol", "lower(transformedCol)"),
            new TransformConfig("derivedColOnDerivedCol", "Groovy({derivedCol + transformedCol}, derivedCol, transformedCol)")))).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail due to derived column on derived column");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test
  public void ingestionStreamConfigsTest() {
    Map<String, String> fakeMap = FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap();
    IngestionConfig ingestionConfig =
        new IngestionConfig(null, new StreamIngestionConfig(Lists.newArrayList(fakeMap, fakeMap)), null, null);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName("myTable_REALTIME").setTimeColumnName("timeColumn")
            .setIngestionConfig(ingestionConfig).build();

    // only 1 stream config allowed
    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, null);
      Assert.fail("Should fail for more than 1 stream config");
    } catch (IllegalStateException e) {
      // expected
    }

    // stream config should be valid
    ingestionConfig = new IngestionConfig(null, new StreamIngestionConfig(Lists.newArrayList(fakeMap)), null, null);
    tableConfig.setIngestionConfig(ingestionConfig);
    TableConfigUtils.validateIngestionConfig(tableConfig, null);

    fakeMap.remove(StreamConfigProperties.STREAM_TYPE);
    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, null);
      Assert.fail("Should fail for invalid stream configs map");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void ingestionBatchConfigsTest() {
    Map<String, String> batchConfigMap = new HashMap<>();
    batchConfigMap.put(BatchConfigProperties.INPUT_DIR_URI, "s3://foo");
    batchConfigMap.put(BatchConfigProperties.OUTPUT_DIR_URI, "gs://bar");
    batchConfigMap.put(BatchConfigProperties.INPUT_FS_CLASS, "org.foo.S3FS");
    batchConfigMap.put(BatchConfigProperties.OUTPUT_FS_CLASS, "org.foo.GcsFS");
    batchConfigMap.put(BatchConfigProperties.INPUT_FORMAT, "avro");
    batchConfigMap.put(BatchConfigProperties.RECORD_READER_CLASS, "org.foo.Reader");

    IngestionConfig ingestionConfig =
        new IngestionConfig(new BatchIngestionConfig(Lists.newArrayList(batchConfigMap, batchConfigMap), null, null),
            null, null, null);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable_OFFLINE").setIngestionConfig(ingestionConfig)
            .build();
    TableConfigUtils.validateIngestionConfig(tableConfig, null);
    batchConfigMap.remove(BatchConfigProperties.INPUT_FORMAT);
    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, null);
      Assert.fail("Should fail for invalid batch config map");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test
  public void ingestionConfigForDimensionTableTest() {
    Map<String, String> batchConfigMap = new HashMap<>();
    batchConfigMap.put(BatchConfigProperties.INPUT_DIR_URI, "s3://foo");
    batchConfigMap.put(BatchConfigProperties.OUTPUT_DIR_URI, "gs://bar");
    batchConfigMap.put(BatchConfigProperties.INPUT_FS_CLASS, "org.foo.S3FS");
    batchConfigMap.put(BatchConfigProperties.OUTPUT_FS_CLASS, "org.foo.GcsFS");
    batchConfigMap.put(BatchConfigProperties.INPUT_FORMAT, "avro");
    batchConfigMap.put(BatchConfigProperties.RECORD_READER_CLASS, "org.foo.Reader");

    // valid dimension table ingestion config
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setIsDimTable(true)
        .setIngestionConfig(
            new IngestionConfig(new BatchIngestionConfig(Lists.newArrayList(batchConfigMap, batchConfigMap), "REFRESH",
                null), null, null, null)
        ).build();
    TableConfigUtils.validateIngestionConfig(tableConfig, null);

    // dimension tables should have batch ingestion config
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setIsDimTable(true)
        .setIngestionConfig(
            new IngestionConfig(null, null, null, null)
        ).build();
    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, null);
      Assert.fail("Should fail for Dimension table without batch ingestion config");
    } catch (IllegalStateException e) {
      // expected
    }

    // dimension tables should have batch ingestion config of type REFRESH
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setIsDimTable(true)
        .setIngestionConfig(
            new IngestionConfig(new BatchIngestionConfig(Lists.newArrayList(batchConfigMap, batchConfigMap), "APPEND",
                null), null, null, null)
        ).build();
    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, null);
      Assert.fail("Should fail for Dimension table with ingestion type APPEND (should be REFRESH)");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test
  public void validateTierConfigs() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();
    // null tier configs
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTierConfigList(null).build();
    TableConfigUtils.validate(tableConfig, schema);

    // empty tier configs
    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTierConfigList(Collections.emptyList())
            .build();
    TableConfigUtils.validate(tableConfig, schema);

    // 1 tier configs
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTierConfigList(Lists
        .newArrayList(new TierConfig("tier1", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "30d",
            TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier1_tag_OFFLINE"))).build();
    TableConfigUtils.validate(tableConfig, schema);

    // 2 tier configs, case insensitive check
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTierConfigList(Lists
        .newArrayList(new TierConfig("tier1", TierFactory.TIME_SEGMENT_SELECTOR_TYPE.toLowerCase(), "30d",
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier1_tag_OFFLINE"),
            new TierConfig("tier2", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "40d",
                TierFactory.PINOT_SERVER_STORAGE_TYPE.toLowerCase(), "tier2_tag_OFFLINE"))).build();
    TableConfigUtils.validate(tableConfig, schema);

    //realtime table
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).setTimeColumnName(TIME_COLUMN)
        .setTierConfigList(Lists.newArrayList(new TierConfig("tier1", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "30d",
                TierFactory.PINOT_SERVER_STORAGE_TYPE.toLowerCase(), "tier1_tag_OFFLINE"),
            new TierConfig("tier2", TierFactory.TIME_SEGMENT_SELECTOR_TYPE.toLowerCase(), "40d",
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier2_tag_OFFLINE"))).build();
    TableConfigUtils.validate(tableConfig, schema);

    // tier name empty
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTierConfigList(Lists
        .newArrayList(
            new TierConfig("", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "30d", TierFactory.PINOT_SERVER_STORAGE_TYPE,
                "tier1_tag_OFFLINE"))).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should have failed due to empty tier name");
    } catch (IllegalStateException e) {
      // expected
    }

    // tier name repeats
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTierConfigList(Lists
        .newArrayList(new TierConfig("sameTierName", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "30d",
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier1_tag_OFFLINE"),
            new TierConfig("sameTierName", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "100d",
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier2_tag_OFFLINE"))).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should have failed due to duplicate tier name");
    } catch (IllegalStateException e) {
      // expected
    }

    // segmentSelectorType invalid
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTierConfigList(Lists
        .newArrayList(new TierConfig("tier1", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "30d",
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier1_tag_OFFLINE"),
            new TierConfig("tier2", "unsupportedSegmentSelector", "40d", TierFactory.PINOT_SERVER_STORAGE_TYPE,
                "tier2_tag_OFFLINE"))).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should have failed due to invalid segmentSelectorType");
    } catch (IllegalStateException e) {
      // expected
    }

    // segmentAge not provided for TIME segmentSelectorType
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTierConfigList(Lists
        .newArrayList(
            new TierConfig("tier1", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, null, TierFactory.PINOT_SERVER_STORAGE_TYPE,
                "tier1_tag_OFFLINE"), new TierConfig("tier2", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "40d",
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier2_tag_OFFLINE"))).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should have failed due to missing segmentAge");
    } catch (IllegalStateException e) {
      // expected
    }

    // segmentAge invalid
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTierConfigList(Lists
        .newArrayList(new TierConfig("tier1", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "30d",
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier1_tag_OFFLINE"),
            new TierConfig("tier2", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "3600",
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier2_tag_OFFLINE"))).build();

    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should have failed due to invalid segment age");
    } catch (IllegalStateException e) {
      // expected
    }

    // storageType invalid
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTierConfigList(Lists
        .newArrayList(new TierConfig("tier1", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "30d", "unsupportedStorageType",
            "tier1_tag_OFFLINE"), new TierConfig("tier2", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "40d",
            TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier2_tag_OFFLINE"))).build();

    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should have failed due to invalid storage type");
    } catch (IllegalStateException e) {
      // expected
    }

    // serverTag not provided for PINOT_SERVER storageType
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTierConfigList(Lists
        .newArrayList(new TierConfig("tier1", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "30d",
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier1_tag_OFFLINE"),
            new TierConfig("tier2", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "40d",
                TierFactory.PINOT_SERVER_STORAGE_TYPE, null))).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should have failed due to ");
    } catch (IllegalStateException e) {
      // expected
    }

    // serverTag invalid
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTierConfigList(Lists
        .newArrayList(new TierConfig("tier1", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "30d",
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier1_tag"),
            new TierConfig("tier2", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "40d",
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier2_tag_OFFLINE"))).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should have failed due to invalid server tag");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test
  public void testTableName() {
    String[] malformedTableName = {"test.table", "test table"};
    for (int i = 0; i < 2; i++) {
      String tableName = malformedTableName[i];
      TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).build();
      try {
        TableConfigUtils.validateTableName(tableConfig);
        Assert.fail("Should fail for malformed table name : " + tableName);
      } catch (IllegalStateException e) {
        // expected
      }
    }
  }

  @Test
  public void testValidateFieldConfig() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension("myCol1", FieldSpec.DataType.STRING).build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(Arrays.asList("myCol1")).build();

    try {
      FieldConfig fieldConfig =
          new FieldConfig("myCol1", FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.FST, null);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for with conflicting encoding type of myCol1");
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(),
          "FieldConfig encoding type is different from indexingConfig for column: myCol1");
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(Arrays.asList("myCol1")).build();
    try {
      FieldConfig fieldConfig =
          new FieldConfig("myCol1", FieldConfig.EncodingType.RAW, FieldConfig.IndexType.FST, null);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail since FST index is enabled on RAW encoding type");
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "FST Index is only enabled on dictionary encoded columns");
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(Arrays.asList("myCol1")).build();
    try {
      FieldConfig fieldConfig =
          new FieldConfig("myCol21", FieldConfig.EncodingType.RAW, FieldConfig.IndexType.FST, null);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail since field name is not persent in schema");
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(),
          "Column Name myCol21 defined in field config list must be a valid column defined in the schema");
    }
  }

  @Test
  public void testValidateIndexingConfig() {
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
            .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setBloomFilterColumns(Arrays.asList("myCol2")).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for invalid Bloom filter column name");
    } catch (Exception e) {
      // expected
    }

    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setInvertedIndexColumns(Arrays.asList(""))
            .build();
    TableConfigUtils.validate(tableConfig, schema);

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(Arrays.asList("myCol2")).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for invalid Inverted Index column name");
    } catch (Exception e) {
      // expected
    }

    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setNoDictionaryColumns(Arrays.asList(""))
            .build();
    TableConfigUtils.validate(tableConfig, schema);

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(Arrays.asList("myCol2")).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for invalid No Dictionary column name");
    } catch (Exception e) {
      // expected
    }

    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setOnHeapDictionaryColumns(Arrays.asList(""))
            .build();
    TableConfigUtils.validate(tableConfig, schema);

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setOnHeapDictionaryColumns(Arrays.asList("myCol2")).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for invalid On Heap Dictionary column name");
    } catch (Exception e) {
      // expected
    }

    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setRangeIndexColumns(Arrays.asList(""))
            .build();
    TableConfigUtils.validate(tableConfig, schema);

    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setRangeIndexColumns(Arrays.asList("myCol2"))
            .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for invalid Range Index column name");
    } catch (Exception e) {
      // expected
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setSortedColumn("").build();
    TableConfigUtils.validate(tableConfig, schema);

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setSortedColumn("myCol2").build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for invalid Sorted column name");
    } catch (Exception e) {
      // expected
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setVarLengthDictionaryColumns(Arrays.asList("")).build();
    TableConfigUtils.validate(tableConfig, schema);

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setVarLengthDictionaryColumns(Arrays.asList("myCol2")).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for invalid Var Length Dictionary column name");
    } catch (Exception e) {
      // expected
    }

    ColumnPartitionConfig columnPartitionConfig = new ColumnPartitionConfig("Murmur", 4);
    Map<String, ColumnPartitionConfig> partitionConfigMap = new HashMap<>();
    partitionConfigMap.put("myCol2", columnPartitionConfig);
    SegmentPartitionConfig partitionConfig = new SegmentPartitionConfig(partitionConfigMap);
    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setSegmentPartitionConfig(partitionConfig)
            .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for invalid Segment Partition column name");
    } catch (Exception e) {
      // expected
    }

    // Although this config makes no sense, it should pass the validation phase
    StarTreeIndexConfig starTreeIndexConfig =
        new StarTreeIndexConfig(Arrays.asList("myCol"), Arrays.asList("myCol"), Arrays.asList("SUM__myCol"), 1);
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setStarTreeIndexConfigs(Arrays.asList(starTreeIndexConfig)).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
    } catch (Exception e) {
      Assert.fail("Should not fail for valid StarTreeIndex config column name");
    }

    starTreeIndexConfig =
        new StarTreeIndexConfig(Arrays.asList("myCol2"), Arrays.asList("myCol"), Arrays.asList("SUM__myCol"), 1);
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setStarTreeIndexConfigs(Arrays.asList(starTreeIndexConfig)).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for invalid StarTreeIndex config column name in dimension split order");
    } catch (Exception e) {
      // expected
    }

    starTreeIndexConfig =
        new StarTreeIndexConfig(Arrays.asList("myCol"), Arrays.asList("myCol2"), Arrays.asList("SUM__myCol"), 1);
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setStarTreeIndexConfigs(Arrays.asList(starTreeIndexConfig)).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for invalid StarTreeIndex config column name in skip star node for dimension");
    } catch (Exception e) {
      // expected
    }

    starTreeIndexConfig =
        new StarTreeIndexConfig(Arrays.asList("myCol"), Arrays.asList("myCol"), Arrays.asList("SUM__myCol2"), 1);
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setStarTreeIndexConfigs(Arrays.asList(starTreeIndexConfig)).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for invalid StarTreeIndex config column name in function column pair");
    } catch (Exception e) {
      // expected
    }

    FieldConfig fieldConfig = new FieldConfig("myCol2", null, null, null);
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setFieldConfigList(Arrays.asList(fieldConfig)).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for invalid column name in Field Config List");
    } catch (Exception e) {
      // expected
    }

    List<String> columnList = Arrays.asList("myCol");
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setNoDictionaryColumns(columnList)
        .setInvertedIndexColumns(columnList).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for valid column name in both no dictionary and inverted index column config");
    } catch (Exception e) {
      // expected
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setNoDictionaryColumns(columnList)
        .setBloomFilterColumns(columnList).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for valid column name in both no dictionary and bloom filter column config");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testValidateRetentionConfig() {
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
            .build();
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setRetentionTimeUnit("hours")
            .setRetentionTimeValue("24").build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
    } catch (Exception e) {
      Assert.fail("Should not fail for valid retention time unit value");
    }

    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setRetentionTimeUnit("abc").build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for invalid retention time unit value");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testValidateUpsertConfig() {
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
            .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL)).build();
    try {
      TableConfigUtils.validateUpsertConfig(tableConfig, schema);
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "Upsert table is for realtime table only.");
    }
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL)).build();
    try {
      TableConfigUtils.validateUpsertConfig(tableConfig, schema);
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "Upsert table must have primary key columns in the schema");
    }
    schema =
        new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
            .setPrimaryKeyColumns(Lists.newArrayList("myCol")).build();
    try {
      TableConfigUtils.validateUpsertConfig(tableConfig, schema);
    } catch (Exception e) {
      Assert
          .assertEquals(e.getMessage(), "Could not find streamConfigs for REALTIME table: " + TABLE_NAME + "_REALTIME");
    }
    Map<String, String> streamConfigs = new HashMap<>();
    streamConfigs.put("stream.kafka.consumer.type", "highLevel");
    streamConfigs.put("streamType", "kafka");
    streamConfigs.put("stream.kafka.topic.name", "test");
    streamConfigs
        .put("stream.kafka.decoder.class.name", "org.apache.pinot.plugin.stream.kafka.KafkaJSONMessageDecoder");
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL)).setStreamConfigs(streamConfigs).build();
    try {
      TableConfigUtils.validateUpsertConfig(tableConfig, schema);
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "Upsert table must use low-level streaming consumer type");
    }
    streamConfigs.put("stream.kafka.consumer.type", "simple");
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL)).setStreamConfigs(streamConfigs).build();
    try {
      TableConfigUtils.validateUpsertConfig(tableConfig, schema);
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(),
          "Upsert table must use strict replica-group (i.e. strictReplicaGroup) based routing");
    }
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL))
        .setRoutingConfig(new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE))
        .setStreamConfigs(streamConfigs).build();
    try {
      TableConfigUtils.validateUpsertConfig(tableConfig, schema);
    } catch (Exception e) {
      Assert.fail("Should not fail upsert validation");
    }
    StarTreeIndexConfig starTreeIndexConfig = new StarTreeIndexConfig(Lists.newArrayList("myCol"), null, Collections
        .singletonList(new AggregationFunctionColumnPair(AggregationFunctionType.COUNT, "myCol").toColumnName()), 10);
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL))
        .setRoutingConfig(new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE))
        .setStarTreeIndexConfigs(Lists.newArrayList(starTreeIndexConfig)).setStreamConfigs(streamConfigs).build();
    try {
      TableConfigUtils.validateUpsertConfig(tableConfig, schema);
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "The upsert table cannot have star-tree index.");
    }
  }
}
