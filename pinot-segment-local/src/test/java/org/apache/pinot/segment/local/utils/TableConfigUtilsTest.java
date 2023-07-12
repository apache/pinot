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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.tier.TierFactory;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.ReplicaGroupStrategyConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
import org.apache.pinot.spi.config.table.ingestion.AggregationConfig;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.ComplexTypeConfig;
import org.apache.pinot.spi.config.table.ingestion.FilterConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mockito;
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
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).setIsDimTable(true)
        .setTimeColumnName(TIME_COLUMN).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail with a Dimension table of type REALTIME");
    } catch (IllegalStateException e) {
      // expected
    }

    // dimension table without a schema
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setIsDimTable(true)
        .setTimeColumnName(TIME_COLUMN).build();
    try {
      TableConfigUtils.validate(tableConfig, null);
      Assert.fail("Should fail with a Dimension table without a schema");
    } catch (IllegalStateException e) {
      // expected
    }

    // dimension table without a Primary Key
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(TIME_COLUMN, FieldSpec.DataType.STRING).build();
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setIsDimTable(true)
        .setTimeColumnName(TIME_COLUMN).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail with a Dimension without a primary key");
    } catch (IllegalStateException e) {
      // expected
    }

    // valid dimension table
    schema =
        new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
            .setPrimaryKeyColumns(Lists.newArrayList("myCol")).build();
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setIsDimTable(true).build();
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
    IngestionConfig ingestionConfig = new IngestionConfig();
    tableConfig.setIngestionConfig(ingestionConfig);
    TableConfigUtils.validate(tableConfig, schema);

    // null filter function
    ingestionConfig.setFilterConfig(new FilterConfig(null));
    TableConfigUtils.validate(tableConfig, schema);

    // valid filterFunction
    ingestionConfig.setFilterConfig(new FilterConfig("startsWith(columnX, \"myPrefix\")"));
    TableConfigUtils.validate(tableConfig, schema);

    // valid filterFunction
    ingestionConfig.setFilterConfig(new FilterConfig("Groovy({x == 10}, x)"));
    TableConfigUtils.validate(tableConfig, schema);

    // invalid filter function
    ingestionConfig.setFilterConfig(new FilterConfig("Groovy(badExpr)"));
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail on invalid filter function string");
    } catch (IllegalStateException e) {
      // expected
    }

    ingestionConfig.setFilterConfig(new FilterConfig("fakeFunction(xx)"));
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for invalid filter function");
    } catch (IllegalStateException e) {
      // expected
    }

    // empty transform configs
    ingestionConfig.setFilterConfig(null);
    ingestionConfig.setTransformConfigs(Collections.emptyList());
    TableConfigUtils.validate(tableConfig, schema);

    // transformed column not in schema
    ingestionConfig.setTransformConfigs(Collections.singletonList(new TransformConfig("myCol", "reverse(anotherCol)")));
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for transformedColumn not present in schema");
    } catch (IllegalStateException e) {
      // expected
    }

    // using a transformation column in an aggregation
    schema =
        new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addMetric("twiceSum", FieldSpec.DataType.DOUBLE).build();
    ingestionConfig.setTransformConfigs(Collections.singletonList(new TransformConfig("twice", "col * 2")));
    ingestionConfig.setAggregationConfigs(Collections.singletonList(new AggregationConfig("twiceSum", "SUM(twice)")));
    TableConfigUtils.validate(tableConfig, schema);

    // valid transform configs
    schema =
        new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
            .build();
    ingestionConfig.setAggregationConfigs(null);
    ingestionConfig.setTransformConfigs(Collections.singletonList(new TransformConfig("myCol", "reverse(anotherCol)")));
    TableConfigUtils.validate(tableConfig, schema);

    // valid transform configs
    schema =
        new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
            .addMetric("transformedCol", FieldSpec.DataType.LONG).build();
    ingestionConfig.setTransformConfigs(Arrays.asList(new TransformConfig("myCol", "reverse(anotherCol)"),
        new TransformConfig("transformedCol", "Groovy({x+y}, x, y)")));
    TableConfigUtils.validate(tableConfig, schema);

    // invalid transform config since Groovy is disabled
    try {
      TableConfigUtils.validate(tableConfig, schema, null, true);
      Assert.fail("Should fail when Groovy functions disabled but found in transform config");
    } catch (IllegalStateException e) {
      // expected
    }

    // invalid filter config since Groovy is disabled
    ingestionConfig.setTransformConfigs(null);
    ingestionConfig.setFilterConfig(new FilterConfig("Groovy({timestamp > 0}, timestamp)"));
    try {
      TableConfigUtils.validate(tableConfig, schema, null, true);
      Assert.fail("Should fail when Groovy functions disabled but found in filter config");
    } catch (IllegalStateException e) {
      // expected
    }

    // null transform column name
    ingestionConfig.setFilterConfig(null);
    ingestionConfig.setTransformConfigs(Collections.singletonList(new TransformConfig(null, "reverse(anotherCol)")));
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for null column name in transform config");
    } catch (IllegalStateException e) {
      // expected
    }

    // null transform function string
    ingestionConfig.setTransformConfigs(Collections.singletonList(new TransformConfig("myCol", null)));
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for null transform function in transform config");
    } catch (IllegalStateException e) {
      // expected
    }

    // invalid function
    ingestionConfig.setTransformConfigs(Collections.singletonList(new TransformConfig("myCol", "fakeFunction(col)")));
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for invalid transform function in transform config");
    } catch (IllegalStateException e) {
      // expected
    }

    // invalid function
    ingestionConfig.setTransformConfigs(Collections.singletonList(new TransformConfig("myCol", "Groovy(badExpr)")));
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for invalid transform function in transform config");
    } catch (IllegalStateException e) {
      // expected
    }

    // input field name used as destination field
    ingestionConfig.setTransformConfigs(Collections.singletonList(new TransformConfig("myCol", "reverse(myCol)")));
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail due to use of myCol as arguments and columnName");
    } catch (IllegalStateException e) {
      // expected
    }

    // input field name used as destination field
    ingestionConfig.setTransformConfigs(
        Collections.singletonList(new TransformConfig("myCol", "Groovy({x + y + myCol}, x, myCol, y)")));
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail due to use of myCol as arguments and columnName");
    } catch (IllegalStateException e) {
      // expected
    }

    // duplicate transform config
    ingestionConfig.setTransformConfigs(
        Arrays.asList(new TransformConfig("myCol", "reverse(x)"), new TransformConfig("myCol", "lower(y)")));
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail due to duplicate transform config");
    } catch (IllegalStateException e) {
      // expected
    }

    // derived columns - should pass
    ingestionConfig.setTransformConfigs(Arrays.asList(new TransformConfig("transformedCol", "reverse(x)"),
        new TransformConfig("myCol", "lower(transformedCol)")));
    TableConfigUtils.validate(tableConfig, schema);

    // invalid field name in schema with matching prefix from complexConfigType's prefixesToRename
    ingestionConfig.setTransformConfigs(null);
    ingestionConfig.setComplexTypeConfig(
        new ComplexTypeConfig(null, ".", null, Collections.singletonMap("after.", "")));
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addMultiValueDimension("after.test", FieldSpec.DataType.STRING).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail due to name conflict from field name in schema with a prefix in prefixesToRename");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test
  public void ingestionAggregationConfigsTest() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addDateTime("timeColumn", FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setAggregationConfigs(Collections.singletonList(new AggregationConfig("d1", "SUM(s1)")));
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).setTimeColumnName("timeColumn")
            .setIngestionConfig(ingestionConfig).build();
    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, schema);
      Assert.fail("Should fail due to destination column not being in schema");
    } catch (IllegalStateException e) {
      // expected
    }

    schema.addField(new DimensionFieldSpec("d1", FieldSpec.DataType.DOUBLE, true));
    tableConfig.getIndexingConfig().setAggregateMetrics(true);
    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, schema);
      Assert.fail("Should fail due to aggregateMetrics being set");
    } catch (IllegalStateException e) {
      // expected
    }

    tableConfig.getIndexingConfig().setAggregateMetrics(false);
    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, schema);
      Assert.fail("Should fail due to aggregation column being a dimension");
    } catch (IllegalStateException e) {
      // expected
    }

    schema.addField(new MetricFieldSpec("m1", FieldSpec.DataType.DOUBLE));
    ingestionConfig.setAggregationConfigs(Collections.singletonList(new AggregationConfig(null, null)));
    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, schema);
      Assert.fail("Should fail due to null columnName/aggregationFunction");
    } catch (IllegalStateException e) {
      // expected
    }

    ingestionConfig.setAggregationConfigs(
        Arrays.asList(new AggregationConfig("m1", "SUM(s1)"), new AggregationConfig("m1", "SUM(s2)")));
    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, schema);
      Assert.fail("Should fail due to duplicate destination column");
    } catch (IllegalStateException e) {
      // expected
    }

    ingestionConfig.setAggregationConfigs(Collections.singletonList(new AggregationConfig("m1", "SUM s1")));
    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, schema);
      Assert.fail("Should fail due to invalid aggregation function");
    } catch (IllegalStateException e) {
      // expected
    }

    ingestionConfig.setAggregationConfigs(
        Collections.singletonList(new AggregationConfig("m1", "DISTINCTCOUNTHLL(s1)")));
    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, schema);
      Assert.fail("Should fail due to not supported aggregation function");
    } catch (IllegalStateException e) {
      // expected
    }

    ingestionConfig.setAggregationConfigs(Collections.singletonList(new AggregationConfig("m1", "s1 + s2")));
    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, schema);
      Assert.fail("Should fail due to multiple arguments");
    } catch (IllegalStateException e) {
      // expected
    }

    ingestionConfig.setAggregationConfigs(Collections.singletonList(new AggregationConfig("m1", "SUM(s1 - s2)")));
    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, schema);
      Assert.fail("Should fail due to inner value not being a column");
    } catch (IllegalStateException e) {
      // expected
    }

    ingestionConfig.setAggregationConfigs(Collections.singletonList(new AggregationConfig("m1", "SUM(m1)")));
    TableConfigUtils.validateIngestionConfig(tableConfig, schema);

    ingestionConfig.setAggregationConfigs(Collections.singletonList(new AggregationConfig("m1", "SUM(s1)")));
    TableConfigUtils.validateIngestionConfig(tableConfig, schema);

    schema.addField(new MetricFieldSpec("m2", FieldSpec.DataType.DOUBLE));
    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, schema);
      Assert.fail("Should fail due to one metric column not being aggregated");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test
  public void ingestionStreamConfigsTest() {
    Map<String, String> streamConfigs = getStreamConfigs();
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setStreamIngestionConfig(new StreamIngestionConfig(Arrays.asList(streamConfigs, streamConfigs)));
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).setTimeColumnName("timeColumn")
            .setIngestionConfig(ingestionConfig).build();

    // only 1 stream config allowed
    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, null);
      Assert.fail("Should fail for more than 1 stream config");
    } catch (IllegalStateException e) {
      // expected
    }

    // stream config should be valid
    ingestionConfig.setStreamIngestionConfig(new StreamIngestionConfig(Collections.singletonList(streamConfigs)));
    TableConfigUtils.validateIngestionConfig(tableConfig, null);

    streamConfigs.remove(StreamConfigProperties.STREAM_TYPE);
    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, null);
      Assert.fail("Should fail for invalid stream configs map");
    } catch (IllegalStateException e) {
      // expected
    }

    // validate the proto decoder
    streamConfigs = getStreamConfigs();
    streamConfigs.put("stream.kafka.decoder.class.name",
        "org.apache.pinot.plugin.inputformat.protobuf.ProtoBufMessageDecoder");
    streamConfigs.put("stream.kafka.decoder.prop.descriptorFile", "file://test");
    try {
      TableConfigUtils.validateDecoder(new StreamConfig("test", streamConfigs));
    } catch (IllegalStateException e) {
      // expected
    }
    streamConfigs.remove("stream.kafka.decoder.prop.descriptorFile");
    streamConfigs.put("stream.kafka.decoder.prop.protoClassName", "test");
    try {
      TableConfigUtils.validateDecoder(new StreamConfig("test", streamConfigs));
    } catch (IllegalStateException e) {
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

    IngestionConfig ingestionConfig = new IngestionConfig();
    // TODO: Check if we should allow duplicate config maps
    ingestionConfig.setBatchIngestionConfig(
        new BatchIngestionConfig(Arrays.asList(batchConfigMap, batchConfigMap), null, null));
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setIngestionConfig(ingestionConfig).build();
    TableConfigUtils.validateIngestionConfig(tableConfig, null);
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
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setBatchIngestionConfig(
        new BatchIngestionConfig(Collections.singletonList(batchConfigMap), "REFRESH", null));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setIsDimTable(true)
        .setIngestionConfig(ingestionConfig).build();
    TableConfigUtils.validateIngestionConfig(tableConfig, null);

    // dimension tables should have batch ingestion config
    ingestionConfig.setBatchIngestionConfig(null);
    try {
      TableConfigUtils.validateIngestionConfig(tableConfig, null);
      Assert.fail("Should fail for Dimension table without batch ingestion config");
    } catch (IllegalStateException e) {
      // expected
    }

    // dimension tables should have batch ingestion config of type REFRESH
    ingestionConfig.setBatchIngestionConfig(
        new BatchIngestionConfig(Collections.singletonList(batchConfigMap), "APPEND", null));
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
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTierConfigList(
        Lists.newArrayList(new TierConfig("tier1", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "30d", null,
            TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier1_tag_OFFLINE", null, null))).build();
    TableConfigUtils.validate(tableConfig, schema);

    // 2 tier configs, case insensitive check
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTierConfigList(
        Lists.newArrayList(new TierConfig("tier1", TierFactory.TIME_SEGMENT_SELECTOR_TYPE.toLowerCase(), "30d", null,
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier1_tag_OFFLINE", null, null),
            new TierConfig("tier2", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "40d", null,
                TierFactory.PINOT_SERVER_STORAGE_TYPE.toLowerCase(), "tier2_tag_OFFLINE", null, null))).build();
    TableConfigUtils.validate(tableConfig, schema);

    //realtime table
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).setTimeColumnName(TIME_COLUMN)
        .setTierConfigList(Lists.newArrayList(
            new TierConfig("tier1", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "30d", null,
                TierFactory.PINOT_SERVER_STORAGE_TYPE.toLowerCase(), "tier1_tag_OFFLINE", null, null),
            new TierConfig("tier2", TierFactory.TIME_SEGMENT_SELECTOR_TYPE.toLowerCase(), "40d", null,
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier2_tag_OFFLINE", null, null))).build();
    TableConfigUtils.validate(tableConfig, schema);

    // tier name empty
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTierConfigList(
        Lists.newArrayList(new TierConfig("", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "30d", null,
            TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier1_tag_OFFLINE", null, null))).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should have failed due to empty tier name");
    } catch (IllegalStateException e) {
      // expected
    }

    // tier name repeats
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTierConfigList(
        Lists.newArrayList(new TierConfig("sameTierName", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "30d", null,
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier1_tag_OFFLINE", null, null),
            new TierConfig("sameTierName", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "100d", null,
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier2_tag_OFFLINE", null, null))).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should have failed due to duplicate tier name");
    } catch (IllegalStateException e) {
      // expected
    }

    // segmentSelectorType invalid
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTierConfigList(
        Lists.newArrayList(new TierConfig("tier1", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "30d", null,
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier1_tag_OFFLINE", null, null),
            new TierConfig("tier2", "unsupportedSegmentSelector", "40d", null, TierFactory.PINOT_SERVER_STORAGE_TYPE,
                "tier2_tag_OFFLINE", null, null))).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should have failed due to invalid segmentSelectorType");
    } catch (IllegalStateException e) {
      // expected
    }

    // segmentAge not provided for TIME segmentSelectorType
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTierConfigList(
        Lists.newArrayList(new TierConfig("tier1", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, null, null,
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier1_tag_OFFLINE", null, null),
            new TierConfig("tier2", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "40d", null,
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier2_tag_OFFLINE", null, null))).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should have failed due to missing segmentAge");
    } catch (IllegalStateException e) {
      // expected
    }

    // segmentAge invalid
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTierConfigList(
        Lists.newArrayList(new TierConfig("tier1", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "30d", null,
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier1_tag_OFFLINE", null, null),
            new TierConfig("tier2", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "3600", null,
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier2_tag_OFFLINE", null, null))).build();

    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should have failed due to invalid segment age");
    } catch (IllegalStateException e) {
      // expected
    }

    // fixedSegmentSelector
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTierConfigList(
        Lists.newArrayList(new TierConfig("tier1", TierFactory.FIXED_SEGMENT_SELECTOR_TYPE, null, null,
            TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier1_tag_OFFLINE", null, null))).build();
    TableConfigUtils.validate(tableConfig, schema);
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTierConfigList(
        Lists.newArrayList(new TierConfig("tier1", TierFactory.FIXED_SEGMENT_SELECTOR_TYPE, "30d", Lists.newArrayList(),
            TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier1_tag_OFFLINE", null, null))).build();
    TableConfigUtils.validate(tableConfig, schema);
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTierConfigList(
        Lists.newArrayList(
            new TierConfig("tier1", TierFactory.FIXED_SEGMENT_SELECTOR_TYPE, null, Lists.newArrayList("seg0", "seg1"),
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier1_tag_OFFLINE", null, null))).build();
    TableConfigUtils.validate(tableConfig, schema);

    // storageType invalid
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTierConfigList(
        Lists.newArrayList(
            new TierConfig("tier1", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "30d", null, "unsupportedStorageType",
                "tier1_tag_OFFLINE", null, null),
            new TierConfig("tier2", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "40d", null,
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier2_tag_OFFLINE", null, null))).build();

    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should have failed due to invalid storage type");
    } catch (IllegalStateException e) {
      // expected
    }

    // serverTag not provided for PINOT_SERVER storageType
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTierConfigList(
        Lists.newArrayList(new TierConfig("tier1", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "30d", null,
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier1_tag_OFFLINE", null, null),
            new TierConfig("tier2", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "40d", null,
                TierFactory.PINOT_SERVER_STORAGE_TYPE, null, null, null))).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should have failed due to ");
    } catch (IllegalStateException e) {
      // expected
    }

    // serverTag invalid
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTierConfigList(
        Lists.newArrayList(new TierConfig("tier1", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "30d", null,
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier1_tag", null, null),
            new TierConfig("tier2", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "40d", null,
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier2_tag_OFFLINE", null, null))).build();
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
        TableConfigUtils.validateTableName(tableConfig, CommonConstants.Helix.DEFAULT_ALLOW_TABLE_NAME_WITH_DATABASE);
        Assert.fail("Should fail for malformed table name : " + tableName);
      } catch (IllegalStateException e) {
        // expected
      }
    }

    String allowedWithConfig = "test.table";
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(allowedWithConfig).build();
    try {
      TableConfigUtils.validateTableName(tableConfig, true);
    } catch (IllegalStateException e) {
      Assert.fail("Should allow table name with dot if configuration is turned on");
    }

    String rejected = "test.another.table";
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(rejected).build();
    try {
      TableConfigUtils.validateTableName(tableConfig, true);
      Assert.fail("Should fail for malformed table name : " + rejected);
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test
  public void testValidateFieldConfig() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension("myCol1", FieldSpec.DataType.STRING)
        .addMultiValueDimension("myCol2", FieldSpec.DataType.INT)
        .addSingleValueDimension("intCol", FieldSpec.DataType.INT).build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(Arrays.asList("myCol1")).build();

    try {
      FieldConfig fieldConfig = new FieldConfig("myCol1", FieldConfig.EncodingType.RAW, null, null, null, null, null);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
    } catch (Exception e) {
      Assert.fail("all nullable fields set for fieldConfig should pass", e);
    }

    try {
      FieldConfig fieldConfig =
          new FieldConfig("myCol1", FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.FST, null, null);
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
          new FieldConfig("myCol1", FieldConfig.EncodingType.RAW, FieldConfig.IndexType.FST, null, null);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail since FST index is enabled on RAW encoding type");
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "FST Index is only enabled on dictionary encoded columns");
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    try {
      FieldConfig fieldConfig =
          new FieldConfig("myCol2", FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.FST, null, null);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail since FST index is enabled on multi value column");
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "FST Index is only supported for single value string columns");
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    try {
      FieldConfig fieldConfig =
          new FieldConfig("intCol", FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.FST, null, null);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail since FST index is enabled on non String column");
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "FST Index is only supported for single value string columns");
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(Arrays.asList("myCol2", "intCol")).build();
    try {
      FieldConfig fieldConfig =
          new FieldConfig("intCol", FieldConfig.EncodingType.RAW, FieldConfig.IndexType.TEXT, null, null);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail since TEXT index is enabled on non String column");
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "TEXT Index is only supported for string columns");
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(Arrays.asList("myCol1")).build();
    try {
      FieldConfig fieldConfig =
          new FieldConfig("myCol21", FieldConfig.EncodingType.RAW, FieldConfig.IndexType.FST, null, null);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail since field name is not present in schema");
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(),
          "Column Name myCol21 defined in field config list must be a valid column defined in the schema");
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    try {
      FieldConfig fieldConfig = new FieldConfig("intCol", FieldConfig.EncodingType.DICTIONARY, Collections.emptyList(),
          FieldConfig.CompressionCodec.SNAPPY, null);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail since dictionary encoding does not support compression codec snappy");
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "Set compression codec to null for dictionary encoding type");
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    try {
      FieldConfig fieldConfig = new FieldConfig("intCol", FieldConfig.EncodingType.DICTIONARY, Collections.emptyList(),
          FieldConfig.CompressionCodec.ZSTANDARD, null);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail since dictionary encoding does not support compression codec zstandard");
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "Set compression codec to null for dictionary encoding type");
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(Arrays.asList("myCol1")).build();
    try {
      // Enable forward index disabled flag for a raw column. This should succeed as though the forward index cannot
      // be rebuilt without a dictionary, the constraint to have a dictionary has been lifted.
      Map<String, String> fieldConfigProperties = new HashMap<>();
      fieldConfigProperties.put(FieldConfig.FORWARD_INDEX_DISABLED, Boolean.TRUE.toString());
      FieldConfig fieldConfig = new FieldConfig("myCol1", FieldConfig.EncodingType.RAW, null, null, null, null,
          fieldConfigProperties);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
    } catch (Exception e) {
      Assert.fail("Validation should pass since forward index can be disabled for a column without a dictionary");
    }

    try {
      // Enable forward index disabled flag for a column without inverted index. This should succeed as though the
      // forward index cannot be rebuilt without an inverted index, the constraint to have an inverted index has been
      // lifted.
      Map<String, String> fieldConfigProperties = new HashMap<>();
      fieldConfigProperties.put(FieldConfig.FORWARD_INDEX_DISABLED, Boolean.TRUE.toString());
      FieldConfig fieldConfig = new FieldConfig("myCol2", FieldConfig.EncodingType.DICTIONARY, null, null, null, null,
          fieldConfigProperties);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
    } catch (Exception e) {
      Assert.fail("Validation should pass since forward index can be disabled for a column without an inverted index");
    }

    try {
      // Enable forward index disabled flag for a column and verify that dictionary override options are not set.
      Map<String, String> fieldConfigProperties = new HashMap<>();
      fieldConfigProperties.put(FieldConfig.FORWARD_INDEX_DISABLED, Boolean.TRUE.toString());
      tableConfig.getIndexingConfig().setOptimizeDictionaryForMetrics(true);
      tableConfig.getIndexingConfig().setOptimizeDictionaryForMetrics(true);
      TableConfigUtils.validate(tableConfig, schema);
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "Dictionary override optimization options (OptimizeDictionary, "
          + "optimizeDictionaryForMetrics) not supported with forward index for column: myCol2, disabled");
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(Arrays.asList("myCol1")).setInvertedIndexColumns(Arrays.asList("myCol2")).build();
    try {
      // Enable forward index disabled flag for a column with inverted index
      Map<String, String> fieldConfigProperties = new HashMap<>();
      fieldConfigProperties.put(FieldConfig.FORWARD_INDEX_DISABLED, Boolean.TRUE.toString());
      FieldConfig fieldConfig = new FieldConfig("myCol2", FieldConfig.EncodingType.DICTIONARY,
          FieldConfig.IndexType.INVERTED, null, null, null, fieldConfigProperties);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
    } catch (Exception e) {
      Assert.fail("Should not fail as myCol2 has forward index disabled but inverted index enabled");
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(Arrays.asList("myCol1")).setInvertedIndexColumns(Arrays.asList("myCol2"))
        .setSortedColumn("myCol2").build();
    try {
      // Enable forward index disabled flag for a column with inverted index and is sorted
      Map<String, String> fieldConfigProperties = new HashMap<>();
      fieldConfigProperties.put(FieldConfig.FORWARD_INDEX_DISABLED, Boolean.TRUE.toString());
      FieldConfig fieldConfig = new FieldConfig("myCol2", FieldConfig.EncodingType.DICTIONARY,
          FieldConfig.IndexType.INVERTED, null, null, null, fieldConfigProperties);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
    } catch (Exception e) {
      Assert.fail("Should not fail for myCol2 with forward index disabled but is sorted, this is a no-op");
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(Arrays.asList("myCol1")).setInvertedIndexColumns(Arrays.asList("myCol2"))
        .setRangeIndexColumns(Arrays.asList("myCol2")).build();
    try {
      // Enable forward index disabled flag for a multi-value column with inverted index and range index
      Map<String, String> fieldConfigProperties = new HashMap<>();
      fieldConfigProperties.put(FieldConfig.FORWARD_INDEX_DISABLED, Boolean.TRUE.toString());
      FieldConfig fieldConfig = new FieldConfig("myCol2", FieldConfig.EncodingType.DICTIONARY,
          FieldConfig.IndexType.INVERTED, Arrays.asList(FieldConfig.IndexType.INVERTED, FieldConfig.IndexType.RANGE),
          null, null, fieldConfigProperties);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for MV myCol2 with forward index disabled but has range and inverted index");
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "Feature not supported for multi-value columns with range index. "
          + "Cannot disable forward index for column myCol2. Disable range index on this column to use this feature");
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(Arrays.asList("myCol1")).setRangeIndexColumns(Arrays.asList("myCol1")).build();
    try {
      // Enable forward index disabled flag for a singe-value column with inverted index and range index v1
      Map<String, String> fieldConfigProperties = new HashMap<>();
      fieldConfigProperties.put(FieldConfig.FORWARD_INDEX_DISABLED, Boolean.TRUE.toString());
      FieldConfig fieldConfig = new FieldConfig("myCol1", FieldConfig.EncodingType.DICTIONARY,
          FieldConfig.IndexType.INVERTED, Arrays.asList(FieldConfig.IndexType.INVERTED, FieldConfig.IndexType.RANGE),
          null, null, fieldConfigProperties);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      tableConfig.getIndexingConfig().setRangeIndexVersion(1);
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for SV myCol1 with forward index disabled but has range v1 and inverted index");
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "Feature not supported for single-value columns with range index version "
          + "< 2. Cannot disable forward index for column myCol1. Either disable range index or create range index "
          + "with version >= 2 to use this feature");
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(Arrays.asList("myCol2")).setInvertedIndexColumns(Arrays.asList("myCol2")).build();
    try {
      // Enable forward index disabled flag for a column with inverted index and disable dictionary
      Map<String, String> fieldConfigProperties = new HashMap<>();
      fieldConfigProperties.put(FieldConfig.FORWARD_INDEX_DISABLED, Boolean.TRUE.toString());
      FieldConfig fieldConfig = new FieldConfig("myCol2", FieldConfig.EncodingType.RAW,
          FieldConfig.IndexType.INVERTED, null, null, null, fieldConfigProperties);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should not be able to disable dictionary but keep inverted index");
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "Cannot create an Inverted index on column myCol2 specified in the "
          + "noDictionaryColumns config");
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(Arrays.asList("myCol2")).build();
    try {
      // Enable forward index disabled flag for a column with FST index and disable dictionary
      Map<String, String> fieldConfigProperties = new HashMap<>();
      fieldConfigProperties.put(FieldConfig.FORWARD_INDEX_DISABLED, Boolean.TRUE.toString());
      FieldConfig fieldConfig = new FieldConfig("myCol2", FieldConfig.EncodingType.RAW,
          FieldConfig.IndexType.FST, null, null, null, fieldConfigProperties);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should not be able to disable dictionary but keep inverted index");
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "FST Index is only enabled on dictionary encoded columns");
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(Arrays.asList("intCol")).setRangeIndexColumns(Arrays.asList("intCol")).build();
    try {
      // Enable forward index disabled flag for a column with FST index and disable dictionary
      Map<String, String> fieldConfigProperties = new HashMap<>();
      fieldConfigProperties.put(FieldConfig.FORWARD_INDEX_DISABLED, Boolean.TRUE.toString());
      FieldConfig fieldConfig = new FieldConfig("intCol", FieldConfig.EncodingType.RAW,
          FieldConfig.IndexType.RANGE, null, null, null, fieldConfigProperties);
      tableConfig.setFieldConfigList(Arrays.asList(fieldConfig));
      TableConfigUtils.validate(tableConfig, schema);
    } catch (Exception e) {
      Assert.fail("Range index with forward index disabled no dictionary column is allowed");
    }
  }

  @Test
  public void testValidateIndexingConfig() {
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
            .addSingleValueDimension("bytesCol", FieldSpec.DataType.BYTES)
            .addSingleValueDimension("intCol", FieldSpec.DataType.INT)
            .addMultiValueDimension("multiValCol", FieldSpec.DataType.STRING).build();
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

    starTreeIndexConfig = new StarTreeIndexConfig(Arrays.asList("multiValCol"), Arrays.asList("multiValCol"),
        Arrays.asList("SUM__multiValCol"), 1);
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setStarTreeIndexConfigs(Arrays.asList(starTreeIndexConfig)).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for multi-value column name in StarTreeIndex config");
    } catch (Exception e) {
      // expected
    }

    FieldConfig fieldConfig = new FieldConfig("myCol2", null, Collections.emptyList(), null, null);
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

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setJsonIndexColumns(Arrays.asList("non-existent-column")).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for non existent column in Json index config");
    } catch (Exception e) {
      // expected
    }

    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setJsonIndexColumns(Arrays.asList("intCol"))
            .build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for Json index defined on non string column");
    } catch (Exception e) {
      // expected
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setJsonIndexColumns(Arrays.asList("multiValCol")).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for Json index defined on multi-value column");
    } catch (Exception e) {
      // expected
    }

    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setRangeIndexColumns(columnList).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
    } catch (Exception e) {
      Assert.fail("Should work for range index defined on dictionary encoded string column");
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setRangeIndexColumns(columnList)
        .setNoDictionaryColumns(columnList).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for range index defined on non numeric/no-dictionary column");
    } catch (Exception e) {
      // Expected
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setVarLengthDictionaryColumns(Arrays.asList("intCol")).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for Var length dictionary defined for non string/bytes column");
    } catch (Exception e) {
      // expected
    }

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setJsonIndexColumns(Arrays.asList("multiValCol")).build();
    try {
      TableConfigUtils.validate(tableConfig, schema);
      Assert.fail("Should fail for Json Index defined on a multi value column");
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
  public void testValidateDedupConfig() {
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
            .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setDedupConfig(new DedupConfig(true, HashFunction.NONE)).build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertEquals(e.getMessage(), "Upsert/Dedup table is for realtime table only.");
    }

    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setDedupConfig(new DedupConfig(true, HashFunction.NONE)).build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertEquals(e.getMessage(), "Upsert/Dedup table must have primary key columns in the schema");
    }

    schema =
        new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
            .setPrimaryKeyColumns(Lists.newArrayList("myCol")).build();
    Map<String, String> streamConfigs = getStreamConfigs();
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setDedupConfig(new DedupConfig(true, HashFunction.NONE)).setStreamConfigs(streamConfigs).build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertEquals(e.getMessage(), "Upsert/Dedup table must use low-level streaming consumer type");
    }

    streamConfigs.put("stream.kafka.consumer.type", "simple");
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setDedupConfig(new DedupConfig(true, HashFunction.NONE)).setStreamConfigs(streamConfigs).build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertEquals(e.getMessage(),
          "Upsert/Dedup table must use strict replica-group (i.e. strictReplicaGroup) based routing");
    }
    StarTreeIndexConfig starTreeIndexConfig = new StarTreeIndexConfig(Lists.newArrayList("myCol"), null,
        Collections.singletonList(
            new AggregationFunctionColumnPair(AggregationFunctionType.COUNT, "myCol").toColumnName()), 10);
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setDedupConfig(new DedupConfig(true, HashFunction.NONE))
        .setRoutingConfig(new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE))
        .setStarTreeIndexConfigs(Lists.newArrayList(starTreeIndexConfig)).setStreamConfigs(streamConfigs).build();
    TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);

    // Dedup and upsert can't be enabled simultaneously
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setDedupConfig(new DedupConfig(true, HashFunction.NONE))
        .setRoutingConfig(new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE))
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL)).setStreamConfigs(streamConfigs).build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertEquals(e.getMessage(), "A table can have either Upsert or Dedup enabled, but not both");
    }
  }

  @Test
  public void testValidateUpsertConfig() {
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
            .addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
            .build();
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setUpsertConfig(upsertConfig).build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertEquals(e.getMessage(), "Upsert/Dedup table is for realtime table only.");
    }

    tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).setUpsertConfig(upsertConfig).build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertEquals(e.getMessage(), "Upsert/Dedup table must have primary key columns in the schema");
    }

    schema =
        new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
            .setPrimaryKeyColumns(Lists.newArrayList("myCol")).build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertEquals(e.getMessage(),
          "Could not find streamConfigs for REALTIME table: " + TABLE_NAME + "_REALTIME");
    }

    Map<String, String> streamConfigs = getStreamConfigs();
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).setUpsertConfig(upsertConfig)
        .setStreamConfigs(streamConfigs).build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertEquals(e.getMessage(), "Upsert/Dedup table must use low-level streaming consumer type");
    }

    streamConfigs.put("stream.kafka.consumer.type", "simple");
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).setUpsertConfig(upsertConfig)
        .setStreamConfigs(streamConfigs).build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertEquals(e.getMessage(),
          "Upsert/Dedup table must use strict replica-group (i.e. strictReplicaGroup) based routing");
    }

    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).setUpsertConfig(upsertConfig)
        .setRoutingConfig(new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE))
        .setStreamConfigs(streamConfigs).build();
    TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);

    StarTreeIndexConfig starTreeIndexConfig = new StarTreeIndexConfig(Lists.newArrayList("myCol"), null,
        Collections.singletonList(
            new AggregationFunctionColumnPair(AggregationFunctionType.COUNT, "myCol").toColumnName()), 10);
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).setUpsertConfig(upsertConfig)
        .setRoutingConfig(new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE))
        .setStarTreeIndexConfigs(Lists.newArrayList(starTreeIndexConfig)).setStreamConfigs(streamConfigs).build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertEquals(e.getMessage(), "The upsert table cannot have star-tree index.");
    }

    //With Aggregate Metrics
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).setUpsertConfig(upsertConfig)
        .setRoutingConfig(new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE))
        .setStreamConfigs(streamConfigs).setAggregateMetrics(true).build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertEquals(e.getMessage(), "Metrics aggregation and upsert cannot be enabled together");
    }

    //With aggregation Configs in Ingestion Config
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setAggregationConfigs(Collections.singletonList(new AggregationConfig("twiceSum", "SUM(twice)")));
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).setUpsertConfig(upsertConfig)
        .setRoutingConfig(new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE))
        .setStreamConfigs(streamConfigs).setIngestionConfig(ingestionConfig).build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertEquals(e.getMessage(), "Metrics aggregation and upsert cannot be enabled together");
    }

    //With aggregation Configs in Ingestion Config and IndexingConfig at the same time
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).setUpsertConfig(upsertConfig)
        .setRoutingConfig(new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE))
        .setStreamConfigs(streamConfigs).setAggregateMetrics(true).setIngestionConfig(ingestionConfig).build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertEquals(e.getMessage(),
          "Metrics aggregation cannot be enabled in the Indexing Config and Ingestion Config at the same time");
    }

    // Table upsert with delete column
    String incorrectTypeDelCol = "incorrectTypeDeleteCol";
    String delCol = "myDelCol";
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .setPrimaryKeyColumns(Lists.newArrayList("myPkCol"))
        .addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
        .addSingleValueDimension(incorrectTypeDelCol, FieldSpec.DataType.STRING)
        .addSingleValueDimension(delCol, FieldSpec.DataType.BOOLEAN)
        .build();
    streamConfigs = getStreamConfigs();
    streamConfigs.put("stream.kafka.consumer.type", "simple");

    upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setDeleteRecordColumn(incorrectTypeDelCol);
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).setStreamConfigs(streamConfigs)
        .setUpsertConfig(upsertConfig)
        .setRoutingConfig(new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE))
        .build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
      Assert.fail("Invalid delete column type (string) should have failed table creation");
    } catch (IllegalStateException e) {
      Assert.assertEquals(e.getMessage(), "The delete record column must be a single-valued BOOLEAN column");
    }

    upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    upsertConfig.setDeleteRecordColumn(delCol);
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).setStreamConfigs(streamConfigs)
        .setUpsertConfig(upsertConfig)
        .setRoutingConfig(new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE))
        .build();
    try {
      TableConfigUtils.validateUpsertAndDedupConfig(tableConfig, schema);
    } catch (IllegalStateException e) {
      Assert.fail("Shouldn't fail table creation when delete column type is boolean.");
    }
  }

  @Test
  public void testValidatePartialUpsertConfig() {
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addSingleValueDimension("myCol1", FieldSpec.DataType.LONG)
            .addSingleValueDimension("myCol2", FieldSpec.DataType.STRING)
            .addDateTime("myTimeCol", FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
            .setPrimaryKeyColumns(Lists.newArrayList("myCol1")).build();

    Map<String, String> streamConfigs = getStreamConfigs();
    streamConfigs.put("stream.kafka.consumer.type", "simple");
    Map<String, UpsertConfig.Strategy> partialUpsertStratgies = new HashMap<>();
    partialUpsertStratgies.put("myCol2", UpsertConfig.Strategy.IGNORE);
    UpsertConfig partialUpsertConfig = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    partialUpsertConfig.setPartialUpsertStrategies(partialUpsertStratgies);
    partialUpsertConfig.setDefaultPartialUpsertStrategy(UpsertConfig.Strategy.OVERWRITE);
    partialUpsertConfig.setComparisonColumn("myCol2");
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).setUpsertConfig(partialUpsertConfig)
            .setNullHandlingEnabled(true)
            .setRoutingConfig(new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE))
            .setStreamConfigs(streamConfigs).build();
    try {
      TableConfigUtils.validatePartialUpsertStrategies(tableConfig, schema);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertEquals(e.getMessage(), "Merger cannot be applied to comparison column");
    }

    partialUpsertConfig = new UpsertConfig(UpsertConfig.Mode.PARTIAL);
    partialUpsertConfig.setPartialUpsertStrategies(partialUpsertStratgies);
    partialUpsertConfig.setDefaultPartialUpsertStrategy(UpsertConfig.Strategy.OVERWRITE);
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).setTimeColumnName("myCol2")
        .setUpsertConfig(partialUpsertConfig).setNullHandlingEnabled(true)
        .setRoutingConfig(new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE))
        .setStreamConfigs(streamConfigs).build();
    try {
      TableConfigUtils.validatePartialUpsertStrategies(tableConfig, schema);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertEquals(e.getMessage(), "Merger cannot be applied to time column");
    }

    partialUpsertStratgies.put("myCol1", UpsertConfig.Strategy.INCREMENT);
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).setTimeColumnName("timeCol")
        .setUpsertConfig(partialUpsertConfig).setNullHandlingEnabled(false)
        .setRoutingConfig(new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE))
        .setStreamConfigs(streamConfigs).build();
    try {
      TableConfigUtils.validatePartialUpsertStrategies(tableConfig, schema);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertEquals(e.getMessage(), "Null handling must be enabled for partial upsert tables");
    }

    tableConfig.getIndexingConfig().setNullHandlingEnabled(true);
    try {
      TableConfigUtils.validatePartialUpsertStrategies(tableConfig, schema);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertEquals(e.getMessage(), "Merger cannot be applied to primary key columns");
    }

    partialUpsertStratgies.clear();
    partialUpsertStratgies.put("randomCol", UpsertConfig.Strategy.OVERWRITE);
    try {
      TableConfigUtils.validatePartialUpsertStrategies(tableConfig, schema);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertEquals(e.getMessage(), "Merger cannot be applied to non-existing column: randomCol");
    }

    partialUpsertStratgies.clear();
    partialUpsertStratgies.put("myCol2", UpsertConfig.Strategy.INCREMENT);
    try {
      TableConfigUtils.validatePartialUpsertStrategies(tableConfig, schema);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertEquals(e.getMessage(), "INCREMENT merger cannot be applied to non-numeric column: myCol2");
    }

    partialUpsertStratgies.clear();
    partialUpsertStratgies.put("myCol2", UpsertConfig.Strategy.APPEND);
    try {
      TableConfigUtils.validatePartialUpsertStrategies(tableConfig, schema);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertEquals(e.getMessage(), "APPEND merger cannot be applied to single-value column: myCol2");
    }

    partialUpsertStratgies.clear();
    partialUpsertStratgies.put("myTimeCol", UpsertConfig.Strategy.INCREMENT);
    try {
      TableConfigUtils.validatePartialUpsertStrategies(tableConfig, schema);
      Assert.fail();
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "INCREMENT merger cannot be applied to date time column: myTimeCol");
    }
  }

  @Test
  public void testTaskConfig() {
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
            .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
            .setPrimaryKeyColumns(Lists.newArrayList("myCol")).build();
    Map<String, String> realtimeToOfflineTaskConfig =
        ImmutableMap.of("schedule", "0 */10 * ? * * *", "bucketTimePeriod", "6h", "bufferTimePeriod", "5d", "mergeType",
            "rollup", "myCol.aggregationType", "max");
    Map<String, String> segmentGenerationAndPushTaskConfig = ImmutableMap.of("schedule", "0 */10 * ? * * *");
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTaskConfig(
        new TableTaskConfig(ImmutableMap.of("RealtimeToOfflineSegmentsTask", realtimeToOfflineTaskConfig,
            "SegmentGenerationAndPushTask", segmentGenerationAndPushTaskConfig))).build();

    // validate valid config
    TableConfigUtils.validateTaskConfigs(tableConfig, schema);

    // invalid schedule
    HashMap<String, String> invalidScheduleConfig = new HashMap<>(segmentGenerationAndPushTaskConfig);
    invalidScheduleConfig.put("schedule", "garbage");
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTaskConfig(new TableTaskConfig(
        ImmutableMap.of("RealtimeToOfflineSegmentsTask", realtimeToOfflineTaskConfig, "SegmentGenerationAndPushTask",
            invalidScheduleConfig))).build();
    try {
      TableConfigUtils.validateTaskConfigs(tableConfig, schema);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("contains an invalid cron schedule"));
    }

    // invalid Upsert config with RealtimeToOfflineTask
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).setTimeColumnName(TIME_COLUMN)
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL)).setTaskConfig(new TableTaskConfig(
            ImmutableMap.of("RealtimeToOfflineSegmentsTask", realtimeToOfflineTaskConfig,
                "SegmentGenerationAndPushTask", segmentGenerationAndPushTaskConfig))).build();
    try {
      TableConfigUtils.validateTaskConfigs(tableConfig, schema);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("RealtimeToOfflineTask doesn't support upsert table"));
    }
    // validate that TASK config will be skipped with skip string.
    TableConfigUtils.validate(tableConfig, schema, "TASK,UPSERT", false);

    // invalid period
    HashMap<String, String> invalidPeriodConfig = new HashMap<>(realtimeToOfflineTaskConfig);
    invalidPeriodConfig.put("roundBucketTimePeriod", "garbage");
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTaskConfig(new TableTaskConfig(
        ImmutableMap.of("RealtimeToOfflineSegmentsTask", invalidPeriodConfig, "SegmentGenerationAndPushTask",
            segmentGenerationAndPushTaskConfig))).build();
    try {
      TableConfigUtils.validateTaskConfigs(tableConfig, schema);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("Invalid time spec"));
    }

    // invalid mergeType
    HashMap<String, String> invalidMergeType = new HashMap<>(realtimeToOfflineTaskConfig);
    invalidMergeType.put("mergeType", "garbage");
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTaskConfig(new TableTaskConfig(
        ImmutableMap.of("RealtimeToOfflineSegmentsTask", invalidMergeType, "SegmentGenerationAndPushTask",
            segmentGenerationAndPushTaskConfig))).build();
    try {
      TableConfigUtils.validateTaskConfigs(tableConfig, schema);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("MergeType must be one of"));
    }

    // invalid column
    HashMap<String, String> invalidColumnConfig = new HashMap<>(realtimeToOfflineTaskConfig);
    invalidColumnConfig.put("score.aggregationType", "max");
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTaskConfig(new TableTaskConfig(
        ImmutableMap.of("RealtimeToOfflineSegmentsTask", invalidColumnConfig, "SegmentGenerationAndPushTask",
            segmentGenerationAndPushTaskConfig))).build();
    try {
      TableConfigUtils.validateTaskConfigs(tableConfig, schema);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("not found in schema"));
    }

    // invalid agg
    HashMap<String, String> invalidAggConfig = new HashMap<>(realtimeToOfflineTaskConfig);
    invalidAggConfig.put("myCol.aggregationType", "garbage");
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTaskConfig(new TableTaskConfig(
        ImmutableMap.of("RealtimeToOfflineSegmentsTask", invalidAggConfig, "SegmentGenerationAndPushTask",
            segmentGenerationAndPushTaskConfig))).build();
    try {
      TableConfigUtils.validateTaskConfigs(tableConfig, schema);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("has invalid aggregate type"));
    }
  }

  @Test
  public void testValidateInstancePartitionsMap() {
    InstanceAssignmentConfig instanceAssignmentConfig = Mockito.mock(InstanceAssignmentConfig.class);

    TableConfig tableConfigWithoutInstancePartitionsMap =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
            .build();

    // Call validate with a table-config without any instance partitions or instance assignment config
    TableConfigUtils.validateInstancePartitionsTypeMapConfig(tableConfigWithoutInstancePartitionsMap);

    TableConfig tableConfigWithInstancePartitionsMap =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
            .setInstancePartitionsMap(ImmutableMap.of(InstancePartitionsType.OFFLINE, "test_OFFLINE"))
            .build();

    // Call validate with a table-config with instance partitions set but not instance assignment config
    TableConfigUtils.validateInstancePartitionsTypeMapConfig(tableConfigWithInstancePartitionsMap);

    TableConfig invalidTableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInstancePartitionsMap(ImmutableMap.of(InstancePartitionsType.OFFLINE, "test_OFFLINE"))
        .setInstanceAssignmentConfigMap(
            ImmutableMap.of(InstancePartitionsType.OFFLINE.toString(), instanceAssignmentConfig))
            .build();
    try {
      // Call validate with instance partitions and config set for the same type
      TableConfigUtils.validateInstancePartitionsTypeMapConfig(invalidTableConfig);
      Assert.fail("Validation should have failed since both instancePartitionsMap and config are set");
    } catch (IllegalStateException ignored) {
    }
  }

  @Test
  public void testUpsertCompactionTaskConfig() {
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
            .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
            .setPrimaryKeyColumns(Lists.newArrayList("myCol")).build();
    Map<String, String> upsertCompactionTaskConfig =
        ImmutableMap.of(
            "bufferTimePeriod", "5d",
            "invalidRecordsThresholdPercent", "1",
            "minRecordCount", "1"
        );
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(TABLE_NAME)
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL))
        .setTaskConfig(new TableTaskConfig(
            ImmutableMap.of("UpsertCompactionTask", upsertCompactionTaskConfig)))
        .build();

    TableConfigUtils.validateTaskConfigs(tableConfig, schema);

    // test with invalid invalidRecordsThresholdPercents
    upsertCompactionTaskConfig =
        ImmutableMap.of(
            "invalidRecordsThresholdPercent", "0"
        );
    TableConfig zeroPercentTableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(TABLE_NAME)
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL))
        .setTaskConfig(new TableTaskConfig(
            ImmutableMap.of("UpsertCompactionTask", upsertCompactionTaskConfig)))
        .build();
    Assert.assertThrows(IllegalStateException.class,
        () -> TableConfigUtils.validateTaskConfigs(zeroPercentTableConfig, schema));
    upsertCompactionTaskConfig =
        ImmutableMap.of(
            "invalidRecordsThresholdPercent", "110"
        );
    TableConfig hundredTenPercentTableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(TABLE_NAME)
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL))
        .setTaskConfig(new TableTaskConfig(
            ImmutableMap.of("UpsertCompactionTask", upsertCompactionTaskConfig)))
        .build();
    Assert.assertThrows(IllegalStateException.class,
        () -> TableConfigUtils.validateTaskConfigs(hundredTenPercentTableConfig, schema));

    // test with invalid minRecordCount
    upsertCompactionTaskConfig =
        ImmutableMap.of(
            "minRecordCount", "0"
        );
    TableConfig invalidCountTableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(TABLE_NAME)
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL))
        .setTaskConfig(new TableTaskConfig(
            ImmutableMap.of("UpsertCompactionTask", upsertCompactionTaskConfig)))
        .build();
    Assert.assertThrows(IllegalStateException.class,
        () -> TableConfigUtils.validateTaskConfigs(invalidCountTableConfig, schema));

    // test without invalidRecordsThresholdPercent or minRecordCount
    upsertCompactionTaskConfig =
        ImmutableMap.of(
            "bufferTimePeriod", "5d"
        );
    TableConfig invalidTableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(TABLE_NAME)
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL))
        .setTaskConfig(new TableTaskConfig(
            ImmutableMap.of("UpsertCompactionTask", upsertCompactionTaskConfig)))
        .build();
    Assert.assertThrows(IllegalStateException.class,
        () -> TableConfigUtils.validateTaskConfigs(invalidTableConfig, schema));
  }

  @Test
  public void testValidatePartitionedReplicaGroupInstance() {
    String partitionColumn = "testPartitionCol";
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig =
        new ReplicaGroupStrategyConfig(partitionColumn, 2);

    TableConfig tableConfigWithoutReplicaGroupStrategyConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
            .build();
    // Call validate with a table-config without replicaGroupStrategyConfig or replicaGroupPartitionConfig.
    TableConfigUtils.validatePartitionedReplicaGroupInstance(tableConfigWithoutReplicaGroupStrategyConfig);

    TableConfig tableConfigWithReplicaGroupStrategyConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    tableConfigWithReplicaGroupStrategyConfig.getValidationConfig()
        .setReplicaGroupStrategyConfig(replicaGroupStrategyConfig);

    // Call validate with a table-config with replicaGroupStrategyConfig and without replicaGroupPartitionConfig.
    TableConfigUtils.validatePartitionedReplicaGroupInstance(tableConfigWithReplicaGroupStrategyConfig);

    InstanceAssignmentConfig instanceAssignmentConfig = Mockito.mock(InstanceAssignmentConfig.class);
    InstanceReplicaGroupPartitionConfig instanceReplicaGroupPartitionConfig =
        new InstanceReplicaGroupPartitionConfig(true, 0, 0, 0, 2, 0, false, partitionColumn);
    Mockito.doReturn(instanceReplicaGroupPartitionConfig)
        .when(instanceAssignmentConfig).getReplicaGroupPartitionConfig();

    TableConfig invalidTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(TABLE_NAME).setInstanceAssignmentConfigMap(
            ImmutableMap.of(TableType.OFFLINE.toString(), instanceAssignmentConfig)).build();
    invalidTableConfig.getValidationConfig().setReplicaGroupStrategyConfig(replicaGroupStrategyConfig);

    try {
      // Call validate with a table-config with replicaGroupStrategyConfig and replicaGroupPartitionConfig.
      TableConfigUtils.validatePartitionedReplicaGroupInstance(invalidTableConfig);
      Assert.fail("Validation should have failed since both replicaGroupStrategyConfig "
          + "and replicaGroupPartitionConfig are set");
    } catch (IllegalStateException ignored) {
    }
  }

  private Map<String, String> getStreamConfigs() {
    Map<String, String> streamConfigs = new HashMap<>();
    streamConfigs.put("streamType", "kafka");
    streamConfigs.put("stream.kafka.consumer.type", "highLevel");
    streamConfigs.put("stream.kafka.topic.name", "test");
    streamConfigs.put("stream.kafka.decoder.class.name",
        "org.apache.pinot.plugin.stream.kafka.KafkaJSONMessageDecoder");
    return streamConfigs;
  }
}
