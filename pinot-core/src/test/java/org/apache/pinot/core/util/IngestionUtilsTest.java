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
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.Batch;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.FilterConfig;
import org.apache.pinot.spi.config.table.ingestion.Stream;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests fields extraction from Schema dna TableConfig for ingestion
 */
public class IngestionUtilsTest {

  /**
   * Transform functions have moved to table config. This test remains for backward compatible handling testing
   */
  @Test
  public void testExtractFieldsSchema() {

    Schema schema;

    // from groovy function
    schema = new Schema();
    DimensionFieldSpec dimensionFieldSpec = new DimensionFieldSpec("d1", FieldSpec.DataType.STRING, true);
    dimensionFieldSpec.setTransformFunction("Groovy({function}, argument1, argument2)");
    schema.addField(dimensionFieldSpec);
    List<String> extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(null, schema));
    Assert.assertEquals(extract.size(), 3);
    Assert.assertTrue(extract.containsAll(Arrays.asList("d1", "argument1", "argument2")));

    // groovy function, no arguments
    schema = new Schema();
    dimensionFieldSpec = new DimensionFieldSpec("d1", FieldSpec.DataType.STRING, true);
    dimensionFieldSpec.setTransformFunction("Groovy({function})");
    schema.addField(dimensionFieldSpec);
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(null, schema));
    Assert.assertEquals(extract.size(), 1);
    Assert.assertTrue(extract.contains("d1"));

    // Map implementation for Avro - map__KEYS indicates map is source column
    schema = new Schema();
    dimensionFieldSpec = new DimensionFieldSpec("map__KEYS", FieldSpec.DataType.INT, false);
    schema.addField(dimensionFieldSpec);
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(null, schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Arrays.asList("map", "map__KEYS")));

    // Map implementation for Avro - map__VALUES indicates map is source column
    schema = new Schema();
    dimensionFieldSpec = new DimensionFieldSpec("map__VALUES", FieldSpec.DataType.LONG, false);
    schema.addField(dimensionFieldSpec);
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(null, schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Arrays.asList("map", "map__VALUES")));

    // Time field spec
    // only incoming
    schema = new Schema.SchemaBuilder()
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "time"), null).build();
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(null, schema));
    Assert.assertEquals(extract.size(), 1);
    Assert.assertTrue(extract.contains("time"));

    // incoming and outgoing different column name
    schema = new Schema.SchemaBuilder()
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "in"),
            new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "out")).build();
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(null, schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Arrays.asList("in", "out")));

    // inbuilt functions
    schema = new Schema();
    dimensionFieldSpec = new DimensionFieldSpec("hoursSinceEpoch", FieldSpec.DataType.LONG, true);
    dimensionFieldSpec.setTransformFunction("toEpochHours(timestamp)");
    schema.addField(dimensionFieldSpec);
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(null, schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Arrays.asList("timestamp", "hoursSinceEpoch")));

    // inbuilt functions with literal
    schema = new Schema();
    dimensionFieldSpec = new DimensionFieldSpec("tenMinutesSinceEpoch", FieldSpec.DataType.LONG, true);
    dimensionFieldSpec.setTransformFunction("toEpochMinutesBucket(timestamp, 10)");
    schema.addField(dimensionFieldSpec);
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(null, schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Lists.newArrayList("tenMinutesSinceEpoch", "timestamp")));

    // inbuilt functions on DateTimeFieldSpec
    schema = new Schema();
    DateTimeFieldSpec dateTimeFieldSpec =
        new DateTimeFieldSpec("date", FieldSpec.DataType.STRING, "1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd", "1:DAYS");
    dateTimeFieldSpec.setTransformFunction("toDateTime(timestamp, 'yyyy-MM-dd')");
    schema.addField(dateTimeFieldSpec);
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(null, schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Lists.newArrayList("date", "timestamp")));
  }

  @Test
  public void testExtractFieldsIngestionConfig() {
    Schema schema = new Schema();

    // filter config
    IngestionConfig ingestionConfig = new IngestionConfig(null, null, new FilterConfig("Groovy({x > 100}, x)"), null);
    Set<String> fields = IngestionUtils.getFieldsForRecordExtractor(ingestionConfig, schema);
    Assert.assertEquals(fields.size(), 1);
    Assert.assertTrue(fields.containsAll(Sets.newHashSet("x")));

    schema.addField(new DimensionFieldSpec("y", FieldSpec.DataType.STRING, true));
    fields = IngestionUtils.getFieldsForRecordExtractor(ingestionConfig, schema);
    Assert.assertEquals(fields.size(), 2);
    Assert.assertTrue(fields.containsAll(Sets.newHashSet("x", "y")));

    // transform configs
    schema = new Schema.SchemaBuilder().addSingleValueDimension("d1", FieldSpec.DataType.STRING).build();
    List<TransformConfig> transformConfigs =
        Lists.newArrayList(new TransformConfig("d1", "Groovy({function}, argument1, argument2)"));
    ingestionConfig = new IngestionConfig(null, null, null, transformConfigs);
    List<String> extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(ingestionConfig, schema));
    Assert.assertEquals(extract.size(), 3);
    Assert.assertTrue(extract.containsAll(Arrays.asList("d1", "argument1", "argument2")));

    // groovy function, no arguments
    transformConfigs = Lists.newArrayList(new TransformConfig("d1", "Groovy({function})"));
    ingestionConfig = new IngestionConfig(null, null, null, transformConfigs);
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(ingestionConfig, schema));
    Assert.assertEquals(extract.size(), 1);
    Assert.assertTrue(extract.contains("d1"));

    // inbuilt functions
    schema = new Schema.SchemaBuilder().addSingleValueDimension("hoursSinceEpoch", FieldSpec.DataType.LONG).build();
    transformConfigs = Lists.newArrayList(new TransformConfig("hoursSinceEpoch", "toEpochHours(timestampColumn)"));
    ingestionConfig = new IngestionConfig(null, null, null, transformConfigs);
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(ingestionConfig, schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Arrays.asList("timestampColumn", "hoursSinceEpoch")));

    // inbuilt functions with literal
    schema =
        new Schema.SchemaBuilder().addSingleValueDimension("tenMinutesSinceEpoch", FieldSpec.DataType.LONG).build();
    transformConfigs =
        Lists.newArrayList(new TransformConfig("tenMinutesSinceEpoch", "toEpochMinutesBucket(timestampColumn, 10)"));
    ingestionConfig = new IngestionConfig(null, null, null, transformConfigs);
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(ingestionConfig, schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Lists.newArrayList("tenMinutesSinceEpoch", "timestampColumn")));

    // inbuilt functions on DateTimeFieldSpec
    schema = new Schema.SchemaBuilder()
        .addDateTime("dateColumn", FieldSpec.DataType.STRING, "1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd", "1:DAYS").build();
    transformConfigs =
        Lists.newArrayList(new TransformConfig("dateColumn", "toDateTime(timestampColumn, 'yyyy-MM-dd')"));
    ingestionConfig = new IngestionConfig(null, null, null, transformConfigs);
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(ingestionConfig, schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Lists.newArrayList("dateColumn", "timestampColumn")));

    // filter + transform configs + schema fields  + schema transform
    schema = new Schema.SchemaBuilder().addSingleValueDimension("d1", FieldSpec.DataType.STRING)
        .addSingleValueDimension("d2", FieldSpec.DataType.STRING).addMetric("m1", FieldSpec.DataType.INT)
        .addDateTime("dateColumn", FieldSpec.DataType.STRING, "1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd", "1:DAYS").build();
    schema.getFieldSpecFor("d2").setTransformFunction("reverse(xy)");
    transformConfigs =
        Lists.newArrayList(new TransformConfig("dateColumn", "toDateTime(timestampColumn, 'yyyy-MM-dd')"));
    ingestionConfig = new IngestionConfig(null, null, new FilterConfig("Groovy({d1 == \"10\"}, d1)"), transformConfigs);
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(ingestionConfig, schema));
    Assert.assertEquals(extract.size(), 6);
    Assert.assertTrue(extract.containsAll(Lists.newArrayList("d1", "d2", "m1", "dateColumn", "xy", "timestampColumn")));
  }

  @Test
  public void testGetStreamConfigsMap() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").build();
    try {
      IngestionUtils.getStreamConfigsMap(tableConfig);
      Assert.fail("Should fail for OFFLINE table");
    } catch (IllegalStateException e) {
      // expected
    }

    tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName("myTable").setTimeColumnName("timeColumn").build();

    // get from ingestion config (when not present in indexing config)
    Map<String, String> streamConfigsMap = new HashMap<>();
    streamConfigsMap.put("streamType", "kafka");
    tableConfig
        .setIngestionConfig(new IngestionConfig(null, new Stream(Lists.newArrayList(streamConfigsMap)), null, null));
    Map<String, String> actualStreamConfigsMap = IngestionUtils.getStreamConfigsMap(tableConfig);
    Assert.assertEquals(actualStreamConfigsMap.size(), 1);
    Assert.assertEquals(actualStreamConfigsMap.get("streamType"), "kafka");

    // get from ingestion config (even if present in indexing config)
    Map<String, String> deprecatedStreamConfigsMap = new HashMap<>();
    deprecatedStreamConfigsMap.put("streamType", "foo");
    deprecatedStreamConfigsMap.put("customProp", "foo");
    IndexingConfig indexingConfig = new IndexingConfig();
    indexingConfig.setStreamConfigs(deprecatedStreamConfigsMap);
    tableConfig.setIndexingConfig(indexingConfig);
    actualStreamConfigsMap = IngestionUtils.getStreamConfigsMap(tableConfig);
    Assert.assertEquals(actualStreamConfigsMap.size(), 1);
    Assert.assertEquals(actualStreamConfigsMap.get("streamType"), "kafka");

    // fail if multiple found
    tableConfig.setIngestionConfig(
        new IngestionConfig(null, new Stream(Lists.newArrayList(streamConfigsMap, deprecatedStreamConfigsMap)), null,
            null));
    try {
      IngestionUtils.getStreamConfigsMap(tableConfig);
      Assert.fail("Should fail for multiple stream configs");
    } catch (IllegalStateException e) {
      // expected
    }

    // get from indexing config
    tableConfig.setIngestionConfig(null);
    actualStreamConfigsMap = IngestionUtils.getStreamConfigsMap(tableConfig);
    Assert.assertEquals(actualStreamConfigsMap.size(), 2);
    Assert.assertEquals(actualStreamConfigsMap.get("streamType"), "foo");

    // fail if found nowhere
    tableConfig.setIndexingConfig(null);
    try {
      IngestionUtils.getStreamConfigsMap(tableConfig);
      Assert.fail("Should fail for no stream config found");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test
  public void testGetPushFrequency() {
    // get from ingestion config, when not present in segmentsConfig
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").build();
    tableConfig.setIngestionConfig(new IngestionConfig(new Batch(null, null, "HOURLY"), null, null, null));
    Assert.assertEquals(IngestionUtils.getBatchSegmentPushFrequency(tableConfig), "HOURLY");

    // get from ingestion config, even if present in segmentsConfig
    SegmentsValidationAndRetentionConfig segmentsValidationAndRetentionConfig =
        new SegmentsValidationAndRetentionConfig();
    segmentsValidationAndRetentionConfig.setSegmentPushFrequency("DAILY");
    tableConfig.setValidationConfig(segmentsValidationAndRetentionConfig);
    Assert.assertEquals(IngestionUtils.getBatchSegmentPushFrequency(tableConfig), "HOURLY");

    // get from segmentsConfig
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").build();
    tableConfig.setValidationConfig(segmentsValidationAndRetentionConfig);
    Assert.assertEquals(IngestionUtils.getBatchSegmentPushFrequency(tableConfig), "DAILY");

    // present nowhere
    segmentsValidationAndRetentionConfig.setSegmentPushFrequency(null);
    Assert.assertNull(IngestionUtils.getBatchSegmentPushFrequency(tableConfig));
  }

  @Test
  public void testGetPushType() {
    // get from ingestion config, when not present in segmentsConfig
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").build();
    tableConfig.setIngestionConfig(new IngestionConfig(new Batch(null, "APPEND", null), null, null, null));
    Assert.assertEquals(IngestionUtils.getBatchSegmentPushType(tableConfig), "APPEND");

    // get from ingestion config, even if present in segmentsConfig
    SegmentsValidationAndRetentionConfig segmentsValidationAndRetentionConfig =
        new SegmentsValidationAndRetentionConfig();
    segmentsValidationAndRetentionConfig.setSegmentPushType("REFRESH");
    tableConfig.setValidationConfig(segmentsValidationAndRetentionConfig);
    Assert.assertEquals(IngestionUtils.getBatchSegmentPushType(tableConfig), "APPEND");

    // get from segmentsConfig
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").build();
    tableConfig.setValidationConfig(segmentsValidationAndRetentionConfig);
    Assert.assertEquals(IngestionUtils.getBatchSegmentPushType(tableConfig), "REFRESH");

    // present nowhere
    segmentsValidationAndRetentionConfig.setSegmentPushType(null);
    Assert.assertNull(IngestionUtils.getBatchSegmentPushType(tableConfig));
  }
}
