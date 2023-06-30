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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.config.table.ingestion.AggregationConfig;
import org.apache.pinot.spi.config.table.ingestion.ComplexTypeConfig;
import org.apache.pinot.spi.config.table.ingestion.FilterConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
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
    schema = new Schema.SchemaBuilder().addTime(
        new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "time"), null).build();
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(null, schema));
    Assert.assertEquals(extract.size(), 1);
    Assert.assertTrue(extract.contains("time"));

    // incoming and outgoing different column name
    schema = new Schema.SchemaBuilder().addTime(
        new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "in"),
        new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "out")).build();
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(null, schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Arrays.asList("in", "out")));

    // inbuilt functions
    schema = new Schema();
    dimensionFieldSpec = new DimensionFieldSpec("hoursSinceEpoch", FieldSpec.DataType.LONG, true);
    dimensionFieldSpec.setTransformFunction("toEpochHours(\"timestamp\")");
    schema.addField(dimensionFieldSpec);
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(null, schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Arrays.asList("timestamp", "hoursSinceEpoch")));

    // inbuilt functions with literal
    schema = new Schema();
    dimensionFieldSpec = new DimensionFieldSpec("tenMinutesSinceEpoch", FieldSpec.DataType.LONG, true);
    dimensionFieldSpec.setTransformFunction("toEpochMinutesBucket(\"timestamp\", 10)");
    schema.addField(dimensionFieldSpec);
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(null, schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Lists.newArrayList("tenMinutesSinceEpoch", "timestamp")));

    // inbuilt functions on DateTimeFieldSpec
    schema = new Schema();
    DateTimeFieldSpec dateTimeFieldSpec =
        new DateTimeFieldSpec("date", FieldSpec.DataType.STRING, "1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd", "1:DAYS");
    dateTimeFieldSpec.setTransformFunction("toDateTime(\"timestamp\", 'yyyy-MM-dd')");
    schema.addField(dateTimeFieldSpec);
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(null, schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Lists.newArrayList("date", "timestamp")));
  }

  @Test
  public void testExtractFieldsIngestionConfig() {
    Schema schema = new Schema();

    // filter config
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setFilterConfig(new FilterConfig("Groovy({x > 100}, x)"));
    Set<String> fields = IngestionUtils.getFieldsForRecordExtractor(ingestionConfig, schema);
    Assert.assertEquals(fields.size(), 1);
    Assert.assertTrue(fields.containsAll(Sets.newHashSet("x")));

    schema.addField(new DimensionFieldSpec("y", FieldSpec.DataType.STRING, true));
    fields = IngestionUtils.getFieldsForRecordExtractor(ingestionConfig, schema);
    Assert.assertEquals(fields.size(), 2);
    Assert.assertTrue(fields.containsAll(Sets.newHashSet("x", "y")));

    // transform configs
    schema = new Schema.SchemaBuilder().addSingleValueDimension("d1", FieldSpec.DataType.STRING).build();
    ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(
        Collections.singletonList(new TransformConfig("d1", "Groovy({function}, argument1, argument2)")));
    List<String> extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(ingestionConfig, schema));
    Assert.assertEquals(extract.size(), 3);
    Assert.assertTrue(extract.containsAll(Arrays.asList("d1", "argument1", "argument2")));

    // groovy function, no arguments
    ingestionConfig.setTransformConfigs(Collections.singletonList(new TransformConfig("d1", "Groovy({function})")));
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(ingestionConfig, schema));
    Assert.assertEquals(extract.size(), 1);
    Assert.assertTrue(extract.contains("d1"));

    // inbuilt functions
    schema = new Schema.SchemaBuilder().addSingleValueDimension("hoursSinceEpoch", FieldSpec.DataType.LONG).build();
    ingestionConfig.setTransformConfigs(
        Collections.singletonList(new TransformConfig("hoursSinceEpoch", "toEpochHours(timestampColumn)")));
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(ingestionConfig, schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Arrays.asList("timestampColumn", "hoursSinceEpoch")));

    // inbuilt functions with literal
    schema =
        new Schema.SchemaBuilder().addSingleValueDimension("tenMinutesSinceEpoch", FieldSpec.DataType.LONG).build();
    ingestionConfig.setTransformConfigs(Collections.singletonList(
        new TransformConfig("tenMinutesSinceEpoch", "toEpochMinutesBucket(timestampColumn, 10)")));
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(ingestionConfig, schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Lists.newArrayList("tenMinutesSinceEpoch", "timestampColumn")));

    // inbuilt functions on DateTimeFieldSpec
    schema = new Schema.SchemaBuilder().addDateTime("dateColumn", FieldSpec.DataType.STRING,
        "1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd", "1:DAYS").build();
    ingestionConfig.setTransformConfigs(
        Collections.singletonList(new TransformConfig("dateColumn", "toDateTime(timestampColumn, 'yyyy-MM-dd')")));
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(ingestionConfig, schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Lists.newArrayList("dateColumn", "timestampColumn")));

    // filter + transform configs + schema fields  + schema transform
    schema = new Schema.SchemaBuilder().addSingleValueDimension("d1", FieldSpec.DataType.STRING)
        .addSingleValueDimension("d2", FieldSpec.DataType.STRING).addMetric("m1", FieldSpec.DataType.INT)
        .addDateTime("dateColumn", FieldSpec.DataType.STRING, "1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd", "1:DAYS").build();
    schema.getFieldSpecFor("d2").setTransformFunction("reverse(xy)");
    ingestionConfig.setFilterConfig(new FilterConfig("Groovy({d1 == \"10\"}, d1)"));
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(ingestionConfig, schema));
    Assert.assertEquals(extract.size(), 6);
    Assert.assertTrue(extract.containsAll(Lists.newArrayList("d1", "d2", "m1", "dateColumn", "xy", "timestampColumn")));

    // filter + transform configs + schema fields  + schema transform + complex type configs
    schema = new Schema.SchemaBuilder().addSingleValueDimension("d1", FieldSpec.DataType.STRING)
        .addSingleValueDimension("d2", FieldSpec.DataType.STRING).addMetric("m1", FieldSpec.DataType.INT)
        .addDateTime("dateColumn", FieldSpec.DataType.STRING, "1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd", "1:DAYS").build();
    schema.getFieldSpecFor("d2").setTransformFunction("reverse(xy)");
    ingestionConfig.setComplexTypeConfig(new ComplexTypeConfig(Arrays.asList("before.test", "after.test"), ".",
        ComplexTypeConfig.CollectionNotUnnestedToJson.NON_PRIMITIVE, Collections.singletonMap("before", "after")));
    extract = new ArrayList<>(IngestionUtils.getFieldsForRecordExtractor(ingestionConfig, schema));
    Assert.assertEquals(extract.size(), 8);
    List<String> expectedColumns =
        Arrays.asList("d1", "d2", "m1", "dateColumn", "xy", "timestampColumn", "before", "after");
    Assert.assertTrue(extract.containsAll(expectedColumns));
  }

  @Test
  public void testExtractFieldsAggregationConfig() {
    IngestionConfig ingestionConfig = new IngestionConfig();
    Schema schema = new Schema();

    ingestionConfig.setAggregationConfigs(Collections.singletonList(new AggregationConfig("d1", "SUM(s1)")));
    Set<String> fields = IngestionUtils.getFieldsForRecordExtractor(ingestionConfig, schema);
    Assert.assertEquals(fields.size(), 1);
    Assert.assertTrue(fields.containsAll(Sets.newHashSet("s1")));

    ingestionConfig.setAggregationConfigs(Collections.singletonList(new AggregationConfig("d1", "MIN(s1)")));
    fields = IngestionUtils.getFieldsForRecordExtractor(ingestionConfig, schema);
    Assert.assertEquals(fields.size(), 1);
    Assert.assertTrue(fields.containsAll(Sets.newHashSet("s1")));

    ingestionConfig.setAggregationConfigs(Collections.singletonList(new AggregationConfig("d1", "MAX(s1)")));
    fields = IngestionUtils.getFieldsForRecordExtractor(ingestionConfig, schema);
    Assert.assertEquals(fields.size(), 1);
    Assert.assertTrue(fields.containsAll(Sets.newHashSet("s1")));
  }

  @Test
  public void testComplexTypeConfig() {
    IngestionConfig ingestionConfig = new IngestionConfig();
    ComplexTypeConfig complexTypeConfig = new ComplexTypeConfig(null, "__",
        ComplexTypeConfig.CollectionNotUnnestedToJson.NON_PRIMITIVE, null);
    Schema schema = new Schema();

    ingestionConfig.setComplexTypeConfig(complexTypeConfig);
    schema.addField(new DimensionFieldSpec("a_b__c_d", FieldSpec.DataType.STRING, true));
    schema.addField(new DimensionFieldSpec("f_d", FieldSpec.DataType.STRING, false));
    schema.addField(new DimensionFieldSpec("ab__cd", FieldSpec.DataType.STRING, true));
    Set<String> fields = IngestionUtils.getFieldsForRecordExtractor(ingestionConfig, schema);
    Assert.assertEquals(fields.size(), 3);
    Assert.assertTrue(fields.containsAll(Sets.newHashSet("a_b", "f_d", "ab")));
  }
}
