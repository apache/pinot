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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests that the source field names are extracted correctly
 */
public class SchemaUtilsTest {

  @Test
  public void testSourceFieldExtractorName() {

    Schema schema;

    // from groovy function
    schema = new Schema();
    DimensionFieldSpec dimensionFieldSpec = new DimensionFieldSpec("d1", FieldSpec.DataType.STRING, true);
    dimensionFieldSpec.setTransformFunction("Groovy({function}, argument1, argument2)");
    schema.addField(dimensionFieldSpec);
    List<String> extract = new ArrayList<>(SchemaUtils.extractSourceFields(schema));
    Assert.assertEquals(extract.size(), 3);
    Assert.assertTrue(extract.containsAll(Arrays.asList("d1", "argument1", "argument2")));

    // groovy function, no arguments
    schema = new Schema();
    dimensionFieldSpec = new DimensionFieldSpec("d1", FieldSpec.DataType.STRING, true);
    dimensionFieldSpec.setTransformFunction("Groovy({function})");
    schema.addField(dimensionFieldSpec);
    extract = new ArrayList<>(SchemaUtils.extractSourceFields(schema));
    Assert.assertEquals(extract.size(), 1);
    Assert.assertTrue(extract.contains("d1"));

    // Map implementation for Avro - map__KEYS indicates map is source column
    schema = new Schema();
    dimensionFieldSpec = new DimensionFieldSpec("map__KEYS", FieldSpec.DataType.INT, false);
    schema.addField(dimensionFieldSpec);
    extract = new ArrayList<>(SchemaUtils.extractSourceFields(schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Arrays.asList("map", "map__KEYS")));

    // Map implementation for Avro - map__VALUES indicates map is source column
    schema = new Schema();
    dimensionFieldSpec = new DimensionFieldSpec("map__VALUES", FieldSpec.DataType.LONG, false);
    schema.addField(dimensionFieldSpec);
    extract = new ArrayList<>(SchemaUtils.extractSourceFields(schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Arrays.asList("map", "map__VALUES")));

    // Time field spec
    // only incoming
    schema = new Schema.SchemaBuilder()
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "time"), null).build();
    extract = new ArrayList<>(SchemaUtils.extractSourceFields(schema));
    Assert.assertEquals(extract.size(), 1);
    Assert.assertTrue(extract.contains("time"));

    // incoming and outgoing different column name
    schema = new Schema.SchemaBuilder()
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "in"),
            new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "out")).build();
    extract = new ArrayList<>(SchemaUtils.extractSourceFields(schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Arrays.asList("in", "out")));

    // inbuilt functions
    schema = new Schema();
    dimensionFieldSpec = new DimensionFieldSpec("hoursSinceEpoch", FieldSpec.DataType.LONG, true);
    dimensionFieldSpec.setTransformFunction("toEpochHours(timestamp)");
    schema.addField(dimensionFieldSpec);
    extract = new ArrayList<>(SchemaUtils.extractSourceFields(schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Arrays.asList("timestamp", "hoursSinceEpoch")));

    // inbuilt functions with literal
    schema = new Schema();
    dimensionFieldSpec = new DimensionFieldSpec("tenMinutesSinceEpoch", FieldSpec.DataType.LONG, true);
    dimensionFieldSpec.setTransformFunction("toEpochMinutesBucket(timestamp, 10)");
    schema.addField(dimensionFieldSpec);
    extract = new ArrayList<>(SchemaUtils.extractSourceFields(schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Lists.newArrayList("tenMinutesSinceEpoch", "timestamp")));

    // inbuilt functions on DateTimeFieldSpec
    schema = new Schema();
    DateTimeFieldSpec dateTimeFieldSpec =
        new DateTimeFieldSpec("date", FieldSpec.DataType.STRING, "1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd", "1:DAYS");
    dateTimeFieldSpec.setTransformFunction("toDateTime(timestamp, 'yyyy-MM-dd')");
    schema.addField(dateTimeFieldSpec);
    extract = new ArrayList<>(SchemaUtils.extractSourceFields(schema));
    Assert.assertEquals(extract.size(), 2);
    Assert.assertTrue(extract.containsAll(Lists.newArrayList("date", "timestamp")));
  }

  @Test
  public void testValidate() {
    Schema pinotSchema;
    // source name used as destination name
    pinotSchema = new Schema();
    DimensionFieldSpec dimensionFieldSpec = new DimensionFieldSpec("dim1", FieldSpec.DataType.STRING, true);
    dimensionFieldSpec.setTransformFunction("Groovy({function}, argument1, dim1, argument3)");
    pinotSchema.addField(dimensionFieldSpec);
    Assert.assertFalse(SchemaUtils.validate(pinotSchema));

    pinotSchema = new Schema();
    MetricFieldSpec metricFieldSpec = new MetricFieldSpec("m1", FieldSpec.DataType.LONG);
    metricFieldSpec.setTransformFunction("Groovy({function}, m1, m1)");
    pinotSchema.addField(metricFieldSpec);
    Assert.assertFalse(SchemaUtils.validate(pinotSchema));

    pinotSchema = new Schema();
    DateTimeFieldSpec dateTimeFieldSpec = new DateTimeFieldSpec("dt1", FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:HOURS");
    dateTimeFieldSpec.setTransformFunction("Groovy({function}, m1, dt1)");
    pinotSchema.addField(dateTimeFieldSpec);
    Assert.assertFalse(SchemaUtils.validate(pinotSchema));

    pinotSchema = new Schema.SchemaBuilder()
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "time"), null).build();
    pinotSchema.getFieldSpecFor("time").setTransformFunction("Groovy({function}, time)");
    Assert.assertFalse(SchemaUtils.validate(pinotSchema));

    // time field spec using same name for incoming and outgoing
    pinotSchema = new Schema.SchemaBuilder()
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "time"),
            new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "time")).build();
    Assert.assertFalse(SchemaUtils.validate(pinotSchema));

    // time field spec using SIMPLE_DATE_FORMAT, not allowed when conversion is needed
    pinotSchema = new Schema.SchemaBuilder()
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "incoming"),
            new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS,
                TimeGranularitySpec.TimeFormat.SIMPLE_DATE_FORMAT.toString(), "outgoing")).build();
    Assert.assertFalse(SchemaUtils.validate(pinotSchema));

    // incorrect groovy function syntax
    pinotSchema = new Schema();
    dimensionFieldSpec = new DimensionFieldSpec("dim1", FieldSpec.DataType.STRING, true);
    dimensionFieldSpec.setTransformFunction("Groovy(function, argument3)");
    pinotSchema.addField(dimensionFieldSpec);
    Assert.assertFalse(SchemaUtils.validate(pinotSchema));

    // valid schema, empty arguments
    pinotSchema = new Schema();
    dimensionFieldSpec = new DimensionFieldSpec("dim1", FieldSpec.DataType.STRING, true);
    dimensionFieldSpec.setTransformFunction("Groovy({function})");
    pinotSchema.addField(dimensionFieldSpec);
    Assert.assertTrue(SchemaUtils.validate(pinotSchema));

    // valid schema
    pinotSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("dim1", FieldSpec.DataType.STRING)
        .addMetric("m1", FieldSpec.DataType.LONG)
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "time"), null)
        .build();
    pinotSchema.getFieldSpecFor("dim1").setTransformFunction("Groovy({function}, argument1, argument2, argument3)");
    pinotSchema.getFieldSpecFor("m1").setTransformFunction("Groovy({function}, m2, m3)");
    pinotSchema.getFieldSpecFor("time").setTransformFunction("Groovy({function}, millis)");
    Assert.assertTrue(SchemaUtils.validate(pinotSchema));

    // valid time field spec
    pinotSchema = new Schema.SchemaBuilder().addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "incoming"),
            new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "outgoing")).build();
    Assert.assertTrue(SchemaUtils.validate(pinotSchema));
  }
}
