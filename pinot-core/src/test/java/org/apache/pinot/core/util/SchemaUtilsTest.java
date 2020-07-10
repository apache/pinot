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

import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests that the source field names are extracted correctly
 */
public class SchemaUtilsTest {

  /**
   * TODO: transform functions have moved to tableConfig#ingestionConfig. However, these tests remain to test backward compatibility/
   *  Remove these when we totally stop honoring transform functions in schema
   */
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
