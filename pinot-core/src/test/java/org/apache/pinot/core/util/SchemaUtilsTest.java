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
 * Tests schema validations
 */
public class SchemaUtilsTest {

  @Test
  public void testValidateTransformFunctionArguments() {
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
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "timeColumn"), null).build();
    pinotSchema.getFieldSpecFor("timeColumn").setTransformFunction("Groovy({function}, timeColumn)");
    Assert.assertFalse(SchemaUtils.validate(pinotSchema));
  }

  @Test
  public void testValidateTimeFieldSpec() {
    Schema pinotSchema;
    // time field spec using same name for incoming and outgoing
    pinotSchema = new Schema.SchemaBuilder()
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "timeColumn"),
            new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "timeColumn")).build();
    Assert.assertFalse(SchemaUtils.validate(pinotSchema));

    // time field spec using SIMPLE_DATE_FORMAT, not allowed when conversion is needed
    pinotSchema = new Schema.SchemaBuilder()
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "incoming"),
            new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS,
                TimeGranularitySpec.TimeFormat.SIMPLE_DATE_FORMAT.toString(), "outgoing")).build();
    Assert.assertFalse(SchemaUtils.validate(pinotSchema));

    // valid time field spec
    pinotSchema = new Schema.SchemaBuilder()
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "incoming"), new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "outgoing")).build();
    Assert.assertTrue(SchemaUtils.validate(pinotSchema));
  }

  @Test
  public void testGroovyFunctionSyntax() {
    Schema pinotSchema;
    // incorrect groovy function syntax
    pinotSchema = new Schema();
    DimensionFieldSpec dimensionFieldSpec = new DimensionFieldSpec("dim1", FieldSpec.DataType.STRING, true);
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
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "t"), null)
        .build();
    pinotSchema.getFieldSpecFor("dim1").setTransformFunction("Groovy({function}, argument1, argument2, argument3)");
    pinotSchema.getFieldSpecFor("m1").setTransformFunction("Groovy({function}, m2, m3)");
    pinotSchema.getFieldSpecFor("t").setTransformFunction("Groovy({function}, millis)");
    Assert.assertTrue(SchemaUtils.validate(pinotSchema));
  }

  @Test
  public void testValidateFieldName() {
    // test some keywords which are not allowed by BABEL
    Schema pinotSchema =
        new Schema.SchemaBuilder().addSingleValueDimension("timestamp", FieldSpec.DataType.STRING, true).build();
    Assert.assertFalse(SchemaUtils.validate(pinotSchema));

    pinotSchema =
        new Schema.SchemaBuilder().addSingleValueDimension("date", FieldSpec.DataType.STRING, true).build();
    Assert.assertFalse(SchemaUtils.validate(pinotSchema));

    pinotSchema =
        new Schema.SchemaBuilder().addSingleValueDimension("time", FieldSpec.DataType.STRING, true).build();
    Assert.assertFalse(SchemaUtils.validate(pinotSchema));

    pinotSchema =
        new Schema.SchemaBuilder().addSingleValueDimension("table", FieldSpec.DataType.STRING, true).build();
    Assert.assertFalse(SchemaUtils.validate(pinotSchema));

    // test keywords which would not have been allowed in a conformance level stricter than BABEL
    pinotSchema =
        new Schema.SchemaBuilder()
            .addSingleValueDimension("count", FieldSpec.DataType.STRING, true)
            .addSingleValueDimension("language", FieldSpec.DataType.STRING, true)
            .addSingleValueDimension("value", FieldSpec.DataType.STRING, true)
            .addSingleValueDimension("system", FieldSpec.DataType.STRING, true)
            .addSingleValueDimension("position", FieldSpec.DataType.STRING, true)
            .addSingleValueDimension("module", FieldSpec.DataType.STRING, true)
            .addSingleValueDimension("dateTime", FieldSpec.DataType.STRING, true)
            .build();
    Assert.assertTrue(SchemaUtils.validate(pinotSchema));
  }

  @Test
  public void testDisableFieldNameValidation() {
    Schema pinotSchema =
        new Schema.SchemaBuilder()
            .addSingleValueDimension("timestamp", FieldSpec.DataType.STRING, true)
            .addSingleValueDimension("date", FieldSpec.DataType.STRING, true)
            .addSingleValueDimension("time", FieldSpec.DataType.STRING, true)
            .addSingleValueDimension("table", FieldSpec.DataType.STRING, true)
            .addSingleValueDimension("group", FieldSpec.DataType.STRING, true)
            .build();
    Assert.assertTrue(SchemaUtils.validate(pinotSchema, false, null));
  }
}
