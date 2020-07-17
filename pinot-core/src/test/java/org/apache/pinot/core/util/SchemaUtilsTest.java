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

import java.io.IOException;
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

  /**
   * TODO: transform functions have moved to tableConfig#ingestionConfig. However, these tests remain to test backward compatibility/
   *  Remove these when we totally stop honoring transform functions in schema
   */
  @Test
  public void testValidateTransformFunctionArguments() {
    Schema pinotSchema;
    // source name used as destination name
    pinotSchema = new Schema();
    DimensionFieldSpec dimensionFieldSpec = new DimensionFieldSpec("dim1", FieldSpec.DataType.STRING, true);
    dimensionFieldSpec.setTransformFunction("Groovy({function}, argument1, dim1, argument3)");
    pinotSchema.addField(dimensionFieldSpec);
    try {
      SchemaUtils.validate(pinotSchema);
      Assert.fail("Schema validation should have failed.");
    } catch (IllegalStateException e) {
      // expected
    }

    pinotSchema = new Schema();
    MetricFieldSpec metricFieldSpec = new MetricFieldSpec("m1", FieldSpec.DataType.LONG);
    metricFieldSpec.setTransformFunction("Groovy({function}, m1, m1)");
    pinotSchema.addField(metricFieldSpec);
    checkValidationFails(pinotSchema);

    pinotSchema = new Schema();
    DateTimeFieldSpec dateTimeFieldSpec =
        new DateTimeFieldSpec("dt1", FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:HOURS");
    dateTimeFieldSpec.setTransformFunction("Groovy({function}, m1, dt1)");
    pinotSchema.addField(dateTimeFieldSpec);
    checkValidationFails(pinotSchema);

    pinotSchema = new Schema.SchemaBuilder()
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "time"), null).build();
    pinotSchema.getFieldSpecFor("time").setTransformFunction("Groovy({function}, time)");
    checkValidationFails(pinotSchema);

    // derived transformations
    pinotSchema = new Schema.SchemaBuilder().addSingleValueDimension("x", FieldSpec.DataType.INT)
        .addSingleValueDimension("z", FieldSpec.DataType.INT).build();
    pinotSchema.getFieldSpecFor("x").setTransformFunction("Groovy({y + 10}, y)");
    pinotSchema.getFieldSpecFor("z").setTransformFunction("Groovy({x*w*20}, x, w)");
    checkValidationFails(pinotSchema);
  }

  @Test
  public void testValidateTimeFieldSpec() {
    Schema pinotSchema;
    // time field spec using same name for incoming and outgoing
    pinotSchema = new Schema.SchemaBuilder()
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "time"),
            new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "time")).build();
    checkValidationFails(pinotSchema);

    // time field spec using SIMPLE_DATE_FORMAT, not allowed when conversion is needed
    pinotSchema = new Schema.SchemaBuilder()
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "incoming"),
            new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS,
                TimeGranularitySpec.TimeFormat.SIMPLE_DATE_FORMAT.toString(), "outgoing")).build();
    checkValidationFails(pinotSchema);

    // valid time field spec
    pinotSchema = new Schema.SchemaBuilder()
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "incoming"),
            new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "outgoing")).build();
    SchemaUtils.validate(pinotSchema);
  }

  @Test
  public void testGroovyFunctionSyntax() {
    Schema pinotSchema;
    // incorrect groovy function syntax
    pinotSchema = new Schema();

    DimensionFieldSpec dimensionFieldSpec = new DimensionFieldSpec("dim1", FieldSpec.DataType.STRING, true);
    dimensionFieldSpec.setTransformFunction("Groovy(function, argument3)");
    pinotSchema.addField(dimensionFieldSpec);
    checkValidationFails(pinotSchema);

    // valid schema, empty arguments
    pinotSchema = new Schema();

    dimensionFieldSpec = new DimensionFieldSpec("dim1", FieldSpec.DataType.STRING, true);
    dimensionFieldSpec.setTransformFunction("Groovy({function})");
    pinotSchema.addField(dimensionFieldSpec);
    SchemaUtils.validate(pinotSchema);

    // valid schema
    pinotSchema = new Schema.SchemaBuilder().addSingleValueDimension("dim1", FieldSpec.DataType.STRING)
        .addMetric("m1", FieldSpec.DataType.LONG)
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "time"), null).build();
    pinotSchema.getFieldSpecFor("dim1").setTransformFunction("Groovy({function}, argument1, argument2, argument3)");
    pinotSchema.getFieldSpecFor("m1").setTransformFunction("Groovy({function}, m2, m3)");
    pinotSchema.getFieldSpecFor("time").setTransformFunction("Groovy({function}, millis)");
    SchemaUtils.validate(pinotSchema);
  }

  @Test
  public void testDateTimeFieldSpec()
      throws IOException {
    Schema pinotSchema;
    pinotSchema = Schema.fromString(
        "{\"schemaName\":\"testSchema\"," + "\"dimensionFieldSpecs\":[ {\"name\":\"dim1\",\"dataType\":\"STRING\"}],"
            + "\"dateTimeFieldSpecs\":[{\"name\":\"dt1\",\"dataType\":\"INT\",\"format\":\"x:HOURS:EPOCH\",\"granularity\":\"1:HOURS\"}]}");
    checkValidationFails(pinotSchema);

    pinotSchema = Schema.fromString(
        "{\"schemaName\":\"testSchema\"," + "\"dimensionFieldSpecs\":[ {\"name\":\"dim1\",\"dataType\":\"STRING\"}],"
            + "\"dateTimeFieldSpecs\":[{\"name\":\"dt1\",\"dataType\":\"INT\",\"format\":\"1:DUMMY:EPOCH\",\"granularity\":\"1:HOURS\"}]}");
    checkValidationFails(pinotSchema);

    pinotSchema = Schema.fromString(
        "{\"schemaName\":\"testSchema\"," + "\"dimensionFieldSpecs\":[ {\"name\":\"dim1\",\"dataType\":\"STRING\"}],"
            + "\"dateTimeFieldSpecs\":[{\"name\":\"dt1\",\"dataType\":\"INT\",\"format\":\"1:HOURS:DUMMY\",\"granularity\":\"1:HOURS\"}]}");
    checkValidationFails(pinotSchema);

    pinotSchema = Schema.fromString(
        "{\"schemaName\":\"testSchema\"," + "\"dimensionFieldSpecs\":[ {\"name\":\"dim1\",\"dataType\":\"STRING\"}],"
            + "\"dateTimeFieldSpecs\":[{\"name\":\"dt1\",\"dataType\":\"INT\",\"format\":\"1:HOURS:EPOCH\",\"granularity\":\"x:HOURS\"}]}");
    checkValidationFails(pinotSchema);

    pinotSchema = Schema.fromString(
        "{\"schemaName\":\"testSchema\"," + "\"dimensionFieldSpecs\":[ {\"name\":\"dim1\",\"dataType\":\"STRING\"}],"
            + "\"dateTimeFieldSpecs\":[{\"name\":\"dt1\",\"dataType\":\"INT\",\"format\":\"1:HOURS:EPOCH\",\"granularity\":\"1:DUMMY\"}]}");
    checkValidationFails(pinotSchema);

    pinotSchema = Schema.fromString(
        "{\"schemaName\":\"testSchema\"," + "\"dimensionFieldSpecs\":[ {\"name\":\"dim1\",\"dataType\":\"STRING\"}],"
            + "\"dateTimeFieldSpecs\":[{\"name\":\"dt1\",\"dataType\":\"INT\",\"format\":\"1:DAYS:SIMPLE_DATE_FORMAT\",\"granularity\":\"1:DAYS\"}]}");
    checkValidationFails(pinotSchema);

    pinotSchema = Schema.fromString(
        "{\"schemaName\":\"testSchema\"," + "\"dimensionFieldSpecs\":[ {\"name\":\"dim1\",\"dataType\":\"STRING\"}],"
            + "\"dateTimeFieldSpecs\":[{\"name\":\"dt1\",\"dataType\":\"INT\",\"format\":\"1:HOURS:EPOCH\",\"granularity\":\"1:HOURS\"}]}");
    SchemaUtils.validate(pinotSchema);

    pinotSchema = Schema.fromString(
        "{\"schemaName\":\"testSchema\"," + "\"dimensionFieldSpecs\":[ {\"name\":\"dim1\",\"dataType\":\"STRING\"}],"
            + "\"dateTimeFieldSpecs\":[{\"name\":\"dt1\",\"dataType\":\"INT\",\"format\":\"1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd\",\"granularity\":\"1:DAYS\"}]}");
    SchemaUtils.validate(pinotSchema);
  }

  private void checkValidationFails(Schema pinotSchema) {
    try {
      SchemaUtils.validate(pinotSchema);
      Assert.fail("Schema validation should have failed.");
    } catch (IllegalStateException e) {
      // expected
    }
  }
}
