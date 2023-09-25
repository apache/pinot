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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.segment.local.utils.SchemaUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertThrows;


/**
 * Tests schema validations
 */
public class SchemaUtilsTest {
  private static final String TABLE_NAME = "testTable";
  private static final String TIME_COLUMN = "timeColumn";

  @Test
  public void testCompatibilityWithTableConfig() {
    // empty list
    List<TableConfig> tableConfigs = new ArrayList<>();
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).build();
    SchemaUtils.validate(schema, tableConfigs);

    // offline table
    // null timeColumnName
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    SchemaUtils.validate(schema, Lists.newArrayList(tableConfig));

    // schema doesn't have timeColumnName
    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTimeColumnName(TIME_COLUMN).build();
    try {
      SchemaUtils.validate(schema, Lists.newArrayList(tableConfig));
      Assert.fail("Should fail schema validation, as timeColumn is absent");
    } catch (IllegalStateException e) {
      // expected
    }

    // schema doesn't have timeColumnName as time spec
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addSingleValueDimension(TIME_COLUMN, DataType.STRING)
        .build();
    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTimeColumnName(TIME_COLUMN).build();
    try {
      SchemaUtils.validate(schema, Lists.newArrayList(tableConfig));
      Assert.fail("Should fail schema validation, as timeColumn is not present as time spec");
    } catch (IllegalStateException e) {
      // expected
    }

    // schema has timeColumnName
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addDateTime(TIME_COLUMN, DataType.LONG, "1:MILLISECONDS:EPOCH", "1:HOURS").build();
    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTimeColumnName(TIME_COLUMN).build();
    SchemaUtils.validate(schema, Lists.newArrayList(tableConfig));

    // schema doesn't have destination columns from transformConfigs
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).build();
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(Collections.singletonList(new TransformConfig("colA", "round(colB, 1000)")));
    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setIngestionConfig(ingestionConfig).build();
    try {
      SchemaUtils.validate(schema, Lists.newArrayList(tableConfig));
      Assert.fail("Should fail schema validation, as colA is not present in schema");
    } catch (IllegalStateException e) {
      // expected
    }

    schema =
        new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addSingleValueDimension("colA", DataType.STRING).build();
    SchemaUtils.validate(schema, Lists.newArrayList(tableConfig));

    // realtime table
    // schema doesn't have timeColumnName
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).build();
    tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).setTimeColumnName(TIME_COLUMN).build();
    try {
      SchemaUtils.validate(schema, Lists.newArrayList(tableConfig));
      Assert.fail("Should fail schema validation, as timeColumn is absent");
    } catch (IllegalStateException e) {
      // expected
    }

    // schema doesn't have timeColumnName as time spec
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addSingleValueDimension(TIME_COLUMN, DataType.STRING)
        .build();
    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTimeColumnName(TIME_COLUMN).build();
    try {
      SchemaUtils.validate(schema, Lists.newArrayList(tableConfig));
      Assert.fail("Should fail schema validation, as timeColumn is not present as time spec");
    } catch (IllegalStateException e) {
      // expected
    }

    // schema has timeColumnName
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addDateTime(TIME_COLUMN, DataType.LONG, "1:MILLISECONDS:EPOCH", "1:HOURS").build();
    tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
            .setStreamConfigs(getStreamConfigs())
            .setTimeColumnName(TIME_COLUMN).build();
    SchemaUtils.validate(schema, Lists.newArrayList(tableConfig));

    // schema doesn't have destination columns from transformConfigs
    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addDateTime(TIME_COLUMN, DataType.LONG, "1:MILLISECONDS:EPOCH", "1:HOURS").build();
    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setStreamConfigs(getStreamConfigs())
        .setTimeColumnName(TIME_COLUMN)
        .setIngestionConfig(ingestionConfig).build();
    try {
      SchemaUtils.validate(schema, Lists.newArrayList(tableConfig));
      Assert.fail("Should fail schema validation, as colA is not present in schema");
    } catch (IllegalStateException e) {
      // expected
    }

    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addDateTime(TIME_COLUMN, DataType.LONG, "1:MILLISECONDS:EPOCH", "1:HOURS")
        .addSingleValueDimension("colA", DataType.STRING).build();
    SchemaUtils.validate(schema, Lists.newArrayList(tableConfig));

    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
            .addMetric("double", DataType.DOUBLE, "NaN").build();
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME) .build();
    try {
      SchemaUtils.validate(schema, Lists.newArrayList(tableConfig));
      Assert.fail("Should fail schema validation, as double has NaN default value");
    } catch (IllegalStateException e) {
      // expected
      Assert.assertTrue(e.getMessage().startsWith("NaN as null default value is not managed yet for"));
    }

    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
            .addMetric("float", DataType.FLOAT, "NaN").build();
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME) .build();
    try {
      SchemaUtils.validate(schema, Lists.newArrayList(tableConfig));
      Assert.fail("Should fail schema validation, as float has NaN default value");
    } catch (IllegalStateException e) {
      // expected
      Assert.assertTrue(e.getMessage().startsWith("NaN as null default value is not managed yet for"));
    }

    schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
            .addSingleValueDimension("string", DataType.STRING, "NaN").build();
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME) .build();
    SchemaUtils.validate(schema, Lists.newArrayList(tableConfig));
  }

  private Map<String, String> getStreamConfigs() {
    Map<String, String> streamConfigs = new HashMap<>();
    streamConfigs.put("streamType", "kafka");
    streamConfigs.put("stream.kafka.consumer.type", "lowlevel");
    streamConfigs.put("stream.kafka.topic.name", "test");
    streamConfigs.put("stream.kafka.decoder.class.name",
        "org.apache.pinot.plugin.stream.kafka.KafkaJSONMessageDecoder");
    return streamConfigs;
  }

  /**
   * TODO: transform functions have moved to tableConfig#ingestionConfig. However, these tests remain to test
   * backward compatibility/
   *  Remove these when we totally stop honoring transform functions in schema
   */
  @Test
  public void testValidateTransformFunctionArguments() {
    Schema pinotSchema;
    // source name used as destination name
    pinotSchema = new Schema();
    DimensionFieldSpec dimensionFieldSpec = new DimensionFieldSpec("dim1", DataType.STRING, true);
    dimensionFieldSpec.setTransformFunction("Groovy({function}, argument1, dim1, argument3)");
    pinotSchema.addField(dimensionFieldSpec);
    try {
      SchemaUtils.validate(pinotSchema);
      Assert.fail("Schema validation should have failed.");
    } catch (IllegalStateException e) {
      // expected
    }

    pinotSchema = new Schema();
    MetricFieldSpec metricFieldSpec = new MetricFieldSpec("m1", DataType.LONG);
    metricFieldSpec.setTransformFunction("Groovy({function}, m1, m1)");
    pinotSchema.addField(metricFieldSpec);
    checkValidationFails(pinotSchema);

    pinotSchema = new Schema();
    DateTimeFieldSpec dateTimeFieldSpec = new DateTimeFieldSpec("dt1", DataType.LONG, "1:HOURS:EPOCH", "1:HOURS");
    dateTimeFieldSpec.setTransformFunction("Groovy({function}, m1, dt1)");
    pinotSchema.addField(dateTimeFieldSpec);
    checkValidationFails(pinotSchema);

    pinotSchema =
        new Schema.SchemaBuilder().addTime(new TimeGranularitySpec(DataType.LONG, TimeUnit.MILLISECONDS, "time"), null)
            .build();
    pinotSchema.getFieldSpecFor("time").setTransformFunction("Groovy({function}, time)");
    checkValidationFails(pinotSchema);

    // derived transformations
    pinotSchema =
        new Schema.SchemaBuilder().addSingleValueDimension("x", DataType.INT).addSingleValueDimension("z", DataType.INT)
            .build();
    pinotSchema.getFieldSpecFor("x").setTransformFunction("Groovy({y + 10}, y)");
    pinotSchema.getFieldSpecFor("z").setTransformFunction("Groovy({x*w*20}, x, w)");
    checkValidationFails(pinotSchema);
  }

  @Test
  public void testValidateTimeFieldSpec() {
    Schema pinotSchema;
    // time field spec using same name for incoming and outgoing
    pinotSchema =
        new Schema.SchemaBuilder().addTime(new TimeGranularitySpec(DataType.LONG, TimeUnit.MILLISECONDS, "time"),
            new TimeGranularitySpec(DataType.INT, TimeUnit.DAYS, "time")).build();
    checkValidationFails(pinotSchema);

    // time field spec using SIMPLE_DATE_FORMAT, not allowed when conversion is needed
    pinotSchema =
        new Schema.SchemaBuilder().addTime(new TimeGranularitySpec(DataType.LONG, TimeUnit.MILLISECONDS, "incoming"),
            new TimeGranularitySpec(DataType.INT, TimeUnit.DAYS,
                TimeGranularitySpec.TimeFormat.SIMPLE_DATE_FORMAT.toString(), "outgoing")).build();
    checkValidationFails(pinotSchema);

    // valid time field spec
    pinotSchema =
        new Schema.SchemaBuilder().addTime(new TimeGranularitySpec(DataType.LONG, TimeUnit.MILLISECONDS, "incoming"),
            new TimeGranularitySpec(DataType.INT, TimeUnit.DAYS, "outgoing")).build();
    SchemaUtils.validate(pinotSchema);
  }

  @Test
  public void testValidateDateTimeFieldSpec() {
    Schema pinotSchema;
    // valid date time.
    pinotSchema = new Schema.SchemaBuilder().addDateTime("datetime1", FieldSpec.DataType.STRING,
            "1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd", "1:DAYS")
        .addDateTime("datetime2", FieldSpec.DataType.STRING, "1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-ww-dd", "1:DAYS")
        .build();
    SchemaUtils.validate(pinotSchema);

    // date time field spec using SIMPLE_DATE_FORMAT needs to be valid.
    assertThrows(IllegalArgumentException.class,
        () -> new Schema.SchemaBuilder().addDateTime("datetime3", FieldSpec.DataType.STRING,
            "1:DAYS:SIMPLE_DATE_FORMAT:foo_bar", "1:DAYS").build());

    // date time field spec using SIMPLE_DATE_FORMAT needs to be lexicographical order.
    pinotSchema = new Schema.SchemaBuilder().addDateTime("datetime4", FieldSpec.DataType.STRING,
        "1:DAYS:SIMPLE_DATE_FORMAT:M/d/yyyy", "1:DAYS").build();
    checkValidationFails(pinotSchema);
  }

  @Test
  public void testValidateCaseInsensitive() {
    Schema pinotSchema;
    pinotSchema =
      new Schema.SchemaBuilder().addTime(new TimeGranularitySpec(DataType.LONG, TimeUnit.MILLISECONDS, "incoming"),
          new TimeGranularitySpec(DataType.INT, TimeUnit.DAYS, "outgoing"))
        .addSingleValueDimension("dim1", DataType.INT)
        .addSingleValueDimension("Dim1", DataType.INT)
        .build();

    checkValidationFails(pinotSchema, true);
  }

  @Test
  public void testValidatePrimaryKeyColumns() {
    Schema pinotSchema;
    // non-existing column used as primary key
    pinotSchema =
        new Schema.SchemaBuilder().addTime(new TimeGranularitySpec(DataType.LONG, TimeUnit.MILLISECONDS, "incoming"),
                new TimeGranularitySpec(DataType.INT, TimeUnit.DAYS, "outgoing"))
            .addSingleValueDimension("col", DataType.INT).setPrimaryKeyColumns(Lists.newArrayList("test")).build();
    checkValidationFails(pinotSchema);

    // valid primary key
    pinotSchema =
        new Schema.SchemaBuilder().addTime(new TimeGranularitySpec(DataType.LONG, TimeUnit.MILLISECONDS, "incoming"),
                new TimeGranularitySpec(DataType.INT, TimeUnit.DAYS, "outgoing"))
            .addSingleValueDimension("col", DataType.INT).setPrimaryKeyColumns(Lists.newArrayList("col")).build();
    SchemaUtils.validate(pinotSchema);
  }

  @Test
  public void testGroovyFunctionSyntax() {
    Schema pinotSchema;
    // incorrect groovy function syntax
    pinotSchema = new Schema();

    DimensionFieldSpec dimensionFieldSpec = new DimensionFieldSpec("dim1", DataType.STRING, true);
    dimensionFieldSpec.setTransformFunction("Groovy(function, argument3)");
    pinotSchema.addField(dimensionFieldSpec);
    checkValidationFails(pinotSchema);

    // valid schema, empty arguments
    pinotSchema = new Schema();

    dimensionFieldSpec = new DimensionFieldSpec("dim1", DataType.STRING, true);
    dimensionFieldSpec.setTransformFunction("Groovy({function})");
    pinotSchema.addField(dimensionFieldSpec);
    SchemaUtils.validate(pinotSchema);

    // valid schema
    pinotSchema =
        new Schema.SchemaBuilder().addSingleValueDimension("dim1", DataType.STRING).addMetric("m1", DataType.LONG)
            .addTime(new TimeGranularitySpec(DataType.LONG, TimeUnit.MILLISECONDS, "time"), null).build();
    pinotSchema.getFieldSpecFor("dim1").setTransformFunction("Groovy({function}, argument1, argument2, argument3)");
    pinotSchema.getFieldSpecFor("m1").setTransformFunction("Groovy({function}, m2, m3)");
    pinotSchema.getFieldSpecFor("time").setTransformFunction("Groovy({function}, millis)");
    SchemaUtils.validate(pinotSchema);
  }

  @Test
  public void testDateTimeFieldSpec()
      throws IOException {
    Schema schema = Schema.fromString(
        "{\"schemaName\":\"testSchema\"," + "\"dimensionFieldSpecs\":[ {\"name\":\"dim1\",\"dataType\":\"STRING\"}],"
            + "\"dateTimeFieldSpecs\":[{\"name\":\"dt1\",\"dataType\":\"INT\",\"format\":\"x:HOURS:EPOCH\","
            + "\"granularity\":\"1:HOURS\"}]}");
    checkValidationFails(schema);

    schema = Schema.fromString(
        "{\"schemaName\":\"testSchema\"," + "\"dimensionFieldSpecs\":[ {\"name\":\"dim1\",\"dataType\":\"STRING\"}],"
            + "\"dateTimeFieldSpecs\":[{\"name\":\"dt1\",\"dataType\":\"INT\",\"format\":\"1:DUMMY:EPOCH\","
            + "\"granularity\":\"1:HOURS\"}]}");
    checkValidationFails(schema);

    schema = Schema.fromString(
        "{\"schemaName\":\"testSchema\"," + "\"dimensionFieldSpecs\":[ {\"name\":\"dim1\",\"dataType\":\"STRING\"}],"
            + "\"dateTimeFieldSpecs\":[{\"name\":\"dt1\",\"dataType\":\"INT\",\"format\":\"1:HOURS:DUMMY\","
            + "\"granularity\":\"1:HOURS\"}]}");
    checkValidationFails(schema);

    schema = Schema.fromString(
        "{\"schemaName\":\"testSchema\"," + "\"dimensionFieldSpecs\":[ {\"name\":\"dim1\",\"dataType\":\"STRING\"}],"
            + "\"dateTimeFieldSpecs\":[{\"name\":\"dt1\",\"dataType\":\"INT\",\"format\":\"1:HOURS:EPOCH\","
            + "\"granularity\":\"x:HOURS\"}]}");
    checkValidationFails(schema);

    schema = Schema.fromString(
        "{\"schemaName\":\"testSchema\"," + "\"dimensionFieldSpecs\":[ {\"name\":\"dim1\",\"dataType\":\"STRING\"}],"
            + "\"dateTimeFieldSpecs\":[{\"name\":\"dt1\",\"dataType\":\"INT\",\"format\":\"1:HOURS:EPOCH\","
            + "\"granularity\":\"1:DUMMY\"}]}");
    checkValidationFails(schema);

    schema = Schema.fromString(
        "{\"schemaName\":\"testSchema\"," + "\"dimensionFieldSpecs\":[ {\"name\":\"dim1\",\"dataType\":\"STRING\"}],"
            + "\"dateTimeFieldSpecs\":[{\"name\":\"dt1\",\"dataType\":\"INT\","
            + "\"format\":\"1:DAYS:SIMPLE_DATE_FORMAT\",\"granularity\":\"1:DAYS\"}]}");
    SchemaUtils.validate(schema);

    schema = Schema.fromString(
        "{\"schemaName\":\"testSchema\"," + "\"dimensionFieldSpecs\":[ {\"name\":\"dim1\",\"dataType\":\"STRING\"}],"
            + "\"dateTimeFieldSpecs\":[{\"name\":\"dt1\",\"dataType\":\"INT\","
            + "\"format\":\"1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd\",\"granularity\":\"1:DAYS\","
            + " \"sampleValue\" : \"19700101\"}]}");
    checkValidationFails(schema);

    schema = Schema.fromString(
        "{\"schemaName\":\"testSchema\"," + "\"dimensionFieldSpecs\":[ {\"name\":\"dim1\",\"dataType\":\"STRING\"}],"
            + "\"dateTimeFieldSpecs\":[{\"name\":\"dt1\",\"dataType\":\"INT\","
            + "\"format\":\"1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd\",\"granularity\":\"1:DAYS\","
            + " \"sampleValue\" : \"19720101\"}]}");
    SchemaUtils.validate(schema);

    schema = Schema.fromString(
        "{\"schemaName\":\"testSchema\"," + "\"dimensionFieldSpecs\":[ {\"name\":\"dim1\",\"dataType\":\"STRING\"}],"
            + "\"dateTimeFieldSpecs\":[{\"name\":\"dt1\",\"dataType\":\"LONG\","
            + "\"format\":\"1:SECONDS:EPOCH\",\"granularity\":\"1:SECONDS\", \"sampleValue\" : 1663170923}]}");
    SchemaUtils.validate(schema);

    schema = Schema.fromString(
        "{\"schemaName\":\"testSchema\"," + "\"dimensionFieldSpecs\":[ {\"name\":\"dim1\",\"dataType\":\"STRING\"}],"
            + "\"dateTimeFieldSpecs\":[{\"name\":\"dt1\",\"dataType\":\"LONG\","
            + "\"format\":\"1:MILLISECONDS:EPOCH\",\"granularity\":\"1:MILLISECONDS\","
            + " \"sampleValue\" : 1663170923}]}");
    checkValidationFails(schema);

    schema = Schema.fromString(
        "{\"schemaName\":\"testSchema\"," + "\"dimensionFieldSpecs\":[ {\"name\":\"dim1\",\"dataType\":\"STRING\"}],"
            + "\"dateTimeFieldSpecs\":[{\"name\":\"dt1\",\"dataType\":\"INT\",\"format\":\"1:HOURS:EPOCH\","
            + "\"granularity\":\"1:HOURS\"}]}");
    SchemaUtils.validate(schema);

    schema = Schema.fromString(
        "{\"schemaName\":\"testSchema\"," + "\"dimensionFieldSpecs\":[ {\"name\":\"dim1\",\"dataType\":\"STRING\"}],"
            + "\"dateTimeFieldSpecs\":[{\"name\":\"dt1\",\"dataType\":\"INT\","
            + "\"format\":\"1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd\",\"granularity\":\"1:DAYS\"}]}");
    SchemaUtils.validate(schema);
  }

  /**
   * Testcases for testing column name validation logic.
   * Currently column name validation only checks no blank space in column names. Should we add more validation on
   * column
   * names later on, we can corresponding tests here.
   */
  @Test
  public void testColumnNameValidation()
      throws IOException {
    Schema pinotSchema;
    // A schema all column names does not contains blank space should pass the validation.
    pinotSchema = Schema.fromString(
        "{\"schemaName\":\"testSchema\"," + "\"dimensionFieldSpecs\":[ {\"name\":\"dim1\",\"dataType\":\"STRING\"}],"
            + "\"dateTimeFieldSpecs\":[{\"name\":\"dt1\",\"dataType\":\"INT\","
            + "\"format\":\"1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd\",\"granularity\":\"1:DAYS\"}]}");
    SchemaUtils.validate(pinotSchema);
    // Validation will fail if dimensionFieldSpecs column name contain blank space.
    pinotSchema = Schema.fromString(
        "{\"schemaName\":\"testSchema\"," + "\"dimensionFieldSpecs\":[ {\"name\":\"dim 1\",\"dataType\":\"STRING\"}],"
            + "\"dateTimeFieldSpecs\":[{\"name\":\"dt1\",\"dataType\":\"INT\","
            + "\"format\":\"1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd\",\"granularity\":\"1:DAYS\"}]}");
    checkValidationFails(pinotSchema);
    // Validation will fail if dateTimeFieldSpecs column name contain blank space.
    pinotSchema = Schema.fromString(
        "{\"schemaName\":\"testSchema\"," + "\"dimensionFieldSpecs\":[ {\"name\":\"dim1\",\"dataType\":\"STRING\"}],"
            + "\"dateTimeFieldSpecs\":[{\"name\":\"dt 1\",\"dataType\":\"INT\","
            + "\"format\":\"1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd\",\"granularity\":\"1:DAYS\"}]}");
    checkValidationFails(pinotSchema);
    // Test case for column name has leading blank space.
    pinotSchema = Schema.fromString(
        "{\"schemaName\":\"testSchema\"," + "\"dimensionFieldSpecs\":[ {\"name\":\" dim1\",\"dataType\":\"STRING\"}],"
            + "\"dateTimeFieldSpecs\":[{\"name\":\"dt1\",\"dataType\":\"INT\","
            + "\"format\":\"1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd\",\"granularity\":\"1:DAYS\"}]}");
    checkValidationFails(pinotSchema);
    // Test case for column name has trailing blank space.
    pinotSchema = Schema.fromString(
        "{\"schemaName\":\"testSchema\"," + "\"dimensionFieldSpecs\":[ {\"name\":\"dim1\",\"dataType\":\"STRING\"}],"
            + "\"dateTimeFieldSpecs\":[{\"name\":\"dt1  \",\"dataType\":\"INT\","
            + "\"format\":\"1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd\",\"granularity\":\"1:DAYS\"}]}");
    checkValidationFails(pinotSchema);
  }

  private void checkValidationFails(Schema pinotSchema, boolean isIgnoreCase) {
    try {
      SchemaUtils.validate(pinotSchema, isIgnoreCase);
      Assert.fail("Schema validation should have failed.");
    } catch (IllegalArgumentException | IllegalStateException e) {
      // expected
    }
  }

  private void checkValidationFails(Schema pinotSchema) {
    checkValidationFails(pinotSchema, false);
  }
}
