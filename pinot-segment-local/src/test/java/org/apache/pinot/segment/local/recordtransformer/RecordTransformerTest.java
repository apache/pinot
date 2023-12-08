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
package org.apache.pinot.segment.local.recordtransformer;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.FilterConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.SchemaConformingTransformerConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class RecordTransformerTest {
  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      // For data type conversion
      .addSingleValueDimension("svInt", DataType.INT).addSingleValueDimension("svLong", DataType.LONG)
      .addSingleValueDimension("svFloat", DataType.FLOAT).addSingleValueDimension("svDouble", DataType.DOUBLE)
      .addSingleValueDimension("svBoolean", DataType.BOOLEAN).addSingleValueDimension("svTimestamp", DataType.TIMESTAMP)
      .addSingleValueDimension("svBytes", DataType.BYTES).addMultiValueDimension("mvInt", DataType.INT)
      .addSingleValueDimension("svJson", DataType.JSON).addMultiValueDimension("mvLong", DataType.LONG)
      .addMultiValueDimension("mvFloat", DataType.FLOAT).addMultiValueDimension("mvDouble", DataType.DOUBLE)
      // For sanitation
      .addSingleValueDimension("svStringWithNullCharacters", DataType.STRING)
      .addSingleValueDimension("svStringWithLengthLimit", DataType.STRING)
      .addMultiValueDimension("mvString1", DataType.STRING).addMultiValueDimension("mvString2", DataType.STRING)
      // For negative zero and NaN conversions
      .addSingleValueDimension("svFloatNegativeZero", DataType.FLOAT)
      .addMultiValueDimension("mvFloatNegativeZero", DataType.FLOAT)
      .addSingleValueDimension("svDoubleNegativeZero", DataType.DOUBLE)
      .addMultiValueDimension("mvDoubleNegativeZero", DataType.DOUBLE)
      .addSingleValueDimension("svFloatNaN", DataType.FLOAT).addMultiValueDimension("mvFloatNaN", DataType.FLOAT)
      .addSingleValueDimension("svDoubleNaN", DataType.DOUBLE).addMultiValueDimension("mvDoubleNaN", DataType.DOUBLE)
      .build();
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();

  static {
    SCHEMA.getFieldSpecFor("svStringWithLengthLimit").setMaxLength(2);
    SCHEMA.addField(new DimensionFieldSpec("$virtual", DataType.STRING, true, Object.class));
  }

  // Transform multiple times should return the same result
  private static final int NUM_ROUNDS = 5;
  private static final int NUMBER_OF_TRANSFORMERS = 8;

  private static GenericRow getRecord() {
    GenericRow record = new GenericRow();
    record.putValue("svInt", (byte) 123);
    record.putValue("svLong", (char) 123);
    record.putValue("svFloat", Collections.singletonList((short) 123));
    record.putValue("svDouble", new String[]{"123"});
    record.putValue("svBoolean", "true");
    record.putValue("svTimestamp", "2020-02-02 22:22:22.222");
    record.putValue("svBytes", "7b7b"/*new byte[]{123, 123}*/);
    record.putValue("svJson", "{\"first\": \"daffy\", \"last\": \"duck\"}");
    record.putValue("mvInt", new Object[]{123L});
    record.putValue("mvLong", Collections.singletonList(123f));
    record.putValue("mvFloat", new Double[]{123d});
    record.putValue("mvDouble", Collections.singletonMap("key", 123));
    record.putValue("svStringWithNullCharacters", "1\0002\0003");
    record.putValue("svStringWithLengthLimit", "123");
    record.putValue("mvString1", new Object[]{"123", 123, 123L, 123f, 123.0});
    record.putValue("mvString2", new Object[]{123, 123L, 123f, 123.0, "123"});
    record.putValue("svNullString", null);
    record.putValue("svFloatNegativeZero", -0.00f);
    record.putValue("svDoubleNegativeZero", -0.00d);
    record.putValue("mvFloatNegativeZero", new Float[]{-0.0f, 1.0f, 0.0f, 3.0f});
    record.putValue("mvDoubleNegativeZero", new Double[]{-0.0d, 1.0d, 0.0d, 3.0d});
    record.putValue("svFloatNaN", Float.NaN);
    record.putValue("svDoubleNaN", Double.NaN);
    record.putValue("mvFloatNaN", new Float[]{-0.0f, Float.NaN, 2.0f});
    record.putValue("mvDoubleNaN", new Double[]{-0.0d, Double.NaN, 2.0d});
    return record;
  }

  @Test
  public void testFilterTransformer() {
    IngestionConfig ingestionConfig = new IngestionConfig();
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setIngestionConfig(ingestionConfig).build();

    // expression false, not filtered
    ingestionConfig.setFilterConfig(new FilterConfig("Groovy({svInt > 123}, svInt)"));
    GenericRow genericRow = getRecord();
    tableConfig.setIngestionConfig(ingestionConfig);
    RecordTransformer transformer = new FilterTransformer(tableConfig);
    transformer.transform(genericRow);
    Assert.assertFalse(genericRow.getFieldToValueMap().containsKey(GenericRow.SKIP_RECORD_KEY));

    // expression true, filtered
    ingestionConfig.setFilterConfig(new FilterConfig("Groovy({svInt <= 123}, svInt)"));
    genericRow = getRecord();
    transformer = new FilterTransformer(tableConfig);
    transformer.transform(genericRow);
    Assert.assertTrue(genericRow.getFieldToValueMap().containsKey(GenericRow.SKIP_RECORD_KEY));

    // value not found
    ingestionConfig.setFilterConfig(new FilterConfig("Groovy({notPresent == 123}, notPresent)"));
    genericRow = getRecord();
    transformer = new FilterTransformer(tableConfig);
    transformer.transform(genericRow);
    Assert.assertFalse(genericRow.getFieldToValueMap().containsKey(GenericRow.SKIP_RECORD_KEY));

    // invalid function
    ingestionConfig.setFilterConfig(new FilterConfig("Groovy(svInt == 123)"));
    try {
      new FilterTransformer(tableConfig);
      Assert.fail("Should have failed constructing FilterTransformer");
    } catch (Exception e) {
      // expected
    }

    // multi value column
    ingestionConfig.setFilterConfig(new FilterConfig("Groovy({svFloat.max() < 500}, svFloat)"));
    genericRow = getRecord();
    transformer = new FilterTransformer(tableConfig);
    transformer.transform(genericRow);
    Assert.assertTrue(genericRow.getFieldToValueMap().containsKey(GenericRow.SKIP_RECORD_KEY));
  }

  @Test
  public void testDataTypeTransformer() {
    RecordTransformer transformer = new DataTypeTransformer(TABLE_CONFIG, SCHEMA);
    GenericRow record = getRecord();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      record = transformer.transform(record);
      assertNotNull(record);
      assertEquals(record.getValue("svInt"), 123);
      assertEquals(record.getValue("svLong"), 123L);
      assertEquals(record.getValue("svFloat"), 123f);
      assertEquals(record.getValue("svDouble"), 123d);
      assertEquals(record.getValue("svBoolean"), 1);
      assertEquals(record.getValue("svTimestamp"), Timestamp.valueOf("2020-02-02 22:22:22.222").getTime());
      assertEquals(record.getValue("svBytes"), new byte[]{123, 123});
      assertEquals(record.getValue("svJson"), "{\"first\":\"daffy\",\"last\":\"duck\"}");
      assertEquals(record.getValue("mvInt"), new Object[]{123});
      assertEquals(record.getValue("mvLong"), new Object[]{123L});
      assertEquals(record.getValue("mvFloat"), new Object[]{123f});
      assertEquals(record.getValue("mvDouble"), new Object[]{123d});
      assertEquals(record.getValue("svStringWithNullCharacters"), "1\0002\0003");
      assertEquals(record.getValue("svStringWithLengthLimit"), "123");
      // NOTE: We used to speculate the array type by the first element, but has changed
      // to convert type for values in array based on their real value, making the logic
      // safe to handle array of values with mixing types.
      assertEquals(record.getValue("mvString1"), new Object[]{"123", "123", "123", "123.0", "123.0"});
      assertEquals(record.getValue("mvString2"), new Object[]{"123", "123", "123.0", "123.0", "123"});
      assertNull(record.getValue("$virtual"));
      assertTrue(record.getNullValueFields().isEmpty());
    }
  }

  @Test
  public void testDataTypeTransformerIncorrectDataTypes() {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("svInt", DataType.BYTES)
        .addSingleValueDimension("svLong", DataType.LONG).build();

    RecordTransformer transformer = new DataTypeTransformer(TABLE_CONFIG, schema);
    GenericRow record = getRecord();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      assertThrows(() -> transformer.transform(record));
    }

    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setContinueOnError(true);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setIngestionConfig(ingestionConfig).setTableName("testTable").build();

    RecordTransformer transformerWithDefaultNulls = new DataTypeTransformer(tableConfig, schema);
    GenericRow record1 = getRecord();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      record1 = transformerWithDefaultNulls.transform(record1);
      assertNotNull(record1);
      assertNull(record1.getValue("svInt"));
    }
  }

  @Test
  public void testTimeValidationTransformer() {
    // Invalid timestamp, validation disabled
    String timeCol = "timeCol";
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setTimeColumnName(timeCol).build();
    Schema schema = new Schema.SchemaBuilder().addDateTime(timeCol, DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP",
        "1:MILLISECONDS").build();
    RecordTransformer transformer = new TimeValidationTransformer(tableConfig, schema);
    GenericRow record = getRecord();
    record.putValue(timeCol, 1L);
    for (int i = 0; i < NUM_ROUNDS; i++) {
      record = transformer.transform(record);
      assertNotNull(record);
      assertEquals(record.getValue(timeCol), 1L);
    }

    // Invalid timestamp, validation enabled
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setRowTimeValueCheck(true);
    tableConfig.setIngestionConfig(ingestionConfig);
    RecordTransformer transformerWithValidation = new TimeValidationTransformer(tableConfig, schema);
    GenericRow record1 = getRecord();
    record1.putValue(timeCol, 1L);
    for (int i = 0; i < NUM_ROUNDS; i++) {
      assertThrows(() -> transformerWithValidation.transform(record1));
    }

    // Invalid timestamp, validation enabled and ignoreErrors enabled
    ingestionConfig.setContinueOnError(true);
    transformer = new TimeValidationTransformer(tableConfig, schema);
    GenericRow record2 = getRecord();
    record2.putValue(timeCol, 1L);
    for (int i = 0; i < NUM_ROUNDS; i++) {
      record2 = transformer.transform(record2);
      assertNotNull(record2);
      assertNull(record2.getValue(timeCol));
    }

    // Valid timestamp, validation enabled
    ingestionConfig.setContinueOnError(false);
    transformer = new TimeValidationTransformer(tableConfig, schema);
    GenericRow record3 = getRecord();
    Long currentTimeMillis = System.currentTimeMillis();
    record3.putValue(timeCol, currentTimeMillis);
    for (int i = 0; i < NUM_ROUNDS; i++) {
      record3 = transformer.transform(record3);
      assertNotNull(record3);
      assertEquals(record3.getValue(timeCol), currentTimeMillis);
    }
  }

  @Test
  public void testSanitationTransformer() {
    RecordTransformer transformer = new SanitizationTransformer(SCHEMA);
    GenericRow record = getRecord();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      record = transformer.transform(record);
      assertNotNull(record);
      assertEquals(record.getValue("svStringWithNullCharacters"), "1");
      assertEquals(record.getValue("svStringWithLengthLimit"), "12");
      assertEquals(record.getValue("mvString1"), new Object[]{"123", "123", "123", "123.0", "123.0"});
      assertEquals(record.getValue("mvString2"), new Object[]{"123", "123", "123.0", "123.0", "123"});
      assertNull(record.getValue("$virtual"));
      assertTrue(record.getNullValueFields().isEmpty());
    }
  }

  @Test
  public void testSpecialValueTransformer() {
    SpecialValueTransformer transformer = new SpecialValueTransformer(SCHEMA);
    GenericRow record = getRecord();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      record = transformer.transform(record);
      assertNotNull(record);
      assertEquals(Float.floatToRawIntBits((float) record.getValue("svFloatNegativeZero")),
          Float.floatToRawIntBits(0.0f));
      assertEquals(Double.doubleToRawLongBits((double) record.getValue("svDoubleNegativeZero")),
          Double.doubleToRawLongBits(0.0d));
      assertEquals(record.getValue("mvFloatNegativeZero"), new Float[]{0.0f, 1.0f, 0.0f, 3.0f});
      assertEquals(record.getValue("mvDoubleNegativeZero"), new Double[]{0.0d, 1.0d, 0.0d, 3.0d});
      assertNull(record.getValue("svFloatNaN"));
      assertNull(record.getValue("svDoubleNaN"));
      assertEquals(record.getValue("mvFloatNaN"),
          new Float[]{0.0f, 2.0f});
      assertEquals(record.getValue("mvDoubleNaN"),
          new Double[]{0.0d, 2.0d});
      assertEquals(transformer.getNegativeZeroConversionCount(),6);
      assertEquals(transformer.getNanConversionCount(), 4);
    }
  }

  @Test
  public void testOrderForTransformers() {
    // This test checks that the specified order is maintained for different transformers.

    // Build Schema and ingestionConfig in such a way that all the transformers are loaded.
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("svInt", DataType.INT)
        .addSingleValueDimension("svDouble", DataType.DOUBLE)
        .addSingleValueDimension("expressionTestColumn", DataType.INT)
        .addSingleValueDimension("svNaN", DataType.FLOAT).addMultiValueDimension("mvNaN", DataType.FLOAT)
        .addSingleValueDimension("emptyDimensionForNullValueTransformer", DataType.FLOAT)
        .addSingleValueDimension("svStringNull", DataType.STRING)
        .addSingleValueDimension("indexableExtras", DataType.JSON)
        .addDateTime("timeCol", DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS").build();

    IngestionConfig ingestionConfig = new IngestionConfig();
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setIngestionConfig(ingestionConfig)
            .setTimeColumnName("timeCol").build();
    ingestionConfig.setFilterConfig(new FilterConfig("svInt = 123 AND svDouble <= 200"));
    ingestionConfig.setTransformConfigs(List.of(new TransformConfig("expressionTestColumn", "plus(x,10)")));
    ingestionConfig.setSchemaConformingTransformerConfig(
        new SchemaConformingTransformerConfig("indexableExtras", null, null, null));
    ingestionConfig.setRowTimeValueCheck(true);
    ingestionConfig.setContinueOnError(false);

    // Get the list of transformers.
    List<RecordTransformer> currentListOfTransformers =
        CompositeTransformer.getDefaultTransformers(tableConfig, schema);

    // Create a list of transformers in the original order to compare.
    List<RecordTransformer> expectedListOfTransformers =
        List.of(new ExpressionTransformer(tableConfig, schema), new FilterTransformer(tableConfig),
            new SchemaConformingTransformer(tableConfig, schema), new DataTypeTransformer(tableConfig, schema),
            new TimeValidationTransformer(tableConfig, schema), new SpecialValueTransformer(schema),
            new NullValueTransformer(tableConfig, schema), new SanitizationTransformer(schema));

    // Check that the number of current transformers match the expected number of transformers.
    assertEquals(currentListOfTransformers.size(), NUMBER_OF_TRANSFORMERS);

    GenericRow record = new GenericRow();

    // Data for expression Transformer.
    record.putValue("expressionTestColumn", 100);

    // Data for filter transformer.
    record.putValue("svDouble", 123d);

    // Data for DataType Transformer.
    record.putValue("svInt", (byte) 123);

    // Data for TimeValidation transformer.
    record.putValue("timeCol", System.currentTimeMillis());

    // Data for SpecialValue Transformer.
    record.putValue("svNaN", Float.NaN);
    record.putValue("mvNaN", new Float[]{1.0f, Float.NaN, 2.0f});

    // Data for sanitization transformer.
    record.putValue("svStringNull", null);

    for (int i = 0; i < NUMBER_OF_TRANSFORMERS; i++) {
      GenericRow copyRecord = record.copy();
      GenericRow currentRecord = currentListOfTransformers.get(i).transform(record);
      GenericRow expectedRecord = expectedListOfTransformers.get(i).transform(copyRecord);
      assertEquals(currentRecord, expectedRecord);
      record = expectedRecord;
    }
  }

  @Test
  public void testScalarOps() {
    IngestionConfig ingestionConfig = new IngestionConfig();
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setIngestionConfig(ingestionConfig).build();

    // expression true, filtered
    ingestionConfig.setFilterConfig(new FilterConfig("svInt = 123"));
    GenericRow genericRow = getRecord();
    RecordTransformer transformer = new FilterTransformer(tableConfig);
    transformer.transform(genericRow);
    Assert.assertTrue(genericRow.getFieldToValueMap().containsKey(GenericRow.SKIP_RECORD_KEY));

    // expression true, filtered
    ingestionConfig.setFilterConfig(new FilterConfig("svDouble > 120"));
    genericRow = getRecord();
    transformer = new FilterTransformer(tableConfig);
    transformer.transform(genericRow);
    Assert.assertTrue(genericRow.getFieldToValueMap().containsKey(GenericRow.SKIP_RECORD_KEY));

    // expression true, filtered
    ingestionConfig.setFilterConfig(new FilterConfig("svDouble >= 123"));
    genericRow = getRecord();
    transformer = new FilterTransformer(tableConfig);
    transformer.transform(genericRow);
    Assert.assertTrue(genericRow.getFieldToValueMap().containsKey(GenericRow.SKIP_RECORD_KEY));

    // expression true, filtered
    ingestionConfig.setFilterConfig(new FilterConfig("svDouble < 200"));
    genericRow = getRecord();
    transformer = new FilterTransformer(tableConfig);
    transformer.transform(genericRow);
    Assert.assertTrue(genericRow.getFieldToValueMap().containsKey(GenericRow.SKIP_RECORD_KEY));

    // expression true, filtered
    ingestionConfig.setFilterConfig(new FilterConfig("svDouble <= 123"));
    genericRow = getRecord();
    transformer = new FilterTransformer(tableConfig);
    transformer.transform(genericRow);
    Assert.assertTrue(genericRow.getFieldToValueMap().containsKey(GenericRow.SKIP_RECORD_KEY));

    // expression true, filtered
    ingestionConfig.setFilterConfig(new FilterConfig("svLong != 125"));
    genericRow = getRecord();
    transformer = new FilterTransformer(tableConfig);
    transformer.transform(genericRow);
    Assert.assertTrue(genericRow.getFieldToValueMap().containsKey(GenericRow.SKIP_RECORD_KEY));

    // expression true, filtered
    ingestionConfig.setFilterConfig(new FilterConfig("svLong = 123"));
    genericRow = getRecord();
    transformer = new FilterTransformer(tableConfig);
    transformer.transform(genericRow);
    Assert.assertTrue(genericRow.getFieldToValueMap().containsKey(GenericRow.SKIP_RECORD_KEY));

    // expression true, filtered
    ingestionConfig.setFilterConfig(new FilterConfig("between(svLong, 100, 125)"));
    genericRow = getRecord();
    transformer = new FilterTransformer(tableConfig);
    transformer.transform(genericRow);
    Assert.assertTrue(genericRow.getFieldToValueMap().containsKey(GenericRow.SKIP_RECORD_KEY));
  }

  private GenericRow getNullColumnsRecord() {
    GenericRow record = new GenericRow();
    record.putValue("svNullString", null);
    record.putValue("svInt", (byte) 123);

    record.putValue("mvLong", Collections.singletonList(123f));
    record.putValue("mvNullFloat", null);
    return record;
  }

  @Test
  public void testObjectOps() {
    IngestionConfig ingestionConfig = new IngestionConfig();
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setIngestionConfig(ingestionConfig).build();

    // expression true, filtered
    ingestionConfig.setFilterConfig(new FilterConfig("svNullString is null"));
    GenericRow genericRow = getNullColumnsRecord();
    RecordTransformer transformer = new FilterTransformer(tableConfig);
    transformer.transform(genericRow);
    Assert.assertTrue(genericRow.getFieldToValueMap().containsKey(GenericRow.SKIP_RECORD_KEY));

    // expression true, filtered
    ingestionConfig.setFilterConfig(new FilterConfig("svInt is not null"));
    genericRow = getNullColumnsRecord();
    transformer = new FilterTransformer(tableConfig);
    transformer.transform(genericRow);
    Assert.assertTrue(genericRow.getFieldToValueMap().containsKey(GenericRow.SKIP_RECORD_KEY));

    // expression true, filtered
    ingestionConfig.setFilterConfig(new FilterConfig("mvLong is not null"));
    genericRow = getNullColumnsRecord();
    transformer = new FilterTransformer(tableConfig);
    transformer.transform(genericRow);
    Assert.assertTrue(genericRow.getFieldToValueMap().containsKey(GenericRow.SKIP_RECORD_KEY));

    // expression true, filtered
    ingestionConfig.setFilterConfig(new FilterConfig("mvNullFloat is null"));
    genericRow = getNullColumnsRecord();
    transformer = new FilterTransformer(tableConfig);
    transformer.transform(genericRow);
    Assert.assertTrue(genericRow.getFieldToValueMap().containsKey(GenericRow.SKIP_RECORD_KEY));
  }

  @Test
  public void testLogicalScalarOps() {
    IngestionConfig ingestionConfig = new IngestionConfig();
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setIngestionConfig(ingestionConfig).build();

    // expression true, filtered
    ingestionConfig.setFilterConfig(new FilterConfig("svInt = 123 AND svDouble <= 200"));
    GenericRow genericRow = getRecord();
    RecordTransformer transformer = new FilterTransformer(tableConfig);
    transformer.transform(genericRow);
    Assert.assertTrue(genericRow.getFieldToValueMap().containsKey(GenericRow.SKIP_RECORD_KEY));

    // expression true, filtered
    ingestionConfig.setFilterConfig(new FilterConfig("svInt = 125 OR svLong <= 200"));
    genericRow = getRecord();
    transformer = new FilterTransformer(tableConfig);
    transformer.transform(genericRow);
    Assert.assertTrue(genericRow.getFieldToValueMap().containsKey(GenericRow.SKIP_RECORD_KEY));
  }

  @Test
  public void testNullValueTransformer() {
    RecordTransformer transformer = new NullValueTransformer(TABLE_CONFIG, SCHEMA);
    GenericRow record = new GenericRow();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      record = transformer.transform(record);
      assertNotNull(record);
      validateNullValueTransformerResult(record);
    }

    String timeColumn = "timeColumn";
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName("testTable").setTimeColumnName(timeColumn).build();

    // Test null time value with valid default time in epoch
    String epochFormat = "1:DAYS:EPOCH";
    Schema schema =
        new Schema.SchemaBuilder().addDateTime(timeColumn, DataType.LONG, epochFormat, "1:DAYS", 12345, null).build();
    transformer = new NullValueTransformer(tableConfig, schema);
    record = transformer.transform(new GenericRow());
    assertNotNull(record);
    assertTrue(record.isNullValue(timeColumn));
    assertEquals(record.getValue(timeColumn), 12345L);

    // Test null time value without default time in epoch
    schema = new Schema.SchemaBuilder().addDateTime(timeColumn, DataType.LONG, epochFormat, "1:DAYS").build();
    long startTimeMs = System.currentTimeMillis();
    transformer = new NullValueTransformer(tableConfig, schema);
    record = transformer.transform(new GenericRow());
    long endTimeMs = System.currentTimeMillis();
    assertNotNull(record);
    assertTrue(record.isNullValue(timeColumn));
    assertTrue((long) record.getValue(timeColumn) >= TimeUnit.MILLISECONDS.toDays(startTimeMs)
        && (long) record.getValue(timeColumn) <= TimeUnit.MILLISECONDS.toDays(endTimeMs));

    // Test null time value with invalid default time in epoch
    schema = new Schema.SchemaBuilder().addDateTime(timeColumn, DataType.LONG, epochFormat, "1:DAYS", 0, null).build();
    startTimeMs = System.currentTimeMillis();
    transformer = new NullValueTransformer(tableConfig, schema);
    record = transformer.transform(new GenericRow());
    endTimeMs = System.currentTimeMillis();
    assertNotNull(record);
    assertTrue(record.isNullValue(timeColumn));
    assertTrue((long) record.getValue(timeColumn) >= TimeUnit.MILLISECONDS.toDays(startTimeMs)
        && (long) record.getValue(timeColumn) <= TimeUnit.MILLISECONDS.toDays(endTimeMs));

    // Test null time value with valid default time in SDF
    String sdfFormat = "1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd";
    schema =
        new Schema.SchemaBuilder().addDateTime(timeColumn, DataType.STRING, sdfFormat, "1:DAYS", "2020-02-02", null)
            .build();
    transformer = new NullValueTransformer(tableConfig, schema);
    record = transformer.transform(new GenericRow());
    assertNotNull(record);
    assertTrue(record.isNullValue(timeColumn));
    assertEquals(record.getValue(timeColumn), "2020-02-02");

    // Test null time value without default time in SDF
    schema = new Schema.SchemaBuilder().addDateTime(timeColumn, DataType.STRING, sdfFormat, "1:DAYS").build();
    startTimeMs = System.currentTimeMillis();
    transformer = new NullValueTransformer(tableConfig, schema);
    record = transformer.transform(new GenericRow());
    endTimeMs = System.currentTimeMillis();
    assertNotNull(record);
    assertTrue(record.isNullValue(timeColumn));
    DateTimeFormatSpec dateTimeFormatSpec = new DateTimeFormatSpec(sdfFormat);
    assertTrue(((String) record.getValue(timeColumn)).compareTo(dateTimeFormatSpec.fromMillisToFormat(startTimeMs)) >= 0
        && ((String) record.getValue(timeColumn)).compareTo(dateTimeFormatSpec.fromMillisToFormat(endTimeMs)) <= 0);

    // Test null time value with invalid default time in SDF
    schema =
        new Schema.SchemaBuilder().addDateTime(timeColumn, DataType.STRING, sdfFormat, "1:DAYS", 12345, null).build();
    transformer = new NullValueTransformer(tableConfig, schema);
    record = transformer.transform(new GenericRow());
    assertNotNull(record);
    assertTrue(record.isNullValue(timeColumn));
    dateTimeFormatSpec = new DateTimeFormatSpec(sdfFormat);
    assertTrue(((String) record.getValue(timeColumn)).compareTo(dateTimeFormatSpec.fromMillisToFormat(startTimeMs)) >= 0
        && ((String) record.getValue(timeColumn)).compareTo(dateTimeFormatSpec.fromMillisToFormat(endTimeMs)) <= 0);
  }

  private void validateNullValueTransformerResult(GenericRow record) {
    assertEquals(record.getValue("svInt"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT);
    assertEquals(record.getValue("svLong"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG);
    assertEquals(record.getValue("svFloat"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT);
    assertEquals(record.getValue("svDouble"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE);
    assertEquals(record.getValue("svBoolean"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BOOLEAN);
    assertEquals(record.getValue("svTimestamp"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_TIMESTAMP);
    assertEquals(record.getValue("svBytes"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BYTES);
    assertEquals(record.getValue("svJson"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_JSON);
    assertEquals(record.getValue("mvInt"), new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT});
    assertEquals(record.getValue("mvLong"), new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG});
    assertEquals(record.getValue("mvFloat"), new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT});
    assertEquals(record.getValue("mvDouble"), new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE});
    assertEquals(record.getValue("svStringWithNullCharacters"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING);
    assertEquals(record.getValue("svStringWithLengthLimit"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING);
    assertEquals(record.getValue("mvString1"), new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING});
    assertEquals(record.getValue("mvString2"), new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING});
    assertNull(record.getValue("$virtual"));
    validateNullValueFields(record);
  }

  private void validateNullValueFields(GenericRow record) {
    assertTrue(record.isNullValue("svInt"));
    assertTrue(record.isNullValue("svLong"));
    assertTrue(record.isNullValue("svFloat"));
    assertTrue(record.isNullValue("svDouble"));
    assertTrue(record.isNullValue("svBoolean"));
    assertTrue(record.isNullValue("svTimestamp"));
    assertTrue(record.isNullValue("svBytes"));
    assertTrue(record.isNullValue("svJson"));
    assertTrue(record.isNullValue("mvInt"));
    assertTrue(record.isNullValue("mvLong"));
    assertTrue(record.isNullValue("mvDouble"));
    assertTrue(record.isNullValue("svStringWithNullCharacters"));
    assertTrue(record.isNullValue("svStringWithLengthLimit"));
    assertTrue(record.isNullValue("mvString1"));
    assertTrue(record.isNullValue("mvString2"));
    assertFalse(record.isNullValue("$virtual"));
    assertEquals(record.getNullValueFields(), SCHEMA.getPhysicalColumnNames());
  }

  @Test
  public void testDefaultTransformer() {
    RecordTransformer transformer = CompositeTransformer.getDefaultTransformer(TABLE_CONFIG, SCHEMA);
    GenericRow record = getRecord();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      record = transformer.transform(record);
      assertNotNull(record);
      assertEquals(record.getValue("svInt"), 123);
      assertEquals(record.getValue("svLong"), 123L);
      assertEquals(record.getValue("svFloat"), 123f);
      assertEquals(record.getValue("svDouble"), 123d);
      assertEquals(record.getValue("svBoolean"), 1);
      assertEquals(record.getValue("svTimestamp"), Timestamp.valueOf("2020-02-02 22:22:22.222").getTime());
      assertEquals(record.getValue("svJson"), "{\"first\":\"daffy\",\"last\":\"duck\"}");
      assertEquals(record.getValue("svBytes"), new byte[]{123, 123});
      assertEquals(record.getValue("mvInt"), new Object[]{123});
      assertEquals(record.getValue("mvLong"), new Object[]{123L});
      assertEquals(record.getValue("mvFloat"), new Object[]{123f});
      assertEquals(record.getValue("mvDouble"), new Object[]{123d});
      assertEquals(record.getValue("svStringWithNullCharacters"), "1");
      assertEquals(record.getValue("svStringWithLengthLimit"), "12");
      assertEquals(record.getValue("mvString1"), new Object[]{"123", "123", "123", "123.0", "123.0"});
      assertEquals(record.getValue("mvString2"), new Object[]{"123", "123", "123.0", "123.0", "123"});
      assertNull(record.getValue("$virtual"));
      assertEquals(Float.floatToRawIntBits((float) record.getValue("svFloatNegativeZero")),
          Float.floatToRawIntBits(0.0f));
      assertEquals(Double.doubleToRawLongBits((double) record.getValue("svDoubleNegativeZero")),
          Double.doubleToRawLongBits(0.0d));
      assertEquals(record.getValue("mvFloatNegativeZero"), new Float[]{0.0f, 1.0f, 0.0f, 3.0f});
      assertEquals(record.getValue("mvDoubleNegativeZero"), new Double[]{0.0d, 1.0d, 0.0d, 3.0d});
      assertEquals(record.getValue("svFloatNaN"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT);
      assertEquals(record.getValue("svDoubleNaN"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE);
      assertEquals(record.getValue("mvFloatNaN"),
          new Float[]{0.0f, 2.0f});
      assertEquals(record.getValue("mvDoubleNaN"),
          new Double[]{0.0d, 2.0d});
      assertEquals(new ArrayList<>(record.getNullValueFields()),
          new ArrayList<>(Arrays.asList("svFloatNaN", "svDoubleNaN")));
    }

    // Test empty record
    record = new GenericRow();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      record = transformer.transform(record);
      assertNotNull(record);
      assertEquals(record.getValue("svInt"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT);
      assertEquals(record.getValue("svLong"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG);
      assertEquals(record.getValue("svFloat"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT);
      assertEquals(record.getValue("svDouble"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE);
      assertEquals(record.getValue("svBoolean"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BOOLEAN);
      assertEquals(record.getValue("svTimestamp"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_TIMESTAMP);
      assertEquals(record.getValue("svBytes"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BYTES);
      assertEquals(record.getValue("svJson"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_JSON);
      assertEquals(record.getValue("mvInt"), new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT});
      assertEquals(record.getValue("mvLong"), new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG});
      assertEquals(record.getValue("mvFloat"), new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT});
      assertEquals(record.getValue("mvDouble"), new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE});
      assertEquals(record.getValue("svStringWithNullCharacters"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING);
      assertEquals(record.getValue("svStringWithLengthLimit"),
          FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING.substring(0, 2));
      assertEquals(record.getValue("mvString1"), new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING});
      assertEquals(record.getValue("mvString2"), new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING});
      assertNull(record.getValue("$virtual"));
      validateNullValueFields(record);
    }
  }

  @Test
  public void testPassThroughTransformer() {
    RecordTransformer transformer = CompositeTransformer.getPassThroughTransformer();
    GenericRow record = getRecord();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      record = transformer.transform(record);
      assertNotNull(record);
    }
  }
}
