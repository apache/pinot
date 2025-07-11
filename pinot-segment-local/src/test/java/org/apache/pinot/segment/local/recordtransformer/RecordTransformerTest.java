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

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.utils.ServiceStartableUtils;
import org.apache.pinot.segment.local.segment.creator.TransformPipeline;
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
import org.apache.pinot.spi.data.FieldSpec.MaxLengthExceedStrategy;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.recordtransformer.RecordTransformer;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class RecordTransformerTest {
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
  private static final Schema SCHEMA = createSchema();

  // Transform multiple times should return the same result
  private static final int NUM_ROUNDS = 5;

  private static Schema createSchema() {
    Schema schema = new Schema.SchemaBuilder()
        // For data type conversion
        .addSingleValueDimension("svInt", DataType.INT)
        .addSingleValueDimension("svLong", DataType.LONG)
        .addSingleValueDimension("svFloat", DataType.FLOAT)
        .addSingleValueDimension("svDouble", DataType.DOUBLE)
        .addSingleValueDimension("svBoolean", DataType.BOOLEAN)
        .addSingleValueDimension("svTimestamp", DataType.TIMESTAMP)
        .addSingleValueDimension("svBytes", DataType.BYTES)
        .addMultiValueDimension("mvInt", DataType.INT)
        .addSingleValueDimension("svJson", DataType.JSON)
        .addMultiValueDimension("mvLong", DataType.LONG)
        .addMultiValueDimension("mvFloat", DataType.FLOAT)
        .addMultiValueDimension("mvDouble", DataType.DOUBLE)
        // For sanitation
        .addSingleValueDimension("svStringWithNullCharacters", DataType.STRING)
        .addSingleValueDimension("svStringWithLengthLimit", DataType.STRING)
        .addMultiValueDimension("mvString1", DataType.STRING)
        .addMultiValueDimension("mvString2", DataType.STRING)
        // For negative zero and NaN conversions
        .addSingleValueDimension("svFloatNegativeZero", DataType.FLOAT)
        .addMultiValueDimension("mvFloatNegativeZero", DataType.FLOAT)
        .addSingleValueDimension("svDoubleNegativeZero", DataType.DOUBLE)
        .addMultiValueDimension("mvDoubleNegativeZero", DataType.DOUBLE)
        .addSingleValueDimension("svFloatNaN", DataType.FLOAT)
        .addMultiValueDimension("mvFloatNaN", DataType.FLOAT)
        .addSingleValueDimension("svDoubleNaN", DataType.DOUBLE)
        .addMultiValueDimension("mvDoubleNaN", DataType.DOUBLE)
        .addMetric("bigDecimalZero", DataType.BIG_DECIMAL)
        .addMetric("bigDecimalZeroWithPoint", DataType.BIG_DECIMAL)
        .addMetric("bigDecimalZeroWithExponent", DataType.BIG_DECIMAL)
        .build();
    schema.getFieldSpecFor("svStringWithLengthLimit").setMaxLength(2);
    schema.addField(new DimensionFieldSpec("$virtual", DataType.STRING, true, Object.class));
    return schema;
  }

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
    record.putValue("bigDecimalZero", new BigDecimal("0"));
    record.putValue("bigDecimalZeroWithPoint", new BigDecimal("0.0"));
    record.putValue("bigDecimalZeroWithExponent", new BigDecimal("0E-18"));
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
    FilterTransformer transformer = new FilterTransformer(tableConfig);
    assertFalse(transformer.transform(List.of(genericRow)).isEmpty());
    assertEquals(transformer.getNumRecordsFiltered(), 0);

    // expression true, filtered
    ingestionConfig.setFilterConfig(new FilterConfig("Groovy({svInt <= 123}, svInt)"));
    transformer = new FilterTransformer(tableConfig);
    assertTrue(transformer.transform(List.of(genericRow)).isEmpty());
    assertEquals(transformer.getNumRecordsFiltered(), 1);

    // value not found
    ingestionConfig.setFilterConfig(new FilterConfig("Groovy({notPresent == 123}, notPresent)"));
    transformer = new FilterTransformer(tableConfig);
    assertFalse(transformer.transform(List.of(genericRow)).isEmpty());
    assertEquals(transformer.getNumRecordsFiltered(), 0);

    // invalid function
    ingestionConfig.setFilterConfig(new FilterConfig("Groovy(svInt == 123)"));
    try {
      new FilterTransformer(tableConfig);
      fail("Should have failed constructing FilterTransformer");
    } catch (Exception e) {
      // expected
    }

    // multi value column
    ingestionConfig.setFilterConfig(new FilterConfig("Groovy({svFloat.max() < 500}, svFloat)"));
    transformer = new FilterTransformer(tableConfig);
    assertTrue(transformer.transform(List.of(genericRow)).isEmpty());
    assertEquals(transformer.getNumRecordsFiltered(), 1);
  }

  @Test
  public void testDataTypeTransformer() {
    RecordTransformer transformer = new DataTypeTransformer(TABLE_CONFIG, SCHEMA);
    GenericRow record = getRecord();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      transformer.transform(record);
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
        .addSingleValueDimension("svLong", DataType.LONG)
        .build();

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
      transformerWithDefaultNulls.transform(record1);
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
    assertTrue(transformer.isNoOp());

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
      transformer.transform(record2);
      assertNull(record2.getValue(timeCol));
    }

    // Valid timestamp, validation enabled
    ingestionConfig.setContinueOnError(false);
    transformer = new TimeValidationTransformer(tableConfig, schema);
    GenericRow record3 = getRecord();
    Long currentTimeMillis = System.currentTimeMillis();
    record3.putValue(timeCol, currentTimeMillis);
    for (int i = 0; i < NUM_ROUNDS; i++) {
      transformer.transform(record3);
      assertEquals(record3.getValue(timeCol), currentTimeMillis);
    }
  }

  @Test
  public void testSanitizationTransformer() {
    // scenario where string contains null and exceeds max length
    // and fieldSpec maxLengthExceedStrategy is default (TRIM_LENGTH)
    RecordTransformer transformer = new SanitizationTransformer(SCHEMA);
    GenericRow record = getRecord();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      transformer.transform(record);
      assertEquals(record.getValue("svStringWithNullCharacters"), "1");
      assertEquals(record.getValue("svStringWithLengthLimit"), "12");
      assertEquals(record.getValue("mvString1"), new Object[]{"123", "123", "123", "123.0", "123.0"});
      assertEquals(record.getValue("mvString2"), new Object[]{"123", "123", "123.0", "123.0", "123"});
      assertNull(record.getValue("$virtual"));
      assertTrue(record.getNullValueFields().isEmpty());
      assertTrue(record.isSanitized());
    }

    // Remove 'svStringWithLengthLimit' field to test null characters alone
    Schema schema = createSchema();
    FieldSpec svStringWithLengthLimit = schema.getFieldSpecFor("svStringWithLengthLimit");
    schema.removeField("svStringWithLengthLimit");

    // scenario where string contains null and fieldSpec maxLengthExceedStrategy is to ERROR
    FieldSpec svStringWithNullCharacters = schema.getFieldSpecFor("svStringWithNullCharacters");
    svStringWithNullCharacters.setMaxLengthExceedStrategy(MaxLengthExceedStrategy.ERROR);
    transformer = new SanitizationTransformer(schema);
    record = getRecord();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      try {
        transformer.transform(record);
        fail();
      } catch (IllegalStateException e) {
        assertEquals(e.getMessage(), "Throwing exception as value: 1\0002\0003 for column "
            + "svStringWithNullCharacters contains null character.");
      }
    }

    // scenario where string contains null and fieldSpec maxLengthExceedStrategy is to SUBSTITUTE_DEFAULT_VALUE
    svStringWithNullCharacters.setMaxLengthExceedStrategy(MaxLengthExceedStrategy.SUBSTITUTE_DEFAULT_VALUE);
    transformer = new SanitizationTransformer(schema);
    record = getRecord();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      transformer.transform(record);
      assertEquals(record.getValue("svStringWithNullCharacters"), "null");
      assertTrue(record.isSanitized());
    }

    // scenario where string contains null and fieldSpec maxLengthExceedStrategy is to NO_ACTION
    svStringWithNullCharacters.setMaxLengthExceedStrategy(MaxLengthExceedStrategy.NO_ACTION);
    transformer = new SanitizationTransformer(schema);
    record = getRecord();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      transformer.transform(record);
      assertEquals(record.getValue("svStringWithNullCharacters"), "1");
      assertTrue(record.isSanitized());
    }

    // Remove 'svStringWithNullCharacters' field to test length limit alone
    schema.removeField("svStringWithNullCharacters");
    schema.addField(svStringWithLengthLimit);

    // scenario where string exceeds max length and fieldSpec maxLengthExceedStrategy is to ERROR
    svStringWithLengthLimit.setMaxLengthExceedStrategy(MaxLengthExceedStrategy.ERROR);
    transformer = new SanitizationTransformer(schema);
    record = getRecord();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      try {
        transformer.transform(record);
        fail();
      } catch (IllegalStateException e) {
        assertEquals(e.getMessage(), "Throwing exception as value: 123 for column svStringWithLengthLimit "
            + "exceeds configured max length 2.");
      }
    }

    // scenario where string exceeds max length and fieldSpec maxLengthExceedStrategy is to SUBSTITUTE_DEFAULT_VALUE
    svStringWithLengthLimit.setMaxLengthExceedStrategy(MaxLengthExceedStrategy.SUBSTITUTE_DEFAULT_VALUE);
    transformer = new SanitizationTransformer(schema);
    record = getRecord();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      transformer.transform(record);
      assertEquals(record.getValue("svStringWithLengthLimit"), "null");
      assertTrue(record.isSanitized());
    }

    // scenario where string exceeds max length and fieldSpec maxLengthExceedStrategy is to NO_ACTION
    svStringWithLengthLimit.setMaxLengthExceedStrategy(MaxLengthExceedStrategy.NO_ACTION);
    transformer = new SanitizationTransformer(schema);
    record = getRecord();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      transformer.transform(record);
      assertEquals(record.getValue("svStringWithLengthLimit"), "123");
      assertFalse(record.isSanitized());
    }

    // Remove 'svStringWithLengthLimit' field to test other fields
    schema.removeField("svStringWithLengthLimit");

    // scenario where json field exceeds max length and fieldSpec maxLengthExceedStrategy is to NO_ACTION
    FieldSpec svJson = schema.getFieldSpecFor("svJson");
    svJson.setMaxLength(10);
    svJson.setMaxLengthExceedStrategy(MaxLengthExceedStrategy.NO_ACTION);
    transformer = new SanitizationTransformer(schema);
    record = getRecord();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      transformer.transform(record);
      assertEquals(record.getValue("svJson"), "{\"first\": \"daffy\", \"last\": \"duck\"}");
      assertFalse(record.isSanitized());
    }

    // scenario where json field exceeds max length and fieldSpec maxLengthExceedStrategy is to TRIM_LENGTH
    svJson.setMaxLengthExceedStrategy(MaxLengthExceedStrategy.TRIM_LENGTH);
    transformer = new SanitizationTransformer(schema);
    record = getRecord();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      transformer.transform(record);
      assertEquals(record.getValue("svJson"), "{\"first\": ");
      assertTrue(record.isSanitized());
    }

    // scenario where json field exceeds max length and fieldSpec maxLengthExceedStrategy is to SUBSTITUTE_DEFAULT_VALUE
    svJson.setMaxLengthExceedStrategy(MaxLengthExceedStrategy.SUBSTITUTE_DEFAULT_VALUE);
    transformer = new SanitizationTransformer(schema);
    record = getRecord();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      transformer.transform(record);
      assertEquals(record.getValue("svJson"), "null");
      assertTrue(record.isSanitized());
    }

    // scenario where json field exceeds max length and fieldSpec maxLengthExceedStrategy is to ERROR
    svJson.setMaxLengthExceedStrategy(MaxLengthExceedStrategy.ERROR);
    transformer = new SanitizationTransformer(schema);
    record = getRecord();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      try {
        transformer.transform(record);
        fail();
      } catch (IllegalStateException e) {
        assertEquals(e.getMessage(),
            "Throwing exception as value: {\"first\": \"daffy\", \"last\": \"duck\"} for column svJson exceeds "
                + "configured max length 10.");
      }
    }

    // Remove 'svJson' field to test other fields
    schema.removeField("svJson");

    // scenario where bytes field exceeds max length and fieldSpec maxLengthExceedStrategy is to NO_ACTION
    FieldSpec svBytes = schema.getFieldSpecFor("svBytes");
    svBytes.setMaxLength(2);
    svBytes.setMaxLengthExceedStrategy(MaxLengthExceedStrategy.NO_ACTION);
    transformer = new SanitizationTransformer(schema);
    record = getRecord();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      transformer.transform(record);
      assertEquals(record.getValue("svBytes"), "7b7b");
      assertFalse(record.isSanitized());
    }

    // scenario where bytes field exceeds max length and fieldSpec maxLengthExceedStrategy is to TRIM_LENGTH
    svBytes.setMaxLengthExceedStrategy(MaxLengthExceedStrategy.TRIM_LENGTH);
    transformer = new SanitizationTransformer(schema);
    record = getRecord();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      transformer.transform(record);
      assertEquals(record.getValue("svBytes"), "7b");
      assertTrue(record.isSanitized());
    }

    // scenario where bytes field exceeds max length and fieldSpec maxLengthExceedStrategy is to
    // SUBSTITUTE_DEFAULT_VALUE
    svBytes.setMaxLengthExceedStrategy(MaxLengthExceedStrategy.SUBSTITUTE_DEFAULT_VALUE);
    transformer = new SanitizationTransformer(schema);
    record = getRecord();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      transformer.transform(record);
      assertEquals(record.getValue("svBytes"), BytesUtils.toHexString(new byte[0]));
      assertTrue(record.isSanitized());
    }

    // scenario where bytes field exceeds max length and fieldSpec maxLengthExceedStrategy is to ERROR
    svBytes.setMaxLengthExceedStrategy(MaxLengthExceedStrategy.ERROR);
    transformer = new SanitizationTransformer(schema);
    record = getRecord();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      try {
        transformer.transform(record);
        fail();
      } catch (IllegalStateException e) {
        assertEquals(e.getMessage(),
            "Throwing exception as value: 7b7b for column svBytes exceeds configured max length 2.");
      }
    }
  }

  @Test
  public void testSpecialValueTransformer() {
    RecordTransformer transformer = new SpecialValueTransformer(SCHEMA);
    GenericRow record = getRecord();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      transformer.transform(record);
      assertEquals(Float.floatToRawIntBits((float) record.getValue("svFloatNegativeZero")),
          Float.floatToRawIntBits(0.0f));
      assertEquals(Double.doubleToRawLongBits((double) record.getValue("svDoubleNegativeZero")),
          Double.doubleToRawLongBits(0.0d));
      assertEquals(record.getValue("mvFloatNegativeZero"), new Float[]{0.0f, 1.0f, 0.0f, 3.0f});
      assertEquals(record.getValue("mvDoubleNegativeZero"), new Double[]{0.0d, 1.0d, 0.0d, 3.0d});
      assertNull(record.getValue("svFloatNaN"));
      assertNull(record.getValue("svDoubleNaN"));
      assertEquals(record.getValue("mvFloatNaN"), new Float[]{0.0f, 2.0f});
      assertEquals(record.getValue("mvDoubleNaN"), new Double[]{0.0d, 2.0d});
      assertEquals(record.getValue("bigDecimalZero"), BigDecimal.ZERO);
      assertEquals(record.getValue("bigDecimalZeroWithPoint"), BigDecimal.ZERO);
      assertEquals(record.getValue("bigDecimalZeroWithExponent"), BigDecimal.ZERO);
    }
  }

  @Test
  public void testOrderForTransformers() {
    // This test checks that the specified order is maintained for different transformers.

    // Build Schema and ingestionConfig in such a way that all the transformers are loaded.
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("svInt", DataType.INT)
        .addSingleValueDimension("svDouble", DataType.DOUBLE)
        .addSingleValueDimension("expressionTestColumn", DataType.INT)
        .addSingleValueDimension("svNaN", DataType.FLOAT)
        .addMultiValueDimension("mvNaN", DataType.FLOAT)
        .addSingleValueDimension("emptyDimensionForNullValueTransformer", DataType.FLOAT)
        .addSingleValueDimension("svStringNull", DataType.STRING)
        .addSingleValueDimension("indexableExtras", DataType.JSON)
        .addDateTime("timeCol", DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    IngestionConfig ingestionConfig = new IngestionConfig();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setIngestionConfig(ingestionConfig)
        .setTimeColumnName("timeCol")
        .build();
    ingestionConfig.setFilterConfig(new FilterConfig("svInt = 123 AND svDouble <= 200"));
    ingestionConfig.setTransformConfigs(List.of(new TransformConfig("expressionTestColumn", "plus(x,10)")));
    ingestionConfig.setSchemaConformingTransformerConfig(
        new SchemaConformingTransformerConfig(null, "indexableExtras", false, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null, null, null, null, null, null, null));
    ingestionConfig.setRowTimeValueCheck(true);
    ingestionConfig.setContinueOnError(false);

    // Get the list of transformers.
    List<RecordTransformer> transformers = RecordTransformerUtils.getDefaultTransformers(tableConfig, schema);
    assertEquals(transformers.size(), 8);
    assertTrue(transformers.get(0) instanceof ExpressionTransformer);
    assertTrue(transformers.get(1) instanceof FilterTransformer);
    assertTrue(transformers.get(2) instanceof SchemaConformingTransformer);
    assertTrue(transformers.get(3) instanceof DataTypeTransformer);
    assertTrue(transformers.get(4) instanceof TimeValidationTransformer);
    assertTrue(transformers.get(5) instanceof SpecialValueTransformer);
    assertTrue(transformers.get(6) instanceof NullValueTransformer);
    assertTrue(transformers.get(7) instanceof SanitizationTransformer);
  }

  @Test
  public void testScalarOps() {
    IngestionConfig ingestionConfig = new IngestionConfig();
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setIngestionConfig(ingestionConfig).build();
    GenericRow record = getRecord();

    // expression true, filtered
    ingestionConfig.setFilterConfig(new FilterConfig("svInt = 123"));
    assertTrue(new FilterTransformer(tableConfig).transform(List.of(record)).isEmpty());

    // expression true, filtered
    ingestionConfig.setFilterConfig(new FilterConfig("svDouble > 120"));
    assertTrue(new FilterTransformer(tableConfig).transform(List.of(record)).isEmpty());

    // expression true, filtered
    ingestionConfig.setFilterConfig(new FilterConfig("svDouble >= 123"));
    assertTrue(new FilterTransformer(tableConfig).transform(List.of(record)).isEmpty());

    // expression true, filtered
    ingestionConfig.setFilterConfig(new FilterConfig("svDouble < 200"));
    assertTrue(new FilterTransformer(tableConfig).transform(List.of(record)).isEmpty());

    // expression true, filtered
    ingestionConfig.setFilterConfig(new FilterConfig("svDouble <= 123"));
    assertTrue(new FilterTransformer(tableConfig).transform(List.of(record)).isEmpty());

    // expression true, filtered
    ingestionConfig.setFilterConfig(new FilterConfig("svLong != 125"));
    assertTrue(new FilterTransformer(tableConfig).transform(List.of(record)).isEmpty());

    // expression true, filtered
    ingestionConfig.setFilterConfig(new FilterConfig("svLong = 123"));
    assertTrue(new FilterTransformer(tableConfig).transform(List.of(record)).isEmpty());

    // expression true, filtered
    ingestionConfig.setFilterConfig(new FilterConfig("between(svLong, 100, 125)"));
    assertTrue(new FilterTransformer(tableConfig).transform(List.of(record)).isEmpty());
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
    GenericRow record = getNullColumnsRecord();

    // expression true, filtered
    ingestionConfig.setFilterConfig(new FilterConfig("svNullString is null"));
    assertTrue(new FilterTransformer(tableConfig).transform(List.of(record)).isEmpty());

    // expression true, filtered
    ingestionConfig.setFilterConfig(new FilterConfig("svInt is not null"));
    assertTrue(new FilterTransformer(tableConfig).transform(List.of(record)).isEmpty());

    // expression true, filtered
    ingestionConfig.setFilterConfig(new FilterConfig("mvLong is not null"));
    assertTrue(new FilterTransformer(tableConfig).transform(List.of(record)).isEmpty());

    // expression true, filtered
    ingestionConfig.setFilterConfig(new FilterConfig("mvNullFloat is null"));
    assertTrue(new FilterTransformer(tableConfig).transform(List.of(record)).isEmpty());
  }

  @Test
  public void testLogicalScalarOps() {
    IngestionConfig ingestionConfig = new IngestionConfig();
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setIngestionConfig(ingestionConfig).build();
    GenericRow record = getRecord();

    // expression true, filtered
    ingestionConfig.setFilterConfig(new FilterConfig("svInt = 123 AND svDouble <= 200"));
    assertTrue(new FilterTransformer(tableConfig).transform(List.of(record)).isEmpty());

    // expression true, filtered
    ingestionConfig.setFilterConfig(new FilterConfig("svInt = 125 OR svLong <= 200"));
    assertTrue(new FilterTransformer(tableConfig).transform(List.of(record)).isEmpty());
  }

  @Test
  public void testNullValueTransformer() {
    RecordTransformer transformer = new NullValueTransformer(TABLE_CONFIG, SCHEMA);
    GenericRow record = new GenericRow();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      transformer.transform(record);
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
    record.clear();
    transformer.transform(record);
    assertTrue(record.isNullValue(timeColumn));
    assertEquals(record.getValue(timeColumn), 12345L);

    // Test null time value without default time in epoch
    schema = new Schema.SchemaBuilder().addDateTime(timeColumn, DataType.LONG, epochFormat, "1:DAYS").build();
    transformer = new NullValueTransformer(tableConfig, schema);
    record.clear();
    long startTimeMs = System.currentTimeMillis();
    transformer.transform(record);
    long endTimeMs = System.currentTimeMillis();
    assertTrue(record.isNullValue(timeColumn));
    assertTrue((long) record.getValue(timeColumn) >= TimeUnit.MILLISECONDS.toDays(startTimeMs)
        && (long) record.getValue(timeColumn) <= TimeUnit.MILLISECONDS.toDays(endTimeMs));

    // Test null time value with invalid default time in epoch
    schema = new Schema.SchemaBuilder().addDateTime(timeColumn, DataType.LONG, epochFormat, "1:DAYS", 0, null).build();
    transformer = new NullValueTransformer(tableConfig, schema);
    record.clear();
    startTimeMs = System.currentTimeMillis();
    transformer.transform(record);
    endTimeMs = System.currentTimeMillis();
    assertTrue(record.isNullValue(timeColumn));
    assertTrue((long) record.getValue(timeColumn) >= TimeUnit.MILLISECONDS.toDays(startTimeMs)
        && (long) record.getValue(timeColumn) <= TimeUnit.MILLISECONDS.toDays(endTimeMs));

    // Test null time value with valid default time in SDF
    String sdfFormat = "1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd";
    schema =
        new Schema.SchemaBuilder().addDateTime(timeColumn, DataType.STRING, sdfFormat, "1:DAYS", "2020-02-02", null)
            .build();
    transformer = new NullValueTransformer(tableConfig, schema);
    record.clear();
    transformer.transform(record);
    assertTrue(record.isNullValue(timeColumn));
    assertEquals(record.getValue(timeColumn), "2020-02-02");

    // Test null time value without default time in SDF
    schema = new Schema.SchemaBuilder().addDateTime(timeColumn, DataType.STRING, sdfFormat, "1:DAYS").build();
    transformer = new NullValueTransformer(tableConfig, schema);
    record.clear();
    startTimeMs = System.currentTimeMillis();
    transformer.transform(record);
    endTimeMs = System.currentTimeMillis();
    assertTrue(record.isNullValue(timeColumn));
    DateTimeFormatSpec dateTimeFormatSpec = new DateTimeFormatSpec(sdfFormat);
    assertTrue(((String) record.getValue(timeColumn)).compareTo(dateTimeFormatSpec.fromMillisToFormat(startTimeMs)) >= 0
        && ((String) record.getValue(timeColumn)).compareTo(dateTimeFormatSpec.fromMillisToFormat(endTimeMs)) <= 0);

    // Test null time value with invalid default time in SDF
    schema =
        new Schema.SchemaBuilder().addDateTime(timeColumn, DataType.STRING, sdfFormat, "1:DAYS", 12345, null).build();
    transformer = new NullValueTransformer(tableConfig, schema);
    record.clear();
    transformer.transform(record);
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
    TransformPipeline transformPipeline = new TransformPipeline(TABLE_CONFIG, SCHEMA);
    GenericRow record = getRecord();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      TransformPipeline.Result result = transformPipeline.processRow(record);
      assertEquals(result.getTransformedRows().size(), 1);
      record = result.getTransformedRows().get(0);
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
      assertEquals(record.getValue("mvFloatNaN"), new Float[]{0.0f, 2.0f});
      assertEquals(record.getValue("mvDoubleNaN"), new Double[]{0.0d, 2.0d});
      assertEquals(new ArrayList<>(record.getNullValueFields()),
          new ArrayList<>(Arrays.asList("svFloatNaN", "svDoubleNaN")));
    }

    // Test empty record
    record = new GenericRow();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      TransformPipeline.Result result = transformPipeline.processRow(record);
      assertEquals(result.getTransformedRows().size(), 1);
      record = result.getTransformedRows().get(0);
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
    TransformPipeline transformPipeline = TransformPipeline.getPassThroughPipeline(TABLE_CONFIG.getTableName());
    GenericRow record = getRecord();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      TransformPipeline.Result result = transformPipeline.processRow(record);
      assertEquals(result.getTransformedRows().size(), 1);
    }
  }

  @Test
  public void testConfigurableJsonDefaults() {
    // Save original defaults
    FieldSpec.MaxLengthExceedStrategy originalStrategy = FieldSpec.getDefaultJsonMaxLengthExceedStrategy();
    int originalMaxLength = FieldSpec.getDefaultJsonMaxLength();

    try {
      // Test configurable default strategy
      FieldSpec.setDefaultJsonMaxLengthExceedStrategy(FieldSpec.MaxLengthExceedStrategy.SUBSTITUTE_DEFAULT_VALUE);
      FieldSpec.setDefaultJsonMaxLength(1024);

      Schema.SchemaBuilder schemaBuilder = new Schema.SchemaBuilder();
      schemaBuilder.addSingleValueDimension("jsonCol", DataType.JSON);
      DimensionFieldSpec explicitJsonSpec =
          new DimensionFieldSpec("explicitJsonCol", DataType.JSON, true, 2048, "");
      explicitJsonSpec.setMaxLengthExceedStrategy(FieldSpec.MaxLengthExceedStrategy.TRIM_LENGTH);
      schemaBuilder.addField(explicitJsonSpec);

      Schema schema = schemaBuilder.build();

      // Verify max length defaults
      FieldSpec jsonSpec = schema.getFieldSpecFor("jsonCol");
      FieldSpec explicitSpec = schema.getFieldSpecFor("explicitJsonCol");

      assertEquals(jsonSpec.getEffectiveMaxLength(), 1024); // Uses JSON default
      assertEquals(explicitSpec.getEffectiveMaxLength(), 2048); // Explicit override

      // Test strategy defaults with sanitization
      jsonSpec.setMaxLength(10);
      GenericRow record = new GenericRow();
      record.putValue("jsonCol", "{\"test\": \"exceeds 10 chars\"}");
      record.putValue("explicitJsonCol", "{\"test\": \"exceeds 2048 chars easily\"}");
      RecordTransformer transformer = new SanitizationTransformer(schema);
      transformer.transform(record);
      assertEquals(record.getValue("jsonCol"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_JSON);
      assertEquals(record.getValue("explicitJsonCol"), "{\"test\": \"exceeds 2048 chars easily\"}");

      // Test strategy change
      FieldSpec.setDefaultJsonMaxLengthExceedStrategy(FieldSpec.MaxLengthExceedStrategy.ERROR);
      RecordTransformer finalTransformer = new SanitizationTransformer(schema);
      record.putValue("jsonCol", "{\"test\": \"exceeds 10 chars\"}");
      assertThrows(IllegalStateException.class, () -> finalTransformer.transform(record));

      // Test ServiceStartableUtils configuration
      PinotConfiguration config = new PinotConfiguration();
      config.setProperty(CommonConstants.FieldSpecConfigs.CONFIG_OF_DEFAULT_JSON_MAX_LENGTH, "2048");
      config.setProperty(CommonConstants.FieldSpecConfigs.CONFIG_OF_DEFAULT_JSON_MAX_LENGTH_EXCEED_STRATEGY,
          "NO_ACTION");

      ServiceStartableUtils.initFieldSpecConfig(config);
      assertEquals(FieldSpec.getDefaultJsonMaxLength(), 2048);
      assertEquals(FieldSpec.getDefaultJsonMaxLengthExceedStrategy(), FieldSpec.MaxLengthExceedStrategy.NO_ACTION);
    } finally {
      // Restore original defaults
      FieldSpec.setDefaultJsonMaxLengthExceedStrategy(originalStrategy);
      FieldSpec.setDefaultJsonMaxLength(originalMaxLength);
    }
  }
}
