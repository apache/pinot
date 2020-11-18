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
package org.apache.pinot.core.data.recordtransformer;

import java.util.Collections;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.FilterConfig;
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
      .addSingleValueDimension("svBytes", DataType.BYTES).addMultiValueDimension("mvInt", DataType.INT)
      .addMultiValueDimension("mvLong", DataType.LONG).addMultiValueDimension("mvFloat", DataType.FLOAT)
      .addMultiValueDimension("mvDouble", DataType.DOUBLE)
      // For sanitation
      .addSingleValueDimension("svStringWithNullCharacters", DataType.STRING)
      .addSingleValueDimension("svStringWithLengthLimit", DataType.STRING).build();
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();

  static {
    SCHEMA.getFieldSpecFor("svStringWithLengthLimit").setMaxLength(2);
    SCHEMA.addField(new DimensionFieldSpec("$virtual", DataType.STRING, true, Object.class));
  }

  // Transform multiple times should return the same result
  private static final int NUM_ROUNDS = 5;

  private static GenericRow getRecord() {
    GenericRow record = new GenericRow();
    record.putValue("svInt", (byte) 123);
    record.putValue("svLong", (char) 123);
    record.putValue("svFloat", Collections.singletonList((short) 123));
    record.putValue("svDouble", new String[]{"123"});
    record.putValue("svBytes", "7b7b"/*new byte[]{123, 123}*/);
    record.putValue("mvInt", new Object[]{123L});
    record.putValue("mvLong", Collections.singletonList(123f));
    record.putValue("mvFloat", new Double[]{123d});
    record.putValue("mvDouble", Collections.singletonMap("key", 123));
    record.putValue("svStringWithNullCharacters", "1\0002\0003");
    record.putValue("svStringWithLengthLimit", "123");
    return record;
  }

  @Test
  public void testFilterTransformer() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();

    // expression false, not filtered
    GenericRow genericRow = getRecord();
    tableConfig
        .setIngestionConfig(new IngestionConfig(null, null, new FilterConfig("Groovy({svInt > 123}, svInt)"), null));
    RecordTransformer transformer = new FilterTransformer(tableConfig);
    transformer.transform(genericRow);
    Assert.assertFalse(genericRow.getFieldToValueMap().containsKey(GenericRow.SKIP_RECORD_KEY));

    // expression true, filtered
    genericRow = getRecord();
    tableConfig
        .setIngestionConfig(new IngestionConfig(null, null, new FilterConfig("Groovy({svInt <= 123}, svInt)"), null));
    transformer = new FilterTransformer(tableConfig);
    transformer.transform(genericRow);
    Assert.assertTrue(genericRow.getFieldToValueMap().containsKey(GenericRow.SKIP_RECORD_KEY));

    // value not found
    genericRow = getRecord();
    tableConfig.setIngestionConfig(
        new IngestionConfig(null, null, new FilterConfig("Groovy({notPresent == 123}, notPresent)"), null));
    transformer = new FilterTransformer(tableConfig);
    transformer.transform(genericRow);
    Assert.assertFalse(genericRow.getFieldToValueMap().containsKey(GenericRow.SKIP_RECORD_KEY));

    // invalid function
    tableConfig.setIngestionConfig(new IngestionConfig(null, null, new FilterConfig("Groovy(svInt == 123)"), null));
    try {
      new FilterTransformer(tableConfig);
      Assert.fail("Should have failed constructing FilterTransformer");
    } catch (Exception e) {
      // expected
    }

    // multi value column
    genericRow = getRecord();
    tableConfig.setIngestionConfig(
        new IngestionConfig(null, null, new FilterConfig("Groovy({svFloat.max() < 500}, svFloat)"), null));
    transformer = new FilterTransformer(tableConfig);
    transformer.transform(genericRow);
    Assert.assertTrue(genericRow.getFieldToValueMap().containsKey(GenericRow.SKIP_RECORD_KEY));
  }

  @Test
  public void testDataTypeTransformer() {
    RecordTransformer transformer = new DataTypeTransformer(SCHEMA);
    GenericRow record = getRecord();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      record = transformer.transform(record);
      assertNotNull(record);
      assertEquals(record.getValue("svInt"), 123);
      assertEquals(record.getValue("svLong"), 123L);
      assertEquals(record.getValue("svFloat"), 123f);
      assertEquals(record.getValue("svDouble"), 123d);
      assertEquals(record.getValue("svBytes"), new byte[]{123, 123});
      assertEquals(record.getValue("mvInt"), new Object[]{123});
      assertEquals(record.getValue("mvLong"), new Object[]{123L});
      assertEquals(record.getValue("mvFloat"), new Object[]{123f});
      assertEquals(record.getValue("mvDouble"), new Object[]{123d});
      assertEquals(record.getValue("svStringWithNullCharacters"), "1\0002\0003");
      assertEquals(record.getValue("svStringWithLengthLimit"), "123");
      assertNull(record.getValue("$virtual"));
      assertTrue(record.getNullValueFields().isEmpty());
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
      assertNull(record.getValue("$virtual"));
      assertTrue(record.getNullValueFields().isEmpty());
    }
  }

  @Test
  public void testNullValueTransformer() {
    RecordTransformer transformer = new NullValueTransformer(SCHEMA);
    GenericRow record = new GenericRow();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      record = transformer.transform(record);
      assertNotNull(record);
      validateNullValueTransformerResult(record);
    }
  }

  private void validateNullValueTransformerResult(GenericRow record) {
    assertEquals(record.getValue("svInt"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT);
    assertEquals(record.getValue("svLong"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG);
    assertEquals(record.getValue("svFloat"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT);
    assertEquals(record.getValue("svDouble"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE);
    assertEquals(record.getValue("svBytes"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BYTES);
    assertEquals(record.getValue("mvInt"), new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT});
    assertEquals(record.getValue("mvLong"), new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG});
    assertEquals(record.getValue("mvFloat"), new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT});
    assertEquals(record.getValue("mvDouble"), new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE});
    assertEquals(record.getValue("svStringWithNullCharacters"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING);
    assertEquals(record.getValue("svStringWithLengthLimit"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING);
    assertNull(record.getValue("$virtual"));
    validateNullValueFields(record);
  }

  private void validateNullValueFields(GenericRow record) {
    assertTrue(record.isNullValue("svInt"));
    assertTrue(record.isNullValue("svLong"));
    assertTrue(record.isNullValue("svFloat"));
    assertTrue(record.isNullValue("svDouble"));
    assertTrue(record.isNullValue("svBytes"));
    assertTrue(record.isNullValue("mvInt"));
    assertTrue(record.isNullValue("mvLong"));
    assertTrue(record.isNullValue("mvDouble"));
    assertTrue(record.isNullValue("svStringWithNullCharacters"));
    assertTrue(record.isNullValue("svStringWithLengthLimit"));
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
      assertEquals(record.getValue("svBytes"), new byte[]{123, 123});
      assertEquals(record.getValue("mvInt"), new Object[]{123});
      assertEquals(record.getValue("mvLong"), new Object[]{123L});
      assertEquals(record.getValue("mvFloat"), new Object[]{123f});
      assertEquals(record.getValue("mvDouble"), new Object[]{123d});
      assertEquals(record.getValue("svStringWithNullCharacters"), "1");
      assertEquals(record.getValue("svStringWithLengthLimit"), "12");
      assertNull(record.getValue("$virtual"));
      assertTrue(record.getNullValueFields().isEmpty());
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
      assertEquals(record.getValue("svBytes"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BYTES);
      assertEquals(record.getValue("mvInt"), new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT});
      assertEquals(record.getValue("mvLong"), new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG});
      assertEquals(record.getValue("mvFloat"), new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT});
      assertEquals(record.getValue("mvDouble"), new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE});
      assertEquals(record.getValue("svStringWithNullCharacters"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING);
      assertEquals(record.getValue("svStringWithLengthLimit"),
          FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING.substring(0, 2));
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
