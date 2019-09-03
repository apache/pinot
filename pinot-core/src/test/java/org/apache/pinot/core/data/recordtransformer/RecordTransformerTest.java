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

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.core.data.GenericRow;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class RecordTransformerTest {
  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      // For data type conversion
      .addSingleValueDimension("svInt", FieldSpec.DataType.INT)
      .addSingleValueDimension("svLong", FieldSpec.DataType.LONG)
      .addSingleValueDimension("svFloat", FieldSpec.DataType.FLOAT)
      .addSingleValueDimension("svDouble", FieldSpec.DataType.DOUBLE)
      .addSingleValueDimension("svBytes", FieldSpec.DataType.BYTES)
      .addMultiValueDimension("mvInt", FieldSpec.DataType.INT).addMultiValueDimension("mvLong", FieldSpec.DataType.LONG)
      .addMultiValueDimension("mvFloat", FieldSpec.DataType.FLOAT)
      .addMultiValueDimension("mvDouble", FieldSpec.DataType.DOUBLE)
      // For sanitation
      .addSingleValueDimension("svStringWithNullCharacters", FieldSpec.DataType.STRING)
      .addSingleValueDimension("svStringWithLengthLimit", FieldSpec.DataType.STRING).build();

  static {
    SCHEMA.getFieldSpecFor("svStringWithLengthLimit").setMaxLength(2);
  }

  // Transform multiple times should return the same result
  private static final int NUM_ROUNDS = 5;

  private static GenericRow getRecord() {
    GenericRow record = new GenericRow();
    Map<String, Object> fields = new HashMap<>();
    fields.put("svInt", (byte) 123);
    fields.put("svLong", (char) 123);
    fields.put("svFloat", (short) 123);
    fields.put("svDouble", "123");
    fields.put("svBytes", new byte[]{123, 123});
    fields.put("mvInt", new Object[]{123L});
    fields.put("mvLong", new Object[]{123f});
    fields.put("mvFloat", new Object[]{123d});
    fields.put("mvDouble", new Object[]{123});
    fields.put("svStringWithNullCharacters", "1\0002\0003");
    fields.put("svStringWithLengthLimit", "123");
    record.init(fields);
    return record;
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
      assertEquals(record.getValue("svStringWithLengthLimit"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING);
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
    }
  }

  @Test
  public void testDefaultTransformer() {
    RecordTransformer transformer = CompositeTransformer.getDefaultTransformer(SCHEMA);
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
