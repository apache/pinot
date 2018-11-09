/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.data.recordtransformer;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class RecordTransformerTest {
  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      // For data type conversion
      .addSingleValueDimension("svInt", FieldSpec.DataType.INT)
      .addSingleValueDimension("svLong", FieldSpec.DataType.LONG)
      .addSingleValueDimension("svFloat", FieldSpec.DataType.FLOAT)
      .addSingleValueDimension("svDouble", FieldSpec.DataType.DOUBLE)
      .addSingleValueDimension("svBytes", FieldSpec.DataType.BYTES)
      .addMultiValueDimension("mvInt", FieldSpec.DataType.INT)
      .addMultiValueDimension("mvLong", FieldSpec.DataType.LONG)
      .addMultiValueDimension("mvFloat", FieldSpec.DataType.FLOAT)
      .addMultiValueDimension("mvDouble", FieldSpec.DataType.DOUBLE)
      // For sanitation
      .addSingleValueDimension("svStringWithNullCharacters", FieldSpec.DataType.STRING)
      .addSingleValueDimension("svStringWithLengthLimit", FieldSpec.DataType.STRING)
      // For time conversion
      .addTime("incoming", 6, TimeUnit.HOURS, FieldSpec.DataType.INT, "outgoing", 1, TimeUnit.MILLISECONDS,
          FieldSpec.DataType.LONG)
      .build();

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
    fields.put("incoming", "123");
    record.init(fields);
    return record;
  }

  @Test
  public void testTimeTransformer() {
    RecordTransformer transformer = new TimeTransformer(SCHEMA);
    GenericRow record = getRecord();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      record = transformer.transform(record);
      assertNotNull(record);
      // We keep the incoming time field in case other transformers rely on it
      assertEquals(record.getValue("incoming"), "123");
      assertEquals(record.getValue("outgoing"), 123 * 6 * 3600 * 1000L);
    }
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
      // Incoming time field won't be converted (it's ignored in this transformer)
      assertEquals(record.getValue("incoming"), "123");
      // Outgoing time field will be converted (without time transformer, this field will be null before transform, and
      // be filled with default null value after transform)
      assertEquals(record.getValue("outgoing"), Long.MIN_VALUE);
    }
  }

  @Test
  public void testSanitationTransformer() {
    RecordTransformer transformer = new SanitationTransformer(SCHEMA);
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
    RecordTransformer transformer = CompoundTransformer.getDefaultTransformer(SCHEMA);
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
      assertEquals(record.getValue("incoming"), "123");
      assertEquals(record.getValue("outgoing"), 123 * 6 * 3600 * 1000L);
    }
  }

  @Test
  public void testPassThroughTransformer() {
    RecordTransformer transformer = CompoundTransformer.getPassThroughTransformer();
    GenericRow record = getRecord();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      record = transformer.transform(record);
      assertNotNull(record);
    }
  }
}
