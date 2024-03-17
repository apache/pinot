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
package org.apache.pinot.common.utils;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.BytesUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.*;


public class DataSchemaTest {
  private static final String[] COLUMN_NAMES = {
      "int", "long", "float", "double", "string", "object", "int_array", "long_array", "float_array", "double_array",
      "string_array", "boolean_array", "timestamp_array", "bytes_array"
  };
  private static final int NUM_COLUMNS = COLUMN_NAMES.length;
  private static final DataSchema.ColumnDataType[] COLUMN_DATA_TYPES = {
      INT, LONG, FLOAT, DOUBLE, STRING, OBJECT, INT_ARRAY, LONG_ARRAY, FLOAT_ARRAY, DOUBLE_ARRAY, STRING_ARRAY,
      BOOLEAN_ARRAY, TIMESTAMP_ARRAY, BYTES_ARRAY
  };

  @Test
  public void testGetters() {
    DataSchema dataSchema = new DataSchema(COLUMN_NAMES, COLUMN_DATA_TYPES);
    Assert.assertEquals(dataSchema.size(), NUM_COLUMNS);
    for (int i = 0; i < NUM_COLUMNS; i++) {
      Assert.assertEquals(dataSchema.getColumnName(i), COLUMN_NAMES[i]);
      Assert.assertEquals(dataSchema.getColumnDataType(i), COLUMN_DATA_TYPES[i]);
    }
  }

  @Test
  public void testClone() {
    DataSchema dataSchema = new DataSchema(COLUMN_NAMES, COLUMN_DATA_TYPES);
    DataSchema dataSchemaClone = dataSchema.clone();
    Assert.assertEquals(dataSchema, dataSchemaClone);
    Assert.assertEquals(dataSchema.hashCode(), dataSchemaClone.hashCode());
  }

  @Test
  public void testSerDe()
      throws Exception {
    DataSchema dataSchema = new DataSchema(COLUMN_NAMES, COLUMN_DATA_TYPES);
    DataSchema dataSchemaAfterSerDe = DataSchema.fromBytes(ByteBuffer.wrap(dataSchema.toBytes()));
    Assert.assertEquals(dataSchema, dataSchemaAfterSerDe);
    Assert.assertEquals(dataSchema.hashCode(), dataSchemaAfterSerDe.hashCode());
  }

  @Test
  public void testToString() {
    DataSchema dataSchema = new DataSchema(COLUMN_NAMES, COLUMN_DATA_TYPES);
    Assert.assertEquals(dataSchema.toString(),
        "[int(INT),long(LONG),float(FLOAT),double(DOUBLE),string(STRING),object(OBJECT),int_array(INT_ARRAY),"
            + "long_array(LONG_ARRAY),float_array(FLOAT_ARRAY),double_array(DOUBLE_ARRAY),string_array(STRING_ARRAY),"
            + "boolean_array(BOOLEAN_ARRAY),timestamp_array(TIMESTAMP_ARRAY),bytes_array(BYTES_ARRAY)]");
  }

  @Test
  public void testColumnDataType() {
    for (DataSchema.ColumnDataType columnDataType : new DataSchema.ColumnDataType[]{INT, LONG}) {
      Assert.assertTrue(columnDataType.isNumber());
      Assert.assertTrue(columnDataType.isWholeNumber());
      Assert.assertFalse(columnDataType.isArray());
      Assert.assertFalse(columnDataType.isNumberArray());
      Assert.assertFalse(columnDataType.isWholeNumberArray());
      Assert.assertTrue(columnDataType.isCompatible(DOUBLE));
      Assert.assertFalse(columnDataType.isCompatible(STRING));
      Assert.assertFalse(columnDataType.isCompatible(DOUBLE_ARRAY));
      Assert.assertFalse(columnDataType.isCompatible(STRING_ARRAY));
      Assert.assertFalse(columnDataType.isCompatible(BYTES_ARRAY));
    }

    for (DataSchema.ColumnDataType columnDataType : new DataSchema.ColumnDataType[]{FLOAT, DOUBLE}) {
      Assert.assertTrue(columnDataType.isNumber());
      Assert.assertFalse(columnDataType.isWholeNumber());
      Assert.assertFalse(columnDataType.isArray());
      Assert.assertFalse(columnDataType.isNumberArray());
      Assert.assertFalse(columnDataType.isWholeNumberArray());
      Assert.assertTrue(columnDataType.isCompatible(LONG));
      Assert.assertFalse(columnDataType.isCompatible(STRING));
      Assert.assertFalse(columnDataType.isCompatible(LONG_ARRAY));
      Assert.assertFalse(columnDataType.isCompatible(STRING_ARRAY));
      Assert.assertFalse(columnDataType.isCompatible(BYTES_ARRAY));
    }

    Assert.assertFalse(STRING.isNumber());
    Assert.assertFalse(STRING.isWholeNumber());
    Assert.assertFalse(STRING.isArray());
    Assert.assertFalse(STRING.isNumberArray());
    Assert.assertFalse(STRING.isWholeNumberArray());
    Assert.assertFalse(STRING.isCompatible(DOUBLE));
    Assert.assertTrue(STRING.isCompatible(STRING));
    Assert.assertFalse(STRING.isCompatible(DOUBLE_ARRAY));
    Assert.assertFalse(STRING.isCompatible(STRING_ARRAY));
    Assert.assertFalse(STRING.isCompatible(BYTES_ARRAY));

    Assert.assertFalse(OBJECT.isNumber());
    Assert.assertFalse(OBJECT.isWholeNumber());
    Assert.assertFalse(OBJECT.isArray());
    Assert.assertFalse(OBJECT.isNumberArray());
    Assert.assertFalse(OBJECT.isWholeNumberArray());
    Assert.assertFalse(OBJECT.isCompatible(DOUBLE));
    Assert.assertFalse(OBJECT.isCompatible(STRING));
    Assert.assertFalse(OBJECT.isCompatible(DOUBLE_ARRAY));
    Assert.assertFalse(OBJECT.isCompatible(STRING_ARRAY));
    Assert.assertFalse(OBJECT.isCompatible(BYTES_ARRAY));
    Assert.assertTrue(OBJECT.isCompatible(OBJECT));

    for (DataSchema.ColumnDataType columnDataType : new DataSchema.ColumnDataType[]{INT_ARRAY, LONG_ARRAY}) {
      Assert.assertFalse(columnDataType.isNumber());
      Assert.assertFalse(columnDataType.isWholeNumber());
      Assert.assertTrue(columnDataType.isArray());
      Assert.assertTrue(columnDataType.isNumberArray());
      Assert.assertTrue(columnDataType.isWholeNumberArray());
      Assert.assertFalse(columnDataType.isCompatible(DOUBLE));
      Assert.assertFalse(columnDataType.isCompatible(STRING));
      Assert.assertTrue(columnDataType.isCompatible(DOUBLE_ARRAY));
      Assert.assertFalse(columnDataType.isCompatible(STRING_ARRAY));
      Assert.assertFalse(columnDataType.isCompatible(BYTES_ARRAY));
    }

    for (DataSchema.ColumnDataType columnDataType : new DataSchema.ColumnDataType[]{FLOAT_ARRAY, DOUBLE_ARRAY}) {
      Assert.assertFalse(columnDataType.isNumber());
      Assert.assertFalse(columnDataType.isWholeNumber());
      Assert.assertTrue(columnDataType.isArray());
      Assert.assertTrue(columnDataType.isNumberArray());
      Assert.assertFalse(columnDataType.isWholeNumberArray());
      Assert.assertFalse(columnDataType.isCompatible(LONG));
      Assert.assertFalse(columnDataType.isCompatible(STRING));
      Assert.assertTrue(columnDataType.isCompatible(LONG_ARRAY));
      Assert.assertFalse(columnDataType.isCompatible(STRING_ARRAY));
      Assert.assertFalse(columnDataType.isCompatible(BYTES_ARRAY));
    }

    for (DataSchema.ColumnDataType columnDataType : new DataSchema.ColumnDataType[]{
        STRING_ARRAY, BOOLEAN_ARRAY, TIMESTAMP_ARRAY, BYTES_ARRAY
    }) {
      Assert.assertFalse(columnDataType.isNumber());
      Assert.assertFalse(columnDataType.isWholeNumber());
      Assert.assertTrue(columnDataType.isArray());
      Assert.assertFalse(columnDataType.isNumberArray());
      Assert.assertFalse(columnDataType.isWholeNumberArray());
      Assert.assertFalse(columnDataType.isCompatible(DOUBLE));
      Assert.assertFalse(columnDataType.isCompatible(STRING));
      Assert.assertFalse(columnDataType.isCompatible(DOUBLE_ARRAY));
      Assert.assertFalse(columnDataType.isCompatible(INT_ARRAY));
      Assert.assertTrue(columnDataType.isCompatible(columnDataType));
    }

    Assert.assertEquals(fromDataType(FieldSpec.DataType.INT, true), INT);
    Assert.assertEquals(fromDataType(FieldSpec.DataType.INT, false), INT_ARRAY);
    Assert.assertEquals(fromDataType(FieldSpec.DataType.LONG, true), LONG);
    Assert.assertEquals(fromDataType(FieldSpec.DataType.LONG, false), LONG_ARRAY);
    Assert.assertEquals(fromDataType(FieldSpec.DataType.FLOAT, true), FLOAT);
    Assert.assertEquals(fromDataType(FieldSpec.DataType.FLOAT, false), FLOAT_ARRAY);
    Assert.assertEquals(fromDataType(FieldSpec.DataType.DOUBLE, true), DOUBLE);
    Assert.assertEquals(fromDataType(FieldSpec.DataType.DOUBLE, false), DOUBLE_ARRAY);
    Assert.assertEquals(fromDataType(FieldSpec.DataType.STRING, true), STRING);
    Assert.assertEquals(fromDataType(FieldSpec.DataType.STRING, false), STRING_ARRAY);
    Assert.assertEquals(fromDataType(FieldSpec.DataType.BOOLEAN, false), BOOLEAN_ARRAY);
    Assert.assertEquals(fromDataType(FieldSpec.DataType.TIMESTAMP, false), TIMESTAMP_ARRAY);
    Assert.assertEquals(fromDataType(FieldSpec.DataType.BYTES, false), BYTES_ARRAY);

    BigDecimal bigDecimalValue = new BigDecimal("1.2345678901234567890123456789");
    Assert.assertEquals(BIG_DECIMAL.format(bigDecimalValue), bigDecimalValue.toPlainString());
    Timestamp timestampValue = new Timestamp(1234567890123L);
    Assert.assertEquals(TIMESTAMP.format(timestampValue), timestampValue.toString());
    byte[] bytesValue = {12, 34, 56};
    Assert.assertEquals(BYTES.format(bytesValue), BytesUtils.toHexString(bytesValue));
  }

  @Test
  public void equalsVerifier() {
    EqualsVerifier.configure().forClass(DataSchema.class).withIgnoredFields("_storedColumnDataTypes").verify();
  }
}
