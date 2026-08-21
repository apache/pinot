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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Locale;
import org.apache.pinot.segment.spi.memory.DataBufferPinotInputStream;
import org.apache.pinot.segment.spi.memory.PinotByteBuffer;
import org.apache.pinot.segment.spi.memory.PinotInputStream;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.UuidUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.*;


public class DataSchemaTest {
  private static final String[] COLUMN_NAMES = {
      "int", "long", "float", "double", "string", "variant", "uuid", "object", "int_array", "long_array", "float_array",
      "double_array", "string_array", "boolean_array", "timestamp_array", "bytes_array", "uuid_array"
  };
  private static final int NUM_COLUMNS = COLUMN_NAMES.length;
  private static final DataSchema.ColumnDataType[] COLUMN_DATA_TYPES = {
      INT, LONG, FLOAT, DOUBLE, STRING, VARIANT, UUID, OBJECT, INT_ARRAY, LONG_ARRAY, FLOAT_ARRAY, DOUBLE_ARRAY,
      STRING_ARRAY, BOOLEAN_ARRAY, TIMESTAMP_ARRAY, BYTES_ARRAY, UUID_ARRAY
  };
  private static final String UUID_VALUE = "550e8400-e29b-41d4-a716-446655440000";
  private static final String UUID_VALUE_2 = "550e8400-e29b-41d4-a716-446655440001";
  // Fully qualified: the static ColumnDataType.* import below binds the simple name UUID to the enum constant.
  private static final java.util.UUID JAVA_UUID = java.util.UUID.fromString(UUID_VALUE);
  private static final java.util.UUID JAVA_UUID_2 = java.util.UUID.fromString(UUID_VALUE_2);

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
    byte[] serialized = dataSchema.toBytes();
    DataSchema dataSchemaAfterSerDe = DataSchema.fromBytes(ByteBuffer.wrap(serialized));
    Assert.assertEquals(dataSchema, dataSchemaAfterSerDe);
    Assert.assertEquals(dataSchema.hashCode(), dataSchemaAfterSerDe.hashCode());
    try (PinotInputStream input = new DataBufferPinotInputStream(PinotByteBuffer.wrap(serialized))) {
      Assert.assertEquals(DataSchema.fromBytes(input), dataSchema);
    }
  }

  @Test
  public void testToString() {
    DataSchema dataSchema = new DataSchema(COLUMN_NAMES, COLUMN_DATA_TYPES);
    Assert.assertEquals(dataSchema.toString(),
        "[int(INT),long(LONG),float(FLOAT),double(DOUBLE),string(STRING),variant(VARIANT),uuid(UUID),object(OBJECT),"
            + "int_array(INT_ARRAY),long_array(LONG_ARRAY),float_array(FLOAT_ARRAY),double_array(DOUBLE_ARRAY),"
            + "string_array(STRING_ARRAY),boolean_array(BOOLEAN_ARRAY),timestamp_array(TIMESTAMP_ARRAY),"
            + "bytes_array(BYTES_ARRAY),uuid_array(UUID_ARRAY)]");
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

    Assert.assertFalse(UUID.isNumber());
    Assert.assertFalse(UUID.isWholeNumber());
    Assert.assertFalse(UUID.isArray());
    Assert.assertFalse(UUID.isNumberArray());
    Assert.assertFalse(UUID.isWholeNumberArray());
    Assert.assertFalse(UUID.isCompatible(DOUBLE));
    Assert.assertTrue(UUID.isCompatible(UUID));
    Assert.assertFalse(UUID.isCompatible(BYTES));
    Assert.assertFalse(UUID.isCompatible(STRING));

    Assert.assertFalse(VARIANT.isNumber());
    Assert.assertFalse(VARIANT.isArray());
    Assert.assertTrue(VARIANT.isCompatible(VARIANT));
    Assert.assertFalse(VARIANT.isCompatible(BYTES));
    Assert.assertFalse(VARIANT.supportsEquality());
    Assert.assertFalse(VARIANT.supportsHashing());
    Assert.assertFalse(VARIANT.supportsOrdering());
    Assert.assertFalse(VARIANT.supportsMinMax());
    Assert.assertFalse(VARIANT.supportsDirectAggregation());
    Assert.assertFalse(VARIANT.supportsPatternMatching());
    Assert.assertTrue(BYTES.supportsEquality());
    Assert.assertTrue(BYTES.supportsHashing());
    Assert.assertTrue(BYTES.supportsOrdering());
    Assert.assertTrue(BYTES.supportsMinMax());
    Assert.assertTrue(BYTES.supportsDirectAggregation());
    Assert.assertTrue(BYTES.supportsPatternMatching());
    Assert.assertFalse(STRING_ARRAY.supportsEquality());
    Assert.assertFalse(STRING_ARRAY.supportsHashing());
    Assert.assertFalse(STRING_ARRAY.supportsOrdering());
    Assert.assertFalse(STRING_ARRAY.supportsMinMax());
    Assert.assertFalse(STRING_ARRAY.supportsPatternMatching());
    Assert.assertEquals(fromDataType(FieldSpec.DataType.VARIANT, true), VARIANT);
    Assert.expectThrows(IllegalStateException.class, () -> fromDataType(FieldSpec.DataType.VARIANT, false));

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
        STRING_ARRAY, BOOLEAN_ARRAY, TIMESTAMP_ARRAY, BYTES_ARRAY, UUID_ARRAY
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
    Assert.assertEquals(fromDataType(FieldSpec.DataType.UUID, true), UUID);
    Assert.assertEquals(fromDataType(FieldSpec.DataType.UUID, false), UUID_ARRAY);
    Assert.assertEquals(fromDataType(FieldSpec.DataType.BOOLEAN, false), BOOLEAN_ARRAY);
    Assert.assertEquals(fromDataType(FieldSpec.DataType.TIMESTAMP, false), TIMESTAMP_ARRAY);
    Assert.assertEquals(fromDataType(FieldSpec.DataType.BYTES, false), BYTES_ARRAY);

    BigDecimal bigDecimalValue = new BigDecimal("1.2345678901234567890123456789");
    Assert.assertEquals(BIG_DECIMAL.format(bigDecimalValue), bigDecimalValue.toPlainString());
    Timestamp timestampValue = new Timestamp(1234567890123L);
    Assert.assertEquals(TIMESTAMP.format(timestampValue), timestampValue.toString());
    ByteArray uuidValue = new ByteArray(UuidUtils.toBytes(UUID_VALUE));
    Assert.assertEquals(UUID.convert(uuidValue), JAVA_UUID);
    Assert.assertEquals(UUID.format(uuidValue), UUID_VALUE);
    Assert.assertEquals(UUID.convertAndFormat(uuidValue), UUID_VALUE);
    // format() also accepts the external form and re-canonicalizes non-canonical strings.
    Assert.assertEquals(UUID.format(JAVA_UUID), UUID_VALUE);
    Assert.assertEquals(UUID.format(UUID_VALUE.toUpperCase(Locale.ROOT)), UUID_VALUE);
    byte[][] uuidArrayBytesValue = {UuidUtils.toBytes(UUID_VALUE), UuidUtils.toBytes(UUID_VALUE_2)};
    ByteArray[] uuidArrayValue = (ByteArray[]) UUID_ARRAY.toInternal(new String[]{UUID_VALUE, UUID_VALUE_2});
    java.util.UUID[] expectedUuidArray = {JAVA_UUID, JAVA_UUID_2};
    String[] expectedFormatted = {UUID_VALUE, UUID_VALUE_2};
    Assert.assertEquals(UUID_ARRAY.toExternal(uuidArrayValue), expectedUuidArray);
    Assert.assertEquals(UUID_ARRAY.toExternal(uuidArrayBytesValue), expectedUuidArray);
    Assert.assertEquals(UUID_ARRAY.convert(uuidArrayValue), expectedUuidArray);
    Assert.assertEquals(UUID_ARRAY.toInternal(expectedUuidArray), uuidArrayValue);
    Assert.assertEquals(UUID_ARRAY.toInternal(uuidArrayBytesValue), uuidArrayValue);
    Assert.assertEquals(UUID_ARRAY.format(uuidArrayBytesValue), expectedFormatted);
    Assert.assertEquals(UUID_ARRAY.format(expectedUuidArray), expectedFormatted);
    Assert.assertEquals(UUID_ARRAY.format(uuidArrayValue), expectedFormatted);
    Assert.assertEquals(UUID_ARRAY.convertAndFormat(uuidArrayValue), expectedFormatted);
    Assert.assertEquals(UUID_ARRAY.convertAndFormat(uuidArrayBytesValue), expectedFormatted);
    byte[] bytesValue = {12, 34, 56};
    Assert.assertEquals(BYTES.format(bytesValue), BytesUtils.toHexString(bytesValue));
  }

  @Test
  public void testUnknownWireTypeFailsWithUpgradeGuidance()
      throws Exception {
    byte[] columnName = "payload".getBytes(StandardCharsets.UTF_8);
    byte[] unknownType = "FUTURE_LOGICAL_TYPE".getBytes(StandardCharsets.UTF_8);
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream output = new DataOutputStream(bytes)) {
      output.writeInt(1);
      output.writeInt(columnName.length);
      output.write(columnName);
      output.writeInt(unknownType.length);
      output.write(unknownType);
    }

    IllegalArgumentException exception = Assert.expectThrows(IllegalArgumentException.class,
        () -> DataSchema.fromBytes(ByteBuffer.wrap(bytes.toByteArray())));
    Assert.assertTrue(exception.getMessage().contains("Upgrade all brokers and servers"));
    Assert.assertTrue(exception.getMessage().contains("FUTURE_LOGICAL_TYPE"));
    try (PinotInputStream input =
        new DataBufferPinotInputStream(PinotByteBuffer.wrap(bytes.toByteArray()))) {
      exception = Assert.expectThrows(IllegalArgumentException.class, () -> DataSchema.fromBytes(input));
      Assert.assertTrue(exception.getMessage().contains("Upgrade all brokers and servers"));
      Assert.assertTrue(exception.getMessage().contains("FUTURE_LOGICAL_TYPE"));
    }
  }

  @Test
  public void testPreVariantPeerCompatibilityContract()
      throws Exception {
    DataSchema legacySchema =
        new DataSchema(new String[]{"name", "uuid"}, new DataSchema.ColumnDataType[]{STRING, UUID});
    byte[] legacyPayload = legacySchema.toBytes();
    Assert.assertEquals(readWithPreVariantPeer(legacyPayload),
        new PreVariantColumnDataType[]{PreVariantColumnDataType.STRING, PreVariantColumnDataType.UUID});
    Assert.assertEquals(DataSchema.fromBytes(ByteBuffer.wrap(legacyPayload)), legacySchema);

    byte[] variantPayload =
        new DataSchema(new String[]{"payload"}, new DataSchema.ColumnDataType[]{VARIANT}).toBytes();
    IllegalArgumentException exception = Assert.expectThrows(IllegalArgumentException.class,
        () -> readWithPreVariantPeer(variantPayload));
    Assert.assertTrue(exception.getMessage().contains("VARIANT"));
  }

  private static PreVariantColumnDataType[] readWithPreVariantPeer(byte[] serialized)
      throws IOException {
    try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(serialized))) {
      int numColumns = input.readInt();
      for (int i = 0; i < numColumns; i++) {
        int length = input.readInt();
        byte[] ignoredColumnName = new byte[length];
        input.readFully(ignoredColumnName);
      }
      PreVariantColumnDataType[] dataTypes = new PreVariantColumnDataType[numColumns];
      for (int i = 0; i < numColumns; i++) {
        int length = input.readInt();
        dataTypes[i] = PreVariantColumnDataType.valueOf(new String(input.readNBytes(length), StandardCharsets.UTF_8));
      }
      return dataTypes;
    }
  }

  /// Frozen logical-type names understood by the build immediately before VARIANT. DataSchema is serialized by
  /// name, so this small peer model exercises the actual old-reader compatibility boundary without depending on enum
  /// ordinals or loading a second Pinot binary into the test JVM.
  private enum PreVariantColumnDataType {
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    BIG_DECIMAL,
    BOOLEAN,
    TIMESTAMP,
    STRING,
    JSON,
    BYTES,
    UUID,
    MAP,
    OBJECT,
    INT_ARRAY,
    LONG_ARRAY,
    FLOAT_ARRAY,
    DOUBLE_ARRAY,
    BIG_DECIMAL_ARRAY,
    BOOLEAN_ARRAY,
    TIMESTAMP_ARRAY,
    STRING_ARRAY,
    BYTES_ARRAY,
    UUID_ARRAY,
    UNKNOWN
  }

  /// The null placeholder must be resolved on the *logical* type. UUID is the only type whose placeholder differs
  /// from its stored type's: it needs the 16-byte nil UUID, while BYTES supplies a zero-length one. Every other
  /// logical type must agree with its stored type, otherwise callers that resolve the stored type (DataBlockBuilder,
  /// GroupByResultsBlock, GroupByDataTableReducer) would silently write the wrong placeholder.
  @Test
  public void testNullPlaceholderMatchesStoredTypeExceptUuid() {
    for (DataSchema.ColumnDataType columnDataType : DataSchema.ColumnDataType.values()) {
      DataSchema.ColumnDataType storedType = columnDataType.getStoredType();
      if (columnDataType == UUID) {
        Assert.assertEquals(storedType, BYTES);
        Assert.assertEquals(columnDataType.getNullPlaceholder(), new ByteArray(UuidUtils.nullUuidBytes()));
        Assert.assertNotEquals(columnDataType.getNullPlaceholder(), storedType.getNullPlaceholder());
      } else {
        Assert.assertEquals(columnDataType.getNullPlaceholder(), storedType.getNullPlaceholder(),
            "Null placeholder mismatch between " + columnDataType + " and its stored type " + storedType);
      }
    }
  }

  /// The nil-UUID placeholder wraps a mutable 16-byte array, unlike every other placeholder (all empty or
  /// immutable), so each call must hand back a fresh instance.
  @Test
  public void testUuidNullPlaceholderIsNotShared() {
    ByteArray first = (ByteArray) UUID.getNullPlaceholder();
    ByteArray second = (ByteArray) UUID.getNullPlaceholder();
    Assert.assertNotSame(first, second);
    first.getBytes()[0] = 1;
    Assert.assertEquals(second, new ByteArray(UuidUtils.nullUuidBytes()));
  }
}
