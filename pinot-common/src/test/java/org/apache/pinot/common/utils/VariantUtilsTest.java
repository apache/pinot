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

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.parquet.variant.Variant;
import org.apache.parquet.variant.VariantBuilder;
import org.apache.parquet.variant.VariantObjectBuilder;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.VariantUtils.ResultType;
import org.apache.pinot.common.utils.VariantUtils.ReusableResult;
import org.apache.pinot.common.utils.VariantUtils.VariantPath;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.UuidUtils;
import org.apache.pinot.spi.utils.VariantEnvelope;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class VariantUtilsTest {
  @Test
  public void testContainsRawVariantResult() {
    DataSchema variantSchema =
        new DataSchema(new String[]{"payload"}, new ColumnDataType[]{ColumnDataType.VARIANT});
    DataSchema typedSchema =
        new DataSchema(new String[]{"eventType"}, new ColumnDataType[]{ColumnDataType.STRING});

    assertTrue(VariantUtils.containsRawVariantResult(variantSchema));
    assertFalse(VariantUtils.containsRawVariantResult(typedSchema));
  }

  @Test
  public void testResultTypeContract() {
    assertEquals(ResultType.BOOLEAN.getDataType(), DataType.BOOLEAN);
    assertEquals(ResultType.BOOLEAN.getSqlTypeName(), SqlTypeName.BOOLEAN);
    assertEquals(ResultType.INT.getDataType(), DataType.INT);
    assertEquals(ResultType.INT.getSqlTypeName(), SqlTypeName.INTEGER);
    assertEquals(ResultType.LONG.getDataType(), DataType.LONG);
    assertEquals(ResultType.LONG.getSqlTypeName(), SqlTypeName.BIGINT);
    assertEquals(ResultType.FLOAT.getDataType(), DataType.FLOAT);
    assertEquals(ResultType.FLOAT.getSqlTypeName(), SqlTypeName.REAL);
    assertEquals(ResultType.DOUBLE.getDataType(), DataType.DOUBLE);
    assertEquals(ResultType.DOUBLE.getSqlTypeName(), SqlTypeName.DOUBLE);
    assertEquals(ResultType.BIG_DECIMAL.getDataType(), DataType.BIG_DECIMAL);
    assertEquals(ResultType.BIG_DECIMAL.getSqlTypeName(), SqlTypeName.DECIMAL);
    assertEquals(ResultType.STRING.getDataType(), DataType.STRING);
    assertEquals(ResultType.STRING.getSqlTypeName(), SqlTypeName.VARCHAR);
    assertEquals(ResultType.BYTES.getDataType(), DataType.BYTES);
    assertEquals(ResultType.BYTES.getSqlTypeName(), SqlTypeName.VARBINARY);
    assertEquals(ResultType.UUID.getDataType(), DataType.UUID);
    assertEquals(ResultType.UUID.getSqlTypeName(), SqlTypeName.UUID);
    assertEquals(ResultType.TIMESTAMP.getDataType(), DataType.TIMESTAMP);
    assertEquals(ResultType.TIMESTAMP.getSqlTypeName(), SqlTypeName.TIMESTAMP);
    assertEquals(ResultType.VARIANT.getDataType(), DataType.VARIANT);
    assertEquals(ResultType.VARIANT.getSqlTypeName(), SqlTypeName.VARIANT);
    assertEquals(ResultType.JSON.getDataType(), DataType.JSON);
    assertEquals(ResultType.JSON.getSqlTypeName(), SqlTypeName.VARCHAR);
  }

  @Test
  public void testDirectBinaryPathExtractionAndPredicates() {
    byte[] variant = VariantUtils.parseJsonToVariant(
        "{\"eventType\":\"click\",\"items\":[{\"price\":12.5},null],\"active\":true}");

    assertEquals(VariantUtils.variantGet(variant, "$.eventType", "STRING"), "click");
    assertEquals((double) VariantUtils.variantGet(variant, "$.items[0].price", "DOUBLE"), 12.5);
    assertEquals(VariantUtils.variantGet(variant, "$.active", "BOOLEAN"), true);
    assertTrue(VariantUtils.variantExists(variant, "$.items[1]"));
    assertFalse(VariantUtils.variantExists(variant, "$.missing"));
    assertTrue(VariantUtils.isVariantNull(variant, "$.items[1]"));
    assertFalse(VariantUtils.isVariantNull(variant, "$.missing"));
    assertEquals(VariantUtils.variantTypeOf(variant, "$.items[0]"), "OBJECT");
    assertEquals(VariantUtils.variantTypeOf(variant, "$.items[0].price"), "DECIMAL");
  }

  @Test
  public void testStrictAndTolerantExtraction() {
    byte[] variant = VariantUtils.parseJsonToVariant("{\"eventType\":\"click\",\"score\":\"not-a-number\"}");

    assertNull(VariantUtils.variantGet(variant, "$.missing", "STRING"));
    assertThrows(IllegalArgumentException.class, () -> VariantUtils.variantGet(variant, "$.score", "DOUBLE"));
    assertNull(VariantUtils.tryVariantGet(variant, "$.missing", "STRING"));
    assertNull(VariantUtils.tryVariantGet(variant, "$.score", "DOUBLE"));
  }

  @Test
  public void testReusableResultStrictAndTolerantExtraction() {
    byte[] variant = VariantUtils.parseJsonToVariant("{\"value\":7,\"null\":null,\"text\":\"x\"}");
    VariantPath valuePath = VariantUtils.compilePath("$.value");
    ReusableResult result = new ReusableResult();

    assertTrue(VariantUtils.extractInto(variant, valuePath, ResultType.INT, result));
    assertEquals(result.getIntValue(), 7);
    assertFalse(VariantUtils.extractInto(variant, VariantUtils.compilePath("$.missing"), ResultType.INT, result));
    assertFalse(VariantUtils.extractInto(variant, VariantUtils.compilePath("$.null"), ResultType.INT, result));
    assertThrows(IllegalArgumentException.class,
        () -> VariantUtils.extractInto(variant, VariantUtils.compilePath("$.text"), ResultType.DOUBLE, result));
    assertFalse(
        VariantUtils.tryExtractInto(variant, VariantUtils.compilePath("$.text"), ResultType.DOUBLE, result));
    assertFalse(VariantUtils.tryExtractInto(new byte[]{1}, valuePath, ResultType.INT, result));
    assertThrows(NullPointerException.class, () -> VariantUtils.extractInto(
        variant, valuePath, ResultType.INT, null));
    assertThrows(NullPointerException.class, () -> VariantUtils.tryExtractInto(
        variant, valuePath, ResultType.INT, null));
  }

  @Test
  public void testReusableTolerantHeterogeneousMismatchesAndNumericRange() {
    byte[][] rows = {
        VariantUtils.parseJsonToVariant("{\"value\":\"not-an-int\"}"),
        VariantUtils.parseJsonToVariant("{\"value\":true}"),
        VariantUtils.parseJsonToVariant("{\"value\":{}}"),
        VariantUtils.parseJsonToVariant("{\"value\":[]}"),
        VariantUtils.parseJsonToVariant("{\"value\":2147483648}"),
        VariantUtils.parseJsonToVariant("{\"value\":-2147483649}"),
        VariantUtils.parseJsonToVariant("{\"value\":1.5}"),
        VariantUtils.parseJsonToVariant("{\"value\":9223372036854775808}"),
        VariantUtils.parseJsonToVariant("{\"value\":-9223372036854775809}")
    };
    ResultType[] resultTypes = {
        ResultType.INT,
        ResultType.INT,
        ResultType.INT,
        ResultType.INT,
        ResultType.INT,
        ResultType.INT,
        ResultType.INT,
        ResultType.LONG,
        ResultType.LONG
    };
    VariantPath path = VariantUtils.compilePath("$.value");
    ReusableResult result = new ReusableResult();

    for (int i = 0; i < rows.length; i++) {
      assertFalse(VariantUtils.tryExtractInto(rows[i], path, resultTypes[i], result),
          "Expected tolerant conversion to reject row " + i);
    }

    assertThrows(IllegalArgumentException.class,
        () -> VariantUtils.extractInto(rows[0], path, ResultType.INT, result));
    assertThrows(ArithmeticException.class,
        () -> VariantUtils.extractInto(rows[4], path, ResultType.INT, result));
    assertThrows(ArithmeticException.class,
        () -> VariantUtils.extractInto(rows[7], path, ResultType.LONG, result));

    byte[] valid = VariantUtils.parseJsonToVariant("{\"value\":17}");
    assertTrue(VariantUtils.tryExtractInto(valid, path, ResultType.INT, result));
    assertEquals(result.getIntValue(), 17);
  }

  @Test
  public void testReusableResultParityForEveryResultType() {
    VariantBuilder builder = new VariantBuilder();
    builder.appendBoolean(true);
    assertReusableParity(encode(builder), ResultType.BOOLEAN);

    builder = new VariantBuilder();
    builder.appendInt(-17);
    assertReusableParity(encode(builder), ResultType.INT);

    builder = new VariantBuilder();
    builder.appendLong(9_876_543_210L);
    assertReusableParity(encode(builder), ResultType.LONG);

    builder = new VariantBuilder();
    builder.appendFloat(1.25F);
    assertReusableParity(encode(builder), ResultType.FLOAT);

    builder = new VariantBuilder();
    builder.appendDouble(-123.5D);
    assertReusableParity(encode(builder), ResultType.DOUBLE);

    builder = new VariantBuilder();
    builder.appendDecimal(new BigDecimal("12345678901234567890.1234"));
    assertReusableParity(encode(builder), ResultType.BIG_DECIMAL);

    builder = new VariantBuilder();
    builder.appendString("a UTF-8 value \uD83D\uDE00");
    assertReusableParity(encode(builder), ResultType.STRING);

    builder = new VariantBuilder();
    builder.appendBinary(ByteBuffer.wrap(new byte[]{0, 1, -1, 42}));
    assertReusableParity(encode(builder), ResultType.BYTES);

    builder = new VariantBuilder();
    builder.appendUUID(UUID.fromString("00112233-4455-6677-8899-aabbccddeeff"));
    assertReusableParity(encode(builder), ResultType.UUID);

    builder = new VariantBuilder();
    builder.appendTimestampNanosTz(-1_234_567_890L);
    assertReusableParity(encode(builder), ResultType.TIMESTAMP);

    byte[] nested = VariantUtils.parseJsonToVariant("{\"payload\":{\"count\":7}}");
    assertReusableParity(nested, VariantUtils.compilePath("$.payload"), ResultType.VARIANT);
    assertReusableParity(nested, VariantUtils.compilePath("$.payload"), ResultType.JSON);
  }

  @Test
  public void testReusableNumericAndTemporalEncodingParity() {
    VariantBuilder builder = new VariantBuilder();
    builder.appendByte((byte) -8);
    byte[] byteValue = encode(builder);
    assertReusableParity(byteValue, ResultType.INT);
    assertReusableParity(byteValue, ResultType.LONG);
    assertReusableParity(byteValue, ResultType.FLOAT);
    assertReusableParity(byteValue, ResultType.DOUBLE);
    assertReusableParity(byteValue, ResultType.BIG_DECIMAL);

    builder = new VariantBuilder();
    builder.appendShort((short) 32_000);
    assertReusableParity(encode(builder), ResultType.INT);

    builder = new VariantBuilder();
    builder.appendDecimal(new BigDecimal("123.45"));
    assertReusableParity(encode(builder), ResultType.BIG_DECIMAL);

    builder = new VariantBuilder();
    builder.appendDecimal(new BigDecimal("123.00"));
    byte[] integralDecimal = encode(builder);
    assertReusableParity(integralDecimal, ResultType.INT);
    assertReusableParity(integralDecimal, ResultType.LONG);

    builder = new VariantBuilder();
    builder.appendDecimal(new BigDecimal("1234567890123.45"));
    assertReusableParity(encode(builder), ResultType.BIG_DECIMAL);

    builder = new VariantBuilder();
    builder.appendString("x".repeat(128));
    assertReusableParity(encode(builder), ResultType.STRING);

    builder = new VariantBuilder();
    builder.appendDate(-1);
    byte[] dateValue = encode(builder);
    assertReusableParity(dateValue, ResultType.TIMESTAMP);
    assertEquals(((Timestamp) VariantUtils.variantGet(dateValue, "$", "TIMESTAMP")).getTime(),
        -TimeUnit.DAYS.toMillis(1), "DATE conversion must use UTC epoch days");

    builder = new VariantBuilder();
    builder.appendTimestampTz(-1_234_567L);
    assertReusableParity(encode(builder), ResultType.TIMESTAMP);

    builder = new VariantBuilder();
    builder.appendTimestampNtz(1_234_567L);
    assertReusableParity(encode(builder), ResultType.TIMESTAMP);

    builder = new VariantBuilder();
    builder.appendTimestampNanosNtz(-1_234_567L);
    assertReusableParity(encode(builder), ResultType.TIMESTAMP);
  }

  @Test
  public void testDecimalPrimitiveConversionForEveryEncoding() {
    assertDecimalFloatingPointParity("123.45", Variant.Type.DECIMAL4);
    assertDecimalFloatingPointParity("-9999999.99", Variant.Type.DECIMAL4);
    assertDecimalFloatingPointParity("1234567890.12345678", Variant.Type.DECIMAL8);
    assertDecimalFloatingPointParity("-999999999999999999", Variant.Type.DECIMAL8);
    assertDecimalFloatingPointParity("-0.0293221387768523759", Variant.Type.DECIMAL8);
    assertDecimalFloatingPointParity("12345678901234567890.123456789012345678", Variant.Type.DECIMAL16);
    assertDecimalFloatingPointParity("-99999999999999999999999999999999999999", Variant.Type.DECIMAL16);
    assertDecimalFloatingPointParity("-2276255542851026358.8820475617538817020", Variant.Type.DECIMAL16);
    assertDecimalFloatingPointParity("1E-255", Variant.Type.DECIMAL4);
    assertDecimalFloatingPointParity("-1E-255", Variant.Type.DECIMAL4);

    assertDecimalIntegralParity("123.00", Variant.Type.DECIMAL4, 123L);
    assertDecimalIntegralParity("123.0000000000", Variant.Type.DECIMAL8, 123L);
    assertDecimalIntegralParity("123.0000000000000000000", Variant.Type.DECIMAL16, 123L);
    assertDecimalIntegralParity("9223372036854775807.00", Variant.Type.DECIMAL16, Long.MAX_VALUE);
    assertDecimalIntegralParity("-9223372036854775808.00", Variant.Type.DECIMAL16, Long.MIN_VALUE);

    assertDecimalIntegralFailure("123.45", Variant.Type.DECIMAL4);
    assertDecimalIntegralFailure("123.0000000001", Variant.Type.DECIMAL8);
    assertDecimalIntegralFailure("123.0000000000000000001", Variant.Type.DECIMAL16);
    assertDecimalIntegralFailure("9223372036854775808.00", Variant.Type.DECIMAL16);
    assertDecimalIntegralFailure("-9223372036854775809.00", Variant.Type.DECIMAL16);
  }

  @Test
  public void testDecimalFloatingPointParityRandomized() {
    Random random = new Random(0x5EED_C0DEL);
    for (int i = 0; i < 500; i++) {
      int decimal4 = random.nextInt(999_999_999) + 1;
      assertDecimalFloatingPointParity(random.nextBoolean() ? Integer.toString(decimal4) : "-" + decimal4,
          Variant.Type.DECIMAL4, random.nextInt(1 << Byte.SIZE));

      long decimal8 = (random.nextLong() & Long.MAX_VALUE) % 999_000_000_000_000_000L + 1_000_000_000L;
      assertDecimalFloatingPointParity(random.nextBoolean() ? Long.toString(decimal8) : "-" + decimal8,
          Variant.Type.DECIMAL8, random.nextInt(1 << Byte.SIZE));

      BigInteger decimal16 = new BigInteger(126, random).add(BigInteger.TEN.pow(18));
      if (random.nextBoolean()) {
        decimal16 = decimal16.negate();
      }
      assertDecimalFloatingPointParity(decimal16.toString(), Variant.Type.DECIMAL16, random.nextInt(39));
    }
  }

  @Test
  public void testDateToTimestampInt32BoundaryDoesNotOverflow() {
    VariantBuilder builder = new VariantBuilder();
    builder.appendDate(Integer.MAX_VALUE);
    byte[] dateValue = encode(builder);
    VariantPath rootPath = VariantUtils.compilePath("$");
    ReusableResult result = new ReusableResult();
    long expectedMillis = Math.multiplyExact((long) Integer.MAX_VALUE, TimeUnit.DAYS.toMillis(1));

    assertEquals(((Timestamp) VariantUtils.variantGet(dateValue, "$", "TIMESTAMP")).getTime(), expectedMillis);
    assertEquals(((Timestamp) VariantUtils.tryVariantGet(dateValue, "$", "TIMESTAMP")).getTime(), expectedMillis);
    assertTrue(VariantUtils.extractInto(dateValue, rootPath, ResultType.TIMESTAMP, result));
    assertEquals(result.getLongValue(), expectedMillis);
    assertTrue(VariantUtils.tryExtractInto(dateValue, rootPath, ResultType.TIMESTAMP, result));
    assertEquals(result.getLongValue(), expectedMillis);
  }

  @Test
  public void testCursorConstantsMatchParquetVariantCodec()
      throws ReflectiveOperationException {
    Class<?> parquetVariantUtil = Class.forName("org.apache.parquet.variant.VariantUtil");
    String[][] constantNames = {
        {"VARIANT_BASIC_TYPE_MASK", "BASIC_TYPE_MASK"},
        {"VARIANT_PRIMITIVE_TYPE_MASK", "PRIMITIVE_TYPE_MASK"},
        {"VARIANT_PRIMITIVE", "PRIMITIVE"},
        {"VARIANT_SHORT_STRING", "SHORT_STR"},
        {"VARIANT_OBJECT", "OBJECT"},
        {"VARIANT_ARRAY", "ARRAY"},
        {"VARIANT_NULL", "NULL"},
        {"VARIANT_TRUE", "TRUE"},
        {"VARIANT_FALSE", "FALSE"},
        {"VARIANT_INT8", "INT8"},
        {"VARIANT_INT16", "INT16"},
        {"VARIANT_INT32", "INT32"},
        {"VARIANT_INT64", "INT64"},
        {"VARIANT_DOUBLE", "DOUBLE"},
        {"VARIANT_DECIMAL4", "DECIMAL4"},
        {"VARIANT_DECIMAL8", "DECIMAL8"},
        {"VARIANT_DECIMAL16", "DECIMAL16"},
        {"VARIANT_DATE", "DATE"},
        {"VARIANT_TIMESTAMP_TZ", "TIMESTAMP_TZ"},
        {"VARIANT_TIMESTAMP_NTZ", "TIMESTAMP_NTZ"},
        {"VARIANT_FLOAT", "FLOAT"},
        {"VARIANT_BINARY", "BINARY"},
        {"VARIANT_LONG_STRING", "LONG_STR"},
        {"VARIANT_TIME", "TIME"},
        {"VARIANT_TIMESTAMP_NANOS_TZ", "TIMESTAMP_NANOS_TZ"},
        {"VARIANT_TIMESTAMP_NANOS_NTZ", "TIMESTAMP_NANOS_NTZ"},
        {"VARIANT_UUID", "UUID"},
        {"VARIANT_METADATA_VERSION_MASK", "VERSION_MASK"},
        {"VARIANT_METADATA_VERSION", "VERSION"}
    };
    for (String[] constantName : constantNames) {
      assertEquals(readPrivateNumericConstant(VariantUtils.class, constantName[0]),
          readPrivateNumericConstant(parquetVariantUtil, constantName[1]),
          constantName[0] + " must stay synchronized with parquet-variant " + constantName[1]);
    }
    assertEquals(readPrivateNumericConstant(VariantUtils.class, "OBJECT_BINARY_SEARCH_THRESHOLD"),
        readPrivateNumericConstant(Variant.class, "BINARY_SEARCH_THRESHOLD"));
  }

  @Test
  public void testUnknownParquetPrimitiveTypesFailClosed() {
    int firstUnassignedV1PrimitiveType = 21;
    byte unknownHeader = (byte) (firstUnassignedV1PrimitiveType << 2);

    byte[] rootValue = VariantUtils.parseJsonToVariant("1");
    int rootValueOffset = VariantEnvelope.HEADER_SIZE + readBigEndianInt(rootValue, 8);
    rootValue[rootValueOffset] = unknownHeader;
    UnsupportedOperationException rootException = expectThrows(UnsupportedOperationException.class,
        () -> VariantUtils.variantTypeOf(rootValue));
    assertTrue(rootException.getMessage().contains("Unsupported Parquet Variant primitive type: 21"));
    assertNull(VariantUtils.tryVariantGet(rootValue, "$", "INT"));

    byte[] nestedValue = VariantUtils.parseJsonToVariant("{\"a\":1}");
    nestedValue[firstObjectValueOffset(nestedValue)] = unknownHeader;
    UnsupportedOperationException nestedException = expectThrows(UnsupportedOperationException.class,
        () -> VariantUtils.variantGet(nestedValue, "$.a", "INT"));
    assertTrue(nestedException.getMessage().contains("Unsupported Parquet Variant primitive type: 21"));
    assertNull(VariantUtils.tryVariantGet(nestedValue, "$.a", "INT"));
  }

  @Test
  public void testReusableTolerantNonFiniteNumericParity() {
    VariantBuilder builder = new VariantBuilder();
    builder.appendDouble(Double.POSITIVE_INFINITY);
    byte[] positiveInfinity = encode(builder);
    ReusableResult result = new ReusableResult();
    VariantPath rootPath = VariantUtils.compilePath("$");

    assertTrue(VariantUtils.tryExtractInto(positiveInfinity, rootPath, ResultType.FLOAT, result));
    assertEquals(result.getFloatValue(), Float.POSITIVE_INFINITY);
    assertTrue(VariantUtils.tryExtractInto(positiveInfinity, rootPath, ResultType.DOUBLE, result));
    assertEquals(result.getDoubleValue(), Double.POSITIVE_INFINITY);
    assertThrows(NumberFormatException.class,
        () -> VariantUtils.extractInto(positiveInfinity, rootPath, ResultType.BIG_DECIMAL, result));
    assertFalse(VariantUtils.tryExtractInto(positiveInfinity, rootPath, ResultType.BIG_DECIMAL, result));
  }

  @Test
  public void testReusableNestedNavigationAndCompiledPredicates() {
    byte[] variant = VariantUtils.parseJsonToVariant(
        "{\"\uD83D\uDE00\":{\"items\":[{\"value\":11},null]},\"empty\":\"\"}");
    VariantPath valuePath = VariantUtils.compilePath("$.\uD83D\uDE00.items[0].value");
    VariantPath nullPath = VariantUtils.compilePath("$.\uD83D\uDE00.items[1]");
    VariantPath missingPath = VariantUtils.compilePath("$.\uD83D\uDE00.items[2]");
    ReusableResult result = new ReusableResult();

    assertTrue(VariantUtils.extractInto(variant, valuePath, ResultType.INT, result));
    assertEquals(result.getIntValue(), 11);
    assertTrue(VariantUtils.extractInto(variant, VariantUtils.compilePath("$.empty"), ResultType.STRING, result));
    assertEquals(result.getStringValue(), "");

    assertEquals(VariantUtils.variantExists(variant, valuePath, result),
        VariantUtils.variantExists(variant, valuePath));
    assertEquals(VariantUtils.variantExists(variant, missingPath, result),
        VariantUtils.variantExists(variant, missingPath));
    assertEquals(VariantUtils.isVariantNull(variant, nullPath, result),
        VariantUtils.isVariantNull(variant, nullPath));
    assertEquals(VariantUtils.variantTypeOf(variant, valuePath, result),
        VariantUtils.variantTypeOf(variant, valuePath));
    assertNull(VariantUtils.variantExists(new byte[0], valuePath, result));
    assertFalse(VariantUtils.isVariantNull(new byte[0], nullPath, result));
    assertNull(VariantUtils.variantTypeOf(new byte[0], valuePath, result));
  }

  @DataProvider(name = "wideObjectSizes")
  public Object[][] wideObjectSizes() {
    return new Object[][]{{8}, {31}, {32}, {33}, {100}};
  }

  @Test(dataProvider = "wideObjectSizes")
  public void testWideObjectLookupMatchesParquetJava(int numFields) {
    VariantBuilder builder = new VariantBuilder();
    VariantObjectBuilder objectBuilder = builder.startObject();
    // Append in reverse order so the test relies on the encoded object's lexicographic field ordering, not insertion
    // order. Parquet switches from linear to binary lookup at 32 fields, which Pinot deliberately mirrors.
    for (int i = numFields - 1; i >= 0; i--) {
      objectBuilder.appendKey(wideField(i));
      objectBuilder.appendInt(i);
    }
    builder.endObject();
    Variant variant = builder.build();
    byte[] envelope = encode(variant);

    for (String key : List.of(wideField(0), wideField(numFields / 2), wideField(numFields - 1), "missing")) {
      Variant parquetValue = variant.getFieldByKey(key);
      Object expected = parquetValue != null ? parquetValue.getInt() : null;
      assertEquals(VariantUtils.variantGet(envelope, "$." + key, "INT"), expected);
      assertEquals(VariantUtils.tryVariantGet(envelope, "$." + key, "INT"), expected);
    }
  }

  @Test
  public void testWideObjectLookupUsesJavaUtf16Ordering() {
    String supplementary = "\uD800\uDC00";
    String supplementarySuccessor = "\uD800\uDC01";
    String privateUse = "\uE000";
    VariantBuilder builder = new VariantBuilder();
    VariantObjectBuilder objectBuilder = builder.startObject();
    for (int i = 29; i >= 0; i--) {
      objectBuilder.appendKey(wideField(i));
      objectBuilder.appendInt(i);
    }
    objectBuilder.appendKey(privateUse);
    objectBuilder.appendInt(2_000);
    objectBuilder.appendKey(supplementary);
    objectBuilder.appendInt(1_000);
    builder.endObject();
    Variant variant = builder.build();
    byte[] envelope = encode(variant);

    // Java String order places the supplementary key before U+E000, while unsigned UTF-8 byte order does the
    // opposite. Cross-checking parquet-java prevents an allocation-free byte comparator from changing semantics.
    for (String key : List.of(supplementary, supplementarySuccessor, privateUse)) {
      Variant parquetValue = variant.getFieldByKey(key);
      Object expected = parquetValue != null ? parquetValue.getInt() : null;
      assertEquals(VariantUtils.variantGet(envelope, "$." + key, "INT"), expected);
    }
  }

  @Test
  public void testWideObjectLookupSupportsArrowRustOrdering() {
    byte[] envelope = arrowRustOrderedWideObjectEnvelope();
    String supplementary = "\uD800\uDC00";
    String privateUse = "\uE000";

    assertEquals(VariantUtils.variantGet(envelope, "$." + supplementary, "INT"), 1_000);
    assertEquals(VariantUtils.tryVariantGet(envelope, "$." + supplementary, "INT"), 1_000);
    assertTrue(VariantUtils.variantExists(envelope, "$." + supplementary));
    assertEquals(VariantUtils.variantGet(envelope, "$." + privateUse, "INT"), 2_000);
    assertEquals(VariantUtils.tryVariantGet(envelope, "$." + privateUse, "INT"), 2_000);
    assertTrue(VariantUtils.variantExists(envelope, "$." + privateUse));
    assertNull(VariantUtils.variantGet(envelope, "$.missing", "INT"));
    assertNull(VariantUtils.tryVariantGet(envelope, "$.missing", "INT"));
    assertFalse(VariantUtils.variantExists(envelope, "$.missing"));
  }

  @Test
  public void testNonMonotonicObjectOffsets() {
    byte[] envelope = nonMonotonicObjectEnvelope();
    VariantEnvelope.Decoded decoded = VariantEnvelope.decode(envelope);
    Variant parquetVariant = new Variant(decoded.getValue(), decoded.getMetadata());

    for (int i = 0; i < 3; i++) {
      String key = String.valueOf((char) ('a' + i));
      int expected = i + 1;
      assertEquals(parquetVariant.getFieldByKey(key).getInt(), expected);
      assertEquals(VariantUtils.variantGet(envelope, "$." + key, "INT"), expected);
      assertEquals(VariantUtils.tryVariantGet(envelope, "$." + key, "INT"), expected);

      byte[] subtree = VariantUtils.variantGet(envelope, "$." + key);
      VariantEnvelope.Decoded decodedSubtree = VariantEnvelope.decode(subtree);
      assertEquals(decodedSubtree.getValue().remaining(), 2);
      assertEquals(new Variant(decodedSubtree.getValue(), decodedSubtree.getMetadata()).getInt(), expected);
      assertEquals(VariantUtils.variantToJson(subtree), Integer.toString(expected));
    }
    assertEquals(VariantUtils.variantToJson(envelope), "{\"a\":1,\"b\":2,\"c\":3}");
    assertFalse(VariantUtils.variantExists(envelope, "$.missing"));
  }

  @DataProvider(name = "nonMonotonicSubtreeEncodingCases")
  public Object[][] nonMonotonicSubtreeEncodingCases() {
    return new Object[][]{
        nonMonotonicSubtreeEncodingCase("object", Variant.Type.OBJECT, builder -> {
          VariantObjectBuilder nested = builder.startObject();
          nested.appendKey("inner");
          nested.appendInt(7);
          builder.endObject();
        }),
        nonMonotonicSubtreeEncodingCase("array", Variant.Type.ARRAY, builder -> {
          VariantBuilder nested = builder.startArray();
          nested.appendInt(7);
          nested.appendBoolean(true);
          builder.endArray();
        }),
        nonMonotonicSubtreeEncodingCase("null", Variant.Type.NULL, VariantBuilder::appendNull),
        nonMonotonicSubtreeEncodingCase("true", Variant.Type.BOOLEAN, builder -> builder.appendBoolean(true)),
        nonMonotonicSubtreeEncodingCase("false", Variant.Type.BOOLEAN, builder -> builder.appendBoolean(false)),
        nonMonotonicSubtreeEncodingCase("int8", Variant.Type.BYTE, builder -> builder.appendByte((byte) -8)),
        nonMonotonicSubtreeEncodingCase("int16", Variant.Type.SHORT,
            builder -> builder.appendShort((short) 32_000)),
        nonMonotonicSubtreeEncodingCase("int32", Variant.Type.INT, builder -> builder.appendInt(-123_456)),
        nonMonotonicSubtreeEncodingCase("int64", Variant.Type.LONG,
            builder -> builder.appendLong(9_876_543_210L)),
        nonMonotonicSubtreeEncodingCase("short-string", Variant.Type.STRING,
            builder -> builder.appendString("short")),
        nonMonotonicSubtreeEncodingCase("long-string", Variant.Type.STRING,
            builder -> builder.appendString("x".repeat(128))),
        nonMonotonicSubtreeEncodingCase("double", Variant.Type.DOUBLE,
            builder -> builder.appendDouble(-123.5D)),
        nonMonotonicSubtreeEncodingCase("decimal4", Variant.Type.DECIMAL4,
            builder -> builder.appendDecimal(new BigDecimal("12.34"))),
        nonMonotonicSubtreeEncodingCase("decimal8", Variant.Type.DECIMAL8,
            builder -> builder.appendDecimal(new BigDecimal("1234567890.12"))),
        nonMonotonicSubtreeEncodingCase("decimal16", Variant.Type.DECIMAL16,
            builder -> builder.appendDecimal(new BigDecimal("12345678901234567890.1234"))),
        nonMonotonicSubtreeEncodingCase("date", Variant.Type.DATE, builder -> builder.appendDate(1)),
        nonMonotonicSubtreeEncodingCase("timestamp-tz", Variant.Type.TIMESTAMP_TZ,
            builder -> builder.appendTimestampTz(1_234_567L)),
        nonMonotonicSubtreeEncodingCase("timestamp-ntz", Variant.Type.TIMESTAMP_NTZ,
            builder -> builder.appendTimestampNtz(1_234_567L)),
        nonMonotonicSubtreeEncodingCase("float", Variant.Type.FLOAT, builder -> builder.appendFloat(1.25F)),
        nonMonotonicSubtreeEncodingCase("binary", Variant.Type.BINARY,
            builder -> builder.appendBinary(ByteBuffer.wrap(new byte[]{0, 1, -1, 42}))),
        nonMonotonicSubtreeEncodingCase("time", Variant.Type.TIME,
            builder -> builder.appendTime(3_723_004_005L)),
        nonMonotonicSubtreeEncodingCase("timestamp-nanos-tz", Variant.Type.TIMESTAMP_NANOS_TZ,
            builder -> builder.appendTimestampNanosTz(1_234_567_891L)),
        nonMonotonicSubtreeEncodingCase("timestamp-nanos-ntz", Variant.Type.TIMESTAMP_NANOS_NTZ,
            builder -> builder.appendTimestampNanosNtz(1_234_567_891L)),
        nonMonotonicSubtreeEncodingCase("uuid", Variant.Type.UUID,
            builder -> builder.appendUUID(UUID.fromString("00112233-4455-6677-8899-aabbccddeeff")))
    };
  }

  @Test(dataProvider = "nonMonotonicSubtreeEncodingCases")
  public void testNonMonotonicSubtreeForEveryEncoding(String description, Variant.Type expectedType,
      int expectedLength, byte[] envelope) {
    assertTrue(hasNonMonotonicObjectOffsets(envelope), description);
    VariantEnvelope.Decoded decoded = VariantEnvelope.decode(envelope);
    Variant expected = new Variant(decoded.getValue(), decoded.getMetadata()).getFieldByKey("a");
    assertEquals(expected.getType(), expectedType, description);

    byte[] subtree = VariantUtils.variantGet(envelope, "$.a");
    byte[] tolerantSubtree = VariantUtils.tryVariantGet(envelope, "$.a");
    VariantEnvelope.Decoded decodedSubtree = VariantEnvelope.decode(subtree);
    byte[] actualValue = toByteArray(decodedSubtree.getValue());
    byte[] parquetValueAndTrailingData = toByteArray(expected.getValueBuffer());
    assertEquals(actualValue.length, expectedLength, description);
    assertTrue(Arrays.equals(actualValue, Arrays.copyOf(parquetValueAndTrailingData, expectedLength)), description);
    assertTrue(Arrays.equals(tolerantSubtree, subtree), description);

    byte[] expectedEnvelope = VariantEnvelope.encode(expected.getMetadataBuffer(), expected.getValueBuffer());
    assertEquals(VariantUtils.variantToJson(subtree), VariantUtils.variantToJson(expectedEnvelope), description);
  }

  @Test
  public void testNonMonotonicSubtreeRejectsTruncatedVariableLengthValue() {
    byte[] envelope = nonMonotonicSubtreeEnvelope(
        builder -> builder.appendBinary(ByteBuffer.wrap(new byte[]{1, 2, 3})));
    int metadataLength = readBigEndianInt(envelope, 8);
    int valueStart = VariantEnvelope.HEADER_SIZE + metadataLength;
    int valueHeader = Byte.toUnsignedInt(envelope[valueStart]);
    int typeInfo = valueHeader >>> 2 & 0x3F;
    int sizeBytes = ((typeInfo >>> 4) & 1) == 0 ? 1 : Integer.BYTES;
    int numElements = readUnsignedLittleEndian(envelope, valueStart + 1, sizeBytes);
    int idSize = ((typeInfo >>> 2) & 3) + 1;
    int offsetSize = (typeInfo & 3) + 1;
    int offsetStart = valueStart + 1 + sizeBytes + numElements * idSize;
    int dataStart = offsetStart + (numElements + 1) * offsetSize;
    int targetOffset = readUnsignedLittleEndian(envelope, offsetStart, offsetSize);
    int targetStart = dataStart + targetOffset;
    envelope[targetStart + 1] = 0x7F;
    envelope[targetStart + 2] = 0;
    envelope[targetStart + 3] = 0;
    envelope[targetStart + 4] = 0;

    assertThrows(IllegalArgumentException.class, () -> VariantUtils.variantGet(envelope, "$.a"));
    assertNull(VariantUtils.tryVariantGet(envelope, "$.a"));
  }

  @Test
  public void testReusableResultMalformedInputAndReuse() {
    VariantPath path = VariantUtils.compilePath("$.items[0]");
    ReusableResult result = new ReusableResult();
    byte[] first = VariantUtils.parseJsonToVariant("{\"items\":[7]}");
    byte[] second = VariantUtils.parseJsonToVariant("{\"items\":[9]}");

    assertTrue(VariantUtils.extractInto(first, path, ResultType.INT, result));
    assertEquals(result.getIntValue(), 7);
    assertFalse(VariantUtils.extractInto(first, VariantUtils.compilePath("$.missing"), ResultType.INT, result));

    byte[] badMagic = Arrays.copyOf(first, first.length);
    badMagic[0] = 0;
    assertThrows(IllegalArgumentException.class,
        () -> VariantUtils.extractInto(badMagic, path, ResultType.INT, result));
    assertFalse(VariantUtils.tryExtractInto(badMagic, path, ResultType.INT, result));

    byte[] badMetadataVersion = Arrays.copyOf(first, first.length);
    badMetadataVersion[VariantEnvelope.HEADER_SIZE] =
        (byte) ((badMetadataVersion[VariantEnvelope.HEADER_SIZE] & 0xF0) | 2);
    assertThrows(UnsupportedOperationException.class,
        () -> VariantUtils.extractInto(badMetadataVersion, path, ResultType.INT, result));
    assertFalse(VariantUtils.tryExtractInto(badMetadataVersion, path, ResultType.INT, result));

    byte[] badArrayOffset = VariantUtils.parseJsonToVariant("[7]");
    int valueOffset = VariantEnvelope.HEADER_SIZE + readBigEndianInt(badArrayOffset, 8);
    badArrayOffset[valueOffset + 3] = 0x7F;
    assertThrows(IllegalArgumentException.class,
        () -> VariantUtils.extractInto(badArrayOffset, VariantUtils.compilePath("$[0]"), ResultType.INT, result));
    assertFalse(VariantUtils.tryExtractInto(
        badArrayOffset, VariantUtils.compilePath("$[0]"), ResultType.INT, result));

    assertTrue(VariantUtils.extractInto(second, path, ResultType.INT, result));
    assertEquals(result.getIntValue(), 9);
  }

  @Test
  public void testSqlNullAndVariantNullRemainDistinct() {
    byte[] variantNull = VariantUtils.parseJsonToVariant("null");
    byte[] sqlNullPlaceholder = new byte[0];

    assertTrue(VariantUtils.isVariantNull(variantNull));
    assertEquals(VariantUtils.variantTypeOf(variantNull), "NULL");
    assertEquals(VariantUtils.variantToJson(variantNull), "null");
    assertTrue(VariantUtils.isVariantNull(VariantUtils.variantGet(variantNull, "$")));
    assertNull(VariantUtils.variantGet(variantNull, "$", "STRING"));

    assertFalse(VariantUtils.isVariantNull(sqlNullPlaceholder));
    assertNull(VariantUtils.variantTypeOf(sqlNullPlaceholder));
    assertNull(VariantUtils.variantToJson(sqlNullPlaceholder));
    assertNull(VariantUtils.variantGet(sqlNullPlaceholder, "$.anything", "STRING"));
  }

  @Test
  public void testParseRenderAndLogicalDataSchema() {
    byte[] variant = VariantUtils.parseJsonToVariant("{\"a\":[1,true,null],\"b\":\"text\"}");
    assertEquals(VariantUtils.variantToJson(variant), "{\"a\":[1,true,null],\"b\":\"text\"}");
    ReusableResult reusableResult = new ReusableResult();
    assertEquals(VariantUtils.variantToJson(variant, reusableResult), "{\"a\":[1,true,null],\"b\":\"text\"}");
    assertEquals(VariantUtils.variantToJson(VariantUtils.parseJsonToVariant("[2,3]"), reusableResult), "[2,3]");
    assertNull(VariantUtils.variantToJson(new byte[0], reusableResult));
    assertNull(VariantUtils.tryParseJsonToVariant("{not-json"));
    assertThrows(IllegalArgumentException.class, () -> VariantUtils.parseJsonToVariant("{not-json"));

    assertEquals(ColumnDataType.VARIANT.getStoredType(), ColumnDataType.BYTES);
    assertEquals(ColumnDataType.VARIANT.toDataType(), DataType.VARIANT);
    assertEquals(ColumnDataType.VARIANT.toExternal(new ByteArray(variant)), variant);
    assertEquals(ColumnDataType.VARIANT.convertAndFormat(new ByteArray(variant)),
        "{\"a\":[1,true,null],\"b\":\"text\"}");
  }

  @DataProvider(name = "variantJsonRenderingCases")
  public Object[][] variantJsonRenderingCases() {
    return new Object[][]{
        jsonRenderingCase(Variant.Type.OBJECT, VariantUtils.parseJsonToVariant("{\"a\":1}"), "{\"a\":1}"),
        jsonRenderingCase(Variant.Type.ARRAY, VariantUtils.parseJsonToVariant("[1,true]"), "[1,true]"),
        jsonRenderingCase(Variant.Type.NULL, "null", VariantBuilder::appendNull),
        jsonRenderingCase(Variant.Type.BOOLEAN, "true", builder -> builder.appendBoolean(true)),
        jsonRenderingCase(Variant.Type.BYTE, "-8", builder -> builder.appendByte((byte) -8)),
        jsonRenderingCase(Variant.Type.SHORT, "32000", builder -> builder.appendShort((short) 32_000)),
        jsonRenderingCase(Variant.Type.INT, "-123456", builder -> builder.appendInt(-123_456)),
        jsonRenderingCase(Variant.Type.LONG, "9876543210", builder -> builder.appendLong(9_876_543_210L)),
        jsonRenderingCase(Variant.Type.STRING, "\"text\"", builder -> builder.appendString("text")),
        jsonRenderingCase(Variant.Type.DOUBLE, "-123.5", builder -> builder.appendDouble(-123.5D)),
        jsonRenderingCase(Variant.Type.DECIMAL4, "12.34", builder -> builder.appendDecimal(new BigDecimal("12.34"))),
        jsonRenderingCase(Variant.Type.DECIMAL8, "1234567890.12",
            builder -> builder.appendDecimal(new BigDecimal("1234567890.12"))),
        jsonRenderingCase(Variant.Type.DECIMAL16, "12345678901234567890.1234",
            builder -> builder.appendDecimal(new BigDecimal("12345678901234567890.1234"))),
        jsonRenderingCase(Variant.Type.DATE, "\"1970-01-02\"", builder -> builder.appendDate(1)),
        jsonRenderingCase(Variant.Type.TIMESTAMP_TZ, "\"1970-01-01T00:00:01.234567Z\"",
            builder -> builder.appendTimestampTz(1_234_567L)),
        jsonRenderingCase(Variant.Type.TIMESTAMP_NTZ, "\"1970-01-01T00:00:01.234567\"",
            builder -> builder.appendTimestampNtz(1_234_567L)),
        jsonRenderingCase(Variant.Type.FLOAT, "1.25", builder -> builder.appendFloat(1.25F)),
        jsonRenderingCase(Variant.Type.BINARY, "\"AAH/Kg==\"",
            builder -> builder.appendBinary(ByteBuffer.wrap(new byte[]{0, 1, -1, 42}))),
        jsonRenderingCase(Variant.Type.TIME, "\"01:02:03.004005\"",
            builder -> builder.appendTime(3_723_004_005L)),
        jsonRenderingCase(Variant.Type.TIMESTAMP_NANOS_TZ, "\"1970-01-01T00:00:01.234567891Z\"",
            builder -> builder.appendTimestampNanosTz(1_234_567_891L)),
        jsonRenderingCase(Variant.Type.TIMESTAMP_NANOS_NTZ, "\"1970-01-01T00:00:01.234567891\"",
            builder -> builder.appendTimestampNanosNtz(1_234_567_891L)),
        jsonRenderingCase(Variant.Type.UUID, "\"00112233-4455-6677-8899-aabbccddeeff\"",
            builder -> builder.appendUUID(UUID.fromString("00112233-4455-6677-8899-aabbccddeeff")))
    };
  }

  @Test(dataProvider = "variantJsonRenderingCases")
  public void testVariantToJsonForEveryParquetType(Variant.Type expectedType, byte[] envelope, String expectedJson) {
    VariantEnvelope.Decoded decoded = VariantEnvelope.decode(envelope);
    assertEquals(new Variant(decoded.getValue(), decoded.getMetadata()).getType(), expectedType);
    assertEquals(VariantUtils.variantToJson(envelope), expectedJson);
    assertEquals(VariantUtils.variantToJson(envelope, new ReusableResult()), expectedJson);
  }

  @Test
  public void testVariantToJsonCoversEveryParquetTypeAndRejectsMalformedEnvelope() {
    Set<Variant.Type> coveredTypes = EnumSet.noneOf(Variant.Type.class);
    for (Object[] testCase : variantJsonRenderingCases()) {
      coveredTypes.add((Variant.Type) testCase[0]);
    }
    assertEquals(coveredTypes, EnumSet.allOf(Variant.Type.class));

    byte[] valid = VariantUtils.parseJsonToVariant("{\"a\":1}");
    byte[] badMagic = Arrays.copyOf(valid, valid.length);
    badMagic[0] = 0;
    ReusableResult reusableResult = new ReusableResult();
    assertThrows(IllegalArgumentException.class, () -> VariantUtils.variantToJson(badMagic));
    assertThrows(IllegalArgumentException.class, () -> VariantUtils.variantToJson(badMagic, reusableResult));
    assertEquals(VariantUtils.variantToJson(valid, reusableResult), "{\"a\":1}");
  }

  @Test
  public void testJsonIntegerBoundsAndBigIntegerFallback() {
    byte[] variant = VariantUtils.parseJsonToVariant(
        "{\"min\":-9223372036854775808,\"max\":9223372036854775807,"
            + "\"below\":-9223372036854775809,\"above\":9223372036854775808}");

    assertEquals(VariantUtils.variantTypeOf(variant, "$.min"), "LONG");
    assertEquals(VariantUtils.variantGet(variant, "$.min", "LONG"), Long.MIN_VALUE);
    assertEquals(VariantUtils.variantTypeOf(variant, "$.max"), "LONG");
    assertEquals(VariantUtils.variantGet(variant, "$.max", "LONG"), Long.MAX_VALUE);

    assertEquals(VariantUtils.variantTypeOf(variant, "$.below"), "DECIMAL");
    assertEquals(VariantUtils.variantGet(variant, "$.below", "BIG_DECIMAL"),
        new BigDecimal("-9223372036854775809"));
    assertEquals(VariantUtils.variantTypeOf(variant, "$.above"), "DECIMAL");
    assertEquals(VariantUtils.variantGet(variant, "$.above", "BIG_DECIMAL"),
        new BigDecimal("9223372036854775808"));
    assertEquals(VariantUtils.variantToJson(VariantUtils.variantGet(variant, "$.below")),
        "-9223372036854775809");
    assertEquals(VariantUtils.variantToJson(VariantUtils.variantGet(variant, "$.above")),
        "9223372036854775808");

    VariantPath minPath = VariantUtils.compilePath("$.min");
    VariantPath maxPath = VariantUtils.compilePath("$.max");
    VariantPath abovePath = VariantUtils.compilePath("$.above");
    ReusableResult result = new ReusableResult();
    assertTrue(VariantUtils.tryExtractInto(variant, minPath, ResultType.LONG, result));
    assertEquals(result.getLongValue(), Long.MIN_VALUE);
    assertTrue(VariantUtils.tryExtractInto(variant, maxPath, ResultType.LONG, result));
    assertEquals(result.getLongValue(), Long.MAX_VALUE);
    assertThrows(ArithmeticException.class,
        () -> VariantUtils.extractInto(variant, abovePath, ResultType.LONG, result));
    assertFalse(VariantUtils.tryExtractInto(variant, abovePath, ResultType.LONG, result));
  }

  @Test
  public void testJsonDecimalEncodingBoundsAndExponentNormalization() {
    String maxDecimal = "9".repeat(38);
    String minDecimal = "-" + maxDecimal;
    byte[] boundary = VariantUtils.parseJsonToVariant(
        "{\"max\":" + maxDecimal + ",\"min\":" + minDecimal + ",\"positiveExponent\":1e3,"
            + "\"negativeExponent\":-1.25e3,\"maxScale\":1e-38,\"trailingZeros\":1."
            + "0".repeat(39) + ",\"zeroExponent\":0e100000000}");

    assertEquals(VariantUtils.variantGet(boundary, "$.max", "BIG_DECIMAL"), new BigDecimal(maxDecimal));
    assertEquals(VariantUtils.variantGet(boundary, "$.min", "BIG_DECIMAL"), new BigDecimal(minDecimal));
    assertEquals(VariantUtils.variantGet(boundary, "$.positiveExponent", "BIG_DECIMAL"), new BigDecimal("1000"));
    assertEquals(VariantUtils.variantGet(boundary, "$.negativeExponent", "BIG_DECIMAL"), new BigDecimal("-1250"));
    assertEquals(VariantUtils.variantGet(boundary, "$.maxScale", "BIG_DECIMAL"), new BigDecimal("1e-38"));
    assertEquals(VariantUtils.variantGet(boundary, "$.trailingZeros", "BIG_DECIMAL"), BigDecimal.ONE);
    assertEquals(VariantUtils.variantGet(boundary, "$.zeroExponent", "BIG_DECIMAL"), BigDecimal.ZERO);

    String oversizedPositive = "9".repeat(39);
    String oversizedNegative = "-" + oversizedPositive;
    for (String unsupported
        : List.of(oversizedPositive, oversizedNegative, "1e38", "1e-39", "1e100000000")) {
      assertThrows(IllegalArgumentException.class, () -> VariantUtils.parseJsonToVariant(unsupported));
      assertNull(VariantUtils.tryParseJsonToVariant(unsupported));
    }
  }

  @Test
  public void testVariantSubtreeExtraction() {
    byte[] variant = VariantUtils.parseJsonToVariant("{\"payload\":{\"count\":7}}");
    byte[] subtree = VariantUtils.variantGet(variant, "$.payload");

    assertEquals(VariantUtils.variantToJson(subtree), "{\"count\":7}");
    assertEquals(VariantUtils.variantGet(subtree, "$.count", "INT"), 7);
    assertNull(VariantUtils.variantGet(variant, "$.missing"));
    assertNull(VariantUtils.tryVariantGet(variant, "$.missing"));
  }

  private static void assertReusableParity(byte[] envelope, ResultType resultType) {
    assertReusableParity(envelope, VariantUtils.compilePath("$"), resultType);
  }

  private static void assertDecimalFloatingPointParity(String text, Variant.Type expectedType) {
    BigDecimal decimal = new BigDecimal(text);
    assertDecimalFloatingPointParity(decimal, expectedType);
  }

  private static void assertDecimalFloatingPointParity(String unscaled, Variant.Type expectedType, int scale) {
    assertDecimalFloatingPointParity(new BigDecimal(new BigInteger(unscaled), scale), expectedType);
  }

  private static void assertDecimalFloatingPointParity(BigDecimal decimal, Variant.Type expectedType) {
    VariantBuilder builder = new VariantBuilder();
    builder.appendDecimal(decimal);
    Variant variant = builder.build();
    assertEquals(variant.getType(), expectedType, decimal.toString());
    byte[] envelope = encode(variant);
    VariantPath path = VariantUtils.compilePath("$");
    ReusableResult result = new ReusableResult();

    assertTrue(VariantUtils.extractInto(envelope, path, ResultType.FLOAT, result));
    assertEquals(Float.floatToRawIntBits(result.getFloatValue()), Float.floatToRawIntBits(decimal.floatValue()),
        decimal.toString());
    assertTrue(VariantUtils.tryExtractInto(envelope, path, ResultType.FLOAT, result));
    assertEquals(Float.floatToRawIntBits(result.getFloatValue()), Float.floatToRawIntBits(decimal.floatValue()),
        decimal.toString());
    assertTrue(VariantUtils.extractInto(envelope, path, ResultType.DOUBLE, result));
    assertEquals(Double.doubleToRawLongBits(result.getDoubleValue()), Double.doubleToRawLongBits(decimal.doubleValue()),
        decimal.toString());
    assertTrue(VariantUtils.tryExtractInto(envelope, path, ResultType.DOUBLE, result));
    assertEquals(Double.doubleToRawLongBits(result.getDoubleValue()), Double.doubleToRawLongBits(decimal.doubleValue()),
        decimal.toString());
  }

  private static void assertDecimalIntegralParity(String text, Variant.Type expectedType, long expected) {
    BigDecimal decimal = new BigDecimal(text);
    VariantBuilder builder = new VariantBuilder();
    builder.appendDecimal(decimal);
    Variant variant = builder.build();
    assertEquals(variant.getType(), expectedType, text);
    byte[] envelope = encode(variant);
    VariantPath path = VariantUtils.compilePath("$");
    ReusableResult result = new ReusableResult();

    assertTrue(VariantUtils.extractInto(envelope, path, ResultType.LONG, result));
    assertEquals(result.getLongValue(), expected, text);
    assertTrue(VariantUtils.tryExtractInto(envelope, path, ResultType.LONG, result));
    assertEquals(result.getLongValue(), expected, text);
    if (expected >= Integer.MIN_VALUE && expected <= Integer.MAX_VALUE) {
      assertTrue(VariantUtils.extractInto(envelope, path, ResultType.INT, result));
      assertEquals(result.getIntValue(), (int) expected, text);
      assertTrue(VariantUtils.tryExtractInto(envelope, path, ResultType.INT, result));
      assertEquals(result.getIntValue(), (int) expected, text);
    }
  }

  private static void assertDecimalIntegralFailure(String text, Variant.Type expectedType) {
    VariantBuilder builder = new VariantBuilder();
    builder.appendDecimal(new BigDecimal(text));
    Variant variant = builder.build();
    assertEquals(variant.getType(), expectedType, text);
    byte[] envelope = encode(variant);
    VariantPath path = VariantUtils.compilePath("$");
    ReusableResult result = new ReusableResult();

    assertFalse(VariantUtils.tryExtractInto(envelope, path, ResultType.LONG, result), text);
    assertThrows(text, ArithmeticException.class,
        () -> VariantUtils.extractInto(envelope, path, ResultType.LONG, result));
  }

  private static void assertReusableParity(byte[] envelope, VariantPath path, ResultType resultType) {
    Object expected = VariantUtils.variantGet(envelope, path, resultType);
    Object tolerantExpected = VariantUtils.tryVariantGet(envelope, path, resultType);
    ReusableResult result = new ReusableResult();
    assertTrue(VariantUtils.extractInto(envelope, path, resultType, result));
    assertTrue(VariantUtils.tryExtractInto(envelope, path, resultType, result));
    Object externalValue = result.toExternalValue(resultType);
    Object internalValue = result.toInternalValue(resultType);
    switch (resultType) {
      case BOOLEAN:
        assertEquals(result.getIntValue() != 0, expected);
        assertEquals(externalValue, expected);
        assertEquals(internalValue, (boolean) expected ? 1 : 0);
        break;
      case INT:
        assertEquals(result.getIntValue(), expected);
        assertEquals(externalValue, expected);
        assertEquals(internalValue, expected);
        break;
      case LONG:
        assertEquals(result.getLongValue(), expected);
        assertEquals(externalValue, expected);
        assertEquals(internalValue, expected);
        break;
      case FLOAT:
        assertEquals(result.getFloatValue(), expected);
        assertEquals(externalValue, expected);
        assertEquals(internalValue, expected);
        break;
      case DOUBLE:
        assertEquals(result.getDoubleValue(), expected);
        assertEquals(externalValue, expected);
        assertEquals(internalValue, expected);
        break;
      case BIG_DECIMAL:
        assertEquals(result.getBigDecimalValue(), expected);
        assertEquals(externalValue, expected);
        assertEquals(internalValue, expected);
        break;
      case STRING:
      case JSON:
        assertEquals(result.getStringValue(), expected);
        assertEquals(externalValue, expected);
        assertEquals(internalValue, expected);
        break;
      case BYTES:
      case VARIANT:
        assertTrue(Arrays.equals(result.getBytesValue(), (byte[]) expected));
        assertTrue(Arrays.equals((byte[]) externalValue, (byte[]) expected));
        assertSame(((ByteArray) internalValue).getBytes(), result.getBytesValue());
        assertTrue(Arrays.equals(((ByteArray) internalValue).getBytes(), (byte[]) expected));
        break;
      case UUID:
        assertEquals(result.getUuidValue(), expected);
        assertEquals(externalValue, expected);
        assertTrue(Arrays.equals(result.getBytesValue(), UuidUtils.toBytes((UUID) expected)));
        assertSame(((ByteArray) internalValue).getBytes(), result.getBytesValue());
        assertTrue(Arrays.equals(((ByteArray) internalValue).getBytes(), UuidUtils.toBytes((UUID) expected)));
        break;
      case TIMESTAMP:
        assertEquals(result.getLongValue(), ((Timestamp) expected).getTime());
        assertEquals(externalValue, expected);
        assertEquals(internalValue, ((Timestamp) expected).getTime());
        break;
      default:
        throw new IllegalStateException("Unhandled result type: " + resultType);
    }
    if (expected instanceof byte[]) {
      assertTrue(Arrays.equals((byte[]) tolerantExpected, (byte[]) expected));
    } else {
      assertEquals(tolerantExpected, expected);
    }
  }

  private static byte[] encode(VariantBuilder builder) {
    return encode(builder.build());
  }

  private static byte[] encode(Variant variant) {
    return VariantEnvelope.encode(variant.getMetadataBuffer(), variant.getValueBuffer());
  }

  private static String wideField(int index) {
    return String.format("field%03d", index);
  }

  /// Reorders a parquet-java object into Arrow Rust's Unicode scalar/UTF-8 key order while preserving each field's
  /// dictionary id, physical value offset, and shared metadata. The resulting 32-field envelope exercises a valid
  /// external ordering for which Java UTF-16 binary search alone is not authoritative.
  private static byte[] arrowRustOrderedWideObjectEnvelope() {
    String supplementary = "\uD800\uDC00";
    String privateUse = "\uE000";
    VariantBuilder builder = new VariantBuilder();
    VariantObjectBuilder objectBuilder = builder.startObject();
    for (int i = 0; i < 30; i++) {
      objectBuilder.appendKey(wideField(i));
      objectBuilder.appendInt(i);
    }
    objectBuilder.appendKey(supplementary);
    objectBuilder.appendInt(1_000);
    objectBuilder.appendKey(privateUse);
    objectBuilder.appendInt(2_000);
    builder.endObject();

    byte[] envelope = encode(builder);
    VariantEnvelope.Decoded decoded = VariantEnvelope.decode(envelope);
    Variant variant = new Variant(decoded.getValue(), decoded.getMetadata());
    byte[] value = toByteArray(decoded.getValue());
    int typeInfo = Byte.toUnsignedInt(value[0]) >>> 2 & 0x3F;
    int sizeBytes = ((typeInfo >>> 4) & 1) == 0 ? 1 : Integer.BYTES;
    int numElements = readUnsignedLittleEndian(value, 1, sizeBytes);
    int idSize = ((typeInfo >>> 2) & 3) + 1;
    int offsetSize = (typeInfo & 3) + 1;
    int idStart = 1 + sizeBytes;
    int offsetStart = idStart + numElements * idSize;

    String[] keys = new String[numElements];
    Integer[] arrowOrder = new Integer[numElements];
    for (int i = 0; i < numElements; i++) {
      keys[i] = variant.getFieldAtIndex(i).key;
      arrowOrder[i] = i;
    }
    Arrays.sort(arrowOrder, (left, right) -> compareUnsignedUtf8(keys[left], keys[right]));

    byte[] reorderedValue = Arrays.copyOf(value, value.length);
    for (int i = 0; i < numElements; i++) {
      int sourceIndex = arrowOrder[i];
      System.arraycopy(value, idStart + sourceIndex * idSize, reorderedValue, idStart + i * idSize, idSize);
      System.arraycopy(value, offsetStart + sourceIndex * offsetSize, reorderedValue,
          offsetStart + i * offsetSize, offsetSize);
    }
    return VariantEnvelope.encode(decoded.getMetadata(), ByteBuffer.wrap(reorderedValue));
  }

  private static int compareUnsignedUtf8(String left, String right) {
    byte[] leftBytes = left.getBytes(StandardCharsets.UTF_8);
    byte[] rightBytes = right.getBytes(StandardCharsets.UTF_8);
    int length = Math.min(leftBytes.length, rightBytes.length);
    for (int i = 0; i < length; i++) {
      int comparison = Integer.compare(Byte.toUnsignedInt(leftBytes[i]), Byte.toUnsignedInt(rightBytes[i]));
      if (comparison != 0) {
        return comparison;
      }
    }
    return Integer.compare(leftBytes.length, rightBytes.length);
  }

  private static Object[] nonMonotonicSubtreeEncodingCase(String description, Variant.Type expectedType,
      Consumer<VariantBuilder> appender) {
    VariantBuilder standaloneBuilder = new VariantBuilder();
    appender.accept(standaloneBuilder);
    int expectedLength = standaloneBuilder.build().getValueBuffer().remaining();
    return new Object[]{description, expectedType, expectedLength, nonMonotonicSubtreeEnvelope(appender)};
  }

  private static byte[] nonMonotonicSubtreeEnvelope(Consumer<VariantBuilder> appender) {
    VariantBuilder builder = new VariantBuilder();
    VariantObjectBuilder objectBuilder = builder.startObject();
    // Write z first and a second. The object index is key-sorted as a/z, while their physical values remain z/a.
    objectBuilder.appendKey("z");
    objectBuilder.appendByte((byte) 42);
    objectBuilder.appendKey("a");
    appender.accept(objectBuilder);
    builder.endObject();
    return reverseObjectPhysicalValues(encode(builder));
  }

  /// Reverses the physical value region of a canonical object while retaining its key-sorted ids and offset entries.
  /// This models conforming producers that choose a physical value order independent of the logical key order.
  private static byte[] reverseObjectPhysicalValues(byte[] envelope) {
    VariantEnvelope.Decoded decoded = VariantEnvelope.decode(envelope);
    byte[] value = toByteArray(decoded.getValue());
    int typeInfo = Byte.toUnsignedInt(value[0]) >>> 2 & 0x3F;
    int sizeBytes = ((typeInfo >>> 4) & 1) == 0 ? 1 : Integer.BYTES;
    int numElements = readUnsignedLittleEndian(value, 1, sizeBytes);
    int idSize = ((typeInfo >>> 2) & 3) + 1;
    int offsetSize = (typeInfo & 3) + 1;
    int offsetStart = 1 + sizeBytes + numElements * idSize;
    int dataStart = offsetStart + (numElements + 1) * offsetSize;
    int totalDataLength = readUnsignedLittleEndian(value, offsetStart + numElements * offsetSize, offsetSize);

    int[] offsets = new int[numElements + 1];
    for (int i = 0; i <= numElements; i++) {
      offsets[i] = readUnsignedLittleEndian(value, offsetStart + i * offsetSize, offsetSize);
    }
    byte[] reversedData = new byte[totalDataLength];
    int writeOffset = 0;
    for (int i = numElements - 1; i >= 0; i--) {
      int length = offsets[i + 1] - offsets[i];
      System.arraycopy(value, dataStart + offsets[i], reversedData, writeOffset, length);
      writeUnsignedLittleEndian(value, offsetStart + i * offsetSize, offsetSize, writeOffset);
      writeOffset += length;
    }
    System.arraycopy(reversedData, 0, value, dataStart, totalDataLength);
    return VariantEnvelope.encode(decoded.getMetadata(), ByteBuffer.wrap(value));
  }

  private static boolean hasNonMonotonicObjectOffsets(byte[] envelope) {
    byte[] value = toByteArray(VariantEnvelope.decode(envelope).getValue());
    int typeInfo = Byte.toUnsignedInt(value[0]) >>> 2 & 0x3F;
    int sizeBytes = ((typeInfo >>> 4) & 1) == 0 ? 1 : Integer.BYTES;
    int numElements = readUnsignedLittleEndian(value, 1, sizeBytes);
    int idSize = ((typeInfo >>> 2) & 3) + 1;
    int offsetSize = (typeInfo & 3) + 1;
    int offsetStart = 1 + sizeBytes + numElements * idSize;
    int previous = readUnsignedLittleEndian(value, offsetStart, offsetSize);
    for (int i = 1; i < numElements; i++) {
      int current = readUnsignedLittleEndian(value, offsetStart + i * offsetSize, offsetSize);
      if (current < previous) {
        return true;
      }
      previous = current;
    }
    return false;
  }

  private static byte[] toByteArray(ByteBuffer buffer) {
    ByteBuffer copy = buffer.duplicate();
    byte[] bytes = new byte[copy.remaining()];
    copy.get(bytes);
    return bytes;
  }

  private static int readUnsignedLittleEndian(byte[] bytes, int offset, int numBytes) {
    int value = 0;
    for (int i = 0; i < numBytes; i++) {
      value |= Byte.toUnsignedInt(bytes[offset + i]) << Byte.SIZE * i;
    }
    return value;
  }

  private static void writeUnsignedLittleEndian(byte[] bytes, int offset, int numBytes, int value) {
    for (int i = 0; i < numBytes; i++) {
      bytes[offset + i] = (byte) (value >>> Byte.SIZE * i);
    }
  }

  /// Returns a valid Variant object whose lexicographic fields a/b/c point to physically reversed int8 values.
  /// Object offsets are `[4, 2, 0, 6]`, which the Variant specification explicitly permits.
  private static byte[] nonMonotonicObjectEnvelope() {
    byte[] metadata = {0x11, 0x03, 0x00, 0x01, 0x02, 0x03, 'a', 'b', 'c'};
    byte[] value = {
        0x02, 0x03,             // object header, three fields
        0x00, 0x01, 0x02,       // metadata dictionary ids for a, b, c
        0x04, 0x02, 0x00, 0x06, // physical offsets for a, b, c, then total data length
        0x0C, 0x03,             // c = int8(3), at physical offset 0
        0x0C, 0x02,             // b = int8(2), at physical offset 2
        0x0C, 0x01              // a = int8(1), at physical offset 4
    };
    return VariantEnvelope.encode(metadata, 0, metadata.length, value, 0, value.length);
  }

  private static Object[] jsonRenderingCase(Variant.Type type, String expectedJson,
      Consumer<VariantBuilder> appender) {
    VariantBuilder builder = new VariantBuilder();
    appender.accept(builder);
    return jsonRenderingCase(type, encode(builder), expectedJson);
  }

  private static Object[] jsonRenderingCase(Variant.Type type, byte[] envelope, String expectedJson) {
    return new Object[]{type, envelope, expectedJson};
  }

  private static int firstObjectValueOffset(byte[] envelope) {
    int valueStart = VariantEnvelope.HEADER_SIZE + readBigEndianInt(envelope, 8);
    int typeInfo = Byte.toUnsignedInt(envelope[valueStart]) >>> 2 & 0x3F;
    int sizeBytes = ((typeInfo >>> 4) & 1) == 0 ? 1 : Integer.BYTES;
    int numElements = readUnsignedLittleEndian(envelope, valueStart + 1, sizeBytes);
    int idSize = ((typeInfo >>> 2) & 3) + 1;
    int offsetSize = (typeInfo & 3) + 1;
    int offsetStart = valueStart + 1 + sizeBytes + numElements * idSize;
    int dataStart = offsetStart + (numElements + 1) * offsetSize;
    return dataStart + readUnsignedLittleEndian(envelope, offsetStart, offsetSize);
  }

  private static int readPrivateNumericConstant(Class<?> declaringClass, String name)
      throws ReflectiveOperationException {
    Field field = declaringClass.getDeclaredField(name);
    field.setAccessible(true);
    return ((Number) field.get(null)).intValue();
  }

  private static int readBigEndianInt(byte[] bytes, int offset) {
    return Byte.toUnsignedInt(bytes[offset]) << 24
        | Byte.toUnsignedInt(bytes[offset + 1]) << 16
        | Byte.toUnsignedInt(bytes[offset + 2]) << 8
        | Byte.toUnsignedInt(bytes[offset + 3]);
  }

  /// The navigation memo reuses one cursor across rows; every transition it can take must resolve on the current
  /// row's bytes, never on the previous row's.
  /// Filler keys keep every object above the small-object threshold so the memo paths actually engage.
  @Test
  public void testReusableResultCrossRowMemoTransitions() {
    ReusableResult result = new ReusableResult();
    VariantPath path = VariantUtils.compilePath("$.b");

    // Row 1 memoizes id and entry index for "b" under dictionary [a, b].
    assertTrue(VariantUtils.extractInto(
        VariantUtils.parseJsonToVariant(
            "{\"a\":1,\"b\":2,\"f1\":0,\"f2\":0,\"f3\":0,\"f4\":0}"), path, ResultType.LONG, result));
    assertEquals(result.getLongValue(), 2L);

    // Identical layout: the entry-index hint hits.
    assertTrue(VariantUtils.extractInto(
        VariantUtils.parseJsonToVariant(
            "{\"a\":3,\"b\":4,\"f1\":0,\"f2\":0,\"f3\":0,\"f4\":0}"), path, ResultType.LONG, result));
    assertEquals(result.getLongValue(), 4L);

    // Different dictionary where "b" moves to id 0: the memoized id no longer spells "b", so the fresh search runs.
    assertTrue(VariantUtils.extractInto(
        VariantUtils.parseJsonToVariant(
            "{\"b\":5,\"g1\":0,\"g2\":0,\"g3\":0,\"g4\":0,\"g5\":0}"), path, ResultType.LONG, result));
    assertEquals(result.getLongValue(), 5L);

    // Dictionary [b, c] but hostile stale-hint shape: id 0 is now "b" and the object has an entry at the old hint.
    assertTrue(VariantUtils.extractInto(
        VariantUtils.parseJsonToVariant(
            "{\"b\":6,\"c\":99,\"h1\":0,\"h2\":0,\"h3\":0,\"h4\":0}"), path, ResultType.LONG, result));
    assertEquals(result.getLongValue(), 6L);

    // A dictionary that lacks "b" entirely: memoized as absent from the dictionary.
    assertFalse(VariantUtils.extractInto(
        VariantUtils.parseJsonToVariant(
            "{\"c\":7,\"i1\":0,\"i2\":0,\"i3\":0,\"i4\":0,\"i5\":0}"), path, ResultType.LONG, result));
    // Identical metadata again: the absent-from-dictionary memo answers without a search.
    assertFalse(VariantUtils.extractInto(
        VariantUtils.parseJsonToVariant(
            "{\"c\":8,\"i1\":0,\"i2\":0,\"i3\":0,\"i4\":0,\"i5\":0}"), path, ResultType.LONG, result));
    // "b" reappears after an absence memo: the metadata comparison must invalidate the miss.
    assertTrue(VariantUtils.extractInto(
        VariantUtils.parseJsonToVariant(
            "{\"b\":9,\"i1\":0,\"i2\":0,\"i3\":0,\"i4\":0,\"i5\":0}"), path, ResultType.LONG, result));
    assertEquals(result.getLongValue(), 9L);

    // Key in the dictionary but not in the probed object: memoizes the id, then a matching row still finds it.
    assertFalse(VariantUtils.extractInto(
        VariantUtils.parseJsonToVariant(
            "{\"nested\":{\"b\":1},\"z\":2,\"j1\":0,\"j2\":0,\"j3\":0,\"j4\":0}"), path, ResultType.LONG, result));
    assertFalse(VariantUtils.extractInto(
        VariantUtils.parseJsonToVariant(
            "{\"nested\":{\"b\":3},\"z\":4,\"j1\":0,\"j2\":0,\"j3\":0,\"j4\":0}"), path, ResultType.LONG, result));
    assertTrue(VariantUtils.extractInto(
        VariantUtils.parseJsonToVariant(
            "{\"nested\":{\"x\":1},\"b\":10,\"z\":5,\"j1\":0,\"j2\":0,\"j3\":0}"), path, ResultType.LONG,
        result));
    assertEquals(result.getLongValue(), 10L);

    // Switching the compiled path on the same cursor resets the memo.
    assertTrue(VariantUtils.extractInto(
        VariantUtils.parseJsonToVariant("{\"a\":11,\"b\":12,\"f1\":0,\"f2\":0,\"f3\":0,\"f4\":0}"),
        VariantUtils.compilePath("$.a"), ResultType.LONG, result));
    assertEquals(result.getLongValue(), 11L);
  }

  /// Same transitions across the binary-search threshold, where wide objects take the memo's id-scan paths.
  @Test
  public void testReusableResultCrossRowMemoWideObjects() {
    int numKeys = 64;
    ReusableResult result = new ReusableResult();
    VariantPath path = VariantUtils.compilePath("$.k40");

    StringBuilder full = new StringBuilder("{");
    for (int i = 0; i < numKeys; i++) {
      full.append(i > 0 ? "," : "").append("\"k").append(i).append("\":").append(100 + i);
    }
    String fullRow = full.append('}').toString();
    assertTrue(VariantUtils.extractInto(VariantUtils.parseJsonToVariant(fullRow), path, ResultType.LONG, result));
    assertEquals(result.getLongValue(), 140L);

    // Same shape, different values: hint hit.
    StringBuilder shifted = new StringBuilder("{");
    for (int i = 0; i < numKeys; i++) {
      shifted.append(i > 0 ? "," : "").append("\"k").append(i).append("\":").append(200 + i);
    }
    assertTrue(VariantUtils.extractInto(VariantUtils.parseJsonToVariant(shifted.append('}').toString()), path,
        ResultType.LONG, result));
    assertEquals(result.getLongValue(), 240L);

    // Same dictionary cannot be produced for a subset row by the JSON builder, so emulate a narrower object under a
    // changed dictionary: k40 absent from the object while other keys remain.
    StringBuilder without = new StringBuilder("{");
    boolean first = true;
    for (int i = 0; i < numKeys; i++) {
      if (i == 40) {
        continue;
      }
      without.append(first ? "" : ",").append("\"k").append(i).append("\":").append(300 + i);
      first = false;
    }
    assertFalse(VariantUtils.extractInto(VariantUtils.parseJsonToVariant(without.append('}').toString()), path,
        ResultType.LONG, result));

    // And it comes back.
    assertTrue(VariantUtils.extractInto(VariantUtils.parseJsonToVariant(fullRow), path, ResultType.LONG, result));
    assertEquals(result.getLongValue(), 140L);
  }

  /// Regression: a row that bypasses memo consultation (small object, non-object level, or failed navigation) must
  /// not launder a stale absent-from-dictionary verdict. The absence anchor is compared against the metadata it was
  /// proven under, never against the previous navigated row.
  @Test
  public void testAbsentMemoSurvivesRowsThatBypassTheMemo() {
    ReusableResult result = new ReusableResult();
    VariantPath path = VariantUtils.compilePath("$.b");

    // Rows 0 and 1 share metadata M1 lacking "b": the stability gate opens on row 1 and the absence verdict is
    // memoized, anchored to M1.
    assertFalse(VariantUtils.extractInto(
        VariantUtils.parseJsonToVariant("{\"c\":0,\"i1\":0,\"i2\":0,\"i3\":0,\"i4\":0,\"i5\":0}"),
        path, ResultType.LONG, result));
    assertFalse(VariantUtils.extractInto(
        VariantUtils.parseJsonToVariant("{\"c\":1,\"i1\":0,\"i2\":0,\"i3\":0,\"i4\":0,\"i5\":0}"),
        path, ResultType.LONG, result));
    // Row 2: top-level object at or below the small-object threshold; the memo is bypassed entirely, but its
    // metadata M2 matches row 3's. Nested keys push "b" and row 3's keys into the dictionary.
    assertTrue(VariantUtils.extractInto(
        VariantUtils.parseJsonToVariant("{\"b\":{\"k1\":1,\"k2\":2,\"k3\":3,\"k4\":4,\"nest\":5}}"),
        path, ResultType.VARIANT, result));
    // Row 3: wide object under metadata M2 that CONTAINS "b". A previous-row comparison would wrongly confirm the
    // stale absence; the anchored comparison against M1 must reject it and find the value.
    assertTrue(VariantUtils.extractInto(
        VariantUtils.parseJsonToVariant("{\"b\":9,\"k1\":1,\"k2\":2,\"k3\":3,\"k4\":4,\"nest\":0}"),
        path, ResultType.LONG, result));
    assertEquals(result.getLongValue(), 9L);

    // Same laundering attempt through a non-object row and a failed navigation.
    assertFalse(VariantUtils.extractInto(
        VariantUtils.parseJsonToVariant("{\"c\":2,\"i1\":0,\"i2\":0,\"i3\":0,\"i4\":0,\"i5\":0}"),
        path, ResultType.LONG, result));
    assertFalse(VariantUtils.extractInto(VariantUtils.parseJsonToVariant("42"), path, ResultType.LONG, result));
    assertTrue(VariantUtils.extractInto(
        VariantUtils.parseJsonToVariant("{\"b\":7,\"k1\":1,\"k2\":2,\"k3\":3,\"k4\":4,\"nest\":0}"),
        path, ResultType.LONG, result));
    assertEquals(result.getLongValue(), 7L);
  }
}
