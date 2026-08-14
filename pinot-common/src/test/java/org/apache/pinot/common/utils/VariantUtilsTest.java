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
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.parquet.variant.Variant;
import org.apache.parquet.variant.VariantBuilder;
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


public class VariantUtilsTest {
  @Test
  public void testRawVariantResultRequiresNullHandling() {
    DataSchema variantSchema =
        new DataSchema(new String[]{"payload"}, new ColumnDataType[]{ColumnDataType.VARIANT});
    DataSchema typedSchema =
        new DataSchema(new String[]{"eventType"}, new ColumnDataType[]{ColumnDataType.STRING});

    assertTrue(VariantUtils.requiresNullHandlingForRawVariantResult(variantSchema, false));
    assertFalse(VariantUtils.requiresNullHandlingForRawVariantResult(variantSchema, true));
    assertFalse(VariantUtils.requiresNullHandlingForRawVariantResult(typedSchema, false));
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

  private static void assertReusableParity(byte[] envelope, VariantPath path, ResultType resultType) {
    Object expected = VariantUtils.variantGet(envelope, path, resultType);
    Object tolerantExpected = VariantUtils.tryVariantGet(envelope, path, resultType);
    ReusableResult result = new ReusableResult();
    assertTrue(VariantUtils.extractInto(envelope, path, resultType, result));
    assertTrue(VariantUtils.tryExtractInto(envelope, path, resultType, result));
    Object externalValue = result.getExternalValue(resultType);
    Object internalValue = result.getInternalValue(resultType);
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
    Variant variant = builder.build();
    return VariantEnvelope.encode(variant.getMetadataBuffer(), variant.getValueBuffer());
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

  private static int readBigEndianInt(byte[] bytes, int offset) {
    return Byte.toUnsignedInt(bytes[offset]) << 24
        | Byte.toUnsignedInt(bytes[offset + 1]) << 16
        | Byte.toUnsignedInt(bytes[offset + 2]) << 8
        | Byte.toUnsignedInt(bytes[offset + 3]);
  }
}
