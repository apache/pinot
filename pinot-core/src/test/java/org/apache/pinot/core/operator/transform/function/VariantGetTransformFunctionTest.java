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
package org.apache.pinot.core.operator.transform.function;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.parquet.variant.Variant;
import org.apache.parquet.variant.VariantBuilder;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.LiteralContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.utils.VariantUtils;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.utils.CommonConstants.NullValuePlaceHolder;
import org.apache.pinot.spi.utils.UuidUtils;
import org.apache.pinot.spi.utils.VariantEnvelope;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class VariantGetTransformFunctionTest {
  @Test
  public void testLiteralSqlNullRemainsSqlNull() {
    VariantGetTransformFunction function = new VariantGetTransformFunction();
    function.init(
        List.of(new LiteralTransformFunction(new LiteralContext(DataType.UNKNOWN, null)), stringLiteral("$")),
        Map.of(), true);
    ValueBlock block = valueBlock(2);

    assertEquals(function.transformToBytesValuesSV(block), new byte[][]{new byte[0], new byte[0]});
    assertEquals(function.getNullBitmap(block), RoaringBitmap.bitmapOf(0, 1));
  }

  @Test
  public void testMetadataAndTypedExtraction() {
    byte[] variant = VariantUtils.parseJsonToVariant("{\"eventType\":\"click\",\"score\":12.5}");
    ValueBlock block = valueBlock(1);
    BytesTransformFunction input = new BytesTransformFunction(new byte[][]{variant}, null);

    VariantGetTransformFunction stringFunction = new VariantGetTransformFunction();
    stringFunction.init(arguments(input, "$.eventType", "STRING"), Map.of(), true);
    assertEquals(stringFunction.getResultMetadata().getDataType(), DataType.STRING);
    assertEquals(stringFunction.transformToStringValuesSV(block)[0], "click");
    assertNull(stringFunction.getNullBitmap(block));

    VariantGetTransformFunction doubleFunction = new VariantGetTransformFunction();
    doubleFunction.init(arguments(input, "$.score", "DOUBLE"), Map.of(), true);
    assertEquals(doubleFunction.getResultMetadata().getDataType(), DataType.DOUBLE);
    assertEquals(doubleFunction.transformToDoubleValuesSV(block)[0], 12.5);
  }

  @Test
  public void testEverySupportedTargetType() {
    ValueBlock block = valueBlock(1);

    VariantGetTransformFunction function = typedFunction(variant(builder -> builder.appendBoolean(true)), "BOOLEAN");
    assertEquals(function.getResultMetadata().getDataType(), DataType.BOOLEAN);
    assertEquals(function.transformToIntValuesSV(block)[0], 1);

    function = typedFunction(variant(builder -> builder.appendInt(42)), "INT");
    assertEquals(function.getResultMetadata().getDataType(), DataType.INT);
    assertEquals(function.transformToIntValuesSV(block)[0], 42);

    function = typedFunction(variant(builder -> builder.appendLong(4_294_967_296L)), "LONG");
    assertEquals(function.getResultMetadata().getDataType(), DataType.LONG);
    assertEquals(function.transformToLongValuesSV(block)[0], 4_294_967_296L);

    function = typedFunction(variant(builder -> builder.appendFloat(1.25F)), "FLOAT");
    assertEquals(function.getResultMetadata().getDataType(), DataType.FLOAT);
    assertEquals(function.transformToFloatValuesSV(block)[0], 1.25F);

    function = typedFunction(variant(builder -> builder.appendDouble(12.5)), "DOUBLE");
    assertEquals(function.getResultMetadata().getDataType(), DataType.DOUBLE);
    assertEquals(function.transformToDoubleValuesSV(block)[0], 12.5);

    BigDecimal decimal = new BigDecimal("1234567890.12345");
    function = typedFunction(variant(builder -> builder.appendDecimal(decimal)), "BIG_DECIMAL");
    assertEquals(function.getResultMetadata().getDataType(), DataType.BIG_DECIMAL);
    assertEquals(function.transformToBigDecimalValuesSV(block)[0], decimal);

    function = typedFunction(variant(builder -> builder.appendString("click")), "STRING");
    assertEquals(function.getResultMetadata().getDataType(), DataType.STRING);
    assertEquals(function.transformToStringValuesSV(block)[0], "click");

    byte[] binary = new byte[]{0, 1, (byte) 0xFF};
    function = typedFunction(variant(builder -> builder.appendBinary(ByteBuffer.wrap(binary))), "BYTES");
    assertEquals(function.getResultMetadata().getDataType(), DataType.BYTES);
    assertEquals(function.transformToBytesValuesSV(block)[0], binary);

    UUID uuid = UUID.fromString("12345678-1234-5678-9abc-def012345678");
    function = typedFunction(variant(builder -> builder.appendUUID(uuid)), "UUID");
    assertEquals(function.getResultMetadata().getDataType(), DataType.UUID);
    byte[] uuidBytes = function.transformToBytesValuesSV(block)[0];
    assertTrue(UuidUtils.equals(uuidBytes, UuidUtils.toBytes(uuid)),
        "UUID extraction must directly expose the copied 16-byte value");
    assertEquals(UuidUtils.fromBytes(uuidBytes), uuid);

    long timestampMicros = 1_700_000_000_123_000L;
    function = typedFunction(variant(builder -> builder.appendTimestampTz(timestampMicros)), "TIMESTAMP");
    assertEquals(function.getResultMetadata().getDataType(), DataType.TIMESTAMP);
    assertEquals(function.transformToLongValuesSV(block)[0], 1_700_000_000_123L);

    function = typedFunction(variant(builder -> builder.appendString("nested")), "VARIANT");
    assertEquals(function.getResultMetadata().getDataType(), DataType.VARIANT);
    assertEquals(VariantUtils.variantToJson(function.transformToBytesValuesSV(block)[0]), "\"nested\"");

    function = typedFunction(variant(builder -> builder.appendString("json")), "JSON");
    assertEquals(function.getResultMetadata().getDataType(), DataType.JSON);
    assertEquals(function.transformToStringValuesSV(block)[0], "\"json\"");

    assertThrows(IllegalArgumentException.class,
        () -> typedFunction(variant(builder -> builder.appendInt(1)), "UNSUPPORTED"));
  }

  @Test
  public void testStrictMissingPathReturnsNullAndCastFailureThrows() {
    byte[] variant = VariantUtils.parseJsonToVariant("{\"eventType\":\"click\",\"score\":\"not-a-number\"}");
    VariantGetTransformFunction function = new VariantGetTransformFunction();
    function.init(arguments(new BytesTransformFunction(new byte[][]{variant}, null), "$.missing", "STRING"), Map.of(),
        true);

    ValueBlock block = valueBlock(1);
    assertEquals(function.transformToStringValuesSV(block), new String[]{""});
    assertEquals(function.getNullBitmap(block), RoaringBitmap.bitmapOf(0));

    VariantGetTransformFunction castFunction = new VariantGetTransformFunction();
    castFunction.init(
        arguments(new BytesTransformFunction(new byte[][]{variant}, null), "$.score", "DOUBLE"), Map.of(), true);
    assertThrows(IllegalArgumentException.class, () -> castFunction.transformToDoubleValuesSV(block));
  }

  @Test
  public void testDefaultVariantTarget() {
    byte[] variant = VariantUtils.parseJsonToVariant("{\"payload\":{\"count\":7},\"variantNull\":null}");
    ValueBlock block = valueBlock(1);
    VariantGetTransformFunction function = new VariantGetTransformFunction();
    function.init(arguments(new BytesTransformFunction(new byte[][]{variant}, null), "$.payload"), Map.of(), true);

    assertEquals(function.getResultMetadata().getDataType(), DataType.VARIANT);
    assertEquals(VariantUtils.variantToJson(function.transformToBytesValuesSV(block)[0]), "{\"count\":7}");

    VariantGetTransformFunction variantNullFunction = new VariantGetTransformFunction();
    variantNullFunction.init(
        arguments(new BytesTransformFunction(new byte[][]{variant}, null), "$.variantNull"), Map.of(), true);
    assertTrue(VariantUtils.isVariantNull(variantNullFunction.transformToBytesValuesSV(block)[0]));
    assertNull(variantNullFunction.getNullBitmap(block));
  }

  @Test
  public void testTryVariantGetNullBitmap() {
    byte[] populated = VariantUtils.parseJsonToVariant("{\"eventType\":\"click\"}");
    byte[] variantNull = VariantUtils.parseJsonToVariant("null");
    byte[] missing = VariantUtils.parseJsonToVariant("{\"other\":\"value\"}");
    RoaringBitmap inputNulls = RoaringBitmap.bitmapOf(3);
    BytesTransformFunction input =
        new BytesTransformFunction(new byte[][]{populated, variantNull, missing, new byte[0]}, inputNulls);

    VariantGetTransformFunction.Try function = new VariantGetTransformFunction.Try();
    function.init(arguments(input, "$.eventType", "STRING"), Map.of(), true);
    ValueBlock block = valueBlock(4);

    assertEquals(function.getResultMetadata().getDataType(), DataType.STRING);
    assertEquals(function.transformToStringValuesSV(block), new String[]{"click", "", "", ""});
    assertEquals(function.getNullBitmap(block), RoaringBitmap.bitmapOf(1, 2, 3));
  }

  @Test
  public void testMissingPathUsesTypedNullPlaceholders() {
    byte[] variant = VariantUtils.parseJsonToVariant("{}");
    ValueBlock block = valueBlock(1);

    VariantGetTransformFunction function = typedFunction(variant, "$.missing", "INT");
    assertEquals(function.transformToIntValuesSV(block)[0], NullValuePlaceHolder.INT);
    assertEquals(function.getNullBitmap(block), RoaringBitmap.bitmapOf(0));

    function = typedFunction(variant, "$.missing", "LONG");
    assertEquals(function.transformToLongValuesSV(block)[0], NullValuePlaceHolder.LONG);
    assertEquals(function.getNullBitmap(block), RoaringBitmap.bitmapOf(0));

    function = typedFunction(variant, "$.missing", "FLOAT");
    assertEquals(function.transformToFloatValuesSV(block)[0], NullValuePlaceHolder.FLOAT);
    assertEquals(function.getNullBitmap(block), RoaringBitmap.bitmapOf(0));

    function = typedFunction(variant, "$.missing", "DOUBLE");
    assertEquals(function.transformToDoubleValuesSV(block)[0], NullValuePlaceHolder.DOUBLE);
    assertEquals(function.getNullBitmap(block), RoaringBitmap.bitmapOf(0));

    function = typedFunction(variant, "$.missing", "BIG_DECIMAL");
    assertEquals(function.transformToBigDecimalValuesSV(block)[0], NullValuePlaceHolder.BIG_DECIMAL);
    assertEquals(function.getNullBitmap(block), RoaringBitmap.bitmapOf(0));

    function = typedFunction(variant, "$.missing", "STRING");
    assertEquals(function.transformToStringValuesSV(block)[0], NullValuePlaceHolder.STRING);
    assertEquals(function.getNullBitmap(block), RoaringBitmap.bitmapOf(0));

    function = typedFunction(variant, "$.missing", "BYTES");
    assertEquals(function.transformToBytesValuesSV(block)[0], NullValuePlaceHolder.BYTES);
    assertEquals(function.getNullBitmap(block), RoaringBitmap.bitmapOf(0));
  }

  @Test
  public void testVariantExtractionIsCachedPerBlock() {
    byte[] variant = VariantUtils.parseJsonToVariant("{\"eventType\":\"click\"}");
    BytesTransformFunction input = new BytesTransformFunction(new byte[][]{variant}, null);
    VariantGetTransformFunction function = new VariantGetTransformFunction();
    function.init(arguments(input, "$.eventType", "STRING"), Map.of(), true);

    ValueBlock block = valueBlock(1);
    assertNull(function.getNullBitmap(block));
    String[] values = function.transformToStringValuesSV(block);
    assertEquals(values[0], "click");
    assertNull(function.getNullBitmap(block));
    assertSame(function.transformToStringValuesSV(block), values);
    assertEquals(input._transformCalls, 1);
    assertEquals(input._nullBitmapCalls, 1);

    assertSame(function.transformToStringValuesSV(valueBlock(1)), values);
    assertEquals(input._transformCalls, 2);
    assertEquals(input._nullBitmapCalls, 2);
  }

  @Test
  public void testJsonParsingIsCachedPerBlock() {
    StringTransformFunction input =
        new StringTransformFunction(new String[]{"{\"answer\":42}", "null", "{not-json", ""},
            RoaringBitmap.bitmapOf(3));
    ParseJsonToVariantTransformFunction.Try function = new ParseJsonToVariantTransformFunction.Try();
    function.init(List.of(input), Map.of(), true);

    ValueBlock block = valueBlock(4);
    byte[][] values = function.transformToBytesValuesSV(block);
    assertEquals(VariantUtils.variantToJson(values[0]), "{\"answer\":42}");
    assertTrue(VariantUtils.isVariantNull(values[1]));
    assertEquals(function.getNullBitmap(block), RoaringBitmap.bitmapOf(2, 3));
    assertSame(function.transformToBytesValuesSV(block), values);
    assertEquals(input._transformCalls, 1);
    assertEquals(input._nullBitmapCalls, 1);

    ValueBlock nextBlock = valueBlock(4);
    function.getNullBitmap(nextBlock);
    assertSame(function.transformToBytesValuesSV(nextBlock), values);
    assertEquals(input._transformCalls, 2);
    assertEquals(input._nullBitmapCalls, 2);
  }

  @Test
  public void testLiteralJsonIsParsedAtInitializationAndReused() {
    ParseJsonToVariantTransformFunction strict = new ParseJsonToVariantTransformFunction();
    strict.init(List.of(stringLiteral("{\"answer\":42}")), Map.of(), true);

    byte[][] values = strict.transformToBytesValuesSV(valueBlock(3));
    assertEquals(VariantUtils.variantToJson(values[0]), "{\"answer\":42}");
    assertSame(values[1], values[0]);
    assertSame(values[2], values[0]);
    assertNull(strict.getNullBitmap(valueBlock(3)));
    assertSame(strict.transformToBytesValuesSV(valueBlock(1))[0], values[0],
        "The parsed literal must be reused across blocks");

    ParseJsonToVariantTransformFunction invalidStrict = new ParseJsonToVariantTransformFunction();
    assertThrows("Strict literal parsing must fail during initialization", IllegalArgumentException.class,
        () -> invalidStrict.init(List.of(stringLiteral("{not-json")), Map.of(), true));

    ParseJsonToVariantTransformFunction.Try invalidTolerant = new ParseJsonToVariantTransformFunction.Try();
    invalidTolerant.init(List.of(stringLiteral("{not-json")), Map.of(), true);
    ValueBlock invalidBlock = valueBlock(2);
    assertEquals(invalidTolerant.transformToBytesValuesSV(invalidBlock), new byte[][]{new byte[0], new byte[0]});
    assertEquals(invalidTolerant.getNullBitmap(invalidBlock), RoaringBitmap.bitmapOf(0, 1));

    ParseJsonToVariantTransformFunction sqlNull = new ParseJsonToVariantTransformFunction();
    sqlNull.init(List.of(new LiteralTransformFunction(new LiteralContext(DataType.UNKNOWN, null))), Map.of(), true);
    ValueBlock sqlNullBlock = valueBlock(2);
    assertEquals(sqlNull.transformToBytesValuesSV(sqlNullBlock), new byte[][]{new byte[0], new byte[0]});
    assertEquals(sqlNull.getNullBitmap(sqlNullBlock), RoaringBitmap.bitmapOf(0, 1));

    ParseJsonToVariantTransformFunction variantNull = new ParseJsonToVariantTransformFunction();
    variantNull.init(List.of(stringLiteral("null")), Map.of(), true);
    ValueBlock variantNullBlock = valueBlock(1);
    assertTrue(VariantUtils.isVariantNull(variantNull.transformToBytesValuesSV(variantNullBlock)[0]));
    assertNull(variantNull.getNullBitmap(variantNullBlock), "JSON null must remain distinct from SQL null");
  }

  @Test
  public void testStrictFailureInvalidatesPreviousBlockCache() {
    ValueBlock validBlock = valueBlock(1);
    ValueBlock invalidBlock = valueBlock(1);
    byte[] validVariant = VariantUtils.parseJsonToVariant("{\"eventType\":\"click\"}");
    BlockAwareBytesTransformFunction variantInput = new BlockAwareBytesTransformFunction(
        Map.of(validBlock, new byte[][]{validVariant}, invalidBlock, new byte[][]{new byte[]{1}}));
    VariantGetTransformFunction variantGet = new VariantGetTransformFunction();
    variantGet.init(arguments(variantInput, "$.eventType", "STRING"), Map.of(), true);

    assertEquals(variantGet.transformToStringValuesSV(validBlock)[0], "click");
    assertThrows(IllegalArgumentException.class, () -> variantGet.transformToStringValuesSV(invalidBlock));
    assertEquals(variantGet.transformToStringValuesSV(validBlock)[0], "click");
    assertEquals(variantInput._transformCalls, 3);

    BlockAwareStringTransformFunction jsonInput = new BlockAwareStringTransformFunction(
        Map.of(validBlock, new String[]{"{\"answer\":42}"}, invalidBlock, new String[]{"{not-json"}));
    ParseJsonToVariantTransformFunction parseJson = new ParseJsonToVariantTransformFunction();
    parseJson.init(List.of(jsonInput), Map.of(), true);

    assertEquals(VariantUtils.variantToJson(parseJson.transformToBytesValuesSV(validBlock)[0]), "{\"answer\":42}");
    assertThrows(IllegalArgumentException.class, () -> parseJson.transformToBytesValuesSV(invalidBlock));
    assertEquals(VariantUtils.variantToJson(parseJson.transformToBytesValuesSV(validBlock)[0]), "{\"answer\":42}");
    assertEquals(jsonInput._transformCalls, 3);
  }

  @Test
  public void testFactoryRegistrations() {
    Map<String, Class<? extends TransformFunction>> functions = TransformFunctionFactory.getAllFunctions();
    assertSame(functions.get(TransformFunctionFactory.canonicalize("variant_get")),
        VariantGetTransformFunction.class);
    assertSame(functions.get(TransformFunctionFactory.canonicalize("try_variant_get")),
        VariantGetTransformFunction.Try.class);
    assertSame(functions.get(TransformFunctionFactory.canonicalize("parse_json")),
        ParseJsonToVariantTransformFunction.class);
    assertSame(functions.get(TransformFunctionFactory.canonicalize("try_parse_json")),
        ParseJsonToVariantTransformFunction.Try.class);
    assertSame(functions.get(TransformFunctionFactory.canonicalize("parseJsonToVariant")),
        ParseJsonToVariantTransformFunction.class);
    assertSame(functions.get(TransformFunctionFactory.canonicalize("tryParseJsonToVariant")),
        ParseJsonToVariantTransformFunction.Try.class);
  }

  @Test
  public void testVariantFunctionsRequireQueryNullHandling() {
    List<String> expressions = List.of(
        "variant_get(parse_json('{}'), '$.value')",
        "try_variant_get(parse_json('{}'), '$.value')",
        "variant_exists(parse_json('{}'), '$.value')",
        "is_variant_null(parse_json('null'))",
        "variant_type_of(parse_json('{}'))",
        "variant_to_json(parse_json('{}'))",
        "parse_json('{}')",
        "parse_json_to_variant('{}')",
        "try_parse_json('{}')",
        "try_parse_json_to_variant('{}')");
    for (String expressionString : expressions) {
      ExpressionContext expression = RequestContextUtils.getExpression(expressionString);
      BadQueryRequestException exception = expectThrows(BadQueryRequestException.class,
          () -> TransformFunctionFactory.get(expression, Map.of()));
      assertTrue(exception.getMessage().contains("requires query null handling"),
          "Unexpected rejection for " + expressionString + ": " + exception.getMessage());
      TransformFunctionFactory.getNullHandlingEnabled(expression, Map.of());
    }
  }

  private static List<TransformFunction> arguments(TransformFunction input, String path) {
    return List.of(input, stringLiteral(path));
  }

  private static List<TransformFunction> arguments(TransformFunction input, String path, String targetType) {
    return List.of(input, stringLiteral(path), stringLiteral(targetType));
  }

  private static VariantGetTransformFunction typedFunction(byte[] variant, String targetType) {
    return typedFunction(variant, "$", targetType);
  }

  private static VariantGetTransformFunction typedFunction(byte[] variant, String path, String targetType) {
    VariantGetTransformFunction function = new VariantGetTransformFunction();
    function.init(arguments(new BytesTransformFunction(new byte[][]{variant}, null), path, targetType), Map.of(), true);
    return function;
  }

  private static byte[] variant(Consumer<VariantBuilder> writer) {
    VariantBuilder builder = new VariantBuilder();
    writer.accept(builder);
    Variant variant = builder.build();
    return VariantEnvelope.encode(variant.getMetadataBuffer(), variant.getValueBuffer());
  }

  private static LiteralTransformFunction stringLiteral(String value) {
    return new LiteralTransformFunction(new LiteralContext(DataType.STRING, value));
  }

  private static ValueBlock valueBlock(int numDocs) {
    ValueBlock valueBlock = mock(ValueBlock.class);
    when(valueBlock.getNumDocs()).thenReturn(numDocs);
    return valueBlock;
  }

  private static final class BytesTransformFunction extends BaseTransformFunction {
    private static final TransformResultMetadata RESULT_METADATA =
        new TransformResultMetadata(DataType.VARIANT, true, false);
    private final byte[][] _values;
    @Nullable
    private final RoaringBitmap _nullBitmap;
    private int _transformCalls;
    private int _nullBitmapCalls;

    private BytesTransformFunction(byte[][] values, @Nullable RoaringBitmap nullBitmap) {
      _values = values;
      _nullBitmap = nullBitmap;
    }

    @Override
    public String getName() {
      return "variantInput";
    }

    @Override
    public TransformResultMetadata getResultMetadata() {
      return RESULT_METADATA;
    }

    @Override
    public byte[][] transformToBytesValuesSV(ValueBlock valueBlock) {
      _transformCalls++;
      return _values;
    }

    @Nullable
    @Override
    public RoaringBitmap getNullBitmap(ValueBlock valueBlock) {
      _nullBitmapCalls++;
      return _nullBitmap;
    }
  }

  private static final class StringTransformFunction extends BaseTransformFunction {
    private static final TransformResultMetadata RESULT_METADATA =
        new TransformResultMetadata(DataType.STRING, true, false);
    private final String[] _values;
    @Nullable
    private final RoaringBitmap _nullBitmap;
    private int _transformCalls;
    private int _nullBitmapCalls;

    private StringTransformFunction(String[] values, @Nullable RoaringBitmap nullBitmap) {
      _values = values;
      _nullBitmap = nullBitmap;
    }

    @Override
    public String getName() {
      return "stringInput";
    }

    @Override
    public TransformResultMetadata getResultMetadata() {
      return RESULT_METADATA;
    }

    @Override
    public String[] transformToStringValuesSV(ValueBlock valueBlock) {
      _transformCalls++;
      return _values;
    }

    @Nullable
    @Override
    public RoaringBitmap getNullBitmap(ValueBlock valueBlock) {
      _nullBitmapCalls++;
      return _nullBitmap;
    }
  }

  private static final class BlockAwareBytesTransformFunction extends BaseTransformFunction {
    private static final TransformResultMetadata RESULT_METADATA =
        new TransformResultMetadata(DataType.VARIANT, true, false);
    private final IdentityHashMap<ValueBlock, byte[][]> _valuesByBlock;
    private int _transformCalls;

    private BlockAwareBytesTransformFunction(Map<ValueBlock, byte[][]> valuesByBlock) {
      _valuesByBlock = new IdentityHashMap<>(valuesByBlock);
    }

    @Override
    public String getName() {
      return "blockAwareVariantInput";
    }

    @Override
    public TransformResultMetadata getResultMetadata() {
      return RESULT_METADATA;
    }

    @Override
    public byte[][] transformToBytesValuesSV(ValueBlock valueBlock) {
      _transformCalls++;
      return _valuesByBlock.get(valueBlock);
    }

    @Nullable
    @Override
    public RoaringBitmap getNullBitmap(ValueBlock valueBlock) {
      return null;
    }
  }

  private static final class BlockAwareStringTransformFunction extends BaseTransformFunction {
    private static final TransformResultMetadata RESULT_METADATA =
        new TransformResultMetadata(DataType.STRING, true, false);
    private final IdentityHashMap<ValueBlock, String[]> _valuesByBlock;
    private int _transformCalls;

    private BlockAwareStringTransformFunction(Map<ValueBlock, String[]> valuesByBlock) {
      _valuesByBlock = new IdentityHashMap<>(valuesByBlock);
    }

    @Override
    public String getName() {
      return "blockAwareStringInput";
    }

    @Override
    public TransformResultMetadata getResultMetadata() {
      return RESULT_METADATA;
    }

    @Override
    public String[] transformToStringValuesSV(ValueBlock valueBlock) {
      _transformCalls++;
      return _valuesByBlock.get(valueBlock);
    }

    @Nullable
    @Override
    public RoaringBitmap getNullBitmap(ValueBlock valueBlock) {
      return null;
    }
  }
}
