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
package org.apache.pinot.query.runtime.operator.operands;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.parquet.variant.Variant;
import org.apache.parquet.variant.VariantBuilder;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.VariantUtils;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.UuidUtils;
import org.apache.pinot.spi.utils.VariantEnvelope;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class VariantOperandTest {
  private static final DataSchema VARIANT_SCHEMA =
      new DataSchema(new String[]{"payload"}, new ColumnDataType[]{ColumnDataType.VARIANT});

  @Test
  public void testFactoryUsesSpecializedOperandAndPreservesNullSemantics() {
    List<Object> row = row(VariantUtils.parseJsonToVariant(
        "{\"name\":\"alice\",\"number\":7,\"presentNull\":null,\"nested\":{\"enabled\":true}}"));
    List<Object> missingRow = row(VariantUtils.parseJsonToVariant("{}"));
    List<Object> variantNullRow = row(VariantUtils.parseJsonToVariant("null"));
    List<Object> sqlNullRow = Collections.singletonList(null);

    TransformOperand strictGet = operand(ColumnDataType.STRING, "variantGet",
        new RexExpression.InputRef(0), stringLiteral("$.name"), stringLiteral("STRING"));
    assertTrue(strictGet instanceof VariantOperand);
    assertEquals(strictGet.apply(row), "alice");
    assertNull(strictGet.apply(missingRow));
    assertEquals(strictGet.apply(row), "alice", "The reusable cursor must reset after a missing path");
    assertNull(strictGet.apply(sqlNullRow));

    TransformOperand defaultGet = operand(ColumnDataType.VARIANT, "VARIANT_GET",
        new RexExpression.InputRef(0), stringLiteral("$.nested"));
    Object nested = defaultGet.apply(row);
    assertTrue(nested instanceof ByteArray, "VARIANT results must retain the internal BYTES wrapper");
    assertEquals(VariantUtils.variantToJson((byte[]) ColumnDataType.VARIANT.toExternal(nested)), "{\"enabled\":true}");

    TransformOperand tolerantGet = operand(ColumnDataType.DOUBLE, "tryVariantGet",
        new RexExpression.InputRef(0), stringLiteral("$.name"), stringLiteral("DOUBLE"));
    assertNull(tolerantGet.apply(row));
    TransformOperand mismatchedStrictGet = operand(ColumnDataType.DOUBLE, "variantGet",
        new RexExpression.InputRef(0), stringLiteral("$.name"), stringLiteral("DOUBLE"));
    assertThrows(IllegalArgumentException.class, () -> mismatchedStrictGet.apply(row));

    TransformOperand exists = operand(ColumnDataType.BOOLEAN, "variantExists",
        new RexExpression.InputRef(0), stringLiteral("$.presentNull"));
    assertTrue(exists instanceof VariantOperand);
    assertEquals(exists.apply(row), 1, "A present Variant null counts as present");
    assertEquals(exists.apply(missingRow), 0, "The reusable cursor must not retain a prior present result");
    assertNull(exists.apply(sqlNullRow), "variantExists preserves SQL null");
    assertEquals(operand(ColumnDataType.BOOLEAN, "variantExists",
        new RexExpression.InputRef(0), stringLiteral("$.missing")).apply(row), 0);

    TransformOperand isNull = operand(ColumnDataType.BOOLEAN, "isVariantNull",
        new RexExpression.InputRef(0), stringLiteral("$.presentNull"));
    assertTrue(isNull instanceof VariantOperand);
    assertEquals(isNull.apply(row), 1);
    assertEquals(isNull.apply(missingRow), 0);
    assertEquals(isNull.apply(sqlNullRow), 0, "isVariantNull(SQL NULL) is non-null false");
    TransformOperand rootIsNull =
        operand(ColumnDataType.BOOLEAN, "isVariantNull", new RexExpression.InputRef(0));
    assertEquals(rootIsNull.apply(row), 0);
    assertEquals(rootIsNull.apply(variantNullRow), 1);
    assertEquals(rootIsNull.apply(sqlNullRow), 0);

    TransformOperand typeOf = operand(ColumnDataType.STRING, "variantTypeOf",
        new RexExpression.InputRef(0), stringLiteral("$.presentNull"));
    assertTrue(typeOf instanceof VariantOperand);
    assertEquals(typeOf.apply(row), "NULL");
    assertNull(typeOf.apply(missingRow));
    assertNull(typeOf.apply(sqlNullRow));
    TransformOperand rootTypeOf =
        operand(ColumnDataType.STRING, "variantTypeOf", new RexExpression.InputRef(0));
    assertEquals(rootTypeOf.apply(row), "OBJECT");
    assertEquals(rootTypeOf.apply(variantNullRow), "NULL");
    assertNull(rootTypeOf.apply(sqlNullRow));

    TransformOperand toJson =
        operand(ColumnDataType.STRING, "variantToJson", new RexExpression.InputRef(0));
    assertTrue(toJson instanceof VariantOperand);
    assertEquals(toJson.apply(row),
        "{\"name\":\"alice\",\"nested\":{\"enabled\":true},\"number\":7,\"presentNull\":null}");
    assertEquals(toJson.apply(variantNullRow), "null");
    assertNull(toJson.apply(sqlNullRow));
  }

  @Test
  public void testEveryVariantGetTargetUsesInternalDataSchemaRepresentation() {
    assertEquals(typedGet(variant(builder -> builder.appendBoolean(true)), "BOOLEAN", ColumnDataType.BOOLEAN), 1);
    assertEquals(typedGet(variant(builder -> builder.appendInt(42)), "INT", ColumnDataType.INT), 42);
    assertEquals(typedGet(variant(builder -> builder.appendLong(4_294_967_296L)), "LONG", ColumnDataType.LONG),
        4_294_967_296L);
    assertEquals(typedGet(variant(builder -> builder.appendFloat(1.25F)), "FLOAT", ColumnDataType.FLOAT), 1.25F);
    assertEquals(typedGet(variant(builder -> builder.appendDouble(12.5)), "DOUBLE", ColumnDataType.DOUBLE), 12.5);

    BigDecimal decimal = new BigDecimal("1234567890.12345");
    assertEquals(typedGet(variant(builder -> builder.appendDecimal(decimal)), "BIG_DECIMAL",
        ColumnDataType.BIG_DECIMAL), decimal);
    assertEquals(typedGet(variant(builder -> builder.appendString("click")), "STRING", ColumnDataType.STRING), "click");

    byte[] binary = new byte[]{0, 1, (byte) 0xFF};
    Object binaryResult =
        typedGet(variant(builder -> builder.appendBinary(ByteBuffer.wrap(binary))), "BYTES", ColumnDataType.BYTES);
    assertEquals(binaryResult, new ByteArray(binary));

    UUID uuid = UUID.fromString("12345678-1234-5678-9abc-def012345678");
    byte[] uuidVariant = variant(builder -> builder.appendUUID(uuid));
    Object uuidResult = typedGet(uuidVariant, "UUID", ColumnDataType.UUID);
    assertTrue(uuidResult instanceof ByteArray, "UUID uses the internal BYTES wrapper");
    assertTrue(UuidUtils.equals(((ByteArray) uuidResult).getBytes(), UuidUtils.toBytes(uuid)),
        "UUID extraction must copy the encoded 16-byte value directly");
    assertEquals(ColumnDataType.UUID.toExternal(uuidResult), uuid);
    Object tolerantUuidResult = typedTryGet(uuidVariant, "UUID", ColumnDataType.UUID);
    assertTrue(UuidUtils.equals(((ByteArray) tolerantUuidResult).getBytes(), UuidUtils.toBytes(uuid)));

    long timestampMicros = 1_700_000_000_123_000L;
    byte[] timestampVariant = variant(builder -> builder.appendTimestampTz(timestampMicros));
    assertEquals(typedGet(timestampVariant, "TIMESTAMP", ColumnDataType.TIMESTAMP), 1_700_000_000_123L);
    assertEquals(typedTryGet(timestampVariant, "TIMESTAMP", ColumnDataType.TIMESTAMP), 1_700_000_000_123L);

    Object variantResult =
        typedGet(variant(builder -> builder.appendString("nested")), "VARIANT", ColumnDataType.VARIANT);
    assertTrue(variantResult instanceof ByteArray, "VARIANT uses the internal BYTES wrapper");
    assertEquals(VariantUtils.variantToJson((byte[]) ColumnDataType.VARIANT.toExternal(variantResult)), "\"nested\"");

    assertEquals(typedGet(variant(builder -> builder.appendString("json")), "JSON", ColumnDataType.STRING), "\"json\"");
  }

  @Test
  public void testLiteralPathAndTargetTypeAreCompiledAtConstruction() {
    assertThrows("Invalid paths must fail before any row is evaluated", IllegalArgumentException.class,
        () -> operand(ColumnDataType.STRING, "variantGet",
            new RexExpression.InputRef(0), stringLiteral("payload.name"), stringLiteral("STRING")));
    assertThrows("Invalid target types must fail before any row is evaluated", IllegalArgumentException.class,
        () -> operand(ColumnDataType.STRING, "try_variant_get",
            new RexExpression.InputRef(0), stringLiteral("$"), stringLiteral("UNSUPPORTED")));

    DataSchema nonLiteralPathSchema = new DataSchema(new String[]{"payload", "path"},
        new ColumnDataType[]{ColumnDataType.VARIANT, ColumnDataType.STRING});
    RexExpression.FunctionCall nonLiteralPath = new RexExpression.FunctionCall(ColumnDataType.BOOLEAN,
        "variant_exists", List.of(new RexExpression.InputRef(0), new RexExpression.InputRef(1)));
    assertThrows(IllegalArgumentException.class,
        () -> TransformOperandFactory.getTransformOperand(nonLiteralPath, nonLiteralPathSchema));

    TransformOperand snakeCase = operand(ColumnDataType.BOOLEAN, "VARIANT_EXISTS",
        new RexExpression.InputRef(0), stringLiteral("$"));
    assertTrue(snakeCase instanceof VariantOperand, "Factory routing must use canonical function names");
    assertFalse(VariantOperand.isSupported("parsejson"), "Only path-sensitive Variant operations are specialized");
  }

  @Test
  public void testLiteralParseJsonIsEvaluatedOnceAndPreservesNullSemantics() {
    for (String functionName : List.of("parseJson", "parse_json", "parseJsonToVariant",
        "parse_json_to_variant")) {
      TransformOperand strict = operand(ColumnDataType.VARIANT, functionName,
          stringLiteral("{\"name\":\"alice\"}"));
      assertTrue(strict instanceof LiteralParseJsonOperand);
      Object cachedValue = strict.apply(List.of());
      assertSame(strict.apply(List.of()), cachedValue, "The parsed Variant must be reused for every row");
      assertEquals(VariantUtils.variantToJson((byte[]) ColumnDataType.VARIANT.toExternal(cachedValue)),
          "{\"name\":\"alice\"}");
    }

    assertThrows(IllegalArgumentException.class,
        () -> operand(ColumnDataType.VARIANT, "parseJson", stringLiteral("{not-json")));

    for (String functionName : List.of("tryParseJson", "try_parse_json", "tryParseJsonToVariant",
        "try_parse_json_to_variant")) {
      TransformOperand validTolerant = operand(ColumnDataType.VARIANT, functionName, stringLiteral("[1,2,3]"));
      Object cachedValue = validTolerant.apply(List.of());
      assertSame(validTolerant.apply(List.of()), cachedValue,
          "The tolerantly parsed Variant must be reused for every row");
      assertEquals(VariantUtils.variantToJson((byte[]) ColumnDataType.VARIANT.toExternal(cachedValue)), "[1,2,3]");

      TransformOperand tolerant = operand(ColumnDataType.VARIANT, functionName, stringLiteral("{not-json"));
      assertTrue(tolerant instanceof LiteralParseJsonOperand);
      assertNull(tolerant.apply(List.of()));
      assertNull(tolerant.apply(List.of()));
    }

    TransformOperand sqlNull = operand(ColumnDataType.VARIANT, "parseJson",
        new RexExpression.Literal(ColumnDataType.UNKNOWN, null));
    assertTrue(sqlNull instanceof LiteralParseJsonOperand);
    assertNull(sqlNull.apply(List.of()));

    TransformOperand variantNull =
        operand(ColumnDataType.VARIANT, "parseJson", stringLiteral("null"));
    Object encodedVariantNull = variantNull.apply(List.of());
    assertTrue(VariantUtils.isVariantNull((byte[]) ColumnDataType.VARIANT.toExternal(encodedVariantNull)),
        "JSON null must remain distinct from SQL null");

    RexExpression.FunctionCall literalParse = new RexExpression.FunctionCall(ColumnDataType.VARIANT, "parseJson",
        List.of(stringLiteral("{\"nested\":{\"name\":\"alice\"}}")));
    TransformOperand nestedGet = operand(ColumnDataType.STRING, "variantGet",
        literalParse, stringLiteral("$.nested.name"), stringLiteral("STRING"));
    assertEquals(nestedGet.apply(List.of()), "alice");

    DataSchema stringSchema =
        new DataSchema(new String[]{"json"}, new ColumnDataType[]{ColumnDataType.STRING});
    RexExpression.FunctionCall dynamicCall = new RexExpression.FunctionCall(ColumnDataType.VARIANT, "parseJson",
        List.of(new RexExpression.InputRef(0)));
    assertFalse(TransformOperandFactory.getTransformOperand(dynamicCall, stringSchema)
        instanceof LiteralParseJsonOperand, "Non-literal parsing must keep the row-dependent function operand");
  }

  private static Object typedGet(byte[] variant, String targetType, ColumnDataType resultType) {
    return operand(resultType, "variantGet",
        new RexExpression.InputRef(0), stringLiteral("$"), stringLiteral(targetType)).apply(row(variant));
  }

  private static Object typedTryGet(byte[] variant, String targetType, ColumnDataType resultType) {
    return operand(resultType, "tryVariantGet",
        new RexExpression.InputRef(0), stringLiteral("$"), stringLiteral(targetType)).apply(row(variant));
  }

  /// The runtime CAST guard is defense-in-depth behind Calcite validation; this proves the guard itself executes.
  @Test
  public void testCastFromRawVariantIsRejected() {
    IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
        () -> operand(ColumnDataType.STRING, "CAST", new RexExpression.InputRef(0),
            new RexExpression.Literal(ColumnDataType.STRING, "VARCHAR")));
    assertTrue(exception.getMessage().contains("Raw VARIANT values do not support CAST"), exception.getMessage());

    // CAST over a non-VARIANT operand still builds an operand.
    assertNotNull(operand(ColumnDataType.STRING, "CAST",
        new RexExpression.Literal(ColumnDataType.INT, 1), new RexExpression.Literal(ColumnDataType.STRING, "VARCHAR")));
  }

  private static TransformOperand operand(ColumnDataType resultType, String functionName,
      RexExpression... operands) {
    RexExpression.FunctionCall functionCall =
        new RexExpression.FunctionCall(resultType, functionName, List.of(operands));
    return TransformOperandFactory.getTransformOperand(functionCall, VARIANT_SCHEMA);
  }

  private static RexExpression.Literal stringLiteral(String value) {
    return new RexExpression.Literal(ColumnDataType.STRING, value);
  }

  private static List<Object> row(byte[] variant) {
    return List.of(ColumnDataType.VARIANT.toInternal(variant));
  }

  private static byte[] variant(Consumer<VariantBuilder> writer) {
    VariantBuilder builder = new VariantBuilder();
    writer.accept(builder);
    Variant variant = builder.build();
    return VariantEnvelope.encode(variant.getMetadataBuffer(), variant.getValueBuffer());
  }
}
