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
package org.apache.pinot.common.evaluator;

import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;
import org.apache.parquet.variant.Variant;
import org.apache.parquet.variant.VariantBuilder;
import org.apache.pinot.common.function.FunctionUtils;
import org.apache.pinot.common.utils.VariantUtils;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.PinotDataType;
import org.apache.pinot.spi.utils.VariantEnvelope;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class InbuiltFunctionEvaluatorTest {

  @Test
  public void booleanLiteralTest() {
    checkBooleanLiteralExpression("true", 1);
    checkBooleanLiteralExpression("false", 0);
    checkBooleanLiteralExpression("True", 1);
    checkBooleanLiteralExpression("False", 0);
    checkBooleanLiteralExpression("1", 1);
    checkBooleanLiteralExpression("0", 0);
  }

  @Test
  public void testOrWithNulls() {
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator("or(null, false, true)");
    Object output = evaluator.evaluate(new GenericRow());
    assertEquals(output, true);

    evaluator = new InbuiltFunctionEvaluator("or(null, false, null)");
    output = evaluator.evaluate(new GenericRow());
    assertNull(output);

    evaluator = new InbuiltFunctionEvaluator("or(null, null, null)");
    output = evaluator.evaluate(new Object[]{});
    assertNull(output);

    evaluator = new InbuiltFunctionEvaluator("or(null, true, null)");
    output = evaluator.evaluate(new Object[]{});
    assertEquals(output, true);

    evaluator = new InbuiltFunctionEvaluator("or(true, false)");
    output = evaluator.evaluate(new GenericRow());
    assertEquals(output, true);
  }

  @Test
  public void testAndWithNulls() {
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator("and(null, false, true)");
    Object output = evaluator.evaluate(new GenericRow());
    assertEquals(output, false);

    evaluator = new InbuiltFunctionEvaluator("and(null, false, null)");
    output = evaluator.evaluate(new GenericRow());
    assertEquals(output, false);

    evaluator = new InbuiltFunctionEvaluator("and(null, null, null)");
    output = evaluator.evaluate(new Object[]{});
    assertNull(output);

    evaluator = new InbuiltFunctionEvaluator("and(null, true, null)");
    output = evaluator.evaluate(new Object[]{});
    assertNull(output);

    evaluator = new InbuiltFunctionEvaluator("and(true, false)");
    output = evaluator.evaluate(new GenericRow());
    assertEquals(output, false);
  }

  @Test
  public void testNotWithNulls() {
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator("not(null)");
    Object output = evaluator.evaluate(new GenericRow());
    assertNull(output);

    evaluator = new InbuiltFunctionEvaluator("not(false)");
    output = evaluator.evaluate(new Object[]{});
    assertEquals(output, true);

    evaluator = new InbuiltFunctionEvaluator("not(true)");
    output = evaluator.evaluate(new GenericRow());
    assertEquals(output, false);
  }

  private void checkBooleanLiteralExpression(String expression, int value) {
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression);
    Object output = evaluator.evaluate(new GenericRow());
    PinotDataType outputType = FunctionUtils.getArgumentType(output);
    // as INT is the stored type for BOOLEAN
    assertEquals(outputType.toInt(output), value);
  }

  @Test
  public void testColumnExpression() {
    String expression = "testColumn";
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression);
    assertEquals(evaluator.getArguments(), List.of("testColumn"));
    GenericRow row = new GenericRow();
    for (int i = 0; i < 5; i++) {
      String value = "testValue" + i;
      row.putValue("testColumn", value);
      assertEquals(evaluator.evaluate(row), value);
    }
  }

  @Test
  public void testLiteralExpression() {
    String expression = "'testValue'";
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression);
    assertTrue(evaluator.getArguments().isEmpty());
    GenericRow row = new GenericRow();
    for (int i = 0; i < 5; i++) {
      assertEquals(evaluator.evaluate(row), "testValue");
    }
  }

  @Test
  public void testScalarWrapperWithReservedKeywordExpression() {
    String expression = "dateTrunc('MONTH', \"date\")";
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression);
    assertEquals(evaluator.getArguments(), List.of("date"));
    GenericRow row = new GenericRow();
    for (int i = 1; i < 9; i++) {
      DateTime dt = new DateTime(String.format("2020-0%d-15T12:00:00", i));
      long millis = dt.getMillis();
      DateTime truncDt = dt.withZone(DateTimeZone.UTC).withDayOfMonth(1).withHourOfDay(0).withMillisOfDay(0);
      row.putValue("date", millis);
      assertEquals(evaluator.evaluate(row), truncDt.getMillis());
    }
  }

  @Test
  public void testScalarWrapperNameWithOverrides() {
    String expr = String.format("regexp_extract(testColumn, '%s')", "(.*)([\\d]+)");
    String exprWithGroup = String.format("regexp_extract(testColumn, '%s', 2)", "(.*)([\\d]+)");
    String exprWithGroupAndDefault = String.format("regexp_extract(testColumn, '%s', 3, 'null')", "(.*)([\\d]+)");
    GenericRow row = new GenericRow();
    row.putValue("testColumn", "testValue0");
    InbuiltFunctionEvaluator evaluator;
    evaluator = new InbuiltFunctionEvaluator(expr);
    assertEquals(evaluator.getArguments(), List.of("testColumn"));
    assertEquals(evaluator.evaluate(row), "testValue0");
    evaluator = new InbuiltFunctionEvaluator(exprWithGroup);
    assertEquals(evaluator.evaluate(row), "0");
    evaluator = new InbuiltFunctionEvaluator(exprWithGroupAndDefault);
    assertEquals(evaluator.evaluate(row), "null");
  }

  @Test
  public void testFunctionWithColumn() {
    String expression = "reverse(testColumn)";
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression);
    assertEquals(evaluator.getArguments(), List.of("testColumn"));
    GenericRow row = new GenericRow();
    for (int i = 0; i < 5; i++) {
      String value = "testValue" + i;
      row.putValue("testColumn", value);
      assertEquals(evaluator.evaluate(row), new StringBuilder(value).reverse().toString());
    }
  }

  @Test
  public void testFunctionWithLiteral() {
    String expression = "reverse(12345)";
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression);
    assertTrue(evaluator.getArguments().isEmpty());
    GenericRow row = new GenericRow();
    assertEquals(evaluator.evaluate(row), "54321");
  }

  @Test
  public void testNestedFunction() {
    String expression = "reverse(reverse(testColumn))";
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression);
    assertEquals(evaluator.getArguments(), List.of("testColumn"));
    GenericRow row = new GenericRow();
    for (int i = 0; i < 5; i++) {
      String value = "testValue" + i;
      row.putValue("testColumn", value);
      assertEquals(evaluator.evaluate(row), value);
    }
  }

  @Test
  public void testStateSharedBetweenRowsForExecution() {
    // This function is auto registered with @ScalarFunction annotation under MyFunc class
    String expression = "appendToStringAndReturn('test ')";
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression);
    assertTrue(evaluator.getArguments().isEmpty());
    GenericRow row = new GenericRow();
    assertEquals(evaluator.evaluate(row), "test ");
    assertEquals(evaluator.evaluate(row), "test test ");
    assertEquals(evaluator.evaluate(row), "test test test ");
  }

  @Test
  public void testNullReturnedByInbuiltFunctionEvaluatorThatCannotTakeNull() {
    String[] expressions = {
        "fromDateTime(\"NULL\", 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z''')",
        "fromDateTime(\"invalid_identifier\", 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z''')",
        "toDateTime(1648010797, \"invalid_identifier\", \"invalid_identifier\")",
        "toDateTime(\"invalid_identifier\", \"invalid_identifier\", \"invalid_identifier\")",
        "toDateTime(\"NULL\", \"invalid_identifier\", \"invalid_identifier\")",
        "toDateTime(\"invalid_identifier\", \"NULL\", \"invalid_identifier\")"
    };
    for (String expression : expressions) {
      InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression);
      GenericRow row = new GenericRow();
      assertNull(evaluator.evaluate(row));
    }
  }

  @Test
  public void testPolymorphicBitwiseFunctions() {
    // Ingestion evaluator resolves by arity, which returns the LONG overload.
    // INT inputs are widened to LONG via convertTypes, so results use 64-bit semantics.
    // Return type depends on the method: most return long, but bitExtract returns int.
    GenericRow intRow = new GenericRow();
    intRow.putValue("value", 6);
    intRow.putValue("rhs", 3);
    intRow.putValue("shift", 2);
    assertEquals(new InbuiltFunctionEvaluator("bitNot(value)").evaluate(intRow), -7L);
    assertEquals(new InbuiltFunctionEvaluator("bitAnd(value, rhs)").evaluate(intRow), 2L);
    assertEquals(new InbuiltFunctionEvaluator("bitShiftRightUnsigned(value, shift)").evaluate(intRow), 1L);

    GenericRow longRow = new GenericRow();
    longRow.putValue("value", 1L << 40);
    longRow.putValue("shift", 40);
    assertEquals(new InbuiltFunctionEvaluator("bitExtract(value, shift)").evaluate(longRow), 1);
  }

  @Test
  public void testPlannedVariantScalarFunctions() {
    byte[] first = VariantUtils.parseJsonToVariant(
        "{\"value\":7,\"name\":\"pinot\",\"nested\":{\"flag\":true},\"nullValue\":null}");
    byte[] second = VariantUtils.parseJsonToVariant(
        "{\"value\":9,\"name\":\"apache\",\"nested\":{\"flag\":false},\"nullValue\":null}");
    GenericRow row = new GenericRow();

    InbuiltFunctionEvaluator getInt =
        new InbuiltFunctionEvaluator("variantGet(variant, '$.value', 'INT')");
    InbuiltFunctionEvaluator getNested = new InbuiltFunctionEvaluator("variantGet(variant, '$.nested')");
    InbuiltFunctionEvaluator exists =
        new InbuiltFunctionEvaluator("variantExists(variant, '$.nullValue')");
    InbuiltFunctionEvaluator isNull =
        new InbuiltFunctionEvaluator("isVariantNull(variant, '$.nullValue')");
    InbuiltFunctionEvaluator typeOf =
        new InbuiltFunctionEvaluator("variantTypeOf(variant, '$.nested')");

    assertEquals(getInt.getArguments(), List.of("variant"));
    row.putValue("variant", first);
    assertEquals(getInt.evaluate(row), 7);
    assertEquals(getInt.evaluate(new Object[]{first}), 7);
    assertEquals(VariantUtils.variantToJson((byte[]) getNested.evaluate(row)), "{\"flag\":true}");
    assertEquals(exists.evaluate(row), true);
    assertEquals(isNull.evaluate(row), true);
    assertEquals(typeOf.evaluate(row), "OBJECT");

    row.putValue("variant", second);
    assertEquals(getInt.evaluate(row), 9);
    assertEquals(VariantUtils.variantToJson((byte[]) getNested.evaluate(row)), "{\"flag\":false}");
    assertEquals(exists.evaluate(row), true);
    assertEquals(isNull.evaluate(row), true);
    assertEquals(typeOf.evaluate(row), "OBJECT");
  }

  @Test
  public void testPlannedVariantUsesExternalUuidAndTimestampRepresentations() {
    UUID uuid = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");
    VariantBuilder builder = new VariantBuilder();
    builder.appendUUID(uuid);
    GenericRow row = new GenericRow();
    row.putValue("variant", encode(builder));
    assertEquals(new InbuiltFunctionEvaluator("variantGet(variant, '$', 'UUID')").evaluate(row), uuid);
    assertEquals(new InbuiltFunctionEvaluator("tryVariantGet(variant, '$', 'UUID')").evaluate(row), uuid);

    builder = new VariantBuilder();
    builder.appendTimestampTz(1_700_000_000_123_000L);
    row.putValue("variant", encode(builder));
    Timestamp timestamp = new Timestamp(1_700_000_000_123L);
    assertEquals(new InbuiltFunctionEvaluator("variantGet(variant, '$', 'TIMESTAMP')").evaluate(row), timestamp);
    assertEquals(new InbuiltFunctionEvaluator("tryVariantGet(variant, '$', 'TIMESTAMP')").evaluate(row), timestamp);
  }

  @Test
  public void testPlannedVariantStrictTryAndSqlNullSemantics() {
    byte[] variant = VariantUtils.parseJsonToVariant("{\"score\":\"not-a-number\",\"nullValue\":null}");
    GenericRow row = new GenericRow();
    row.putValue("variant", variant);

    InbuiltFunctionEvaluator strict =
        new InbuiltFunctionEvaluator("variantGet(variant, '$.score', 'DOUBLE')");
    InbuiltFunctionEvaluator tolerant =
        new InbuiltFunctionEvaluator("tryVariantGet(variant, '$.score', 'DOUBLE')");
    assertThrows(RuntimeException.class, () -> strict.evaluate(row));
    assertNull(tolerant.evaluate(row));
    assertNull(new InbuiltFunctionEvaluator("variantGet(variant, '$.missing', 'STRING')").evaluate(row));
    assertNull(new InbuiltFunctionEvaluator("tryVariantGet(variant, '$.missing', 'STRING')").evaluate(row));
    assertFalse((Boolean) new InbuiltFunctionEvaluator("variantExists(variant, '$.missing')").evaluate(row));
    assertFalse((Boolean) new InbuiltFunctionEvaluator("isVariantNull(variant, '$.missing')").evaluate(row));
    assertNull(new InbuiltFunctionEvaluator("variantTypeOf(variant, '$.missing')").evaluate(row));

    row.putValue("variant", new byte[]{1});
    assertThrows(RuntimeException.class, () -> strict.evaluate(row));
    assertNull(tolerant.evaluate(row));

    row.putValue("variant", null);
    assertNull(new InbuiltFunctionEvaluator("variantGet(variant, '$.score', 'STRING')").evaluate(row));
    assertNull(new InbuiltFunctionEvaluator("variantExists(variant, '$.score')").evaluate(row));
    assertFalse((Boolean) new InbuiltFunctionEvaluator("isVariantNull(variant)").evaluate(row));
    assertNull(new InbuiltFunctionEvaluator("variantTypeOf(variant)").evaluate(row));
  }

  @Test
  public void testPlannedVariantLiteralsAreValidatedAtConstruction() {
    assertThrows(IllegalArgumentException.class,
        () -> new InbuiltFunctionEvaluator("variantGet(variant, 'not-a-path', 'STRING')"));
    assertThrows(IllegalArgumentException.class,
        () -> new InbuiltFunctionEvaluator("variantGet(variant, '$.value', 'NOT_A_TYPE')"));
  }

  @Test
  public void testDynamicVariantOperandsRetainCompatibilityPath() {
    byte[] variant = VariantUtils.parseJsonToVariant("{\"value\":11}");
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator("variantGet(variant, path, targetType)");
    assertEquals(evaluator.getArguments(), List.of("variant", "path", "targetType"));

    GenericRow row = new GenericRow();
    row.putValue("variant", variant);
    row.putValue("path", "$.value");
    row.putValue("targetType", "INT");
    assertEquals(evaluator.evaluate(row), 11);
  }

  private static byte[] encode(VariantBuilder builder) {
    Variant variant = builder.build();
    return VariantEnvelope.encode(variant.getMetadataBuffer(), variant.getValueBuffer());
  }
}
