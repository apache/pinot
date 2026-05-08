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

import java.util.Collections;
import org.apache.pinot.common.function.FunctionUtils;
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
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
    assertEquals(evaluator.getArguments(), Collections.singletonList("testColumn"));
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
    assertEquals(evaluator.getArguments(), Collections.singletonList("date"));
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
    assertEquals(evaluator.getArguments(), Collections.singletonList("testColumn"));
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
    assertEquals(evaluator.getArguments(), Collections.singletonList("testColumn"));
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
    assertEquals(evaluator.getArguments(), Collections.singletonList("testColumn"));
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

  // ---- Tests for polymorphic function dispatch with schema ----

  /**
   * The original reported bug: CASE WHEN stringCol = 'literal' THEN ... ELSE ... END
   * Without schema, this resolves to doubleEqualsWithTolerance and fails on strings.
   * With schema, it should resolve to stringEquals.
   */
  @Test
  public void testStringEqualsWithSchema() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("lastName", FieldSpec.DataType.STRING)
        .build();
    String expression = "CASE WHEN lastName = 'Brown' THEN 'Brown1' ELSE lastName END";
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression, schema);

    GenericRow row = new GenericRow();
    row.putValue("lastName", "Martinez");
    // Without the fix, this would throw: "For input string: Martinez" trying to convert to double
    assertEquals(evaluator.evaluate(row), "Martinez");

    row.putValue("lastName", "Brown");
    assertEquals(evaluator.evaluate(row), "Brown1");
  }

  /**
   * Test that int equality works correctly with schema (should use intEquals, not doubleEqualsWithTolerance).
   */
  @Test
  public void testIntEqualsWithSchema() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("status", FieldSpec.DataType.INT)
        .build();
    String expression = "CASE WHEN status = 1 THEN 'active' ELSE 'inactive' END";
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression, schema);

    GenericRow row = new GenericRow();
    row.putValue("status", 1);
    assertEquals(evaluator.evaluate(row), "active");

    row.putValue("status", 0);
    assertEquals(evaluator.evaluate(row), "inactive");
  }

  /**
   * Test that string not-equals works correctly with schema.
   */
  @Test
  public void testStringNotEqualsWithSchema() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .build();
    String expression = "CASE WHEN city != 'NYC' THEN 'other' ELSE 'nyc' END";
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression, schema);

    GenericRow row = new GenericRow();
    row.putValue("city", "NYC");
    assertEquals(evaluator.evaluate(row), "nyc");

    row.putValue("city", "Boston");
    assertEquals(evaluator.evaluate(row), "other");
  }

  /**
   * Test string greater-than comparison with schema.
   */
  @Test
  public void testStringGreaterThanWithSchema() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("name", FieldSpec.DataType.STRING)
        .build();
    String expression = "CASE WHEN name > 'M' THEN 'after_M' ELSE 'before_M' END";
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression, schema);

    GenericRow row = new GenericRow();
    row.putValue("name", "Zebra");
    assertEquals(evaluator.evaluate(row), "after_M");

    row.putValue("name", "Alpha");
    assertEquals(evaluator.evaluate(row), "before_M");
  }

  /**
   * Test string less-than-or-equal comparison with schema.
   */
  @Test
  public void testStringLessThanOrEqualWithSchema() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("grade", FieldSpec.DataType.STRING)
        .build();
    String expression = "CASE WHEN grade <= 'C' THEN 'pass' ELSE 'fail' END";
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression, schema);

    GenericRow row = new GenericRow();
    row.putValue("grade", "B");
    assertEquals(evaluator.evaluate(row), "pass");

    row.putValue("grade", "D");
    assertEquals(evaluator.evaluate(row), "fail");
  }

  /**
   * Test nested function whose return type is int: strcmp returns int, so equals resolves to intEquals.
   */
  @Test
  public void testNestedIntReturningFunctionInComparison() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("name", FieldSpec.DataType.STRING)
        .build();
    String expression = "CASE WHEN strcmp(name, 'Brown') = 0 THEN 'match' ELSE 'no_match' END";
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression, schema);

    GenericRow row = new GenericRow();
    row.putValue("name", "Brown");
    assertEquals(evaluator.evaluate(row), "match");

    row.putValue("name", "Smith");
    assertEquals(evaluator.evaluate(row), "no_match");
  }

  /**
   * Test nested function whose return type is String: upper returns String, so equals resolves to stringEquals.
   * This is the key nested-function case for the original bug — the return type of the inner function must
   * propagate through the execution tree so that the outer comparison uses the correct type-specific method.
   */
  @Test
  public void testNestedStringReturningFunctionInComparison() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("name", FieldSpec.DataType.STRING)
        .build();
    String expression = "CASE WHEN upper(name) = 'BROWN' THEN 'match' ELSE 'no_match' END";
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression, schema);

    GenericRow row = new GenericRow();
    row.putValue("name", "Brown");
    assertEquals(evaluator.evaluate(row), "match");

    row.putValue("name", "Smith");
    assertEquals(evaluator.evaluate(row), "no_match");
  }

  /**
   * Test deeply nested functions: reverse(upper(name)) = 'NWORB'.
   * The return type must propagate through multiple levels of nesting.
   */
  @Test
  public void testDeeplyNestedFunctionInComparison() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("name", FieldSpec.DataType.STRING)
        .build();
    String expression = "CASE WHEN reverse(upper(name)) = 'NWORB' THEN 'match' ELSE 'no_match' END";
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression, schema);

    GenericRow row = new GenericRow();
    row.putValue("name", "Brown");
    assertEquals(evaluator.evaluate(row), "match");

    row.putValue("name", "Smith");
    assertEquals(evaluator.evaluate(row), "no_match");
  }

  /**
   * Test that a nested CASE WHEN whose result is used in a comparison falls back gracefully.
   * caseWhen returns Object, so its result type is null — the outer equals falls back to arity-based lookup.
   * This means the outer comparison uses doubleEqualsWithTolerance, which will fail for string values.
   * This is a known limitation: Object-returning functions (caseWhen, coalesce, etc.) cannot propagate
   * type information since their return type depends on runtime values, not static analysis.
   */
  @Test
  public void testNestedCaseWhenInComparisonFallsBackToArityLookup() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("a", FieldSpec.DataType.INT)
        .addSingleValueDimension("b", FieldSpec.DataType.INT)
        .build();
    // Inner CASE WHEN returns Object → outer = uses arity-based lookup → doubleEqualsWithTolerance.
    // This works for numeric types since FunctionInvoker converts int to double.
    String expression = "CASE WHEN (CASE WHEN a = 1 THEN b ELSE 0 END) = 42 THEN 'match' ELSE 'no_match' END";
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression, schema);

    GenericRow row = new GenericRow();
    row.putValue("a", 1);
    row.putValue("b", 42);
    assertEquals(evaluator.evaluate(row), "match");

    row.putValue("a", 1);
    row.putValue("b", 99);
    assertEquals(evaluator.evaluate(row), "no_match");
  }

  /**
   * Test that the schema-aware evaluator handles null values correctly.
   */
  @Test
  public void testStringEqualsWithNullValueAndSchema() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .build();
    String expression = "CASE WHEN city = 'NYC' THEN 'match' ELSE 'no_match' END";
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression, schema);

    GenericRow row = new GenericRow();
    row.putValue("city", null);
    // equals is null-intolerant, so equals(null, 'NYC') returns null, and caseWhen treats null as not-true
    assertEquals(evaluator.evaluate(row), "no_match");
  }

  /**
   * Test column-to-column string comparison (both columns from schema).
   */
  @Test
  public void testColumnToColumnStringComparisonWithSchema() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("firstName", FieldSpec.DataType.STRING)
        .addSingleValueDimension("lastName", FieldSpec.DataType.STRING)
        .build();
    String expression = "CASE WHEN firstName = lastName THEN 'same' ELSE 'different' END";
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression, schema);

    GenericRow row = new GenericRow();
    row.putValue("firstName", "Lee");
    row.putValue("lastName", "Lee");
    assertEquals(evaluator.evaluate(row), "same");

    row.putValue("firstName", "John");
    row.putValue("lastName", "Doe");
    assertEquals(evaluator.evaluate(row), "different");
  }

  /**
   * Test that long equality uses the correct type-specific method.
   */
  @Test
  public void testLongEqualsWithSchema() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("eventTime", FieldSpec.DataType.LONG)
        .build();
    String expression = "CASE WHEN eventTime = 1000000 THEN 'match' ELSE 'no_match' END";
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression, schema);

    GenericRow row = new GenericRow();
    row.putValue("eventTime", 1000000L);
    assertEquals(evaluator.evaluate(row), "match");

    row.putValue("eventTime", 999999L);
    assertEquals(evaluator.evaluate(row), "no_match");
  }

  /**
   * Test that non-polymorphic functions still work when schema is provided (regression check).
   */
  @Test
  public void testNonPolymorphicFunctionWithSchema() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("testColumn", FieldSpec.DataType.STRING)
        .build();
    String expression = "reverse(testColumn)";
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression, schema);
    assertEquals(evaluator.getArguments(), Collections.singletonList("testColumn"));

    GenericRow row = new GenericRow();
    row.putValue("testColumn", "hello");
    assertEquals(evaluator.evaluate(row), "olleh");
  }

  /**
   * Test backward compatibility: the no-schema constructor still works (arity-based lookup).
   * For numeric types, doubleEqualsWithTolerance is fine.
   */
  @Test
  public void testBackwardCompatibilityWithoutSchema() {
    String expression = "CASE WHEN status = 1 THEN 'active' ELSE 'inactive' END";
    // No schema - falls back to arity-based lookup (doubleEqualsWithTolerance)
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression);

    GenericRow row = new GenericRow();
    row.putValue("status", 1);
    assertEquals(evaluator.evaluate(row), "active");

    row.putValue("status", 0);
    assertEquals(evaluator.evaluate(row), "inactive");
  }

  /**
   * Array literals (ARRAY[...]) propagate their inferred element type so comparisons with multi-valued columns
   * dispatch to the typed array overloads (e.g. {@code stringArrayEquals}) instead of falling back to
   * {@code doubleEqualsWithTolerance}, which would throw or misbehave on non-numeric arrays.
   */
  @Test
  public void testStringArrayLiteralEqualityWithSchema() {
    Schema schema = new Schema.SchemaBuilder()
        .addMultiValueDimension("tags", FieldSpec.DataType.STRING)
        .build();
    String expression = "CASE WHEN tags = ARRAY['a', 'b'] THEN 'match' ELSE 'no_match' END";
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression, schema);

    GenericRow row = new GenericRow();
    row.putValue("tags", new String[]{"a", "b"});
    assertEquals(evaluator.evaluate(row), "match");

    row.putValue("tags", new String[]{"a", "c"});
    assertEquals(evaluator.evaluate(row), "no_match");
  }

  /**
   * Mixed-type array literals cannot be unified to a single element type, so polymorphic dispatch falls back
   * to arity-based lookup. Ensures the mixed-type case doesn't throw and the existing behavior is preserved.
   */
  @Test
  public void testMixedTypeArrayLiteralFallsBackToArityLookup() {
    Schema schema = new Schema.SchemaBuilder()
        .addMultiValueDimension("nums", FieldSpec.DataType.INT)
        .build();
    // ARRAY[1, 'a'] has mixed INT/STRING — type can't be inferred, so we fall back to arity-based lookup.
    // The check itself is just that planning doesn't blow up; runtime behavior is preserved.
    String expression = "CASE WHEN nums = ARRAY[1, 'a'] THEN 'match' ELSE 'no_match' END";
    // Constructing the evaluator is the real assertion — it must not throw during planning.
    new InbuiltFunctionEvaluator(expression, schema);
  }

  /**
   * Test that a column not in the schema (e.g., intermediate derived column) falls back gracefully.
   */
  @Test
  public void testColumnNotInSchemaFallsBackToArityLookup() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("knownCol", FieldSpec.DataType.STRING)
        .build();
    // 'unknownCol' is not in the schema — should still work via arity fallback for numeric values
    String expression = "CASE WHEN unknownCol = 1 THEN 'yes' ELSE 'no' END";
    InbuiltFunctionEvaluator evaluator = new InbuiltFunctionEvaluator(expression, schema);

    GenericRow row = new GenericRow();
    row.putValue("unknownCol", 1);
    assertEquals(evaluator.evaluate(row), "yes");
  }
}
