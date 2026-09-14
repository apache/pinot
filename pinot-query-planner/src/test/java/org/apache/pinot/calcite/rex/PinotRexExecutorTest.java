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
package org.apache.pinot.calcite.rex;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.calcite.DataContext;
import org.apache.calcite.DataContexts;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.NlsString;
import org.apache.pinot.calcite.rel.rules.PinotRuleUtils;
import org.apache.pinot.query.type.TypeFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// Compares scalar constant reduction with Calcite and verifies reuse of immutable conversion templates.
public class PinotRexExecutorTest {
  private static final RexExecutor CALCITE_EXECUTOR = new RexExecutorImpl(DataContexts.EMPTY);
  private static final RexExecutor FAILING_FALLBACK = (builder, expressions, results) -> {
    throw new AssertionError("Supported casts must not use the fallback: " + expressions);
  };

  @DataProvider
  public Object[][] scalarLiterals() {
    List<Object[]> cases = new ArrayList<>();
    addScalarCases(cases, SqlTypeName.TINYINT, -1, -1, "127", "-128", "128", "-129", " 42 ", "1.5", "invalid", null);
    addScalarCases(cases, SqlTypeName.SMALLINT, -1, -1, "32767", "-32768", "32768", "-32769", " 42 ", "invalid", null);
    addScalarCases(cases, SqlTypeName.INTEGER, -1, -1, "2147483647", "-2147483648", "2147483648", "-2147483649",
        "\t42\n", "1.5", "invalid", null);
    addScalarCases(cases, SqlTypeName.UTINYINT, -1, -1, "1", "255", "256", "-1", null);
    addScalarCases(cases, SqlTypeName.USMALLINT, -1, -1, "1", "65535", "65536", "-1", null);
    addScalarCases(cases, SqlTypeName.UINTEGER, -1, -1, "1", "4294967295", "4294967296", "-1", null);
    addScalarCases(cases, SqlTypeName.UBIGINT, -1, -1, "1", "18446744073709551615", "18446744073709551616", "-1", null);
    for (SqlTypeName target : List.of(SqlTypeName.FLOAT, SqlTypeName.REAL, SqlTypeName.DOUBLE)) {
      addScalarCases(cases, target, -1, -1, "1.25", "-1.5", "1e100", " 1.25 ", "NaN", "invalid", null);
    }
    addScalarCases(cases, SqlTypeName.DECIMAL, 5, 2, "123.455", "-123.455", "999.995", " 1.25 ", "invalid", null);
    addScalarCases(cases, SqlTypeName.DECIMAL, 8, 3, "123.4555", "-123.4555", "99999.9995", " 1.25 ", "invalid", null);
    addScalarCases(cases, SqlTypeName.BOOLEAN, -1, -1, "true", "FALSE", " true ", "1", "invalid", null);
    addScalarCases(cases, SqlTypeName.DATE, -1, -1, "2000-02-29", "1969-12-31", " 2000-02-29 ", "2026-02-30",
        "invalid", null);
    for (int precision : List.of(0, 3)) {
      addScalarCases(cases, SqlTypeName.TIME, precision, -1, "12:34:56.9876", "23:59:59.9999", " 12:34:56 ",
          "25:00:00", "invalid", null);
    }
    addScalarCases(cases, SqlTypeName.UUID, -1, -1, "123e4567-e89b-12d3-a456-426614174000",
        " 123e4567-e89b-12d3-a456-426614174000 ", "123", "invalid", null);
    for (SqlTypeName target : List.of(SqlTypeName.CHAR, SqlTypeName.VARCHAR, SqlTypeName.BINARY,
        SqlTypeName.VARBINARY)) {
      addScalarCases(cases, target, 3, -1, "ab", "abcdef", " a ", "", null);
    }
    return cases.toArray(new Object[0][]);
  }

  @Test(dataProvider = "scalarLiterals")
  public void testScalarCastsMatchCalciteWithoutFallback(SqlTypeName sourceType, SqlTypeName targetType,
      int precision, int scale, String value, boolean validControl) {
    LiteralRexBuilder builder = new LiteralRexBuilder();
    RelDataTypeFactory factory = builder.getTypeFactory();
    RelDataType target;
    if (precision < 0) {
      target = factory.createSqlType(targetType);
    } else if (scale < 0) {
      target = factory.createSqlType(targetType, precision);
    } else {
      target = factory.createSqlType(targetType, precision, scale);
    }
    target = factory.createTypeWithNullability(target, true);
    RelDataType source = factory.createSqlType(sourceType, 64);
    RexNode operand = value == null ? builder.makeNullLiteral(source) : builder.stringLiteral(value, source);
    RexNode expression = builder.makeAbstractCast(target, operand, false);
    List<RexNode> input = List.of(expression);
    List<RexNode> expected = reduce(CALCITE_EXECUTOR, builder, input);
    if (SqlTypeName.UNSIGNED_TYPES.contains(targetType) && value != null) {
      // Calcite can evaluate unsigned casts but cannot materialize their non-null RexLiterals yet.
      assertEquals(expected, input, "Unsigned casts must remain unchanged when literal construction is unsupported");
    } else if (validControl) {
      assertFalse(expected.equals(input), "The valid control must reduce for " + target);
    }
    List<RexNode> actual = reduce(new PinotRexExecutor(FAILING_FALLBACK), builder, input);
    assertEquals(actual, expected);
    assertEquals(actual.get(0).getType(), expected.get(0).getType());
    if (value == null) {
      assertTrue(RexLiteral.isNullLiteral(actual.get(0)));
    } else if (expected.equals(input)) {
      assertSame(actual.get(0), expression, "A failed cast must retain the original expression");
    }
  }

  private static void addScalarCases(List<Object[]> cases, SqlTypeName target, int precision, int scale,
      String... values) {
    for (SqlTypeName source : List.of(SqlTypeName.CHAR, SqlTypeName.VARCHAR)) {
      for (int i = 0; i < values.length; i++) {
        cases.add(new Object[]{source, target, precision, scale, values[i], i == 0});
      }
    }
  }

  @DataProvider
  public Object[][] bigintLiterals() {
    List<Object[]> cases = new ArrayList<>();
    for (SqlTypeName sourceType : List.of(SqlTypeName.CHAR, SqlTypeName.VARCHAR)) {
      for (String value : new String[]{"0", "-0", "-1", "+1", "000123", " 123 ", "\t123\n", "9007199254740993",
          "9223372036854775807", "-9223372036854775808", "9223372036854775808", "-9223372036854775809",
          "1.0", "1e3", "", "invalid", null}) {
        cases.add(new Object[]{sourceType, value});
      }
    }
    return cases.toArray(new Object[0][]);
  }

  @Test(dataProvider = "bigintLiterals")
  public void testBigintCastsMatchCalciteWithoutFallback(SqlTypeName sourceType, String value) {
    LiteralRexBuilder builder = new LiteralRexBuilder();
    RelDataType source = builder.getTypeFactory().createSqlType(sourceType, 30);
    RelDataType target = builder.getTypeFactory().createTypeWithNullability(
        builder.getTypeFactory().createSqlType(SqlTypeName.BIGINT), true);
    RexNode operand = value == null ? builder.makeNullLiteral(source) : builder.stringLiteral(value, sourceType);
    RexNode expression = builder.makeAbstractCast(target, operand, false);
    List<RexNode> input = List.of(expression);
    CountingFallback fallback = new CountingFallback(CALCITE_EXECUTOR);
    List<RexNode> expected = reduce(CALCITE_EXECUTOR, builder, input);
    List<RexNode> actual = reduce(new PinotRexExecutor(fallback), builder, input);
    assertEquals(actual, expected);
    assertEquals(actual.get(0).getType(), expected.get(0).getType());
    assertEquals(fallback._calls.get(), 0);
    if (expected.equals(input)) {
      assertSame(actual.get(0), expression);
    }
  }

  @Test
  public void testBigintMixedBatchesMatchCalcite() {
    LiteralRexBuilder builder = new LiteralRexBuilder();
    RelDataType bigint = builder.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
    RexNode valid = builder.makeAbstractCast(bigint, builder.makeLiteral("123"), false);
    RexNode invalid = builder.makeAbstractCast(bigint, builder.makeLiteral("invalid"), false);
    RexNode timestamp = timestampCast(builder, SqlTypeName.CHAR, "2026-04-01 00:00:00", 3);
    RexNode unsupported = builder.makeCall(SqlStdOperatorTable.PLUS, builder.makeExactLiteral(BigDecimal.ONE),
        builder.makeExactLiteral(BigDecimal.TEN));
    for (List<RexNode> input : List.of(List.of(valid, invalid), List.of(invalid, valid), List.of(valid, timestamp),
        List.of(timestamp, valid), List.of(valid, unsupported))) {
      CountingFallback fallback = new CountingFallback(CALCITE_EXECUTOR);
      List<RexNode> actual = reduce(new PinotRexExecutor(fallback), builder, input);
      assertEquals(actual, reduce(CALCITE_EXECUTOR, builder, input));
      assertEquals(fallback._calls.get(), input.contains(unsupported) ? 1 : 0);
    }
  }

  @DataProvider
  public Object[][] timestampLiterals() {
    List<Object[]> cases = new ArrayList<>();
    for (SqlTypeName sourceType : List.of(SqlTypeName.CHAR, SqlTypeName.VARCHAR)) {
      for (String value : List.of("2026-09-01 00:00:00", "1970-01-01 00:00:00", "1969-12-31 23:59:59.999",
          "2000-02-29 12:34:56.123", "2026-09-01 12:34:56.1", "2026-09-01 12:34:56.123456789",
          "2026-09-01 23:59:59.9999", "1969-12-31 23:59:59.9999")) {
        cases.add(new Object[]{sourceType, value, 3});
      }
      for (int precision : List.of(0, 1, 2)) {
        cases.add(new Object[]{sourceType, "1969-12-31 23:59:59.987654", precision});
        cases.add(new Object[]{sourceType, "2026-09-01 12:34:56.987654", precision});
      }
      cases.add(new Object[]{sourceType, "2026-09-01 23:59:59.9999", 0});
      cases.add(new Object[]{sourceType, "1969-12-31 23:59:59.9999", 0});
    }
    return cases.toArray(new Object[0][]);
  }

  @Test(dataProvider = "timestampLiterals")
  public void testSupportedCastsDoNotUseFallback(SqlTypeName sourceType, String value, int precision) {
    LiteralRexBuilder builder = new LiteralRexBuilder();
    RexNode expression = timestampCast(builder, sourceType, value, precision);
    assertEquals(expression.getKind(), SqlKind.CAST);
    List<RexNode> expected = reduce(CALCITE_EXECUTOR, builder, List.of(expression));
    assertFalse(expected.get(0).equals(expression), "The Calcite control must reduce the test expression");

    List<RexNode> actual = reduce(new PinotRexExecutor(FAILING_FALLBACK), builder, List.of(expression));
    assertEquals(actual, expected);
    assertEquals(actual.get(0).getType(), expected.get(0).getType());
  }

  @Test
  public void testNullCastsPreserveTypeAndNullabilityWithoutFallback() {
    LiteralRexBuilder builder = new LiteralRexBuilder();
    for (SqlTypeName sourceType : List.of(SqlTypeName.CHAR, SqlTypeName.VARCHAR)) {
      RelDataType type = builder.getTypeFactory().createSqlType(sourceType, 20);
      RelDataType target = builder.getTypeFactory().createTypeWithNullability(
          builder.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP, 3), true);
      RexNode expression = builder.makeAbstractCast(target, builder.makeNullLiteral(type), false);
      List<RexNode> expected = reduce(CALCITE_EXECUTOR, builder, List.of(expression));
      List<RexNode> actual = reduce(new PinotRexExecutor(FAILING_FALLBACK), builder, List.of(expression));
      assertEquals(actual, expected);
      assertTrue(RexLiteral.isNullLiteral(actual.get(0)));
      assertEquals(actual.get(0).getType(), expected.get(0).getType());
    }
  }

  @DataProvider
  public Object[][] timestampEdgeInputs() {
    return new Object[][]{
        {"2026-09-01"}, {" 2026-09-01 12:34:56.123 "}, {"2026-09-01 12:34:56.123   "},
        {"\t2026-09-01 12:34:56.123\n"}, {""}, {"not-a-timestamp"}, {"2026-02-30 00:00:00"},
        {"2026-09-01 25:00:00"}, {"2026-09-01T12:34:56Z"}, {"2026-09-01 12:34:56+02:00"},
        {"1788266096123"}
    };
  }

  @Test(dataProvider = "timestampEdgeInputs")
  public void testWhitespaceAndInvalidInputsMatchCalcite(String value) {
    LiteralRexBuilder builder = new LiteralRexBuilder();
    RexNode expression = timestampCast(builder, SqlTypeName.CHAR, value, 3);
    List<RexNode> input = List.of(expression);
    List<RexNode> expected = reduce(CALCITE_EXECUTOR, builder, input);
    CountingFallback fallback = new CountingFallback(CALCITE_EXECUTOR);

    List<RexNode> actual = reduce(new PinotRexExecutor(fallback), builder, input);
    assertEquals(actual, expected);
    assertEquals(fallback._calls.get(), 0, "Eligible casts must bypass the fallback even when conversion fails");
    if (expected.equals(input)) {
      assertSame(actual.get(0), expression, "Failed casts must retain the original expression");
    }
  }

  @Test
  public void testUnsupportedExpressionsDelegateTheWholeBatchInOrder() {
    LiteralRexBuilder builder = new LiteralRexBuilder();
    RexNode fast = timestampCast(builder, SqlTypeName.CHAR, "2026-09-01 12:34:56.123", 3);
    RelDataType timestamp = builder.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP, 3);
    RexLiteral string = builder.stringLiteral("2026-09-01 12:34:56.123", SqlTypeName.CHAR);
    List<RexNode> unsupported = List.of(
        builder.makeCall(SqlStdOperatorTable.PLUS, builder.makeExactLiteral(BigDecimal.ONE),
            builder.makeExactLiteral(BigDecimal.TEN)),
        builder.makeAbstractCast(timestamp, string, true),
        builder.makeAbstractCast(timestamp, string, false, builder.makeLiteral("YYYY-MM-DD HH24:MI:SS.FF")),
        builder.makeAbstractCast(timestamp, builder.makeAbstractCast(
            builder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR, 30), string, false), false),
        builder.makeCall(timestamp, new SqlSpecialOperator("CAST", SqlKind.CAST), List.of(string)));

    for (RexNode expression : unsupported) {
      List<RexNode> input = List.of(fast, expression, fast);
      CountingFallback fallback = new CountingFallback(CALCITE_EXECUTOR);
      List<RexNode> actual = reduce(new PinotRexExecutor(fallback), builder, input);
      assertEquals(actual, reduce(CALCITE_EXECUTOR, builder, input));
      assertEquals(fallback._calls.get(), 1);
      assertSame(fallback._lastInput, input, "Fallback must receive the original full batch");
      assertSame(input.get(0), fast, "The immutable input must not be modified");
    }
  }

  @Test
  public void testFailedCastDoesNotPartiallyReduceBatch() {
    LiteralRexBuilder builder = new LiteralRexBuilder();
    RexNode fast = timestampCast(builder, SqlTypeName.CHAR, "2026-09-01 12:34:56.123", 3);
    RexNode invalid = timestampCast(builder, SqlTypeName.VARCHAR, "not-a-timestamp", 3);
    for (List<RexNode> input : List.of(List.of(fast, invalid), List.of(invalid, fast))) {
      CountingFallback fallback = new CountingFallback(CALCITE_EXECUTOR);
      List<RexNode> actual = reduce(new PinotRexExecutor(fallback), builder, input);
      assertEquals(actual, reduce(CALCITE_EXECUTOR, builder, input));
      assertEquals(actual, input, "Calcite keeps every expression unchanged when one expression fails");
      assertEquals(fallback._calls.get(), 0, "Failed eligible casts must not retry through the fallback");
      for (int i = 0; i < input.size(); i++) {
        assertSame(actual.get(i), input.get(i), "The entire failed batch must retain its original expressions");
      }
    }
  }

  @Test
  public void testTimezoneDependentTargetsUseTheirFallbackContext() {
    LiteralRexBuilder builder = new LiteralRexBuilder();
    RexNode expression = builder.makeAbstractCast(
        builder.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3),
        builder.stringLiteral("2026-09-01 12:34:56.123", SqlTypeName.CHAR), false);
    List<RexNode> input = List.of(expression);
    for (String zone : List.of("UTC", "America/Los_Angeles", "Asia/Kolkata")) {
      RexExecutor baseline = new RexExecutorImpl(DataContexts.of(
          Map.of(DataContext.Variable.TIME_ZONE.camelName, TimeZone.getTimeZone(zone))));
      CountingFallback fallback = new CountingFallback(baseline);
      assertEquals(reduce(new PinotRexExecutor(fallback), builder, input), reduce(baseline, builder, input), zone);
      assertEquals(fallback._calls.get(), 1);
      assertSame(fallback._lastInput, input);
    }
  }

  @Test
  public void testCastTemplateIsReusedAcrossLiteralValues() throws Exception {
    LiteralRexBuilder builder = new LiteralRexBuilder();
    RelDataType source = builder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR, 20);
    RelDataType target = builder.getTypeFactory().createSqlType(SqlTypeName.INTEGER);
    PinotRexExecutor executor = new PinotRexExecutor(FAILING_FALLBACK);
    Function1<DataContext, Object[]> template = executor.getCast(builder, source, target);
    for (String value : List.of("1", "2345", "-67")) {
      List<RexNode> input = List.of(builder.makeAbstractCast(target, builder.stringLiteral(value, source), false));
      assertEquals(reduce(executor, builder, input), reduce(CALCITE_EXECUTOR, builder, input));
      assertSame(executor.getCast(new LiteralRexBuilder(), source, target), template,
          "Literal values and equivalent builders must reuse the same compiled function");
    }
  }

  @Test
  public void testCastCacheDistinguishesFullTypesAndTypeSystem() throws Exception {
    LiteralRexBuilder builder = new LiteralRexBuilder();
    RelDataTypeFactory factory = builder.getTypeFactory();
    RelDataType source = factory.createSqlType(SqlTypeName.VARCHAR, 20);
    RelDataType target = factory.createSqlType(SqlTypeName.DECIMAL, 8, 2);
    PinotRexExecutor executor = new PinotRexExecutor(FAILING_FALLBACK);
    Function1<DataContext, Object[]> template = executor.getCast(builder, source, target);
    SqlCollation latinCollation = new SqlCollation(SqlCollation.Coercibility.IMPLICIT, Locale.US,
        StandardCharsets.ISO_8859_1, "primary");
    for (RelDataType otherSource : List.of(factory.createSqlType(SqlTypeName.CHAR, 20),
        factory.createSqlType(SqlTypeName.VARCHAR, 21), factory.createTypeWithNullability(source, true),
        factory.createTypeWithCharsetAndCollation(source, StandardCharsets.ISO_8859_1, latinCollation))) {
      assertNotSame(executor.getCast(builder, otherSource, target), template, otherSource.getFullTypeString());
    }
    for (RelDataType otherTarget : List.of(factory.createSqlType(SqlTypeName.DECIMAL, 9, 2),
        factory.createSqlType(SqlTypeName.DECIMAL, 8, 3), factory.createTypeWithNullability(target, true))) {
      assertNotSame(executor.getCast(builder, source, otherTarget), template, otherTarget.getFullTypeString());
    }
    RelDataType textTarget = factory.createSqlType(SqlTypeName.VARCHAR, 8);
    RelDataType latinTarget = factory.createTypeWithCharsetAndCollation(textTarget, StandardCharsets.ISO_8859_1,
        latinCollation);
    assertNotSame(executor.getCast(builder, source, textTarget), executor.getCast(builder, source, latinTarget));
    LiteralRexBuilder otherBuilder = new LiteralRexBuilder(new JavaTypeFactoryImpl(new RelDataTypeSystemImpl() {
      @Override
      public RoundingMode roundingMode() {
        return RoundingMode.FLOOR;
      }
    }));
    assertNotSame(executor.getCast(otherBuilder, source, target), template,
        "Different type systems must not share compiled rounding semantics");
    List<RexNode> input = List.of(builder.makeAbstractCast(target, builder.stringLiteral("-1.239", source), false));
    for (RexBuilder rexBuilder : List.of(builder, otherBuilder)) {
      assertEquals(reduce(executor, rexBuilder, input), reduce(CALCITE_EXECUTOR, rexBuilder, input));
    }
    assertSame(executor.getCast(builder, source, target), template);
  }

  @Test
  public void testCastCacheEvictsOldTemplates() throws Exception {
    LiteralRexBuilder builder = new LiteralRexBuilder();
    RelDataTypeFactory factory = builder.getTypeFactory();
    RelDataType source = factory.createSqlType(SqlTypeName.VARCHAR, 20);
    RelDataType integer = factory.createSqlType(SqlTypeName.INTEGER);
    RelDataType bigint = factory.createSqlType(SqlTypeName.BIGINT);
    RelDataType decimal = factory.createSqlType(SqlTypeName.DECIMAL, 8, 2);
    PinotRexExecutor executor = new PinotRexExecutor(FAILING_FALLBACK, 2);
    Function1<DataContext, Object[]> first = executor.getCast(builder, source, integer);
    Function1<DataContext, Object[]> second = executor.getCast(builder, source, bigint);
    executor.getCast(builder, source, decimal);
    assertSame(executor.getCast(builder, source, bigint), second, "A recently compiled template should remain cached");
    assertNotSame(executor.getCast(builder, source, integer), first, "The oldest template must be evicted at capacity");
    List<RexNode> input = List.of(builder.makeAbstractCast(integer, builder.stringLiteral("123", source), false));
    assertEquals(reduce(executor, builder, input), reduce(CALCITE_EXECUTOR, builder, input));
  }

  @Test
  public void testSharedExecutorHasNoCrossRequestState() throws Exception {
    PinotRexExecutor executor = new PinotRexExecutor(FAILING_FALLBACK);
    ExecutorService threads = Executors.newFixedThreadPool(4);
    CountDownLatch ready = new CountDownLatch(4);
    try {
      List<Callable<Void>> tasks = new ArrayList<>();
      for (int task = 0; task < 16; task++) {
        LiteralRexBuilder localBuilder = new LiteralRexBuilder();
        RelDataType source = localBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR, 20);
        RelDataType target = localBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER);
        List<RexNode> input = List.of(localBuilder.makeAbstractCast(target,
            localBuilder.stringLiteral(Integer.toString(100 + task), source), false));
        List<RexNode> expected = reduce(CALCITE_EXECUTOR, localBuilder, input);
        tasks.add(() -> {
          ready.countDown();
          assertTrue(ready.await(30, TimeUnit.SECONDS), "All workers must reach the fresh cache together");
          for (int i = 0; i < 10; i++) {
            assertEquals(reduce(executor, localBuilder, input), expected);
          }
          return null;
        });
      }
      for (Future<Void> result : threads.invokeAll(tasks)) {
        result.get();
      }
    } finally {
      threads.shutdownNow();
    }
  }

  @Test
  @SuppressWarnings("try") // The resources scope the thread-local hooks; their values are intentionally unused.
  public void testRelBuilderTransformPreservesOptimizedExecutor() {
    AtomicInteger generatedReductions = new AtomicInteger();
    RelNode expected;
    try (Hook.Closeable ignored = Hook.EXPRESSION_REDUCER.addThread(
        (Consumer<Object>) event -> generatedReductions.incrementAndGet())) {
      expected = projectTimestampCast(CALCITE_EXECUTOR);
    }
    assertTrue(generatedReductions.get() > 0, "The control must exercise Calcite's generated reducer");

    generatedReductions.set(0);
    RelNode actual;
    try (Hook.Closeable ignored = Hook.EXPRESSION_REDUCER.addThread(
        (Consumer<Object>) event -> generatedReductions.incrementAndGet())) {
      actual = projectTimestampCast(PinotRexExecutor.INSTANCE);
    }
    assertEquals(RelOptUtil.toString(actual), RelOptUtil.toString(expected));
    assertEquals(generatedReductions.get(), 0, "RelBuilder.transform must preserve the planner's optimized executor");
  }

  @Test
  public void testRelBuilderTransformPreservesExplicitExecutor() {
    CountingFallback explicitExecutor = new CountingFallback(CALCITE_EXECUTOR);
    RelNode actual = projectTimestampCast(explicitExecutor);
    assertTrue(explicitExecutor._calls.get() > 0, "An explicit planner executor must retain precedence");
    assertEquals(RelOptUtil.toString(actual), RelOptUtil.toString(projectTimestampCast(CALCITE_EXECUTOR)));
  }

  private static RelNode projectTimestampCast(RexExecutor executor) {
    LiteralRexBuilder builder = new LiteralRexBuilder();
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.setExecutor(executor);
    RelBuilder relBuilder = PinotRuleUtils.PINOT_REL_FACTORY.create(RelOptCluster.create(planner, builder), null)
        .transform(config -> config.withPruneInputOfAggregate(true));
    RelNode result = relBuilder.values(new String[]{"unused"}, 0)
        .project(timestampCast(builder, SqlTypeName.CHAR, "2026-09-01 12:34:56.123", 3)).build();
    assertSame(planner.getExecutor(), executor, "RelBuilder.transform must not replace the planner executor");
    return result;
  }

  private static RexNode timestampCast(LiteralRexBuilder builder, SqlTypeName sourceType, String value,
      int precision) {
    // makeCast may fold a literal before it reaches the executor, which would make a dispatch test vacuous.
    return builder.makeAbstractCast(builder.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP, precision),
        builder.stringLiteral(value, sourceType), false);
  }

  private static List<RexNode> reduce(RexExecutor executor, RexBuilder builder, List<RexNode> expressions) {
    List<RexNode> reduced = new ArrayList<>();
    executor.reduce(builder, expressions, reduced);
    return reduced;
  }

  private static class CountingFallback implements RexExecutor {
    private final RexExecutor _delegate;
    private final AtomicInteger _calls = new AtomicInteger();
    private List<RexNode> _lastInput;

    CountingFallback(RexExecutor delegate) {
      _delegate = delegate;
    }

    @Override
    public void reduce(RexBuilder builder, List<RexNode> expressions, List<RexNode> reduced) {
      _calls.incrementAndGet();
      _lastInput = expressions;
      _delegate.reduce(builder, expressions, reduced);
    }
  }

  private static class LiteralRexBuilder extends RexBuilder {
    LiteralRexBuilder() {
      this(new TypeFactory());
    }

    LiteralRexBuilder(RelDataTypeFactory typeFactory) {
      super(typeFactory);
    }

    RexLiteral stringLiteral(String value, SqlTypeName sourceType) {
      // Preserve VARCHAR as the source SQL type; the public literal helper normally lowers it to CHAR or a CAST.
      return stringLiteral(value, getTypeFactory().createSqlType(sourceType, Math.max(1, value.length())));
    }

    RexLiteral stringLiteral(String value, RelDataType type) {
      return makeLiteral(new NlsString(value, null, null), type, SqlTypeName.CHAR);
    }
  }
}
