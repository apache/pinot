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
package org.apache.pinot.common.function;

import java.math.BigDecimal;
import java.util.List;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.common.function.AggregationFunctionTypeResolver.TypeInferenceUnavailableException;
import org.apache.pinot.common.request.context.AggregateCallBinding;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Verifies how schema-derived operand types and literal options reach an aggregate's registered return type
/// inference. No built-in aggregate requires a binding yet, so the required path uses a mocked aggregate type.
public class AggregationFunctionTypeResolverTest {
  private static final ExpressionContext VALUE = ExpressionContext.forIdentifier("value");
  private static final ExpressionContext MAX = ExpressionContext.forLiteral(DataType.STRING, "MAX");
  private static final IntFunction<ColumnDataType> NOT_RESOLVED = i -> {
    throw new AssertionError("Argument types must not be resolved when no binding is required");
  };

  @Test
  public void testFixedResultAggregatesAreNotBound() {
    for (AggregationFunctionType type : List.of(AggregationFunctionType.COUNT, AggregationFunctionType.SUM,
        AggregationFunctionType.DISTINCTCOUNT)) {
      assertNull(AggregationFunctionTypeResolver.bind(type, List.of(VALUE, MAX), NOT_RESOLVED), type.name());
      assertFalse(type.supportsLegacyUnboundCalls(), type.name());
    }
  }

  @Test
  public void testRequiredBindingInfersResultType() {
    AggregationFunctionType type = requiringBinding(ReturnTypes.ARG0, false);
    // Overload selection sees which operands are string literals, without resolving any operand type.
    when(type.isTypeBindingRequired(anyInt(), any())).thenAnswer(invocation -> {
      IntPredicate isStringLiteral = invocation.getArgument(1);
      return !isStringLiteral.test(0) && isStringLiteral.test(1);
    });
    AggregateCallBinding binding = AggregationFunctionTypeResolver.bind(type, List.of(VALUE, MAX),
        i -> i == 0 ? ColumnDataType.TIMESTAMP : ColumnDataType.STRING);
    assertEquals(binding,
        new AggregateCallBinding(List.of(ColumnDataType.TIMESTAMP, ColumnDataType.STRING), ColumnDataType.TIMESTAMP));
  }

  @Test
  public void testMissingMetadataFallsBackOnlyForLegacyCalls() {
    IntFunction<ColumnDataType> unavailable = i -> {
      throw new TypeInferenceUnavailableException("serverOnlyTransform");
    };
    assertNull(AggregationFunctionTypeResolver.bind(requiringBinding(ReturnTypes.ARG0, true), List.of(VALUE),
        unavailable));
    TypeInferenceUnavailableException error = expectThrows(TypeInferenceUnavailableException.class,
        () -> AggregationFunctionTypeResolver.bind(requiringBinding(ReturnTypes.ARG0, false), List.of(VALUE),
            unavailable));
    assertTrue(error.getMessage().contains("serverOnlyTransform"), error.getMessage());

    // An invalid expression is not missing metadata, so even a legacy call does not fall back.
    IntFunction<ColumnDataType> invalid = i -> {
      throw new IllegalArgumentException("Cannot resolve type of unknown column: value");
    };
    expectThrows(IllegalArgumentException.class,
        () -> AggregationFunctionTypeResolver.bind(requiringBinding(ReturnTypes.ARG0, true), List.of(VALUE), invalid));
  }

  @Test
  public void testUninferableResultTypeFails() {
    IllegalArgumentException error = expectThrows(IllegalArgumentException.class,
        () -> AggregationFunctionTypeResolver.inferReturnType("pick", binding -> null, List.of(VALUE),
            List.of(ColumnDataType.INT)));
    assertTrue(error.getMessage().contains("Cannot infer result type for pick"), error.getMessage());
  }

  @Test
  public void testLiteralOperandsReachInference() {
    List<ExpressionContext> arguments = List.of(VALUE, MAX, ExpressionContext.forLiteral(DataType.UNKNOWN, null),
        ExpressionContext.forLiteral(DataType.INT, 7));
    SqlReturnTypeInference probe = binding -> {
      assertFalse(binding.isOperandLiteral(0, false));
      assertTrue(binding.isOperandLiteral(1, false));
      assertFalse(binding.isOperandNull(0, false));
      assertFalse(binding.isOperandNull(1, false));
      assertTrue(binding.isOperandNull(2, false));
      assertEquals(binding.getOperandLiteralValue(1, String.class), "MAX");
      assertNull(binding.getOperandLiteralValue(2, String.class));
      assertEquals(binding.getOperandLiteralValue(3, BigDecimal.class), new BigDecimal(7));
      assertEquals(binding.getOperandLiteralValue(3, Integer.class), Integer.valueOf(7));
      IllegalArgumentException error =
          expectThrows(IllegalArgumentException.class, () -> binding.getOperandLiteralValue(0, String.class));
      assertTrue(error.getMessage().contains("must be a literal"), error.getMessage());
      // An aggregate must have a result type even when no row is aggregated.
      assertTrue(binding.hasEmptyGroup());
      return binding.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
    };
    assertEquals(AggregationFunctionTypeResolver.inferReturnType("probe", probe, arguments,
        List.of(ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.UNKNOWN, ColumnDataType.INT)),
        ColumnDataType.LONG);
  }

  @DataProvider
  public Object[][] logicalTypes() {
    return new Object[][]{
        {SqlTypeName.TINYINT, ColumnDataType.INT},
        {SqlTypeName.SMALLINT, ColumnDataType.INT},
        {SqlTypeName.INTEGER, ColumnDataType.INT},
        {SqlTypeName.BIGINT, ColumnDataType.LONG},
        {SqlTypeName.REAL, ColumnDataType.FLOAT},
        {SqlTypeName.FLOAT, ColumnDataType.FLOAT},
        {SqlTypeName.DOUBLE, ColumnDataType.DOUBLE},
        {SqlTypeName.DECIMAL, ColumnDataType.BIG_DECIMAL},
        {SqlTypeName.BOOLEAN, ColumnDataType.BOOLEAN},
        {SqlTypeName.TIMESTAMP, ColumnDataType.TIMESTAMP},
        {SqlTypeName.CHAR, ColumnDataType.STRING},
        {SqlTypeName.VARCHAR, ColumnDataType.STRING},
        {SqlTypeName.BINARY, ColumnDataType.BYTES},
        {SqlTypeName.VARBINARY, ColumnDataType.BYTES},
        {SqlTypeName.UUID, ColumnDataType.UUID},
        {SqlTypeName.NULL, ColumnDataType.UNKNOWN},
        {SqlTypeName.ANY, ColumnDataType.OBJECT},
        {SqlTypeName.OTHER, ColumnDataType.OBJECT}
    };
  }

  @Test(dataProvider = "logicalTypes")
  public void testLogicalTypesArePreserved(SqlTypeName sqlType, ColumnDataType expected) {
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    assertEquals(AggregationFunctionTypeResolver.toColumnDataType(typeFactory.createSqlType(sqlType)), expected);
  }

  @Test
  public void testArrayAndUnsupportedTypes() {
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    assertEquals(AggregationFunctionTypeResolver.toColumnDataType(
        typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), -1)),
        ColumnDataType.TIMESTAMP_ARRAY);
    expectThrows(IllegalArgumentException.class,
        () -> AggregationFunctionTypeResolver.toColumnDataType(typeFactory.createSqlType(SqlTypeName.DATE)));
  }

  /// Mocks an aggregate type whose calls require a binding derived from the given return type inference.
  private static AggregationFunctionType requiringBinding(SqlReturnTypeInference inference,
      boolean supportsLegacyUnboundCalls) {
    AggregationFunctionType type = mock(AggregationFunctionType.class);
    when(type.getName()).thenReturn("polymorphic");
    when(type.getReturnTypeInference()).thenReturn(inference);
    when(type.isTypeBindingRequired(anyInt(), any())).thenReturn(true);
    when(type.supportsLegacyUnboundCalls()).thenReturn(supportsLegacyUnboundCalls);
    return type;
  }
}
