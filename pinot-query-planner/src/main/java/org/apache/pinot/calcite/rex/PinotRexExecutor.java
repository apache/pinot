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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.calcite.DataContext;
import org.apache.calcite.DataContexts;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;


/// Reduces string literal casts using bounded, reusable Calcite executables. Each source/target type pair is compiled
/// with an input reference instead of a literal, so different values share Calcite's conversion code. Casts of exact
/// numeric literals to DOUBLE, FLOAT or REAL are rounded directly. Arrays of approximate literals that already have
/// their final element type and precision are kept as they are. Other expressions use the fallback executor. Cached
/// functions are immutable; input values and results stay local to each call, making this executor thread-safe
/// without sharing RexExecutable's mutable data context.
public final class PinotRexExecutor implements RexExecutor {
  public static final PinotRexExecutor INSTANCE = new PinotRexExecutor(RexUtil.EXECUTOR);

  private final RexExecutor _fallback;
  private final Cache<CastSignature, Function1<DataContext, Object[]>> _casts;

  @VisibleForTesting
  PinotRexExecutor(RexExecutor fallback) {
    this(fallback, 256);
  }

  @VisibleForTesting
  PinotRexExecutor(RexExecutor fallback, int cacheSize) {
    _fallback = fallback;
    _casts = CacheBuilder.newBuilder().maximumSize(cacheSize).build();
  }

  @Override
  public void reduce(RexBuilder rexBuilder, List<RexNode> constExps, List<RexNode> reducedValues) {
    // Calcite treats reduction as all-or-nothing. Delegate the entire batch if any expression is unsupported, so a
    // failure in the fallback cannot leave the batch partially reduced.
    for (RexNode expression : constExps) {
      if (!isSupportedStringCast(expression) && !isReducedVector(expression)
          && !isNumericToApproximateCast(expression)) {
        _fallback.reduce(rexBuilder, constExps, reducedValues);
        return;
      }
    }
    List<RexNode> literals = new ArrayList<>(constExps.size());
    try {
      for (RexNode expression : constExps) {
        RexCall call = (RexCall) expression;
        if (call.getOperator() == SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR) {
          literals.add(call);
          continue;
        }
        RexLiteral operand = (RexLiteral) call.getOperands().get(0);
        if (!SqlTypeUtil.isCharacter(operand.getType())) {
          BigDecimal value = operand.getValueAs(BigDecimal.class);
          // Calcite uses single precision for REAL and double precision for FLOAT and DOUBLE.
          double rounded =
              call.getType().getSqlTypeName() == SqlTypeName.REAL ? value.floatValue() : value.doubleValue();
          literals.add(rexBuilder.makeLiteral(rounded, call.getType(), true));
          continue;
        }
        Object value = null;
        if (!operand.isNull()) {
          Function1<DataContext, Object[]> cast = getCast(rexBuilder, operand.getType(), expression.getType());
          DataContext context = DataContexts.of(Map.of("inputRecord", new Object[]{RexLiteral.stringValue(operand)}));
          value = cast.apply(context)[0];
        }
        literals.add(rexBuilder.makeLiteral(value, expression.getType(), true));
      }
    } catch (RuntimeException | ExecutionException e) {
      // Like Calcite, retain the entire batch if conversion, compilation or literal construction fails. In particular,
      // invalid epoch strings must remain available for Pinot's later timestamp conversion.
      reducedValues.addAll(constExps);
      return;
    }
    reducedValues.addAll(literals);
  }

  @VisibleForTesting
  Function1<DataContext, Object[]> getCast(RexBuilder builder, RelDataType source, RelDataType target)
      throws ExecutionException {
    CastSignature signature = new CastSignature(source, target, builder.getTypeFactory().getTypeSystem());
    return _casts.get(signature, () -> {
      RelDataType rowType = builder.getTypeFactory().builder().add("value", source).build();
      RexNode cast = builder.makeAbstractCast(target, builder.makeInputRef(source, 0), false);
      return RexExecutorImpl.getExecutable(builder, List.of(cast), rowType).getFunction();
    });
  }

  private static boolean isNumericToApproximateCast(RexNode expression) {
    if (!(expression instanceof RexCall call) || call.getOperator() != SqlStdOperatorTable.CAST
        || call.getOperands().size() != 1 || !SqlTypeUtil.isApproximateNumeric(call.getType())
        || !(call.getOperands().get(0) instanceof RexLiteral literal)
        || !(literal.getValue() instanceof BigDecimal value)) {
      return false;
    }
    RelDataType source = literal.getType();
    if (SqlTypeUtil.isDecimal(source)) {
      return true;
    }
    if (!SqlTypeName.INT_TYPES.contains(source.getSqlTypeName())) {
      return false;
    }
    // An INTEGER-typed Rex literal can carry a wider value. Let Calcite narrow it to the source width first.
    long max = SqlTypeUtil.maxValue(source);
    return value.scale() == 0 && value.compareTo(BigDecimal.valueOf(-max - 1)) >= 0
        && value.compareTo(BigDecimal.valueOf(max)) <= 0;
  }

  private static boolean isReducedVector(RexNode expression) {
    if (!(expression instanceof RexCall call) || call.getOperator() != SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR
        || call.getType().getSqlTypeName() != SqlTypeName.ARRAY || call.getType().isNullable()
        || call.getOperands().isEmpty()) {
      return false;
    }
    RelDataType component = call.getType().getComponentType();
    if (component == null || component.isNullable() || !SqlTypeUtil.isApproximateNumeric(component)) {
      return false;
    }
    // Calcite rebuilds each element at the array's component type and rounds REAL elements to single precision.
    for (RexNode operand : call.getOperands()) {
      if (!(operand instanceof RexLiteral literal) || !component.equals(literal.getType())
          || !(literal.getValue() instanceof Double value) || Double.isNaN(value)
          || (component.getSqlTypeName() == SqlTypeName.REAL && value.doubleValue() != value.floatValue())) {
        return false;
      }
    }
    return true;
  }

  private static boolean isSupportedStringCast(RexNode expression) {
    if (!(expression instanceof RexCall)) {
      return false;
    }
    RexCall call = (RexCall) expression;
    if (call.getOperator() != SqlStdOperatorTable.CAST || call.getOperands().size() != 1) {
      return false;
    }
    // Keep timezone-dependent and structured conversions in the fallback's own data context.
    RelDataType target = call.getType();
    if (!SqlTypeUtil.isAtomic(target) || SqlTypeName.TZ_TYPES.contains(target.getSqlTypeName())
        || target.getSqlTypeName() == SqlTypeName.VARIANT) {
      return false;
    }
    RexNode operand = call.getOperands().get(0);
    return operand instanceof RexLiteral && SqlTypeUtil.isCharacter(operand.getType());
  }

  /// Full types retain precision, scale, nullability, charset and collation; the type system supplies rounding rules.
  /// Literal values never enter the key or the generated code.
  private record CastSignature(RelDataType source, RelDataType target, RelDataTypeSystem typeSystem) {
  }
}
