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
/// with an input reference instead of a literal, so different values share Calcite's conversion code. Other expressions
/// use the fallback executor. Cached functions are immutable; input values and results stay local to each call, making
/// this executor thread-safe without sharing RexExecutable's mutable data context.
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
      if (!isSupportedStringCast(expression)) {
        _fallback.reduce(rexBuilder, constExps, reducedValues);
        return;
      }
    }
    List<RexNode> literals = new ArrayList<>(constExps.size());
    try {
      for (RexNode expression : constExps) {
        RexLiteral operand = (RexLiteral) ((RexCall) expression).getOperands().get(0);
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
