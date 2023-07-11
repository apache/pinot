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
package org.apache.calcite.sql.fun;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.sql.PinotSqlAggFunction;
import org.apache.calcite.sql.PinotSqlTransformFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.util.Util;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;


/**
 * {@link PinotOperatorTable} defines the {@link SqlOperator} overrides on top of the {@link SqlStdOperatorTable}.
 *
 * <p>The main purpose of this Pinot specific SQL operator table is to
 * <ul>
 *   <li>Ensure that any specific SQL validation rules can apply with Pinot override entirely over Calcite's.</li>
 *   <li>Ability to create customer operators that are not function and cannot use
 *     {@link org.apache.calcite.prepare.Prepare.CatalogReader} to override</li>
 *   <li>Still maintain minimum customization and benefit from Calcite's original operator table setting.</li>
 * </ul>
 */
@SuppressWarnings("unused") // unused fields are accessed by reflection
public class PinotOperatorTable extends SqlStdOperatorTable {

  private static @MonotonicNonNull PinotOperatorTable _instance;

  public static final SqlFunction COALESCE = new PinotSqlCoalesceFunction();

  private static final Set<SqlKind> KINDS =
      Stream.concat(
              Arrays.stream(AggregationFunctionType.values())
                  .filter(func -> func.getSqlKind() != null)
                  .flatMap(func -> Stream.of(func.getSqlKind())),
              Arrays.stream(TransformFunctionType.values())
                  .filter(func -> func.getSqlKind() != null)
                  .flatMap(func -> Stream.of(func.getSqlKind())))
          .collect(Collectors.toSet());

  private static final Set<String> CALCITE_OPERATOR_TABLE_REGISTERED_FUNCTIONS =
      Stream.concat(
              Arrays.stream(AggregationFunctionType.values())
                  // TODO: remove below once all V1 AGG functions are registered
                  .filter(func -> func.getSqlKind() != null)
                  .flatMap(func -> Stream.of(func.name(), func.getName(), func.getName().toUpperCase(),
                      func.getName().toLowerCase())),
              Arrays.stream(TransformFunctionType.values())
                  // TODO: remove below once all V1 Transform functions are registered
                  .filter(func -> func.getSqlKind() != null)
                  .flatMap(func -> Stream.of(func.name(), func.getName(), func.getName().toUpperCase(),
                      func.getName().toLowerCase())))
          .collect(Collectors.toSet());

  // TODO: clean up lazy init by using Suppliers.memorized(this::computeInstance) and make getter wrapped around
  // supplier instance. this should replace all lazy init static objects in the codebase
  public static synchronized PinotOperatorTable instance() {
    if (_instance == null) {
      // Creates and initializes the standard operator table.
      // Uses two-phase construction, because we can't initialize the
      // table until the constructor of the sub-class has completed.
      _instance = new PinotOperatorTable();
      _instance.initNoDuplicate();
    }
    return _instance;
  }

  public static boolean isAggregationFunctionRegisteredWithOperatorTable(String functionName) {
    return CALCITE_OPERATOR_TABLE_REGISTERED_FUNCTIONS.contains(functionName);
  }

  /**
   * Initialize without duplicate, e.g. when 2 duplicate operator is linked with the same op
   * {@link org.apache.calcite.sql.SqlKind} it causes problem.
   *
   * <p>This is a direct copy of the {@link org.apache.calcite.sql.util.ReflectiveSqlOperatorTable} and can be hard to
   * debug, suggest changing to a non-dynamic registration. Dynamic function support should happen via catalog.
   *
   * This also registers aggregation functions defined in {@link org.apache.pinot.segment.spi.AggregationFunctionType}
   * which are multistage enabled.
   */
  public final void initNoDuplicate() {
    // Use reflection to register the expressions stored in public fields.
    for (Field field : getClass().getFields()) {
      try {
        if (SqlFunction.class.isAssignableFrom(field.getType())) {
          SqlFunction op = (SqlFunction) field.get(this);
          if (op != null && notRegistered(op)) {
            register(op);
          }
        } else if (
            SqlOperator.class.isAssignableFrom(field.getType())) {
          SqlOperator op = (SqlOperator) field.get(this);
          if (op != null && notRegistered(op)) {
            register(op);
          }
        }
      } catch (IllegalArgumentException | IllegalAccessException e) {
        throw Util.throwAsRuntime(Util.causeOrSelf(e));
      }
    }

    // Walk through all the Pinot aggregation types and
    //   1. register those that are supported in multistage in addition to calcite standard opt table.
    //   2. register special handling that differs from calcite standard.
    for (AggregationFunctionType aggregationFunctionType : AggregationFunctionType.values()) {
      if (aggregationFunctionType.getSqlKind() != null) {
        // 1. Register the aggregation function with Calcite
        registerAggregateFunction(aggregationFunctionType.getName(), aggregationFunctionType);
        // 2. Register the aggregation function with Calcite on all alternative names
        List<String> alternativeFunctionNames = aggregationFunctionType.getAlternativeNames();
        for (String alternativeFunctionName : alternativeFunctionNames) {
          registerAggregateFunction(alternativeFunctionName, aggregationFunctionType);
        }
      }
    }

    // Walk through all the Pinot transform types and
    //   1. register those that are supported in multistage in addition to calcite standard opt table.
    //   2. register special handling that differs from calcite standard.
    for (TransformFunctionType transformFunctionType : TransformFunctionType.values()) {
      if (transformFunctionType.getSqlKind() != null) {
        // 1. Register the aggregation function with Calcite
        registerTransformFunction(transformFunctionType.getName(), transformFunctionType);
        // 2. Register the aggregation function with Calcite on all alternative names
        List<String> alternativeFunctionNames = transformFunctionType.getAlternativeNames();
        for (String alternativeFunctionName : alternativeFunctionNames) {
          registerTransformFunction(alternativeFunctionName, transformFunctionType);
        }
      }
    }
  }

  private void registerAggregateFunction(String functionName, AggregationFunctionType functionType) {
    // register function behavior that's different from Calcite
    if (functionType.getOperandTypeChecker() != null && functionType.getReturnTypeInference() != null) {
      PinotSqlAggFunction sqlAggFunction = new PinotSqlAggFunction(functionName.toUpperCase(Locale.ROOT), null,
          functionType.getSqlKind(), functionType.getReturnTypeInference(), null,
          functionType.getOperandTypeChecker(), functionType.getSqlFunctionCategory());
      if (notRegistered(sqlAggFunction)) {
        register(sqlAggFunction);
      }
    }
  }

  private void registerTransformFunction(String functionName, TransformFunctionType functionType) {
    // register function behavior that's different from Calcite
    if (functionType.getOperandTypeChecker() != null && functionType.getReturnTypeInference() != null) {
      PinotSqlTransformFunction sqlTransformFunction =
          new PinotSqlTransformFunction(functionName.toUpperCase(Locale.ROOT),
              functionType.getSqlKind(), functionType.getReturnTypeInference(), null,
              functionType.getOperandTypeChecker(), functionType.getSqlFunctionCategory());
      if (notRegistered(sqlTransformFunction)) {
        register(sqlTransformFunction);
      }
    }
  }

  private boolean notRegistered(SqlFunction op) {
    List<SqlOperator> operatorList = new ArrayList<>();
    lookupOperatorOverloads(op.getNameAsId(), op.getFunctionType(), op.getSyntax(), operatorList,
        SqlNameMatchers.withCaseSensitive(false));
    return operatorList.size() == 0;
  }

  private boolean notRegistered(SqlOperator op) {
    List<SqlOperator> operatorList = new ArrayList<>();
    lookupOperatorOverloads(op.getNameAsId(), null, op.getSyntax(), operatorList,
        SqlNameMatchers.withCaseSensitive(false));
    return operatorList.size() == 0;
  }
}
