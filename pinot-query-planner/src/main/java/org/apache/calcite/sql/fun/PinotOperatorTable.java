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
      Arrays.stream(AggregationFunctionType.values()).filter(func -> func.getSqlKind() != null)
          .flatMap(func -> Stream.of(func.getSqlKind())).collect(Collectors.toSet());

  private static final Set<String> CALCITE_OPERATOR_TABLE_REGISTERED_FUNCTIONS =
      Arrays.stream(AggregationFunctionType.values())
          .filter(func -> func.getSqlKind() != null) // TODO: remove this once all V1 AGG functions are registered
          .flatMap(func -> Stream.of(func.name(), func.getName(), func.getName().toUpperCase(),
              func.getName().toLowerCase()))
          .collect(Collectors.toSet());

  // TODO: clean up lazy init by using Suppliers.memorized(this::computeInstance) and make getter wrapped around
  // supplier instance. this should replace all lazy init static objects in the codebase
  public static synchronized PinotOperatorTable instance() {
    if (_instance == null) {
      // Creates and initializes the standard operator table.
      // Uses two-phase construction, because we can't initialize the
      // table until the constructor of the sub-class has completed.
      _instance = new PinotOperatorTable();
      _instance.initNoDuplicateFunctions();
      _instance.initAggregationFunctions();
      _instance.initTransformFunctions();
    }
    return _instance;
  }

  public static boolean isAggregationKindSupported(SqlKind sqlKind) {
    return KINDS.contains(sqlKind);
  }

  public static boolean isAggregationReduceSupported(String functionName) {
    if (AGGREGATION_REDUCE_SUPPORTED_FUNCTIONS.contains(functionName)) {
      return true;
    }
    String upperCaseFunctionName = AggregationFunctionType.getNormalizedAggregationFunctionName(functionName);
    return AGGREGATION_REDUCE_SUPPORTED_FUNCTIONS.contains(upperCaseFunctionName);
  }

  public static boolean isTransformFunctionRegisteredWithOperatorTable(String functionName) {
    if (CALCITE_OPERATOR_TABLE_REGISTERED_FUNCTIONS.contains(functionName)) {
      return true;
    }
    String upperCaseFunctionName = TransformFunctionType.getNormalizedTransformFunctionName(functionName);
    return CALCITE_OPERATOR_TABLE_REGISTERED_FUNCTIONS.contains(upperCaseFunctionName);
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
   */
  public final void initNoDuplicateFunctions() {
    // Use reflection to register the expressions stored in public fields.
    for (Field field : getClass().getFields()) {
      try {
        if (SqlFunction.class.isAssignableFrom(field.getType())) {
          SqlFunction op = (SqlFunction) field.get(this);
          if (op != null && notRegistered(op)) {
            if (!isPinotAggregationFunction(op.getName()) && !isPinotTransformFunction(op.getName())) {
              // Register the standard Calcite functions and those defined in this class only
              register(op);
            }
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
  }

  // Walk through all the Pinot aggregation types and
  //   1. register those that are supported in multistage in addition to calcite standard opt table.
  //   2. register special handling that differs from calcite standard.
  public final void initAggregationFunctions() {
    // Walk through all the Pinot aggregation types and register those that are supported in multistage and which
    // aren't standard Calcite functions such as SUM / MIN / MAX / COUNT etc.
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
  }

  /**
   * This registers transform functions defined in {@link org.apache.pinot.common.function.TransformFunctionType}
   * which are multistage enabled.
   */
  private void initTransformFunctions() {
    // Walk through all the Pinot transform types and register those that are supported in multistage.
    for (TransformFunctionType transformFunctionType : TransformFunctionType.values()) {
      if (transformFunctionType.getSqlKind() == null) {
        // Skip registering functions which are standard Calcite functions and functions which are not yet supported.
        continue;
      }

      // Register the aggregation function with Calcite along with all alternative names
      List<PinotSqlTransformFunction> sqlTransformFunctions = new ArrayList<>();
      PinotSqlTransformFunction aggFunction = generatePinotSqlTransformFunction(transformFunctionType.getName(),
          transformFunctionType, false);
      sqlTransformFunctions.add(aggFunction);
      List<String> alternativeFunctionNames = transformFunctionType.getAliases();
      if (alternativeFunctionNames == null || alternativeFunctionNames.size() == 0) {
        // If no alternative function names are specified, generate one which converts camel case to have underscores
        // as delimiters instead. E.g. boolAnd -> BOOL_AND
        String alternativeFunctionName =
            convertCamelCaseToUseUnderscores(transformFunctionType.getName());
        PinotSqlTransformFunction function =
            generatePinotSqlTransformFunction(alternativeFunctionName, transformFunctionType,
                false);
        sqlTransformFunctions.add(function);
      } else {
        for (String alternativeFunctionName : alternativeFunctionNames) {
          PinotSqlTransformFunction function =
              generatePinotSqlTransformFunction(alternativeFunctionName, transformFunctionType,
                  false);
          sqlTransformFunctions.add(function);
        }
      }
      for (PinotSqlTransformFunction sqlAggFunction : sqlTransformFunctions) {
        if (notRegistered(sqlAggFunction)) {
          register(sqlAggFunction);
        }
      }
    }
  }

  private static String convertCamelCaseToUseUnderscores(String functionName) {
    // Skip functions that have numbers for now and return their name as is
    return functionName.matches(".*\\d.*")
        ? functionName
        : functionName.replaceAll("(.)(\\p{Upper}+|\\d+)", "$1_$2");
  }

  private static PinotSqlAggFunction generatePinotSqlAggFunction(String functionName,
      AggregationFunctionType aggregationFunctionType, boolean isIntermediateStageFunction) {
    return new PinotSqlAggFunction(functionName.toUpperCase(Locale.ROOT),
        aggregationFunctionType.getSqlIdentifier(),
        aggregationFunctionType.getSqlKind(),
        isIntermediateStageFunction
            ? aggregationFunctionType.getSqlIntermediateReturnTypeInference()
            : aggregationFunctionType.getSqlReturnTypeInference(),
        aggregationFunctionType.getSqlOperandTypeInference(),
        aggregationFunctionType.getSqlOperandTypeChecker(),
        aggregationFunctionType.getSqlFunctionCategory());
  }

  private static PinotSqlTransformFunction generatePinotSqlTransformFunction(String functionName,
      TransformFunctionType transformFunctionType, boolean isIntermediateStageFunction) {
    return new PinotSqlTransformFunction(functionName.toUpperCase(Locale.ROOT),
        transformFunctionType.getSqlKind(),
        transformFunctionType.getSqlReturnTypeInference(),
        transformFunctionType.getSqlOperandTypeInference(),
        transformFunctionType.getSqlOperandTypeChecker(),
        transformFunctionType.getSqlFunctionCategory());
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

  private boolean isPinotAggregationFunction(String name) {
    AggregationFunctionType aggFunctionType = null;
    if (isAggregationFunctionRegisteredWithOperatorTable(name)) {
      aggFunctionType = AggregationFunctionType.getAggregationFunctionType(name);
    }
    return aggFunctionType != null && !aggFunctionType.isNativeCalciteAggregationFunctionType();
  }

  private boolean isPinotTransformFunction(String name) {
    TransformFunctionType transformFunctionType = null;
    if (isTransformFunctionRegisteredWithOperatorTable(name)) {
      try {
        transformFunctionType = TransformFunctionType.valueOf(name);
      } catch (IllegalArgumentException e) {
        // Ignore
      }
    }
    return transformFunctionType != null;
  }
}
