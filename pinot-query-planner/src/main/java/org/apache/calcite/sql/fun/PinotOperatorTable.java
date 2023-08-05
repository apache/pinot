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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.calcite.sql.PinotSqlAggFunction;
import org.apache.calcite.sql.PinotSqlTransformFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotOperatorTable.class);

  private static @MonotonicNonNull PinotOperatorTable _instance;

  // TODO: clean up lazy init by using Suppliers.memorized(this::computeInstance) and make getter wrapped around
  // supplier instance. this should replace all lazy init static objects in the codebase
  public static synchronized PinotOperatorTable instance() {
    if (_instance == null) {
      // Creates and initializes the standard operator table.
      // Uses two-phase construction, because we can't initialize the
      // table until the constructor of the sub-class has completed.
      _instance = new PinotOperatorTable();
      _instance.initialize();
    }
    return _instance;
  }

  /**
   * Initialize sql functions and operators.
   * All duplicates operators linked with the same {@link org.apache.calcite.sql.SqlKind} will cause failure and need
   * to resolved manually by dev.
   *
   * This also registers aggregation functions defined in {@link org.apache.pinot.segment.spi.AggregationFunctionType}
   * which are multistage enabled.
   */
  public final void initialize() {
    // Register some hand-picked Calcite standard SQL functions first, the list is a subset of functions/operators
    // from SqlStdOperatorTable.
    // Note, if Pinot functions duplicate in this list, we need to manually comment the functions registered.
    initCalciteStandardSql();

    // Register Pinot specific SqlFunction or SqlOperator.
    initPinotOverrideSql();

    // Walk through all the Pinot aggregation types and
    //   1. register those that are supported in multistage in addition to calcite standard opt table.
    //   2. register special handling that differs from calcite standard.
    for (AggregationFunctionType aggregationFunctionType : AggregationFunctionType.values()) {
      if (aggregationFunctionType.getSqlKind() != null) {
        Set<String> registeredFunctionNames = new HashSet<>();
        String aggregationFunctionTypeName = aggregationFunctionType.getName();
        // 1. Register the aggregation function with Calcite
        registerAggregateFunction(aggregationFunctionTypeName, aggregationFunctionType);
        registeredFunctionNames.add(aggregationFunctionTypeName.toUpperCase());
        // 2. Register the aggregation function with Calcite on all alternative names
        List<String> alternativeFunctionNames = aggregationFunctionType.getAlternativeNames();
        for (String alternativeFunctionName : alternativeFunctionNames) {
          if (registeredFunctionNames.contains(alternativeFunctionName.toUpperCase())) {
            continue;
          }
          registerAggregateFunction(alternativeFunctionName, aggregationFunctionType);
          registeredFunctionNames.add(alternativeFunctionName.toUpperCase());
        }
      }
    }

    // Walk through all the Pinot transform types and
    //   1. register those that are supported in multistage in addition to calcite standard opt table.
    //   2. register special handling that differs from calcite standard.
    for (TransformFunctionType transformFunctionType : TransformFunctionType.values()) {
      if (transformFunctionType.getSqlKind() != null) {
        Set<String> registeredFunctionNames = new HashSet<>();
        String transformFunctionTypeName = transformFunctionType.getName();
        // 1. Register the aggregation function with Calcite
        registerTransformFunction(transformFunctionTypeName, transformFunctionType);
        registeredFunctionNames.add(transformFunctionTypeName.toUpperCase());

        // 2. Register the aggregation function with Calcite on all alternative names
        List<String> alternativeFunctionNames = transformFunctionType.getAlternativeNames();
        for (String alternativeFunctionName : alternativeFunctionNames) {
          if (registeredFunctionNames.contains(alternativeFunctionName.toUpperCase())) {
            continue;
          }
          registerTransformFunction(alternativeFunctionName, transformFunctionType);
          registeredFunctionNames.add(alternativeFunctionName.toUpperCase());
        }
      }
    }
  }

  private void initPinotOverrideSql() {
    register(new PinotSqlCoalesceFunction());
  }

  private void initCalciteStandardSql() {
    register(UNION);
    register(UNION_ALL);
    register(EXCEPT);
    register(EXCEPT_ALL);
    register(INTERSECT);
    register(INTERSECT_ALL);
    register(AND);
    register(AS);
    register(FILTER);
    register(CONCAT);
    register(DIVIDE);
    register(PERCENT_REMAINDER);
    register(DOT);
    register(EQUALS);
    register(GREATER_THAN);
    register(IS_DISTINCT_FROM);
    register(IS_NOT_DISTINCT_FROM);
    register(IS_DIFFERENT_FROM);
    register(GREATER_THAN_OR_EQUAL);
    register(IN);
    register(NOT_IN);
    register(LESS_THAN);
    register(LESS_THAN_OR_EQUAL);
    register(MINUS);
    register(MULTIPLY);
    register(NOT_EQUALS);
    register(OR);
    register(PLUS);
    register(DESC);
    register(NULLS_FIRST);
    register(NULLS_LAST);
    register(IS_NOT_NULL);
    register(IS_NULL);
    register(IS_NOT_TRUE);
    register(IS_TRUE);
    register(IS_NOT_FALSE);
    register(IS_FALSE);
    register(EXISTS);
    register(NOT);
    register(UNARY_MINUS);
    register(UNARY_PLUS);
    register(EXPLICIT_TABLE);
    register(SUM);
    register(COUNT);
    register(MODE);
    register(APPROX_COUNT_DISTINCT);
    register(MIN);
    register(MAX);
    register(LAST_VALUE);
    register(ANY_VALUE);
    register(FIRST_VALUE);
    register(NTH_VALUE);
    register(LEAD);
    register(SINGLE_VALUE);
    register(AVG);
    register(STDDEV_POP);
    register(COVAR_POP);
    register(COVAR_SAMP);
    register(STDDEV);
    register(VARIANCE);
    register(BIT_AND);
    register(BIT_OR);
    register(BIT_XOR);
    register(HISTOGRAM_AGG);
    register(HISTOGRAM_MIN);
    register(HISTOGRAM_MAX);
    register(HISTOGRAM_FIRST_VALUE);
    register(HISTOGRAM_LAST_VALUE);
    register(SUM0);
    register(DENSE_RANK);
    register(PERCENT_RANK);
    register(RANK);
    register(ROW_NUMBER);
    register(ROW);
    register(ARRAY_VALUE_CONSTRUCTOR);
    register(MAP_VALUE_CONSTRUCTOR);
    register(UNNEST);
    register(LATERAL);
    register(CONTAINS);
    register(EQUALS);
    register(VALUES);
    register(JSON_EXISTS);
    register(JSON_VALUE);
    register(JSON_QUERY);
    register(JSON_DEPTH);
    register(JSON_KEYS);
    register(JSON_LENGTH);
    register(JSON_PRETTY);
    register(JSON_STORAGE_SIZE);
    register(JSON_TYPE);
    register(BETWEEN);
    register(SYMMETRIC_BETWEEN);
    register(NOT_BETWEEN);
    register(SYMMETRIC_NOT_BETWEEN);
    register(NOT_LIKE);
    register(LIKE);
    register(ESCAPE);
    register(CASE);
    register(OVER);
    register(SUBSTRING);
    register(REPLACE);
    register(OVERLAY);
    register(TRIM);
    register(POSITION);
    register(CHAR_LENGTH);
    register(CHARACTER_LENGTH);
    register(OCTET_LENGTH);
    register(UPPER);
    register(LOWER);
    register(ASCII);
    register(POWER);
    register(SQRT);
    register(MOD);
    register(LN);
    register(LOG10);
    register(ABS);
    register(ACOS);
    register(ASIN);
    register(ATAN);
    register(ATAN2);
    register(CBRT);
    register(COS);
    register(COT);
    register(DEGREES);
    register(EXP);
    register(RADIANS);
    register(ROUND);
    register(SIGN);
    register(SIN);
    register(TAN);
    register(TRUNCATE);
    register(PI);
    register(FIRST);
    register(LAST);
    register(NULLIF);
    register(FLOOR);
    register(CEIL);
    register(LOCALTIME);
    register(LOCALTIMESTAMP);
    register(CURRENT_TIME);
    register(CURRENT_TIMESTAMP);
    register(CURRENT_DATE);
    register(TIMESTAMP_ADD);
    register(TIMESTAMP_DIFF);
    register(CAST);
    register(EXTRACT);
    register(YEAR);
    register(QUARTER);
    register(MONTH);
    register(WEEK);
    register(DAYOFWEEK);
    register(DAYOFMONTH);
    register(DAYOFYEAR);
    register(HOUR);
    register(MINUTE);
    register(SECOND);
    register(LAST_DAY);
    register(ELEMENT);
    register(ITEM);
    register(CARDINALITY);
    register(INTERSECTION);
    register(CURRENT_VALUE);
    register(TUMBLE);
    register(HOP);
  }

  private void registerAggregateFunction(String functionName, AggregationFunctionType functionType) {
    // register function behavior that's different from Calcite
    if (functionType.getOperandTypeChecker() != null && functionType.getReturnTypeInference() != null) {
      PinotSqlAggFunction sqlAggFunction = new PinotSqlAggFunction(functionName.toUpperCase(Locale.ROOT), null,
          functionType.getSqlKind(), functionType.getReturnTypeInference(), null,
          functionType.getOperandTypeChecker(), functionType.getSqlFunctionCategory());
      if (notRegistered(sqlAggFunction)) {
        LOGGER.info("Registering Pinot Aggregation Function {}", functionName);
        register(sqlAggFunction);
      } else {
        LOGGER.error("Pinot Aggregation Function {} is already registered", functionName);
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
        LOGGER.info("Registering Pinot Transform Function {}", functionName);
        register(sqlTransformFunction);
      } else {
        LOGGER.error("Pinot Transform Function {} is already registered", functionName);
      }
    }
  }

  private boolean notRegistered(SqlFunction op) {
    List<SqlOperator> operatorList = new ArrayList<>();
    lookupOperatorOverloads(op.getNameAsId(), op.getFunctionType(), op.getSyntax(), operatorList,
        SqlNameMatchers.withCaseSensitive(false));
    return operatorList.size() == 0;
  }
}
