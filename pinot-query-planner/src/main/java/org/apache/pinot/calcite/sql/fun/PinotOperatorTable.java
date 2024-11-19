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
package org.apache.pinot.calcite.sql.fun;

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlLeadLagAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.function.PinotScalarFunction;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.function.sql.PinotSqlAggFunction;
import org.apache.pinot.common.function.sql.PinotSqlFunction;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/**
 * This class defines all the {@link SqlOperator}s allowed by Pinot.
 * <p>It contains the following types of operators:
 * <ul>
 *   <li>Standard operators from {@link SqlStdOperatorTable}</li>
 *   <li>Custom aggregation functions from {@link AggregationFunctionType}</li>
 *   <li>Custom transform functions from {@link TransformFunctionType}</li>
 *   <li>Custom scalar functions from {@link FunctionRegistry}</li>
 *   <li>Other custom operators directly registered</li>
 * </ul>
 * <p>The core method is {@link #lookupOperatorOverloads} which is used to look up the {@link SqlOperator} with the
 * {@link SqlIdentifier} during query parsing.
 */
@SuppressWarnings("unused") // unused fields are accessed by reflection
public class PinotOperatorTable implements SqlOperatorTable {
  private static final Supplier<PinotOperatorTable> INSTANCE = Suppliers.memoize(PinotOperatorTable::new);

  public static PinotOperatorTable instance() {
    return INSTANCE.get();
  }

  /**
   * This list includes the supported standard {@link SqlOperator}s defined in {@link SqlStdOperatorTable}.
   * NOTE: The operator order follows the same order as defined in {@link SqlStdOperatorTable} for easier search.
   *       Some operators are commented out and re-declared in {@link #STANDARD_OPERATORS_WITH_ALIASES}.
   * TODO: Add more operators as needed.
   */
  //@formatter:off
  private static final List<SqlOperator> STANDARD_OPERATORS = List.of(
      // SET OPERATORS
      SqlStdOperatorTable.UNION,
      SqlStdOperatorTable.UNION_ALL,
      SqlStdOperatorTable.EXCEPT,
      SqlStdOperatorTable.EXCEPT_ALL,
      SqlStdOperatorTable.INTERSECT,
      SqlStdOperatorTable.INTERSECT_ALL,

      // BINARY OPERATORS
      SqlStdOperatorTable.AND,
      SqlStdOperatorTable.AS,
      SqlStdOperatorTable.FILTER,
      SqlStdOperatorTable.WITHIN_GROUP,
      SqlStdOperatorTable.WITHIN_DISTINCT,
      SqlStdOperatorTable.CONCAT,
      SqlStdOperatorTable.DIVIDE,
      SqlStdOperatorTable.PERCENT_REMAINDER,
      SqlStdOperatorTable.DOT,
      SqlStdOperatorTable.EQUALS,
      SqlStdOperatorTable.GREATER_THAN,
      SqlStdOperatorTable.IS_DISTINCT_FROM,
      SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
      SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
      SqlStdOperatorTable.IN,
      SqlStdOperatorTable.NOT_IN,
      SqlStdOperatorTable.SEARCH,
      SqlStdOperatorTable.LESS_THAN,
      SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
      SqlStdOperatorTable.MINUS,
      SqlStdOperatorTable.MULTIPLY,
      SqlStdOperatorTable.NOT_EQUALS,
      SqlStdOperatorTable.OR,
      SqlStdOperatorTable.PLUS,
      SqlStdOperatorTable.INTERVAL,

      // POSTFIX OPERATORS
      SqlStdOperatorTable.DESC,
      SqlStdOperatorTable.NULLS_FIRST,
      SqlStdOperatorTable.NULLS_LAST,
      SqlStdOperatorTable.IS_NOT_NULL,
      SqlStdOperatorTable.IS_NULL,
      SqlStdOperatorTable.IS_NOT_TRUE,
      SqlStdOperatorTable.IS_TRUE,
      SqlStdOperatorTable.IS_NOT_FALSE,
      SqlStdOperatorTable.IS_FALSE,
      SqlStdOperatorTable.IS_NOT_UNKNOWN,
      SqlStdOperatorTable.IS_UNKNOWN,

      // PREFIX OPERATORS
      SqlStdOperatorTable.EXISTS,
      SqlStdOperatorTable.NOT,

      // AGGREGATE OPERATORS
      SqlStdOperatorTable.SUM,
      SqlStdOperatorTable.COUNT,
      SqlStdOperatorTable.MODE,
      SqlStdOperatorTable.MIN,
      SqlStdOperatorTable.MAX,
      SqlStdOperatorTable.AVG,
      SqlStdOperatorTable.STDDEV_POP,
      SqlStdOperatorTable.COVAR_POP,
      SqlStdOperatorTable.COVAR_SAMP,
      SqlStdOperatorTable.STDDEV_SAMP,
      SqlStdOperatorTable.VAR_POP,
      SqlStdOperatorTable.VAR_SAMP,
      SqlStdOperatorTable.SUM0,

      // WINDOW Rank Functions
      SqlStdOperatorTable.DENSE_RANK,
      SqlStdOperatorTable.RANK,
      SqlStdOperatorTable.ROW_NUMBER,

      // WINDOW Functions (non-aggregate)
      SqlStdOperatorTable.LAST_VALUE,
      SqlStdOperatorTable.FIRST_VALUE,
      // TODO: Replace these with SqlStdOperatorTable.LEAD and SqlStdOperatorTable.LAG when the function implementations
      // are updated to support the IGNORE NULLS option.
      PinotLeadWindowFunction.INSTANCE,
      PinotLagWindowFunction.INSTANCE,

      // SPECIAL OPERATORS
      SqlStdOperatorTable.IGNORE_NULLS,
      SqlStdOperatorTable.RESPECT_NULLS,
      SqlStdOperatorTable.BETWEEN,
      SqlStdOperatorTable.SYMMETRIC_BETWEEN,
      SqlStdOperatorTable.NOT_BETWEEN,
      SqlStdOperatorTable.SYMMETRIC_NOT_BETWEEN,
      SqlStdOperatorTable.NOT_LIKE,
      SqlStdOperatorTable.LIKE,
//      SqlStdOperatorTable.CASE,
      SqlStdOperatorTable.OVER,
      SqlStdOperatorTable.NULLIF,

      // FUNCTIONS
      // String functions
      SqlStdOperatorTable.SUBSTRING,
      SqlStdOperatorTable.REPLACE,
      SqlStdOperatorTable.TRIM,
      SqlStdOperatorTable.UPPER,
      SqlStdOperatorTable.LOWER,
      // Arithmetic functions
      SqlStdOperatorTable.POWER,
      SqlStdOperatorTable.SQRT,
      SqlStdOperatorTable.MOD,
//      SqlStdOperatorTable.LN,
      SqlStdOperatorTable.LOG10,
      SqlStdOperatorTable.ABS,
      SqlStdOperatorTable.ACOS,
      SqlStdOperatorTable.ASIN,
      SqlStdOperatorTable.ATAN,
      SqlStdOperatorTable.ATAN2,
      SqlStdOperatorTable.COS,
      SqlStdOperatorTable.COT,
      SqlStdOperatorTable.DEGREES,
      SqlStdOperatorTable.EXP,
      SqlStdOperatorTable.RADIANS,
      SqlStdOperatorTable.SIGN,
      SqlStdOperatorTable.SIN,
      SqlStdOperatorTable.TAN,
      SqlStdOperatorTable.TRUNCATE,
      SqlStdOperatorTable.FLOOR,
      SqlStdOperatorTable.CEIL,
      SqlStdOperatorTable.TIMESTAMP_ADD,
      SqlStdOperatorTable.TIMESTAMP_DIFF,
      SqlStdOperatorTable.CAST,

      SqlStdOperatorTable.EXTRACT,
      // TODO: The following operators are all rewritten to EXTRACT. Consider removing them because they are all
      //       supported without rewrite.
      SqlStdOperatorTable.YEAR,
      SqlStdOperatorTable.QUARTER,
      SqlStdOperatorTable.MONTH,
      SqlStdOperatorTable.WEEK,
      SqlStdOperatorTable.DAYOFYEAR,
      SqlStdOperatorTable.DAYOFMONTH,
      SqlStdOperatorTable.DAYOFWEEK,
      SqlStdOperatorTable.HOUR,
      SqlStdOperatorTable.MINUTE,
      SqlStdOperatorTable.SECOND,

      SqlStdOperatorTable.ITEM,
      SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
      SqlStdOperatorTable.LISTAGG
  );

  private static final List<Pair<SqlOperator, List<String>>> STANDARD_OPERATORS_WITH_ALIASES = List.of(
      Pair.of(SqlStdOperatorTable.CASE, List.of("CASE", "CASE_WHEN")),
      Pair.of(SqlStdOperatorTable.LN, List.of("LN", "LOG")),
      Pair.of(SqlStdOperatorTable.EQUALS, List.of("EQUALS")),
      Pair.of(SqlStdOperatorTable.NOT_EQUALS, List.of("NOT_EQUALS")),
      Pair.of(SqlStdOperatorTable.GREATER_THAN, List.of("GREATER_THAN")),
      Pair.of(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, List.of("GREATER_THAN_OR_EQUAL")),
      Pair.of(SqlStdOperatorTable.LESS_THAN, List.of("LESS_THAN")),
      Pair.of(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, List.of("LESS_THAN_OR_EQUAL")),
      Pair.of(SqlStdOperatorTable.MINUS, List.of("SUB", "MINUS")),
      Pair.of(SqlStdOperatorTable.PLUS, List.of("ADD", "PLUS")),
      Pair.of(SqlStdOperatorTable.MULTIPLY, List.of("MULT", "TIMES"))
  );

  /**
   * This list includes the customized {@link SqlOperator}s.
   */
  private static final List<SqlOperator> PINOT_OPERATORS = List.of(
      // Placeholder for special predicates
      new PinotSqlFunction("TEXT_MATCH", ReturnTypes.BOOLEAN, OperandTypes.CHARACTER_CHARACTER),
      new PinotSqlFunction("TEXT_CONTAINS", ReturnTypes.BOOLEAN, OperandTypes.CHARACTER_CHARACTER),
      new PinotSqlFunction("JSON_MATCH", ReturnTypes.BOOLEAN, OperandTypes.CHARACTER_CHARACTER),
      new PinotSqlFunction("VECTOR_SIMILARITY", ReturnTypes.BOOLEAN,
          OperandTypes.family(List.of(SqlTypeFamily.ARRAY, SqlTypeFamily.ARRAY, SqlTypeFamily.INTEGER), i -> i == 2)),

      // Placeholder for special functions to handle MV
      // NOTE:
      // ARRAY_TO_MV is not deterministic.
      // We need to explicitly set it as not deterministic in order to do not let Calcite optimize expressions like
      // `ARRAY_TO_MV(RandomAirports) = 'MFR' and ARRAY_TO_MV(RandomAirports) = 'GTR'` as `false`.
      // If the function were deterministic, its value would never be MFR and GTR at the same time, so Calcite is
      // smart enough to know there is no value that satisfies the condition.
      // In fact what ARRAY_TO_MV does is just to trick Calcite typesystem, but then what the leaf stage executor
      // receives is `RandomAirports = 'MFR' and RandomAirports = 'GTR'`, which in the V1 semantics means:
      // true if and only if RandomAirports contains a value equal to 'MFR' and RandomAirports contains a value equal
      // to 'GTR'
      new PinotSqlFunction("ARRAY_TO_MV", opBinding -> opBinding.getOperandType(0).getComponentType(),
          OperandTypes.ARRAY, false),

      // SqlStdOperatorTable.COALESCE without rewrite
      new SqlFunction("COALESCE", SqlKind.COALESCE,
          ReturnTypes.LEAST_RESTRICTIVE.andThen(SqlTypeTransforms.LEAST_NULLABLE), null, OperandTypes.SAME_VARIADIC,
          SqlFunctionCategory.SYSTEM),

      // The scalar function version returns long instead of Timestamp
      // TODO: Consider unifying the return type to Timestamp
      new PinotSqlFunction("FROM_DATE_TIME", ReturnTypes.TIMESTAMP_LTZ_NULLABLE, OperandTypes.family(
          List.of(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.ANY),
          i -> i > 1))
  );

  private static final List<Pair<SqlOperator, List<String>>> PINOT_OPERATORS_WITH_ALIASES = List.of(
  );
  //@formatter:on

  // Key is canonical name
  private final Map<String, SqlOperator> _operatorMap;
  private final List<SqlOperator> _operatorList;

  private PinotOperatorTable() {
    Map<String, SqlOperator> operatorMap = new HashMap<>();

    // Register standard operators
    for (SqlOperator operator : STANDARD_OPERATORS) {
      register(operator.getName(), operator, operatorMap);
    }
    for (Pair<SqlOperator, List<String>> pair : STANDARD_OPERATORS_WITH_ALIASES) {
      SqlOperator operator = pair.getLeft();
      for (String name : pair.getRight()) {
        register(name, operator, operatorMap);
      }
    }

    // Register Pinot operators
    for (SqlOperator operator : PINOT_OPERATORS) {
      register(operator.getName(), operator, operatorMap);
    }
    for (Pair<SqlOperator, List<String>> pair : PINOT_OPERATORS_WITH_ALIASES) {
      SqlOperator operator = pair.getLeft();
      for (String name : pair.getRight()) {
        register(name, operator, operatorMap);
      }
    }

    registerAggregateFunctions(operatorMap);
    registerTransformFunctions(operatorMap);
    registerScalarFunctions(operatorMap);

    _operatorMap = Map.copyOf(operatorMap);
    _operatorList = List.copyOf(operatorMap.values());
  }

  private void register(String name, SqlOperator sqlOperator, Map<String, SqlOperator> operatorMap) {
    Preconditions.checkState(operatorMap.put(FunctionRegistry.canonicalize(name), sqlOperator) == null,
        "SqlOperator: %s is already registered", name);
  }

  private void registerAggregateFunctions(Map<String, SqlOperator> operatorMap) {
    for (AggregationFunctionType functionType : AggregationFunctionType.values()) {
      if (functionType.getReturnTypeInference() != null) {
        String functionName = functionType.getName();
        PinotSqlAggFunction function = new PinotSqlAggFunction(functionName, functionType.getReturnTypeInference(),
            functionType.getOperandTypeChecker());
        Preconditions.checkState(operatorMap.put(FunctionRegistry.canonicalize(functionName), function) == null,
            "Aggregate function: %s is already registered", functionName);
      }
    }
  }

  private void registerTransformFunctions(Map<String, SqlOperator> operatorMap) {
    for (TransformFunctionType functionType : TransformFunctionType.values()) {
      if (functionType.getReturnTypeInference() != null) {
        PinotSqlFunction function = new PinotSqlFunction(functionType.getName(), functionType.getReturnTypeInference(),
            functionType.getOperandTypeChecker());
        for (String name : functionType.getNames()) {
          Preconditions.checkState(operatorMap.put(FunctionRegistry.canonicalize(name), function) == null,
              "Transform function: %s is already registered", name);
        }
      }
    }
  }

  private void registerScalarFunctions(Map<String, SqlOperator> operatorMap) {
    for (Map.Entry<String, PinotScalarFunction> entry : FunctionRegistry.FUNCTION_MAP.entrySet()) {
      String canonicalName = entry.getKey();
      PinotScalarFunction scalarFunction = entry.getValue();
      PinotSqlFunction sqlFunction = scalarFunction.toPinotSqlFunction();
      if (sqlFunction == null) {
        continue;
      }
      if (operatorMap.containsKey(canonicalName)) {
        // Skip registering ArgumentCountBasedScalarFunction if it is already registered
        Preconditions.checkState(scalarFunction instanceof FunctionRegistry.ArgumentCountBasedScalarFunction,
            "Scalar function: %s is already registered", canonicalName);
        continue;
      }
      operatorMap.put(canonicalName, sqlFunction);
    }
  }

  @Override
  public void lookupOperatorOverloads(SqlIdentifier opName, @Nullable SqlFunctionCategory category, SqlSyntax syntax,
      List<SqlOperator> operatorList, SqlNameMatcher nameMatcher) {
    if (!opName.isSimple()) {
      return;
    }
    String canonicalName = FunctionRegistry.canonicalize(opName.getSimple());
    SqlOperator operator = _operatorMap.get(canonicalName);
    if (operator != null) {
      operatorList.add(operator);
    }
  }

  @Override
  public List<SqlOperator> getOperatorList() {
    return _operatorList;
  }

  private static class PinotLeadWindowFunction extends SqlLeadLagAggFunction {
    static final SqlOperator INSTANCE = new PinotLeadWindowFunction();

    public PinotLeadWindowFunction() {
      super(SqlKind.LEAD);
    }

    @Override
    public boolean allowsNullTreatment() {
      return false;
    }
  }

  private static class PinotLagWindowFunction extends SqlLeadLagAggFunction {
    static final SqlOperator INSTANCE = new PinotLagWindowFunction();

    public PinotLagWindowFunction() {
      super(SqlKind.LAG);
    }

    @Override
    public boolean allowsNullTreatment() {
      return false;
    }
  }
}
