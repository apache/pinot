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

import com.google.common.annotations.VisibleForTesting;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Function;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.util.NameMultimap;
import org.apache.pinot.common.function.schema.PinotFunction;
import org.apache.pinot.common.function.schema.PinotScalarFunction;
import org.apache.pinot.common.function.sql.PinotSqlAggFunction;
import org.apache.pinot.common.function.sql.PinotSqlTransformFunction;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.utils.PinotReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Registry for functions.
 */
public class FunctionRegistry {
  public static final boolean CASE_SENSITIVITY = false;
  private static final Logger LOGGER = LoggerFactory.getLogger(FunctionRegistry.class);
  private static final NameMultimap<SqlOperator> OPERATOR_MAP = new NameMultimap<>();
  private static final NameMultimap<PinotFunction> FUNCTION_MAP = new NameMultimap<>();

  private FunctionRegistry() {
  }

  /**
   * Registers the scalar functions via reflection.
   * NOTE: In order to plugin methods using reflection, the methods should be inside a class that includes ".function."
   *       in its class path. This convention can significantly reduce the time of class scanning.
   */
  static {
    // REGISTER FUNCTIONS
    long startTimeMs = System.currentTimeMillis();
    Set<Method> methods = PinotReflectionUtils.getMethodsThroughReflection(".*\\.function\\..*", ScalarFunction.class);
    for (Method method : methods) {
      if (!Modifier.isPublic(method.getModifiers())) {
        continue;
      }
      ScalarFunction scalarFunction = method.getAnnotation(ScalarFunction.class);
      if (scalarFunction.enabled()) {
        // Parse annotated function names and alias
        Set<String> scalarFunctionNames = Arrays.stream(scalarFunction.names()).collect(Collectors.toSet());
        if (scalarFunctionNames.size() == 0) {
          scalarFunctionNames.add(method.getName());
        }
        boolean nullableParameters = scalarFunction.nullableParameters();
        registerFunction(method, scalarFunctionNames, nullableParameters);
      }
    }
    LOGGER.info("Initialized FunctionRegistry with {} functions: {} in {}ms", FUNCTION_MAP.map().size(),
        FUNCTION_MAP.map().keySet(), System.currentTimeMillis() - startTimeMs);

    // REGISTER OPERATORS
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
        // 1. Register the transform function with Calcite
        registerTransformFunction(transformFunctionType.getName(), transformFunctionType);
        // 2. Register the transform function with Calcite on all alternative names
        List<String> alternativeFunctionNames = transformFunctionType.getAlternativeNames();
        for (String alternativeFunctionName : alternativeFunctionNames) {
          registerTransformFunction(alternativeFunctionName, transformFunctionType);
        }
      }
    }
  }

  /**
   * Initializes the FunctionRegistry.
   * NOTE: This method itself is a NO-OP, but can be used to explicitly trigger the static block of registering the
   *       scalar functions via reflection.
   */
  public static void init() {
  }

  @VisibleForTesting
  public static void registerFunction(Method method, boolean nullableParameters) {
    registerFunction(method, Collections.singleton(method.getName()), nullableParameters);
  }

  @VisibleForTesting
  public static Set<String> getRegisteredCalciteFunctionNames() {
    return getFunctionMap().map().keySet();
  }

  /**
   * Returns {@code true} if the given function name is registered, {@code false} otherwise.
   */
  public static boolean containsFunction(String functionName) {
    return getFunctionMap().containsKey(functionName, CASE_SENSITIVITY);
  }

  /**
   * Returns the {@link FunctionInfo} associated with the given function name and number of parameters, or {@code null}
   * if there is no matching method. This method should be called after the FunctionRegistry is initialized and all
   * methods are already registered.
   */
  @Nullable
  public static FunctionInfo getFunctionInfo(String functionName, int numParams) {
    return getFunctionInfoFromCalciteNamedMap(functionName, numParams);
  }

  /**
   * Returns the {@link FunctionInfo} associated with the given function name and number of parameters, or {@code null}
   * if there is no matching method. This method should be called after the FunctionRegistry is initialized and all
   * methods are already registered.
   */
  @Nullable
  public static FunctionInfo getFunctionInfo(SqlOperatorTable operatorTable, RelDataTypeFactory typeFactory,
      String functionName, List<DataSchema.ColumnDataType> argTypes) {
    PinotScalarFunction scalarFunction = getScalarFunction(operatorTable, typeFactory, functionName, argTypes);
    if (scalarFunction != null) {
      return scalarFunction.getFunctionInfo();
    } else {
      // TODO: convert this to throw IAE when all operator has scalar equivalent.
      return null;
    }
  }

  @Nullable
  private static FunctionInfo getFunctionInfoFromCalciteNamedMap(String functionName, int numParams) {
    List<PinotScalarFunction> candidates = getFunctionMap()
        .range(functionName, CASE_SENSITIVITY).stream()
        .filter(e -> e.getValue() instanceof PinotScalarFunction && e.getValue().getParameters().size() == numParams)
        .map(e -> (PinotScalarFunction) e.getValue()).collect(Collectors.toList());
    if (candidates.size() == 1) {
      return candidates.get(0).getFunctionInfo();
    } else {
      // TODO: convert this to throw IAE when all operator has scalar equivalent.
      return null;
    }
  }

  @Nullable
  private static PinotScalarFunction getScalarFunction(SqlOperatorTable operatorTable, RelDataTypeFactory typeFactory,
      String functionName, List<DataSchema.ColumnDataType> argTypes) {
    List<RelDataType> relArgTypes = convertArgumentTypes(typeFactory, argTypes);
    SqlOperator sqlOperator = SqlUtil.lookupRoutine(operatorTable, typeFactory,
        new SqlIdentifier(functionName, SqlParserPos.QUOTED_ZERO), relArgTypes, null, null, SqlSyntax.FUNCTION,
        SqlKind.OTHER_FUNCTION, SqlNameMatchers.withCaseSensitive(false), true);
    if (sqlOperator instanceof SqlUserDefinedFunction) {
      Function function = ((SqlUserDefinedFunction) sqlOperator).getFunction();
      if (function instanceof PinotScalarFunction) {
        return (PinotScalarFunction) function;
      }
    }
    return null;
  }

  public static NameMultimap<PinotFunction> getFunctionMap() {
    return FUNCTION_MAP;
  }

  public static NameMultimap<SqlOperator> getOperatorMap() {
    return OPERATOR_MAP;
  }

  private static void registerFunction(Method method, Set<String> alias, boolean nullableParameters) {
    if (method.getAnnotation(Deprecated.class) == null) {
      for (String name : alias) {
        registerCalciteNamedFunctionMap(name, method, nullableParameters);
      }
    }
  }

  private static void registerCalciteNamedFunctionMap(String name, Method method, boolean nullableParameters) {
    FUNCTION_MAP.put(name, new PinotScalarFunction(name, method, nullableParameters));
  }

  private static void registerAggregateFunction(String functionName, AggregationFunctionType functionType) {
    if (functionType.getOperandTypeChecker() != null && functionType.getReturnTypeInference() != null) {
      PinotSqlAggFunction sqlAggFunction = new PinotSqlAggFunction(functionName.toUpperCase(Locale.ROOT), null,
          functionType.getSqlKind(), functionType.getReturnTypeInference(), null,
          functionType.getOperandTypeChecker(), functionType.getSqlFunctionCategory());
      OPERATOR_MAP.put(functionName.toUpperCase(Locale.ROOT), sqlAggFunction);
    }
  }

  private static void registerTransformFunction(String functionName, TransformFunctionType functionType) {
    if (functionType.getOperandTypeChecker() != null && functionType.getReturnTypeInference() != null) {
      PinotSqlTransformFunction sqlTransformFunction =
          new PinotSqlTransformFunction(functionName.toUpperCase(Locale.ROOT),
              functionType.getSqlKind(), functionType.getReturnTypeInference(), null,
              functionType.getOperandTypeChecker(), functionType.getSqlFunctionCategory());
      OPERATOR_MAP.put(functionName.toUpperCase(Locale.ROOT), sqlTransformFunction);
    }
  }

  private static List<RelDataType> convertArgumentTypes(RelDataTypeFactory typeFactory,
      List<DataSchema.ColumnDataType> argTypes) {
    return argTypes.stream().map(type -> toRelType(typeFactory, type)).collect(Collectors.toList());
  }

  private static RelDataType toRelType(RelDataTypeFactory typeFactory, DataSchema.ColumnDataType dataType) {
    switch (dataType) {
      case INT:
        return typeFactory.createSqlType(SqlTypeName.INTEGER);
      case LONG:
        return typeFactory.createSqlType(SqlTypeName.BIGINT);
      case FLOAT:
        return typeFactory.createSqlType(SqlTypeName.REAL);
      case DOUBLE:
        return typeFactory.createSqlType(SqlTypeName.DOUBLE);
      case BIG_DECIMAL:
        return typeFactory.createSqlType(SqlTypeName.DECIMAL);
      case BOOLEAN:
        return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
      case TIMESTAMP:
        return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
      case JSON:
      case STRING:
        return typeFactory.createSqlType(SqlTypeName.VARCHAR);
      case BYTES:
        return typeFactory.createSqlType(SqlTypeName.VARBINARY);
      case INT_ARRAY:
        return typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.INTEGER), -1);
      case LONG_ARRAY:
        return typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.BIGINT), -1);
      case FLOAT_ARRAY:
        return typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.REAL), -1);
      case DOUBLE_ARRAY:
        return typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.DOUBLE), -1);
      case BOOLEAN_ARRAY:
        return typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.BOOLEAN), -1);
      case TIMESTAMP_ARRAY:
        return typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), -1);
      case STRING_ARRAY:
        return typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.VARCHAR), -1);
      case BYTES_ARRAY:
        return typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.VARBINARY), -1);
      case UNKNOWN:
      case OBJECT:
      default:
        return typeFactory.createSqlType(SqlTypeName.ANY);
    }
  }

  /**
   * Placeholders for scalar function, they register and represents the signature for transform and filter predicate
   * so that v2 engine can understand and plan them correctly.
   */
  private static class PlaceholderScalarFunctions {

    @ScalarFunction(names = {"textContains", "text_contains"}, isPlaceholder = true)
    public static boolean textContains(String text, String pattern) {
      throw new UnsupportedOperationException("Placeholder scalar function, should not reach here");
    }

    @ScalarFunction(names = {"textMatch", "text_match"}, isPlaceholder = true)
    public static boolean textMatch(String text, String pattern) {
      throw new UnsupportedOperationException("Placeholder scalar function, should not reach here");
    }

    @ScalarFunction(names = {"jsonMatch", "json_match"}, isPlaceholder = true)
    public static boolean jsonMatch(String text, String pattern) {
      throw new UnsupportedOperationException("Placeholder scalar function, should not reach here");
    }

    @ScalarFunction(names = {"vectorSimilarity", "vector_similarity"}, isPlaceholder = true)
    public static double vectorSimilarity(float[] vector1, float[] vector2) {
      throw new UnsupportedOperationException("Placeholder scalar function, should not reach here");
    }
  }
}
