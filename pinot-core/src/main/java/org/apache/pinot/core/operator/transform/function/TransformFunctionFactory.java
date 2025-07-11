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
package org.apache.pinot.core.operator.transform.function;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.LiteralContext;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Factory class for transformation functions.
 */
public class TransformFunctionFactory {
  private TransformFunctionFactory() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(TransformFunctionFactory.class);
  private static final Map<String, Class<? extends TransformFunction>> TRANSFORM_FUNCTION_MAP = createRegistry();

  private static Map<String, Class<? extends TransformFunction>> createRegistry() {
    Map<TransformFunctionType, Class<? extends TransformFunction>> typeToImplementation =
        ManualTransformFunctionMapProvider.manualMap();
    for (Map.Entry<TransformFunctionType, Class<? extends TransformFunction>> fromUdf : fromUdf().entrySet()) {
      TransformFunctionType type = fromUdf.getKey();
      Class<? extends TransformFunction> implementation = fromUdf.getValue();
      if (typeToImplementation.containsKey(type)) {
        LOGGER.warn("Manually registered implementation of transform function type {} using {} has been replaced "
                + "with {} from UDF",
            type, typeToImplementation.get(type).getCanonicalName(), implementation.getCanonicalName());
      }
      typeToImplementation.put(type, implementation);
    }

    Map<String, Class<? extends TransformFunction>> registry =
        new HashMap<>(HashUtil.getHashMapCapacity(typeToImplementation.size()));
    for (Map.Entry<TransformFunctionType, Class<? extends TransformFunction>> entry : typeToImplementation.entrySet()) {
      for (String name : entry.getKey().getNames()) {
        registry.put(canonicalize(name), entry.getValue());
      }
    }
    return registry;
  }

  /**
   * Initializes the factory with a set of transform function classes.
   * <p>Should be called only once before using the factory.
   *
   * @param transformFunctionClasses Set of transform function classes
   */
  public static void init(Set<Class<TransformFunction>> transformFunctionClasses) {
    for (Class<TransformFunction> transformFunctionClass : transformFunctionClasses) {
      TransformFunction transformFunction;
      try {
        transformFunction = transformFunctionClass.getDeclaredConstructor().newInstance();
      } catch (Exception e) {
        throw new RuntimeException(
            "Caught exception while instantiating transform function from class: " + transformFunctionClass, e);
      }
      String transformFunctionName = canonicalize(transformFunction.getName());
      if (TRANSFORM_FUNCTION_MAP.put(transformFunctionName, transformFunctionClass) == null) {
        LOGGER.info("Registering function: {} with class: {}", transformFunctionName, transformFunctionClass);
      } else {
        LOGGER.info("Replacing function: {} with class: {}", transformFunctionName, transformFunctionClass);
      }
    }
  }

  /**
   * Returns an instance of transform function for the given expression.
   *
   * @param expression       Transform expression
   * @param columnContextMap Map from column name to context
   * @param queryContext     Query context
   * @return Transform function
   */
  public static TransformFunction get(ExpressionContext expression, Map<String, ColumnContext> columnContextMap,
      QueryContext queryContext) {
    switch (expression.getType()) {
      case FUNCTION:
        FunctionContext function = expression.getFunction();
        String functionName = canonicalize(function.getFunctionName());
        List<ExpressionContext> arguments = function.getArguments();
        int numArguments = arguments.size();

        // Check if the function is ArrayValueConstructor transform function
        if (functionName.equalsIgnoreCase(ArrayLiteralTransformFunction.FUNCTION_NAME)) {
          return queryContext.getOrComputeSharedValue(ArrayLiteralTransformFunction.class,
              expression.getFunction().getArguments(), ArrayLiteralTransformFunction::new);
        }

        // Check if the function is GenerateArray transform function
        if (functionName.equalsIgnoreCase(GenerateArrayTransformFunction.FUNCTION_NAME)) {
          return queryContext.getOrComputeSharedValue(GenerateArrayTransformFunction.class,
              expression.getFunction().getArguments(),
              GenerateArrayTransformFunction::new);
        }

        TransformFunction transformFunction;
        Class<? extends TransformFunction> transformFunctionClass = TRANSFORM_FUNCTION_MAP.get(functionName);
        if (transformFunctionClass != null) {
          // Transform function
          try {
            transformFunction = transformFunctionClass.getDeclaredConstructor().newInstance();
          } catch (Exception e) {
            throw new RuntimeException("Caught exception while constructing transform function: " + functionName, e);
          }
        } else {
          // Scalar function
          String canonicalName = FunctionRegistry.canonicalize(functionName);
          FunctionInfo functionInfo = FunctionRegistry.lookupFunctionInfo(canonicalName, numArguments);
          if (functionInfo == null) {
            if (FunctionRegistry.contains(canonicalName)) {
              throw new BadQueryRequestException(
                  String.format("Unsupported function: %s with %d arguments", functionName, numArguments));
            } else {
              throw new BadQueryRequestException(String.format("Unsupported function: %s", functionName));
            }
          }
          transformFunction = new ScalarTransformFunctionWrapper(functionInfo);
        }

        List<TransformFunction> transformFunctionArguments = new ArrayList<>(numArguments);
        for (ExpressionContext argument : arguments) {
          transformFunctionArguments.add(TransformFunctionFactory.get(argument, columnContextMap, queryContext));
        }
        try {
          transformFunction.init(transformFunctionArguments, columnContextMap, queryContext.isNullHandlingEnabled());
        } catch (Exception e) {
          throw new BadQueryRequestException("Caught exception while initializing transform function: "
              + functionName + ": " + e.getMessage(), e);
        }
        return transformFunction;
      case IDENTIFIER:
        String columnName = expression.getIdentifier();
        return new IdentifierTransformFunction(columnName, columnContextMap.get(columnName));
      case LITERAL:
        LiteralContext literal = expression.getLiteral();
        if (literal.isSingleValue()) {
          return queryContext.getOrComputeSharedValue(LiteralTransformFunction.class, literal,
              LiteralTransformFunction::new);
        } else {
          return queryContext.getOrComputeSharedValue(ArrayLiteralTransformFunction.class, literal,
              ArrayLiteralTransformFunction::new);
        }
      default:
        throw new IllegalStateException();
    }
  }

  // TODO: Move to a test util class
  @VisibleForTesting
  public static TransformFunction get(ExpressionContext expression, Map<String, DataSource> dataSourceMap) {
    Map<String, ColumnContext> columnContextMap = new HashMap<>(HashUtil.getHashMapCapacity(dataSourceMap.size()));
    dataSourceMap.forEach((k, v) -> columnContextMap.put(k, ColumnContext.fromDataSource(v)));
    QueryContext dummy =
        QueryContextConverterUtils.getQueryContext(CalciteSqlParser.compileToPinotQuery("SELECT * from testTable;"));
    return get(expression, columnContextMap, dummy);
  }

  // TODO: Move to a test util class
  @VisibleForTesting
  public static TransformFunction getNullHandlingEnabled(ExpressionContext expression,
      Map<String, DataSource> dataSourceMap) {
    Map<String, ColumnContext> columnContextMap = new HashMap<>(HashUtil.getHashMapCapacity(dataSourceMap.size()));
    dataSourceMap.forEach((k, v) -> columnContextMap.put(k, ColumnContext.fromDataSource(v)));
    QueryContext dummy = QueryContextConverterUtils.getQueryContext(
        CalciteSqlParser.compileToPinotQuery("SET enableNullHandling = true; SELECT * from testTable;"));
    return get(expression, columnContextMap, dummy);
  }

  /**
   * Converts the transform function name into its canonical form
   *
   * @param functionName Name of the transform function
   * @return canonicalized transform function name
   */
  public static String canonicalize(String functionName) {
    return StringUtils.remove(functionName, '_').toLowerCase();
  }

  @VisibleForTesting
  public static Map<String, Class<? extends TransformFunction>> getAllFunctions() {
    return Collections.unmodifiableMap(TRANSFORM_FUNCTION_MAP);
  }

  private static Map<TransformFunctionType, Class<? extends TransformFunction>> fromUdf() {
    Map<TransformFunctionType, Udf> typeToUdf = new EnumMap<>(TransformFunctionType.class);
    Map<TransformFunctionType, Class<? extends TransformFunction>> typeToImplementation =
        new EnumMap<>(TransformFunctionType.class);
    // Register UDFs as transform functions
    ServiceLoader.load(Udf.class).stream()
        .map(ServiceLoader.Provider::get)
        .forEach(udf -> {
          for (Map.Entry<TransformFunctionType, Class<? extends TransformFunction>> entry :
              udf.getTransformFunctions().entrySet()) {
            Udf oldUdf = typeToUdf.get(entry.getKey());
            if (oldUdf == null) {
              typeToUdf.put(entry.getKey(), udf);
              typeToImplementation.put(entry.getKey(), entry.getValue());
              LOGGER.debug("Adding transform function {} with UDF {}", entry.getKey(),
                  udf.getClass().getCanonicalName());
            } else if (oldUdf.priority() < udf.priority()) {
              typeToUdf.put(entry.getKey(), udf);
              typeToImplementation.put(entry.getKey(), entry.getValue());
              LOGGER.info("Changing transform function {} from lower priority UDF {} with {}",
                  entry.getKey(), oldUdf.getClass().getCanonicalName(), udf.getClass().getCanonicalName());
            } else {
              LOGGER.debug("Keeping transform function {} with UDF {} as it has higher priority than {}",
                  entry.getKey(), oldUdf.getClass().getCanonicalName(), udf.getClass().getCanonicalName());
            }
          }
        });

    return typeToImplementation;
  }
}
