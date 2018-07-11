/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.transform.function;

import com.linkedin.pinot.common.request.transform.TransformExpressionTree;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.query.exception.BadQueryRequestException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;


/**
 * Factory class for transformation functions.
 */
public class TransformFunctionFactory {
  private TransformFunctionFactory() {
  }

  private static final Map<String, Class<? extends TransformFunction>> TRANSFORM_FUNCTION_MAP =
      new HashMap<String, Class<? extends TransformFunction>>() {
        {
          // NOTE: add all built-in transform functions here
          put(AdditionTransformFunction.FUNCTION_NAME.toLowerCase(), AdditionTransformFunction.class);
          put(SubtractionTransformFunction.FUNCTION_NAME.toLowerCase(), SubtractionTransformFunction.class);
          put(MultiplicationTransformFunction.FUNCTION_NAME.toLowerCase(), MultiplicationTransformFunction.class);
          put(DivisionTransformFunction.FUNCTION_NAME.toLowerCase(), DivisionTransformFunction.class);
          put(TimeConversionTransformFunction.FUNCTION_NAME.toLowerCase(), TimeConversionTransformFunction.class);
          put(DateTimeConversionTransformFunction.FUNCTION_NAME.toLowerCase(),
              DateTimeConversionTransformFunction.class);
          put(ValueInTransformFunction.FUNCTION_NAME.toLowerCase(), ValueInTransformFunction.class);
        }
      };

  /**
   * Initializes the factory with a set of transform function classes.
   * <p>Should be called only once before calling {@link #get(TransformExpressionTree, Map)}.
   *
   * @param transformFunctionClasses Set of transform function classes
   */
  public static void init(@Nonnull Set<Class<TransformFunction>> transformFunctionClasses) {
    for (Class<TransformFunction> transformFunctionClass : transformFunctionClasses) {
      TransformFunction transformFunction;
      try {
        transformFunction = transformFunctionClass.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new RuntimeException(
            "Caught exception while instantiating transform function from class: " + transformFunctionClass.toString(),
            e);
      }
      String transformFunctionName = transformFunction.getName().toLowerCase();
      if (TRANSFORM_FUNCTION_MAP.containsKey(transformFunctionName)) {
        throw new IllegalArgumentException("Transform function: " + transformFunctionName + " already exists");
      }
      TRANSFORM_FUNCTION_MAP.put(transformFunctionName, transformFunctionClass);
    }
  }

  /**
   * Returns an instance of transform function for the given expression.
   *
   * @param expression Transform expression
   * @param dataSourceMap Map from column name to column data source
   * @return Transform function
   */
  public static TransformFunction get(@Nonnull TransformExpressionTree expression,
      @Nonnull Map<String, DataSource> dataSourceMap) {
    TransformFunction transformFunction;
    switch (expression.getExpressionType()) {
      case FUNCTION:
        String functionName = expression.getValue();
        Class<? extends TransformFunction> transformFunctionClass = TRANSFORM_FUNCTION_MAP.get(functionName);
        if (transformFunctionClass == null) {
          throw new BadQueryRequestException("Unsupported transform function: " + functionName);
        }
        try {
          transformFunction = transformFunctionClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
          throw new RuntimeException("Caught exception while instantiating transform function: " + functionName, e);
        }
        List<TransformExpressionTree> children = expression.getChildren();
        List<TransformFunction> arguments = new ArrayList<>(children.size());
        for (TransformExpressionTree child : children) {
          arguments.add(TransformFunctionFactory.get(child, dataSourceMap));
        }
        try {
          transformFunction.init(arguments, dataSourceMap);
        } catch (Exception e) {
          throw new BadQueryRequestException(
              "Caught exception while initializing transform function: " + transformFunction.getName(), e);
        }
        return transformFunction;
      case IDENTIFIER:
        String columnName = expression.getValue();
        return new IdentifierTransformFunction(columnName, dataSourceMap.get(columnName));
      case LITERAL:
        return new LiteralTransformFunction(expression.getValue());
      default:
        throw new IllegalStateException();
    }
  }
}
