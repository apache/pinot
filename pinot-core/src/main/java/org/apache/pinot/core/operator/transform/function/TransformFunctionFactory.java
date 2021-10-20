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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.core.geospatial.transform.function.GeoToH3Function;
import org.apache.pinot.core.geospatial.transform.function.StAreaFunction;
import org.apache.pinot.core.geospatial.transform.function.StAsBinaryFunction;
import org.apache.pinot.core.geospatial.transform.function.StAsTextFunction;
import org.apache.pinot.core.geospatial.transform.function.StContainsFunction;
import org.apache.pinot.core.geospatial.transform.function.StDistanceFunction;
import org.apache.pinot.core.geospatial.transform.function.StEqualsFunction;
import org.apache.pinot.core.geospatial.transform.function.StGeogFromTextFunction;
import org.apache.pinot.core.geospatial.transform.function.StGeogFromWKBFunction;
import org.apache.pinot.core.geospatial.transform.function.StGeomFromTextFunction;
import org.apache.pinot.core.geospatial.transform.function.StGeomFromWKBFunction;
import org.apache.pinot.core.geospatial.transform.function.StGeometryTypeFunction;
import org.apache.pinot.core.geospatial.transform.function.StPointFunction;
import org.apache.pinot.core.geospatial.transform.function.StPolygonFunction;
import org.apache.pinot.core.operator.transform.function.SingleParamMathTransformFunction.AbsTransformFunction;
import org.apache.pinot.core.operator.transform.function.SingleParamMathTransformFunction.CeilTransformFunction;
import org.apache.pinot.core.operator.transform.function.SingleParamMathTransformFunction.ExpTransformFunction;
import org.apache.pinot.core.operator.transform.function.SingleParamMathTransformFunction.FloorTransformFunction;
import org.apache.pinot.core.operator.transform.function.SingleParamMathTransformFunction.LnTransformFunction;
import org.apache.pinot.core.operator.transform.function.SingleParamMathTransformFunction.SqrtTransformFunction;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.exception.BadQueryRequestException;


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
          put(canonicalize(TransformFunctionType.ADD.getName().toLowerCase()), AdditionTransformFunction.class);
          put(canonicalize(TransformFunctionType.SUB.getName().toLowerCase()), SubtractionTransformFunction.class);
          put(canonicalize(TransformFunctionType.MULT.getName().toLowerCase()), MultiplicationTransformFunction.class);
          put(canonicalize(TransformFunctionType.DIV.getName().toLowerCase()), DivisionTransformFunction.class);
          put(canonicalize(TransformFunctionType.MOD.getName().toLowerCase()), ModuloTransformFunction.class);

          put(canonicalize(TransformFunctionType.PLUS.getName().toLowerCase()), AdditionTransformFunction.class);
          put(canonicalize(TransformFunctionType.MINUS.getName().toLowerCase()), SubtractionTransformFunction.class);
          put(canonicalize(TransformFunctionType.TIMES.getName().toLowerCase()), MultiplicationTransformFunction.class);
          put(canonicalize(TransformFunctionType.DIVIDE.getName().toLowerCase()), DivisionTransformFunction.class);

          put(canonicalize(TransformFunctionType.ABS.getName().toLowerCase()), AbsTransformFunction.class);
          put(canonicalize(TransformFunctionType.CEIL.getName().toLowerCase()), CeilTransformFunction.class);
          put(canonicalize(TransformFunctionType.EXP.getName().toLowerCase()), ExpTransformFunction.class);
          put(canonicalize(TransformFunctionType.FLOOR.getName().toLowerCase()), FloorTransformFunction.class);
          put(canonicalize(TransformFunctionType.LN.getName().toLowerCase()), LnTransformFunction.class);
          put(canonicalize(TransformFunctionType.SQRT.getName().toLowerCase()), SqrtTransformFunction.class);

          put(canonicalize(TransformFunctionType.CAST.getName().toLowerCase()), CastTransformFunction.class);
          put(canonicalize(TransformFunctionType.JSONEXTRACTSCALAR.getName().toLowerCase()),
              JsonExtractScalarTransformFunction.class);
          put(canonicalize(TransformFunctionType.JSONEXTRACTKEY.getName().toLowerCase()),
              JsonExtractKeyTransformFunction.class);
          put(canonicalize(TransformFunctionType.TIMECONVERT.getName().toLowerCase()),
              TimeConversionTransformFunction.class);
          put(canonicalize(TransformFunctionType.DATETIMECONVERT.getName().toLowerCase()),
              DateTimeConversionTransformFunction.class);
          put(canonicalize(TransformFunctionType.DATETRUNC.getName().toLowerCase()), DateTruncTransformFunction.class);
          put(canonicalize(TransformFunctionType.ARRAYLENGTH.getName().toLowerCase()),
              ArrayLengthTransformFunction.class);
          put(canonicalize(TransformFunctionType.VALUEIN.getName().toLowerCase()), ValueInTransformFunction.class);
          put(canonicalize(TransformFunctionType.MAPVALUE.getName().toLowerCase()), MapValueTransformFunction.class);
          put(canonicalize(TransformFunctionType.INIDSET.getName().toLowerCase()), InIdSetTransformFunction.class);
          put(canonicalize(TransformFunctionType.LOOKUP.getName().toLowerCase()), LookupTransformFunction.class);

          // Regexp functions
          put(canonicalize(TransformFunctionType.REGEXP_EXTRACT.getName().toLowerCase()),
              RegexpExtractTransformFunction.class);

          // Array functions
          put(canonicalize(TransformFunctionType.ARRAYAVERAGE.getName().toLowerCase()),
              ArrayAverageTransformFunction.class);
          put(canonicalize(TransformFunctionType.ARRAYMAX.getName().toLowerCase()), ArrayMaxTransformFunction.class);
          put(canonicalize(TransformFunctionType.ARRAYMIN.getName().toLowerCase()), ArrayMinTransformFunction.class);
          put(canonicalize(TransformFunctionType.ARRAYSUM.getName().toLowerCase()), ArraySumTransformFunction.class);

          put(canonicalize(TransformFunctionType.GROOVY.getName().toLowerCase()), GroovyTransformFunction.class);
          put(canonicalize(TransformFunctionType.CASE.getName().toLowerCase()), CaseTransformFunction.class);

          put(canonicalize(TransformFunctionType.EQUALS.getName().toLowerCase()), EqualsTransformFunction.class);
          put(canonicalize(TransformFunctionType.NOT_EQUALS.getName().toLowerCase()), NotEqualsTransformFunction.class);
          put(canonicalize(TransformFunctionType.GREATER_THAN.getName().toLowerCase()),
              GreaterThanTransformFunction.class);
          put(canonicalize(TransformFunctionType.GREATER_THAN_OR_EQUAL.getName().toLowerCase()),
              GreaterThanOrEqualTransformFunction.class);
          put(canonicalize(TransformFunctionType.LESS_THAN.getName().toLowerCase()), LessThanTransformFunction.class);
          put(canonicalize(TransformFunctionType.LESS_THAN_OR_EQUAL.getName().toLowerCase()),
              LessThanOrEqualTransformFunction.class);
          put(canonicalize(TransformFunctionType.IN.getName().toLowerCase()), InTransformFunction.class);

          // logical functions
          put(canonicalize(TransformFunctionType.AND.getName().toLowerCase()), AndOperatorTransformFunction.class);
          put(canonicalize(TransformFunctionType.OR.getName().toLowerCase()), OrOperatorTransformFunction.class);


          // geo functions
          // geo constructors
          put(canonicalize(TransformFunctionType.ST_GEOG_FROM_TEXT.getName().toLowerCase()),
              StGeogFromTextFunction.class);
          put(canonicalize(TransformFunctionType.ST_GEOG_FROM_WKB.getName().toLowerCase()),
              StGeogFromWKBFunction.class);
          put(canonicalize(TransformFunctionType.ST_GEOM_FROM_TEXT.getName().toLowerCase()),
              StGeomFromTextFunction.class);
          put(canonicalize(TransformFunctionType.ST_GEOM_FROM_WKB.getName().toLowerCase()),
              StGeomFromWKBFunction.class);
          put(canonicalize(TransformFunctionType.ST_POINT.getName().toLowerCase()), StPointFunction.class);
          put(canonicalize(TransformFunctionType.ST_POLYGON.getName().toLowerCase()), StPolygonFunction.class);

          // geo measurements
          put(canonicalize(TransformFunctionType.ST_AREA.getName().toLowerCase()), StAreaFunction.class);
          put(canonicalize(TransformFunctionType.ST_DISTANCE.getName().toLowerCase()), StDistanceFunction.class);
          put(canonicalize(TransformFunctionType.ST_GEOMETRY_TYPE.getName().toLowerCase()),
              StGeometryTypeFunction.class);

          // geo outputs
          put(canonicalize(TransformFunctionType.ST_AS_BINARY.getName().toLowerCase()), StAsBinaryFunction.class);
          put(canonicalize(TransformFunctionType.ST_AS_TEXT.getName().toLowerCase()), StAsTextFunction.class);

          // geo relationship
          put(canonicalize(TransformFunctionType.ST_CONTAINS.getName().toLowerCase()), StContainsFunction.class);
          put(canonicalize(TransformFunctionType.ST_EQUALS.getName().toLowerCase()), StEqualsFunction.class);

          // geo indexing
          put(canonicalize(TransformFunctionType.GEOTOH3.getName().toLowerCase()), GeoToH3Function.class);
        }
      };

  /**
   * Initializes the factory with a set of transform function classes.
   * <p>Should be called only once before calling {@link #get(ExpressionContext, Map)}.
   *
   * @param transformFunctionClasses Set of transform function classes
   */
  public static void init(Set<Class<TransformFunction>> transformFunctionClasses) {
    for (Class<TransformFunction> transformFunctionClass : transformFunctionClasses) {
      TransformFunction transformFunction;
      try {
        transformFunction = transformFunctionClass.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new RuntimeException(
            "Caught exception while instantiating transform function from class: " + transformFunctionClass.toString(),
            e);
      }
      String transformFunctionName = canonicalize(transformFunction.getName());
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
  public static TransformFunction get(ExpressionContext expression, Map<String, DataSource> dataSourceMap) {
    switch (expression.getType()) {
      case FUNCTION:
        FunctionContext function = expression.getFunction();
        String functionName = canonicalize(function.getFunctionName());
        List<ExpressionContext> arguments = function.getArguments();
        int numArguments = arguments.size();

        TransformFunction transformFunction;
        Class<? extends TransformFunction> transformFunctionClass = TRANSFORM_FUNCTION_MAP.get(functionName);
        if (transformFunctionClass != null) {
          // Transform function
          try {
            transformFunction = transformFunctionClass.newInstance();
          } catch (Exception e) {
            throw new RuntimeException("Caught exception while constructing transform function: " + functionName, e);
          }
        } else {
          // Scalar function
          FunctionInfo functionInfo = FunctionRegistry.getFunctionInfo(functionName, numArguments);
          if (functionInfo == null) {
            throw new BadQueryRequestException(
                String.format("Unsupported function: %s with %d parameters", functionName, numArguments));
          }
          transformFunction = new ScalarTransformFunctionWrapper(functionInfo);
        }

        List<TransformFunction> transformFunctionArguments = new ArrayList<>(numArguments);
        for (ExpressionContext argument : arguments) {
          transformFunctionArguments.add(TransformFunctionFactory.get(argument, dataSourceMap));
        }
        try {
          transformFunction.init(transformFunctionArguments, dataSourceMap);
        } catch (Exception e) {
          throw new BadQueryRequestException("Caught exception while initializing transform function: " + functionName,
              e);
        }
        return transformFunction;
      case IDENTIFIER:
        String columnName = expression.getIdentifier();
        return new IdentifierTransformFunction(columnName, dataSourceMap.get(columnName));
      case LITERAL:
        return new LiteralTransformFunction(expression.getLiteral());
      default:
        throw new IllegalStateException();
    }
  }

  private static String canonicalize(String functionName) {
    return StringUtils.remove(functionName, '_').toLowerCase();
  }
}
