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
import java.util.EnumMap;
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
import org.apache.pinot.common.utils.HashUtil;
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
import org.apache.pinot.core.geospatial.transform.function.StWithinFunction;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.transform.function.SingleParamMathTransformFunction.AbsTransformFunction;
import org.apache.pinot.core.operator.transform.function.SingleParamMathTransformFunction.CeilTransformFunction;
import org.apache.pinot.core.operator.transform.function.SingleParamMathTransformFunction.ExpTransformFunction;
import org.apache.pinot.core.operator.transform.function.SingleParamMathTransformFunction.FloorTransformFunction;
import org.apache.pinot.core.operator.transform.function.SingleParamMathTransformFunction.LnTransformFunction;
import org.apache.pinot.core.operator.transform.function.SingleParamMathTransformFunction.Log10TransformFunction;
import org.apache.pinot.core.operator.transform.function.SingleParamMathTransformFunction.Log2TransformFunction;
import org.apache.pinot.core.operator.transform.function.SingleParamMathTransformFunction.SignTransformFunction;
import org.apache.pinot.core.operator.transform.function.SingleParamMathTransformFunction.SqrtTransformFunction;
import org.apache.pinot.core.operator.transform.function.TrigonometricTransformFunctions.AcosTransformFunction;
import org.apache.pinot.core.operator.transform.function.TrigonometricTransformFunctions.AsinTransformFunction;
import org.apache.pinot.core.operator.transform.function.TrigonometricTransformFunctions.Atan2TransformFunction;
import org.apache.pinot.core.operator.transform.function.TrigonometricTransformFunctions.AtanTransformFunction;
import org.apache.pinot.core.operator.transform.function.TrigonometricTransformFunctions.CosTransformFunction;
import org.apache.pinot.core.operator.transform.function.TrigonometricTransformFunctions.CoshTransformFunction;
import org.apache.pinot.core.operator.transform.function.TrigonometricTransformFunctions.CotTransformFunction;
import org.apache.pinot.core.operator.transform.function.TrigonometricTransformFunctions.DegreesTransformFunction;
import org.apache.pinot.core.operator.transform.function.TrigonometricTransformFunctions.RadiansTransformFunction;
import org.apache.pinot.core.operator.transform.function.TrigonometricTransformFunctions.SinTransformFunction;
import org.apache.pinot.core.operator.transform.function.TrigonometricTransformFunctions.SinhTransformFunction;
import org.apache.pinot.core.operator.transform.function.TrigonometricTransformFunctions.TanTransformFunction;
import org.apache.pinot.core.operator.transform.function.TrigonometricTransformFunctions.TanhTransformFunction;
import org.apache.pinot.core.operator.transform.function.VectorTransformFunctions.CosineDistanceTransformFunction;
import org.apache.pinot.core.operator.transform.function.VectorTransformFunctions.InnerProductTransformFunction;
import org.apache.pinot.core.operator.transform.function.VectorTransformFunctions.L1DistanceTransformFunction;
import org.apache.pinot.core.operator.transform.function.VectorTransformFunctions.L2DistanceTransformFunction;
import org.apache.pinot.core.operator.transform.function.VectorTransformFunctions.VectorDimsTransformFunction;
import org.apache.pinot.core.operator.transform.function.VectorTransformFunctions.VectorNormTransformFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
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
        new EnumMap<>(TransformFunctionType.class);
    // NOTE: add all built-in transform functions here
    typeToImplementation.put(TransformFunctionType.ADD, AdditionTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.SUB, SubtractionTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.MULT, MultiplicationTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.DIV, DivisionTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.MOD, ModuloTransformFunction.class);

    typeToImplementation.put(TransformFunctionType.ABS, AbsTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.CEIL, CeilTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.EXP, ExpTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.FLOOR, FloorTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.LOG, LnTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.LOG2, Log2TransformFunction.class);
    typeToImplementation.put(TransformFunctionType.LOG10, Log10TransformFunction.class);
    typeToImplementation.put(TransformFunctionType.SQRT, SqrtTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.SIGN, SignTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.POWER, PowerTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.ROUND_DECIMAL, RoundDecimalTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.TRUNCATE, TruncateDecimalTransformFunction.class);

    typeToImplementation.put(TransformFunctionType.CAST, CastTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.JSONEXTRACTSCALAR, JsonExtractScalarTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.JSONEXTRACTKEY, JsonExtractKeyTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.TIMECONVERT, TimeConversionTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.DATETIMECONVERT, DateTimeConversionTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.DATETRUNC, DateTruncTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.YEAR, DateTimeTransformFunction.Year.class);
    typeToImplementation.put(TransformFunctionType.YEAR_OF_WEEK, DateTimeTransformFunction.YearOfWeek.class);
    typeToImplementation.put(TransformFunctionType.QUARTER, DateTimeTransformFunction.Quarter.class);
    typeToImplementation.put(TransformFunctionType.MONTH_OF_YEAR, DateTimeTransformFunction.Month.class);
    typeToImplementation.put(TransformFunctionType.WEEK_OF_YEAR, DateTimeTransformFunction.WeekOfYear.class);
    typeToImplementation.put(TransformFunctionType.DAY_OF_YEAR, DateTimeTransformFunction.DayOfYear.class);
    typeToImplementation.put(TransformFunctionType.DAY_OF_MONTH, DateTimeTransformFunction.DayOfMonth.class);
    typeToImplementation.put(TransformFunctionType.DAY_OF_WEEK, DateTimeTransformFunction.DayOfWeek.class);
    typeToImplementation.put(TransformFunctionType.HOUR, DateTimeTransformFunction.Hour.class);
    typeToImplementation.put(TransformFunctionType.MINUTE, DateTimeTransformFunction.Minute.class);
    typeToImplementation.put(TransformFunctionType.SECOND, DateTimeTransformFunction.Second.class);
    typeToImplementation.put(TransformFunctionType.MILLISECOND, DateTimeTransformFunction.Millisecond.class);
    typeToImplementation.put(TransformFunctionType.ARRAYLENGTH, ArrayLengthTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.VALUEIN, ValueInTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.MAPVALUE, MapValueTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.INIDSET, InIdSetTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.LOOKUP, LookupTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.CLPDECODE, CLPDecodeTransformFunction.class);

    typeToImplementation.put(TransformFunctionType.EXTRACT, ExtractTransformFunction.class);

    // Regexp functions
    typeToImplementation.put(TransformFunctionType.REGEXP_EXTRACT, RegexpExtractTransformFunction.class);

    // Array functions
    typeToImplementation.put(TransformFunctionType.ARRAYAVERAGE, ArrayAverageTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.ARRAYMAX, ArrayMaxTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.ARRAYMIN, ArrayMinTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.ARRAYSUM, ArraySumTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.ARRAY_VALUE_CONSTRUCTOR, ArrayLiteralTransformFunction.class);

    typeToImplementation.put(TransformFunctionType.GROOVY, GroovyTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.CASE, CaseTransformFunction.class);

    typeToImplementation.put(TransformFunctionType.EQUALS, EqualsTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.NOT_EQUALS, NotEqualsTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.GREATER_THAN, GreaterThanTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.GREATER_THAN_OR_EQUAL, GreaterThanOrEqualTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.LESS_THAN, LessThanTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.LESS_THAN_OR_EQUAL, LessThanOrEqualTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.IN, InTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.NOT_IN, NotInTransformFunction.class);

    // logical functions
    typeToImplementation.put(TransformFunctionType.AND, AndOperatorTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.OR, OrOperatorTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.NOT, NotOperatorTransformFunction.class);

    // geo functions
    // geo constructors
    typeToImplementation.put(TransformFunctionType.ST_GEOG_FROM_TEXT, StGeogFromTextFunction.class);
    typeToImplementation.put(TransformFunctionType.ST_GEOG_FROM_WKB, StGeogFromWKBFunction.class);
    typeToImplementation.put(TransformFunctionType.ST_GEOM_FROM_TEXT, StGeomFromTextFunction.class);
    typeToImplementation.put(TransformFunctionType.ST_GEOM_FROM_WKB, StGeomFromWKBFunction.class);
    typeToImplementation.put(TransformFunctionType.ST_POINT, StPointFunction.class);
    typeToImplementation.put(TransformFunctionType.ST_POLYGON, StPolygonFunction.class);

    // geo measurements
    typeToImplementation.put(TransformFunctionType.ST_AREA, StAreaFunction.class);
    typeToImplementation.put(TransformFunctionType.ST_DISTANCE, StDistanceFunction.class);
    typeToImplementation.put(TransformFunctionType.ST_GEOMETRY_TYPE, StGeometryTypeFunction.class);

    // geo outputs
    typeToImplementation.put(TransformFunctionType.ST_AS_BINARY, StAsBinaryFunction.class);
    typeToImplementation.put(TransformFunctionType.ST_AS_TEXT, StAsTextFunction.class);

    // geo relationship
    typeToImplementation.put(TransformFunctionType.ST_CONTAINS, StContainsFunction.class);
    typeToImplementation.put(TransformFunctionType.ST_EQUALS, StEqualsFunction.class);
    typeToImplementation.put(TransformFunctionType.ST_WITHIN, StWithinFunction.class);

    // geo indexing
    typeToImplementation.put(TransformFunctionType.GEOTOH3, GeoToH3Function.class);

    // tuple selection
    typeToImplementation.put(TransformFunctionType.LEAST, LeastTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.GREATEST, GreatestTransformFunction.class);

    // null handling
    typeToImplementation.put(TransformFunctionType.IS_NULL, IsNullTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.IS_NOT_NULL, IsNotNullTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.COALESCE, CoalesceTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.IS_DISTINCT_FROM, IsDistinctFromTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.IS_NOT_DISTINCT_FROM, IsNotDistinctFromTransformFunction.class);

    // Trignometric functions
    typeToImplementation.put(TransformFunctionType.SIN, SinTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.COS, CosTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.TAN, TanTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.COT, CotTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.ASIN, AsinTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.ACOS, AcosTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.ATAN, AtanTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.ATAN2, Atan2TransformFunction.class);
    typeToImplementation.put(TransformFunctionType.SINH, SinhTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.COSH, CoshTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.TANH, TanhTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.DEGREES, DegreesTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.RADIANS, RadiansTransformFunction.class);

    // Vector functions
    typeToImplementation.put(TransformFunctionType.COSINE_DISTANCE, CosineDistanceTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.INNER_PRODUCT, InnerProductTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.L1_DISTANCE, L1DistanceTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.L2_DISTANCE, L2DistanceTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.VECTOR_DIMS, VectorDimsTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.VECTOR_NORM, VectorNormTransformFunction.class);

    Map<String, Class<? extends TransformFunction>> registry = new HashMap<>(typeToImplementation.size());
    for (Map.Entry<TransformFunctionType, Class<? extends TransformFunction>> entry : typeToImplementation.entrySet()) {
      for (String alias : entry.getKey().getAlternativeNames()) {
        registry.put(canonicalize(alias), entry.getValue());
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

        // Check if the function is ArrayLiteraltransform function
        if (functionName.equalsIgnoreCase(ArrayLiteralTransformFunction.FUNCTION_NAME)) {
          return queryContext.getOrComputeSharedValue(ArrayLiteralTransformFunction.class,
              expression.getFunction().getArguments(),
              ArrayLiteralTransformFunction::new);
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
          FunctionInfo functionInfo = FunctionRegistry.getFunctionInfo(functionName, numArguments);
          if (functionInfo == null) {
            if (FunctionRegistry.containsFunction(functionName)) {
              throw new BadQueryRequestException(
                  String.format("Unsupported function: %s with %d parameters", functionName, numArguments));
            } else {
              throw new BadQueryRequestException(String.format("Unsupported function: %s not found", functionName));
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
          throw new BadQueryRequestException("Caught exception while initializing transform function: " + functionName,
              e);
        }
        return transformFunction;
      case IDENTIFIER:
        String columnName = expression.getIdentifier();
        return new IdentifierTransformFunction(columnName, columnContextMap.get(columnName));
      case LITERAL:
        return queryContext.getOrComputeSharedValue(LiteralTransformFunction.class, expression.getLiteral(),
            LiteralTransformFunction::new);
      default:
        throw new IllegalStateException();
    }
  }

  @VisibleForTesting
  public static TransformFunction get(ExpressionContext expression, Map<String, DataSource> dataSourceMap) {
    Map<String, ColumnContext> columnContextMap = new HashMap<>(HashUtil.getHashMapCapacity(dataSourceMap.size()));
    dataSourceMap.forEach((k, v) -> columnContextMap.put(k, ColumnContext.fromDataSource(v)));
    QueryContext dummy = QueryContextConverterUtils.getQueryContext(
        CalciteSqlParser.compileToPinotQuery("SELECT * from testTable;"));
    return get(expression, columnContextMap, dummy);
  }

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
}
