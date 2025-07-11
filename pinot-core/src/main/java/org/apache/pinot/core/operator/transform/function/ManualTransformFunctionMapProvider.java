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

import java.util.EnumMap;
import java.util.Map;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.core.geospatial.transform.function.GeoToH3Function;
import org.apache.pinot.core.geospatial.transform.function.GridDiskFunction;
import org.apache.pinot.core.geospatial.transform.function.GridDistanceFunction;
import org.apache.pinot.core.geospatial.transform.function.StAreaFunction;
import org.apache.pinot.core.geospatial.transform.function.StAsBinaryFunction;
import org.apache.pinot.core.geospatial.transform.function.StAsGeoJsonFunction;
import org.apache.pinot.core.geospatial.transform.function.StAsTextFunction;
import org.apache.pinot.core.geospatial.transform.function.StContainsFunction;
import org.apache.pinot.core.geospatial.transform.function.StDistanceFunction;
import org.apache.pinot.core.geospatial.transform.function.StEqualsFunction;
import org.apache.pinot.core.geospatial.transform.function.StGeogFromGeoJsonFunction;
import org.apache.pinot.core.geospatial.transform.function.StGeogFromTextFunction;
import org.apache.pinot.core.geospatial.transform.function.StGeogFromWKBFunction;
import org.apache.pinot.core.geospatial.transform.function.StGeomFromGeoJsonFunction;
import org.apache.pinot.core.geospatial.transform.function.StGeomFromTextFunction;
import org.apache.pinot.core.geospatial.transform.function.StGeomFromWKBFunction;
import org.apache.pinot.core.geospatial.transform.function.StGeometryTypeFunction;
import org.apache.pinot.core.geospatial.transform.function.StPointFunction;
import org.apache.pinot.core.geospatial.transform.function.StPolygonFunction;
import org.apache.pinot.core.geospatial.transform.function.StWithinFunction;


/**
 * This utility class provides a manual mapping of {@link TransformFunctionType} to the corresponding
 * {@link TransformFunction} implementation classes.
 *
 * Ideally, this class should not exist. Instead, all transform functions should be registered using the
 * {@link org.apache.pinot.core.udf.Udf} registration mechanism. But we need to keep this class for backward
 * compatibility reasons until all transform functions are registered using the Udf mechanism.
 */
public class ManualTransformFunctionMapProvider {
  private ManualTransformFunctionMapProvider() {
  }

  public static Map<TransformFunctionType, Class<? extends TransformFunction>> manualMap() {
    Map<TransformFunctionType, Class<? extends TransformFunction>> typeToImplementation =
        new EnumMap<>(TransformFunctionType.class);
    // NOTE: add all built-in transform functions here
    typeToImplementation.put(TransformFunctionType.ADD, AdditionTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.SUB, SubtractionTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.MULT, MultiplicationTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.DIV, DivisionTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.MOD, ModuloTransformFunction.class);

    typeToImplementation.put(TransformFunctionType.ABS, SingleParamMathTransformFunction.AbsTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.CEIL, SingleParamMathTransformFunction.CeilTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.EXP, SingleParamMathTransformFunction.ExpTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.FLOOR, SingleParamMathTransformFunction.FloorTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.LOG, SingleParamMathTransformFunction.LnTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.LOG2, SingleParamMathTransformFunction.Log2TransformFunction.class);
    typeToImplementation.put(TransformFunctionType.LOG10, SingleParamMathTransformFunction.Log10TransformFunction.class);
    typeToImplementation.put(TransformFunctionType.SQRT, SingleParamMathTransformFunction.SqrtTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.SIGN, SingleParamMathTransformFunction.SignTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.POWER, PowerTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.ROUND_DECIMAL, RoundDecimalTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.TRUNCATE, TruncateDecimalTransformFunction.class);

    typeToImplementation.put(TransformFunctionType.CAST, CastTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.JSON_EXTRACT_SCALAR, JsonExtractScalarTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.JSON_EXTRACT_KEY, JsonExtractKeyTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.TIME_CONVERT, TimeConversionTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.DATE_TIME_CONVERT, DateTimeConversionTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.DATE_TIME_CONVERT_WINDOW_HOP,
        DateTimeConversionHopTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.DATE_TRUNC, DateTruncTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.JSON_EXTRACT_INDEX, JsonExtractIndexTransformFunction.class);
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
    typeToImplementation.put(TransformFunctionType.ARRAY_LENGTH, ArrayLengthTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.VALUE_IN, ValueInTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.MAP_VALUE, MapValueTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.IN_ID_SET, InIdSetTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.LOOKUP, LookupTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.CLP_DECODE, CLPDecodeTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.CLP_ENCODED_VARS_MATCH, ClpEncodedVarsMatchTransformFunction.class);

    typeToImplementation.put(TransformFunctionType.EXTRACT, ExtractTransformFunction.class);

    // Regexp functions
    typeToImplementation.put(TransformFunctionType.REGEXP_EXTRACT, RegexpExtractTransformFunction.class);

    // Array functions
    typeToImplementation.put(TransformFunctionType.ARRAY_AVERAGE, ArrayAverageTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.ARRAY_MAX, ArrayMaxTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.ARRAY_MIN, ArrayMinTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.ARRAY_SUM, ArraySumTransformFunction.class);

    typeToImplementation.put(TransformFunctionType.GROOVY, GroovyTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.CASE, CaseTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.TEXT_MATCH, TextMatchTransformFunction.class);

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
    typeToImplementation.put(TransformFunctionType.ST_GEOG_FROM_GEO_JSON, StGeogFromGeoJsonFunction.class);

    typeToImplementation.put(TransformFunctionType.ST_GEOM_FROM_TEXT, StGeomFromTextFunction.class);
    typeToImplementation.put(TransformFunctionType.ST_GEOM_FROM_WKB, StGeomFromWKBFunction.class);
    typeToImplementation.put(TransformFunctionType.ST_GEOM_FROM_GEO_JSON, StGeomFromGeoJsonFunction.class);

    typeToImplementation.put(TransformFunctionType.ST_POINT, StPointFunction.class);
    typeToImplementation.put(TransformFunctionType.ST_POLYGON, StPolygonFunction.class);

    // geo measurements
    typeToImplementation.put(TransformFunctionType.ST_AREA, StAreaFunction.class);
    typeToImplementation.put(TransformFunctionType.ST_DISTANCE, StDistanceFunction.class);
    typeToImplementation.put(TransformFunctionType.ST_GEOMETRY_TYPE, StGeometryTypeFunction.class);

    // geo outputs
    typeToImplementation.put(TransformFunctionType.ST_AS_BINARY, StAsBinaryFunction.class);
    typeToImplementation.put(TransformFunctionType.ST_AS_TEXT, StAsTextFunction.class);
    typeToImplementation.put(TransformFunctionType.ST_AS_GEO_JSON, StAsGeoJsonFunction.class);

    // geo relationship
    typeToImplementation.put(TransformFunctionType.ST_CONTAINS, StContainsFunction.class);
    typeToImplementation.put(TransformFunctionType.ST_EQUALS, StEqualsFunction.class);
    typeToImplementation.put(TransformFunctionType.ST_WITHIN, StWithinFunction.class);

    // geo indexing
    typeToImplementation.put(TransformFunctionType.GEO_TO_H3, GeoToH3Function.class);
    typeToImplementation.put(TransformFunctionType.GRID_DISTANCE, GridDistanceFunction.class);
    typeToImplementation.put(TransformFunctionType.GRID_DISK, GridDiskFunction.class);

    // tuple selection
    typeToImplementation.put(TransformFunctionType.LEAST, LeastTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.GREATEST, GreatestTransformFunction.class);

    // null handling
    typeToImplementation.put(TransformFunctionType.IS_TRUE, IsTrueTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.IS_NOT_TRUE, IsNotTrueTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.IS_FALSE, IsFalseTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.IS_NOT_FALSE, IsNotFalseTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.IS_NULL, IsNullTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.IS_NOT_NULL, IsNotNullTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.COALESCE, CoalesceTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.IS_DISTINCT_FROM, IsDistinctFromTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.IS_NOT_DISTINCT_FROM, IsNotDistinctFromTransformFunction.class);

    // Trignometric functions
    typeToImplementation.put(TransformFunctionType.SIN, TrigonometricTransformFunctions.SinTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.COS, TrigonometricTransformFunctions.CosTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.TAN, TrigonometricTransformFunctions.TanTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.COT, TrigonometricTransformFunctions.CotTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.ASIN, TrigonometricTransformFunctions.AsinTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.ACOS, TrigonometricTransformFunctions.AcosTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.ATAN, TrigonometricTransformFunctions.AtanTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.ATAN2, TrigonometricTransformFunctions.Atan2TransformFunction.class);
    typeToImplementation.put(TransformFunctionType.SINH, TrigonometricTransformFunctions.SinhTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.COSH, TrigonometricTransformFunctions.CoshTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.TANH, TrigonometricTransformFunctions.TanhTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.DEGREES, TrigonometricTransformFunctions.DegreesTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.RADIANS, TrigonometricTransformFunctions.RadiansTransformFunction.class);

    // Vector functions
    typeToImplementation.put(TransformFunctionType.COSINE_DISTANCE, VectorTransformFunctions.CosineDistanceTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.INNER_PRODUCT, VectorTransformFunctions.InnerProductTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.L1_DISTANCE, VectorTransformFunctions.L1DistanceTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.L2_DISTANCE, VectorTransformFunctions.L2DistanceTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.VECTOR_DIMS, VectorTransformFunctions.VectorDimsTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.VECTOR_NORM, VectorTransformFunctions.VectorNormTransformFunction.class);

    // Item functions
    typeToImplementation.put(TransformFunctionType.ITEM, ItemTransformFunction.class);

    // Time Series functions
    typeToImplementation.put(TransformFunctionType.TIME_SERIES_BUCKET, TimeSeriesBucketTransformFunction.class);

    return typeToImplementation;
  }
}
