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
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.LiteralContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.HashUtil;
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
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
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
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Factory class for transformation functions.
///
/// The registry always contains the built-in transform functions. On top of those, [#init(Set)] registers external
/// block-oriented [TransformFunction] implementations from two sources:
///
/// 1. **`ServiceLoader` discovery.** Any jar visible through the thread context classloader (the application
///    classpath in a standard server deployment), or through any plugin classloader returned by
///    [PluginManager#getPluginClassLoaders()], can register transform functions by shipping a standard service
///    descriptor:
///
///    `META-INF/services/org.apache.pinot.core.operator.transform.function.TransformFunction`
///
///    listing one implementation class per line. A provider must be a public concrete class with a public
///    no-argument constructor, and must return a non-null, non-blank name from [TransformFunction#getName()]. The
///    instance created at discovery time is used only to read and validate its name; it is never initialized or
///    evaluated — query execution always constructs a fresh instance through this factory. Names are canonicalized
///    with [#canonicalize(String)]. The same implementation class visible through overlapping classloaders is
///    de-duplicated, but a discovered class whose canonical name collides with a built-in or with a different
///    discovered class fails initialization, as does any malformed descriptor or misbehaving provider.
///
/// 2. **Explicit configuration** (`pinot.server.transforms`). Explicitly configured classes are registered after
///    discovered ones and keep their historical override semantics: they may replace built-in as well as discovered
///    registrations. Discovery skips classes that are also explicitly configured, so shipping a service descriptor
///    for an explicitly configured class never causes a collision failure.
///
/// Discovery only runs inside [#init(Set)], which the server calls once during startup after plugins are loaded;
/// the query path ([#get]) performs plain map lookups against an immutable snapshot and never triggers
/// `ServiceLoader` work. [#init(Set)] is synchronized and atomically publishes a fully validated snapshot, so
/// concurrent readers never observe a partially initialized registry; calling it again with the same inputs is
/// idempotent. Note that each function-expression lookup reads the current snapshot, so an expression compiled
/// concurrently with a re-initialization may resolve different sub-expressions against different (complete)
/// snapshots — irrelevant in practice because initialization happens before queries are served.
///
/// Note: this registers functions for server-side single-stage (leaf) execution only. It does not register them
/// with the multi-stage engine's function catalog ([org.apache.pinot.common.function.FunctionRegistry] /
/// PinotOperatorTable).
public class TransformFunctionFactory {
  private TransformFunctionFactory() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(TransformFunctionFactory.class);
  private static final Map<String, Class<? extends TransformFunction>> BUILT_IN_TRANSFORM_FUNCTIONS =
      Collections.unmodifiableMap(createRegistry());

  /// Immutable snapshot of the transform function registry, atomically replaced by [#init(Set)]. Starts as the
  /// built-in registry so the factory is usable without an explicit init call.
  private static volatile Map<String, Class<? extends TransformFunction>> _transformFunctionMap =
      BUILT_IN_TRANSFORM_FUNCTIONS;

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
    typeToImplementation.put(TransformFunctionType.JSON_EXTRACT_SCALAR, JsonExtractScalarTransformFunction.class);
    typeToImplementation.put(TransformFunctionType.JSON_EXTRACT_SCALAR_FAST,
        JsonExtractScalarTransformFunction.Fast.class);
    typeToImplementation.put(TransformFunctionType.JSON_EXTRACT_SCALAR_FIRST_MATCH,
        JsonExtractScalarTransformFunction.FirstMatch.class);
    typeToImplementation.put(TransformFunctionType.JSON_EXTRACT_SCALAR_FORY,
        JsonExtractScalarTransformFunction.Fory.class);
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
    typeToImplementation.put(TransformFunctionType.FILTER_MV, FilterMvTransformFunction.class);
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

    // Item functions
    typeToImplementation.put(TransformFunctionType.ITEM, ItemTransformFunction.class);

    // Time Series functions
    typeToImplementation.put(TransformFunctionType.TIME_SERIES_BUCKET, TimeSeriesBucketTransformFunction.class);

    Map<String, Class<? extends TransformFunction>> registry =
        new HashMap<>(HashUtil.getHashMapCapacity(typeToImplementation.size()));
    for (Map.Entry<TransformFunctionType, Class<? extends TransformFunction>> entry : typeToImplementation.entrySet()) {
      for (String name : entry.getKey().getNames()) {
        registry.put(canonicalize(name), entry.getValue());
      }
    }
    return registry;
  }

  /// Initializes the factory with the explicitly configured transform function classes (`pinot.server.transforms`)
  /// and the [TransformFunction] service providers discovered via [PluginManager#loadServiceProviders(Class)] (see
  /// the class documentation for the provider contract).
  ///
  /// Should be called during server startup, after all plugins have been loaded and before serving queries. The
  /// registry is built and validated locally and published atomically, so concurrent readers never observe a
  /// partially initialized registry; repeated calls with the same inputs are idempotent. Any discovery or
  /// validation failure aborts initialization (with the original cause preserved) and leaves the previously
  /// published registry in place.
  ///
  /// @param transformFunctionClasses Set of explicitly configured transform function classes
  public static synchronized void init(Set<Class<TransformFunction>> transformFunctionClasses) {
    Map<String, Class<? extends TransformFunction>> registry = new HashMap<>(BUILT_IN_TRANSFORM_FUNCTIONS);

    // Classes registered through explicit configuration are skipped during discovery: the explicit registration
    // below handles them with its historical override semantics, so a service descriptor shipped for an explicitly
    // configured class must not fail startup as a collision.
    Set<String> explicitClassNames = new HashSet<>();
    for (Class<TransformFunction> transformFunctionClass : transformFunctionClasses) {
      explicitClassNames.add(transformFunctionClass.getName());
    }

    // Discover service providers from the thread context classloader (the application classpath in a standard
    // server deployment) and from every plugin classloader, de-duplicated by implementation class name (see
    // PluginManager#loadServiceProviders)
    for (PluginManager.ServiceProvider<TransformFunction> discovered : PluginManager.get()
        .loadServiceProviders(TransformFunction.class)) {
      registerServiceProvider(discovered.getProvider(), discovered.getSource(), registry, explicitClassNames);
    }

    // Register the explicitly configured classes last: explicit configuration keeps its historical override
    // semantics and may replace built-in as well as discovered registrations.
    for (Class<TransformFunction> transformFunctionClass : transformFunctionClasses) {
      TransformFunction transformFunction;
      try {
        transformFunction = transformFunctionClass.getDeclaredConstructor().newInstance();
      } catch (Exception e) {
        throw new RuntimeException(
            "Caught exception while instantiating transform function from class: " + transformFunctionClass, e);
      }
      String name = transformFunction.getName();
      if (StringUtils.isBlank(name)) {
        throw new IllegalStateException("Transform function class: " + transformFunctionClass.getName()
            + " returned a " + (name == null ? "null" : "blank") + " name from getName()");
      }
      String transformFunctionName = canonicalize(name);
      if (registry.put(transformFunctionName, transformFunctionClass) == null) {
        LOGGER.info("Registering function: {} with class: {}", transformFunctionName, transformFunctionClass);
      } else {
        LOGGER.info("Replacing function: {} with class: {}", transformFunctionName, transformFunctionClass);
      }
    }

    // Atomically publish the fully built and validated snapshot.
    _transformFunctionMap = Collections.unmodifiableMap(registry);
  }

  /// Registers a discovered [TransformFunction] service provider into the local registry being built. The provider
  /// instance is only used to obtain the implementation class and validate its name; it is never initialized or
  /// evaluated. Fails fast on null/blank function names and canonical name collisions.
  private static void registerServiceProvider(TransformFunction provider, String source,
      Map<String, Class<? extends TransformFunction>> registry, Set<String> explicitClassNames) {
    Class<? extends TransformFunction> providerClass = provider.getClass();
    String providerClassName = providerClass.getName();
    if (explicitClassNames.contains(providerClassName)) {
      LOGGER.info("Skipping service provider: {} (from: {}) which is also explicitly configured", providerClassName,
          source);
      return;
    }
    String name = provider.getName();
    if (StringUtils.isBlank(name)) {
      throw new IllegalStateException("TransformFunction service provider: " + providerClassName + " (classloader: "
          + providerClass.getClassLoader() + ", discovered from: " + source + ") returned a "
          + (name == null ? "null" : "blank") + " name from getName()");
    }
    String canonicalName = canonicalize(name);
    Class<? extends TransformFunction> existing = registry.get(canonicalName);
    if (existing != null) {
      if (existing.getName().equals(providerClassName)) {
        // Repeated registration of the identical implementation is a no-op. A different Class object with the same
        // name indicates version skew between classloaders; the registered copy wins.
        if (existing != providerClass) {
          LOGGER.warn("Function: {} is already registered with class: {} from classloader: {}; ignoring the copy "
                  + "from classloader: {} (discovered from: {})", canonicalName, existing.getName(),
              existing.getClassLoader(), providerClass.getClassLoader(), source);
        }
        return;
      }
      String existingDescription = BUILT_IN_TRANSFORM_FUNCTIONS.get(canonicalName) == existing
          ? "built-in transform function class: " + existing.getName()
          : "discovered transform function class: " + existing.getName();
      throw new IllegalStateException(
          "Transform function name collision on: " + canonicalName + ". Service provider class: "
              + providerClassName + " (classloader: " + providerClass.getClassLoader() + ", discovered from: "
              + source + ") collides with " + existingDescription + " (classloader: " + existing.getClassLoader()
              + ")");
    }
    if (FunctionRegistry.contains(FunctionRegistry.canonicalize(name))) {
      // Not a failure: providing a block-oriented implementation of an existing scalar function is the same
      // pattern the built-ins use — but for an independently owned scalar function the semantics may diverge
      // (e.g. literal-only invocations still constant-fold through the scalar implementation at compile time).
      LOGGER.warn("Service-discovered transform function: {} with class: {} (from: {}) shadows a scalar function "
          + "with the same name for single-stage server-side execution", canonicalName, providerClassName, source);
    }
    registry.put(canonicalName, providerClass);
    LOGGER.info("Registering service-discovered function: {} with class: {} (from: {})", canonicalName,
        providerClassName, source);
  }

  /// Returns an instance of transform function for the given expression.
  ///
  /// @param expression       Transform expression
  /// @param columnContextMap Map from column name to context
  /// @param queryContext     Query context
  /// @return Transform function
  public static TransformFunction get(ExpressionContext expression, Map<String, ColumnContext> columnContextMap,
      QueryContext queryContext) {
    switch (expression.getType()) {
      case FUNCTION:
        FunctionContext function = expression.getFunction();
        String functionName = canonicalize(function.getFunctionName());

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

        List<ExpressionContext> arguments = function.getArguments();
        int numArguments = arguments.size();

        // Build child transform functions first to derive argument data types for scalar function polymorphism
        List<TransformFunction> transformFunctionArguments = new ArrayList<>(numArguments);
        for (ExpressionContext argument : arguments) {
          transformFunctionArguments.add(TransformFunctionFactory.get(argument, columnContextMap, queryContext));
        }

        TransformFunction transformFunction;
        Class<? extends TransformFunction> transformFunctionClass = _transformFunctionMap.get(functionName);
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
          // Get data types for the arguments
          ColumnDataType[] argumentDataTypes = new ColumnDataType[numArguments];
          for (int i = 0; i < numArguments; i++) {
            TransformResultMetadata resultMetadata = transformFunctionArguments.get(i).getResultMetadata();
            argumentDataTypes[i] =
                ColumnDataType.fromDataType(resultMetadata.getDataType(), resultMetadata.isSingleValue());
          }
          FunctionInfo functionInfo = FunctionRegistry.lookupFunctionInfo(canonicalName, argumentDataTypes);
          if (functionInfo == null) {
            if (FunctionRegistry.contains(canonicalName)) {
              throw new BadQueryRequestException(
                  numArguments > 0 ? String.format("Unsupported function: %s with arguments of type: %s", functionName,
                      Arrays.toString(argumentDataTypes))
                      : String.format("Unsupported function: %s with 0 arguments", functionName));
            } else {
              throw new BadQueryRequestException(String.format("Unsupported function: %s", functionName));
            }
          }
          transformFunction = new ScalarTransformFunctionWrapper(functionInfo);
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

  /// Converts the transform function name into its canonical form
  ///
  /// @param functionName Name of the transform function
  /// @return canonicalized transform function name
  public static String canonicalize(String functionName) {
    return StringUtils.remove(functionName, '_').toLowerCase();
  }

  /// Returns an immutable snapshot of the current registry (canonical name to implementation class). Registrations
  /// from later [#init(Set)] calls are not reflected in previously returned maps.
  public static Map<String, Class<? extends TransformFunction>> getAllFunctions() {
    return _transformFunctionMap;
  }
}
