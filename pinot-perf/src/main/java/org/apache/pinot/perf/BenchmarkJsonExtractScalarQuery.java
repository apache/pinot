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
package org.apache.pinot.perf;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.function.ForyJsonPathExtractor;
import org.apache.pinot.common.function.SimpleJsonPath;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.LiteralContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.operator.transform.function.BaseTransformFunction;
import org.apache.pinot.core.operator.transform.function.JsonExtractScalarTransformFunction;
import org.apache.pinot.core.operator.transform.function.LiteralTransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/// Measures the actual query-side `jsonExtractScalar*` ValueBlock loop. Each invocation processes a 128-row block;
/// JMH normalizes throughput and allocation to one row via [OperationsPerInvocation]. Input projection is represented
/// by a pre-materialized String array so the comparison isolates JSON extraction, type coercion, and result writing.
/// Result dispatch is selected once during setup, outside the measured per-row loop. The type-specific JSON literals
/// have equal encoded lengths and occupy the same early/late field locations so STRING, LONG, and DOUBLE comparisons
/// do not accidentally measure different document layouts.
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@State(Scope.Thread)
public class BenchmarkJsonExtractScalarQuery {
  private static final int BLOCK_ROWS = 128;
  private static final TransformResultMetadata STRING_METADATA =
      new TransformResultMetadata(DataType.STRING, true, false);
  private static final String BASE_JSON_FORMAT = "{"
      + "\"earlyValue\":%s,"
      + "\"user\":{\"id\":\"u-19283\",\"country\":\"US\",\"tier\":\"gold\",\"age\":41},"
      + "\"event\":{\"name\":\"checkout\",\"cart\":[{\"sku\":\"A1\",\"qty\":2,\"price\":19.99},"
      + "{\"sku\":\"B7\",\"qty\":1,\"price\":129.0},{\"sku\":\"C3\",\"qty\":4,\"price\":3.5}],"
      + "\"currency\":\"USD\",\"coupon\":null},"
      + "\"device\":{\"os\":\"macOS\",\"version\":\"14.5\",\"browser\":\"Chrome\","
      + "\"screen\":{\"width\":1728,\"height\":1117}},"
      + "\"geo\":{\"city\":\"San Francisco\",\"region\":\"CA\",\"country\":\"US\","
      + "\"coordinates\":[-122.4194,37.7749]},"
      + "\"attributes\":{\"campaign\":\"summer-sale\",\"referrer\":\"search\",\"experiment\":\"checkout-v2\"},"
      + "\"flags\":[\"returning\",\"subscribed\",\"beta\"],"
      + "\"lateValue\":%s} ";
  private static final String LATE_FIELD_MARKER = "\"lateValue\":";

  @Param({"early", "late"})
  private String _fieldPosition;

  @Param({"700", "8192", "65536"})
  private int _documentBytes;

  /// STRING is an intentional Jayway fallback for precision-safe coercion. Add `-p _resultType=STRING` explicitly to
  /// characterize that public-function fallback; default trials cover only actual Fory streaming.
  @Param({"LONG", "DOUBLE"})
  private DataType _resultType;

  /// Use `-p _pathResult=missing` to measure explicit defaults without multiplying the default suite.
  @Param({"hit"})
  private String _pathResult;

  private ValueBlock _valueBlock;
  private JsonExtractScalarTransformFunction _jayway;
  private JsonExtractScalarTransformFunction _fast;
  private JsonExtractScalarTransformFunction _firstMatch;
  private JsonExtractScalarTransformFunction _fory;

  @Setup
  public void setUp() {
    if (_resultType != DataType.STRING && !ForyJsonPathExtractor.isAvailable()) {
      throw new IllegalStateException("Fory JSON is unavailable; refusing to publish fallback results as Fory");
    }
    String json = buildJson(_documentBytes, _resultType);
    String[] jsonRows = new String[BLOCK_ROWS];
    Arrays.fill(jsonRows, json);
    TransformFunction input = new StringArrayTransformFunction(jsonRows);
    boolean early = "early".equals(_fieldPosition);
    boolean hit = "hit".equals(_pathResult);
    String path = "$." + (early ? "early" : "late") + (hit ? "Value" : "Ghost");
    Object defaultValue = defaultValue(_resultType);
    List<TransformFunction> arguments = List.of(input, literal(DataType.STRING, path),
        literal(DataType.STRING, _resultType.name()), literal(_resultType, defaultValue));

    _valueBlock = new FixedValueBlock(BLOCK_ROWS);
    if (_resultType != DataType.STRING) {
      Object directForyResult = ForyJsonPathExtractor.extract(json, SimpleJsonPath.compile(path));
      Object expectedForyResult = hit ? hitValue(_resultType, early) : null;
      if (!Objects.equals(directForyResult, expectedForyResult)) {
        throw new IllegalStateException("Direct Fory extraction produced an unexpected " + _resultType + " result");
      }
    }
    _jayway = initialize(new JsonExtractScalarTransformFunction(), arguments);
    _fast = initialize(new JsonExtractScalarTransformFunction.Fast(), arguments);
    _firstMatch = initialize(new JsonExtractScalarTransformFunction.FirstMatch(), arguments);
    _fory = initialize(new JsonExtractScalarTransformFunction.Fory(), arguments);

    Object expectedRows = expectedRows(_resultType, hit ? hitValue(_resultType, early) : defaultValue);
    Object jaywayRows = apply(_jayway);
    assertResultsEqual("jsonExtractScalar", expectedRows, jaywayRows);
    assertResultsEqual("jsonExtractScalarFast", jaywayRows, apply(_fast));
    assertResultsEqual("jsonExtractScalarFirstMatch", jaywayRows, apply(_firstMatch));
    assertResultsEqual("jsonExtractScalarFory", jaywayRows, apply(_fory));
  }

  private JsonExtractScalarTransformFunction initialize(JsonExtractScalarTransformFunction function,
      List<TransformFunction> arguments) {
    function.init(arguments, Map.<String, ColumnContext>of(), false);
    return function;
  }

  private Object apply(JsonExtractScalarTransformFunction function) {
    switch (_resultType) {
      case STRING:
        return function.transformToStringValuesSV(_valueBlock);
      case LONG:
        return function.transformToLongValuesSV(_valueBlock);
      case DOUBLE:
        return function.transformToDoubleValuesSV(_valueBlock);
      default:
        throw new IllegalStateException("Unsupported benchmark result type: " + _resultType);
    }
  }

  private static LiteralTransformFunction literal(DataType dataType, Object value) {
    return new LiteralTransformFunction(new LiteralContext(dataType, value));
  }

  private static String buildJson(int targetBytes, DataType resultType) {
    String baseJson = String.format(BASE_JSON_FORMAT, jsonLiteral(resultType, true), jsonLiteral(resultType, false));
    if (baseJson.length() >= targetBytes) {
      return baseJson;
    }
    int markerOffset = baseJson.indexOf(LATE_FIELD_MARKER);
    String paddingPrefix = "\"padding\":\"";
    String paddingSuffix = "\",";
    int paddingLength = targetBytes - baseJson.length() - paddingPrefix.length() - paddingSuffix.length();
    if (paddingLength <= 0) {
      return baseJson;
    }
    return baseJson.substring(0, markerOffset) + paddingPrefix + "x".repeat(paddingLength) + paddingSuffix
        + baseJson.substring(markerOffset);
  }

  private static String jsonLiteral(DataType resultType, boolean early) {
    switch (resultType) {
      case STRING:
        return early ? "\"S\"" : "\"T\"";
      case LONG:
        return early ? "170" : "190";
      case DOUBLE:
        return early ? "1.7" : "1.9";
      default:
        throw new IllegalStateException("Unsupported benchmark result type: " + resultType);
    }
  }

  private static Object hitValue(DataType resultType, boolean early) {
    switch (resultType) {
      case STRING:
        return early ? "S" : "T";
      case LONG:
        return early ? 170L : 190L;
      case DOUBLE:
        return early ? 1.7d : 1.9d;
      default:
        throw new IllegalStateException("Unsupported benchmark result type: " + resultType);
    }
  }

  private static Object defaultValue(DataType resultType) {
    switch (resultType) {
      case STRING:
        return "DEFAULT";
      case LONG:
        return -1L;
      case DOUBLE:
        return -1d;
      default:
        throw new IllegalStateException("Unsupported benchmark result type: " + resultType);
    }
  }

  private static Object expectedRows(DataType resultType, Object expectedValue) {
    switch (resultType) {
      case STRING:
        String[] stringValues = new String[BLOCK_ROWS];
        Arrays.fill(stringValues, (String) expectedValue);
        return stringValues;
      case LONG:
        long[] longValues = new long[BLOCK_ROWS];
        Arrays.fill(longValues, (Long) expectedValue);
        return longValues;
      case DOUBLE:
        double[] doubleValues = new double[BLOCK_ROWS];
        Arrays.fill(doubleValues, (Double) expectedValue);
        return doubleValues;
      default:
        throw new IllegalStateException("Unsupported benchmark result type: " + resultType);
    }
  }

  private void assertResultsEqual(String functionName, Object expected, Object actual) {
    boolean equal;
    switch (_resultType) {
      case STRING:
        equal = Arrays.equals((String[]) expected, (String[]) actual);
        break;
      case LONG:
        equal = Arrays.equals((long[]) expected, (long[]) actual);
        break;
      case DOUBLE:
        equal = Arrays.equals((double[]) expected, (double[]) actual);
        break;
      default:
        throw new IllegalStateException("Unsupported benchmark result type: " + _resultType);
    }
    if (!equal) {
      throw new IllegalStateException(functionName + " produced an unexpected " + _resultType + " result for "
          + _fieldPosition + '/' + _pathResult);
    }
  }

  @Benchmark
  @OperationsPerInvocation(BLOCK_ROWS)
  public Object queryJayway() {
    return apply(_jayway);
  }

  @Benchmark
  @OperationsPerInvocation(BLOCK_ROWS)
  public Object queryFast() {
    return apply(_fast);
  }

  /// Auxiliary comparator: unlike the primary parity-preserving variants, FirstMatch intentionally has weaker
  /// duplicate-key and malformed-tail semantics.
  @Benchmark
  @OperationsPerInvocation(BLOCK_ROWS)
  public Object queryFirstMatch() {
    return apply(_firstMatch);
  }

  /// For STRING, this measures the production Fory function's intentional Jayway fallback rather than Fory parsing.
  @Benchmark
  @OperationsPerInvocation(BLOCK_ROWS)
  public Object queryFory() {
    return apply(_fory);
  }

  public static void main(String[] arguments)
      throws Exception {
    Options options = new OptionsBuilder().include(BenchmarkJsonExtractScalarQuery.class.getSimpleName()).build();
    new Runner(options).run();
  }

  private static final class StringArrayTransformFunction extends BaseTransformFunction {
    private final String[] _values;

    private StringArrayTransformFunction(String[] values) {
      _values = values;
    }

    @Override
    public String getName() {
      return "stringArrayInput";
    }

    @Override
    public TransformResultMetadata getResultMetadata() {
      return STRING_METADATA;
    }

    @Override
    public String[] transformToStringValuesSV(ValueBlock valueBlock) {
      return _values;
    }
  }

  private static final class FixedValueBlock implements ValueBlock {
    private final int _numDocs;

    private FixedValueBlock(int numDocs) {
      _numDocs = numDocs;
    }

    @Override
    public int getNumDocs() {
      return _numDocs;
    }

    @Override
    public int[] getDocIds() {
      return null;
    }

    @Override
    public BlockValSet getBlockValueSet(ExpressionContext expression) {
      throw new UnsupportedOperationException();
    }

    @Override
    public BlockValSet getBlockValueSet(String column) {
      throw new UnsupportedOperationException();
    }

    @Override
    public BlockValSet getBlockValueSet(String[] paths) {
      throw new UnsupportedOperationException();
    }
  }
}
