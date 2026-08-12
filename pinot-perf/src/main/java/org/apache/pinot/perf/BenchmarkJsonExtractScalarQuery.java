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
import java.util.concurrent.TimeUnit;
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
  private static final String BASE_JSON = "{"
      + "\"earlyMetric\":17,"
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
      + "\"lateMetric\":19} ";

  @Param({"early", "late"})
  private String _fieldPosition;

  @Param({"700", "8192", "65536"})
  private int _documentBytes;

  private ValueBlock _valueBlock;
  private JsonExtractScalarTransformFunction _jayway;
  private JsonExtractScalarTransformFunction _fast;
  private JsonExtractScalarTransformFunction _firstMatch;
  private JsonExtractScalarTransformFunction _fory;

  @Setup
  public void setUp() {
    String json = buildJson(_documentBytes);
    String[] jsonRows = new String[BLOCK_ROWS];
    Arrays.fill(jsonRows, json);
    TransformFunction input = new StringArrayTransformFunction(jsonRows);
    String path = "early".equals(_fieldPosition) ? "$.earlyMetric" : "$.lateMetric";
    List<TransformFunction> arguments = List.of(input, literal(path), literal("LONG"));

    _valueBlock = new FixedValueBlock(BLOCK_ROWS);
    _jayway = new JsonExtractScalarTransformFunction();
    _fast = new JsonExtractScalarTransformFunction.Fast();
    _firstMatch = new JsonExtractScalarTransformFunction.FirstMatch();
    _fory = new JsonExtractScalarTransformFunction.Fory();
    for (JsonExtractScalarTransformFunction function : List.of(_jayway, _fast, _firstMatch, _fory)) {
      function.init(arguments, Map.<String, ColumnContext>of(), false);
      long expected = "early".equals(_fieldPosition) ? 17L : 19L;
      long[] values = function.transformToLongValuesSV(_valueBlock);
      if (values.length < BLOCK_ROWS || values[0] != expected || values[BLOCK_ROWS - 1] != expected) {
        throw new IllegalStateException(function.getName() + " produced an unexpected query result");
      }
    }
  }

  private static LiteralTransformFunction literal(String value) {
    return new LiteralTransformFunction(new LiteralContext(DataType.STRING, value));
  }

  private static String buildJson(int targetBytes) {
    if (BASE_JSON.length() >= targetBytes) {
      return BASE_JSON;
    }
    String marker = "\"lateMetric\":";
    int markerOffset = BASE_JSON.indexOf(marker);
    String paddingPrefix = "\"padding\":\"";
    String paddingSuffix = "\",";
    int paddingLength = targetBytes - BASE_JSON.length() - paddingPrefix.length() - paddingSuffix.length();
    return BASE_JSON.substring(0, markerOffset) + paddingPrefix + "x".repeat(paddingLength) + paddingSuffix
        + BASE_JSON.substring(markerOffset);
  }

  @Benchmark
  @OperationsPerInvocation(BLOCK_ROWS)
  public long[] queryJayway() {
    return _jayway.transformToLongValuesSV(_valueBlock);
  }

  @Benchmark
  @OperationsPerInvocation(BLOCK_ROWS)
  public long[] queryFast() {
    return _fast.transformToLongValuesSV(_valueBlock);
  }

  @Benchmark
  @OperationsPerInvocation(BLOCK_ROWS)
  public long[] queryFirstMatch() {
    return _firstMatch.transformToLongValuesSV(_valueBlock);
  }

  @Benchmark
  @OperationsPerInvocation(BLOCK_ROWS)
  public long[] queryFory() {
    return _fory.transformToLongValuesSV(_valueBlock);
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
