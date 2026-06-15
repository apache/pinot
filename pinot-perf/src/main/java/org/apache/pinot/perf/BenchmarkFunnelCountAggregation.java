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

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.core.query.aggregation.function.funnel.FunnelCountAggregationFunctionFactory;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


/**
 * Benchmark for FUNNEL_COUNT aggregation strategies to verify no performance regression
 * after the multi-key CORRELATE_BY refactoring.
 *
 * <p>Measures the single-key aggregation path (which must remain as fast as before) and
 * the new multi-key path. The benchmark simulates a realistic block of rows being aggregated.
 *
 * <p>Run with:
 * <pre>
 *   ./mvnw install -pl pinot-perf -am -DskipTests -Drat.skip=true
 *   java -jar pinot-perf/target/benchmarks.jar BenchmarkFunnelCountAggregation
 * </pre>
 */
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-server", "-Xmx4G"})
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class BenchmarkFunnelCountAggregation {

  private static final int BLOCK_SIZE = 10_000;
  private static final int NUM_STEPS = 4;
  private static final int DICT_CARDINALITY = 5000;
  private static final Random RANDOM = new Random(42);

  @Param({"bitmap", "set", "theta_sketch", "partitioned", "partitioned_sorted"})
  private String _strategy;

  private AggregationFunction _singleKeyFunction;
  private AggregationFunction _multiKeyFunction;
  private Map<ExpressionContext, BlockValSet> _blockValSetMap;
  private Map<ExpressionContext, BlockValSet> _multiKeyBlockValSetMap;

  private ExpressionContext _correlateByExpr;
  private ExpressionContext _correlateByExpr2;
  private List<ExpressionContext> _stepExpressions;

  @Setup(Level.Trial)
  public void setup() {
    _correlateByExpr = ExpressionContext.forIdentifier("user_id");
    _correlateByExpr2 = ExpressionContext.forIdentifier("category");

    _stepExpressions = new ArrayList<>(NUM_STEPS);
    for (int i = 0; i < NUM_STEPS; i++) {
      _stepExpressions.add(ExpressionContext.forIdentifier("step_" + i));
    }

    _singleKeyFunction = createFunction(false);
    _multiKeyFunction = createFunction(true);

    MockDictionary userDict = new MockDictionary(DICT_CARDINALITY, FieldSpec.DataType.INT);
    MockDictionary categoryDict = new MockDictionary(200, FieldSpec.DataType.STRING);

    // Single-key block
    _blockValSetMap = new HashMap<>();
    _blockValSetMap.put(_correlateByExpr, new MockBlockValSet(userDict, generateCorrelationIds(DICT_CARDINALITY)));
    for (int n = 0; n < NUM_STEPS; n++) {
      _blockValSetMap.put(_stepExpressions.get(n), new MockBlockValSet(null, generateStepValues(n)));
    }

    // Multi-key block
    _multiKeyBlockValSetMap = new HashMap<>();
    _multiKeyBlockValSetMap.put(_correlateByExpr,
        new MockBlockValSet(userDict, generateCorrelationIds(DICT_CARDINALITY)));
    _multiKeyBlockValSetMap.put(_correlateByExpr2,
        new MockBlockValSet(categoryDict, generateCorrelationIds(200)));
    for (int n = 0; n < NUM_STEPS; n++) {
      _multiKeyBlockValSetMap.put(_stepExpressions.get(n), new MockBlockValSet(null, generateStepValues(n)));
    }
  }

  @Benchmark
  public Object singleKeyAggregate() {
    AggregationResultHolder holder = new ObjectAggregationResultHolder();
    _singleKeyFunction.aggregate(BLOCK_SIZE, holder, _blockValSetMap);
    return holder.getResult();
  }

  @Benchmark
  public Object multiKeyAggregate() {
    AggregationResultHolder holder = new ObjectAggregationResultHolder();
    _multiKeyFunction.aggregate(BLOCK_SIZE, holder, _multiKeyBlockValSetMap);
    return holder.getResult();
  }

  private AggregationFunction createFunction(boolean multiKey) {
    List<ExpressionContext> expressions = new ArrayList<>();

    // STEPS(...)
    ExpressionContext stepsExpr = ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, "steps", _stepExpressions));
    expressions.add(stepsExpr);

    // CORRELATE_BY(...)
    List<ExpressionContext> correlateArgs = new ArrayList<>();
    correlateArgs.add(_correlateByExpr);
    if (multiKey) {
      correlateArgs.add(_correlateByExpr2);
    }
    ExpressionContext correlateExpr = ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, "correlateby", correlateArgs));
    expressions.add(correlateExpr);

    // SETTINGS(...)
    String settingsValue = getSettingsValue();
    if (settingsValue != null) {
      List<ExpressionContext> settingsArgs = new ArrayList<>();
      for (String s : settingsValue.split(",")) {
        settingsArgs.add(ExpressionContext.forLiteral(FieldSpec.DataType.STRING, s.trim()));
      }
      ExpressionContext settingsExpr = ExpressionContext.forFunction(
          new FunctionContext(FunctionContext.Type.TRANSFORM, "settings", settingsArgs));
      expressions.add(settingsExpr);
    }

    return new FunnelCountAggregationFunctionFactory(expressions).get();
  }

  private String getSettingsValue() {
    switch (_strategy) {
      case "bitmap":
        return null;
      case "set":
        return "set";
      case "theta_sketch":
        return "theta_sketch";
      case "partitioned":
        return "partitioned";
      case "partitioned_sorted":
        return "partitioned,sorted";
      default:
        throw new IllegalArgumentException("Unknown strategy: " + _strategy);
    }
  }

  private int[] generateCorrelationIds(int cardinality) {
    int[] ids = new int[BLOCK_SIZE];
    for (int i = 0; i < BLOCK_SIZE; i++) {
      ids[i] = RANDOM.nextInt(cardinality);
    }
    return ids;
  }

  private int[] generateStepValues(int step) {
    int[] values = new int[BLOCK_SIZE];
    double probability = 1.0 - (step * 0.2);
    for (int i = 0; i < BLOCK_SIZE; i++) {
      values[i] = RANDOM.nextDouble() < probability ? 1 : 0;
    }
    return values;
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder()
        .include(BenchmarkFunnelCountAggregation.class.getSimpleName());
    new Runner(opt.build()).run();
  }

  /**
   * Minimal mock Dictionary for benchmarking (avoids full segment infrastructure).
   */
  static class MockDictionary implements Dictionary {
    private final int _length;
    private final FieldSpec.DataType _valueType;

    MockDictionary(int length, FieldSpec.DataType valueType) {
      _length = length;
      _valueType = valueType;
    }

    @Override
    public void close() {
    }

    @Override
    public int length() {
      return _length;
    }

    @Override
    public boolean isSorted() {
      return true;
    }

    @Override
    public FieldSpec.DataType getValueType() {
      return _valueType;
    }

    @Override
    public int indexOf(String stringValue) {
      return Integer.parseInt(stringValue);
    }

    @Override
    public int insertionIndexOf(String stringValue) {
      return indexOf(stringValue);
    }

    @Override
    public it.unimi.dsi.fastutil.ints.IntSet getDictIdsInRange(String lower, String upper,
        boolean includeLower, boolean includeUpper) {
      return it.unimi.dsi.fastutil.ints.IntOpenHashSet.of();
    }

    @Override
    public int compare(int dictId1, int dictId2) {
      return Integer.compare(dictId1, dictId2);
    }

    @Override
    public Comparable getMinVal() {
      return 0;
    }

    @Override
    public Comparable getMaxVal() {
      return _length - 1;
    }

    @Override
    public Object get(int dictId) {
      return _valueType == FieldSpec.DataType.INT ? dictId : "val_" + dictId;
    }

    @Override
    public int getIntValue(int dictId) {
      return dictId;
    }

    @Override
    public long getLongValue(int dictId) {
      return dictId;
    }

    @Override
    public float getFloatValue(int dictId) {
      return dictId;
    }

    @Override
    public double getDoubleValue(int dictId) {
      return dictId;
    }

    @Override
    public java.math.BigDecimal getBigDecimalValue(int dictId) {
      return java.math.BigDecimal.valueOf(dictId);
    }

    @Override
    public String getStringValue(int dictId) {
      return "val_" + dictId;
    }

    @Override
    public byte[] getBytesValue(int dictId) {
      return new byte[0];
    }
  }

  /**
   * Minimal mock BlockValSet that provides dictionary IDs and step indicator values.
   */
  static class MockBlockValSet implements BlockValSet {
    private final Dictionary _dictionary;
    private final int[] _intValues;

    MockBlockValSet(Dictionary dictionary, int[] intValues) {
      _dictionary = dictionary;
      _intValues = intValues;
    }

    @Override
    public org.roaringbitmap.RoaringBitmap getNullBitmap() {
      return null;
    }

    @Override
    public boolean isDictionaryEncoded() {
      return _dictionary != null;
    }

    @Override
    public Dictionary getDictionary() {
      return _dictionary;
    }

    @Override
    public int[] getDictionaryIdsSV() {
      return _intValues;
    }

    @Override
    public int[] getIntValuesSV() {
      return _intValues;
    }

    @Override
    public FieldSpec.DataType getValueType() {
      return _dictionary != null ? _dictionary.getValueType() : FieldSpec.DataType.INT;
    }

    @Override
    public boolean isSingleValue() {
      return true;
    }

    @Override
    public int[][] getDictionaryIdsMV() {
      return null;
    }

    @Override
    public long[] getLongValuesSV() {
      return null;
    }

    @Override
    public float[] getFloatValuesSV() {
      return null;
    }

    @Override
    public double[] getDoubleValuesSV() {
      return null;
    }

    @Override
    public java.math.BigDecimal[] getBigDecimalValuesSV() {
      return null;
    }

    @Override
    public String[] getStringValuesSV() {
      return null;
    }

    @Override
    public byte[][] getBytesValuesSV() {
      return null;
    }

    @Override
    public int[][] getIntValuesMV() {
      return null;
    }

    @Override
    public long[][] getLongValuesMV() {
      return null;
    }

    @Override
    public float[][] getFloatValuesMV() {
      return null;
    }

    @Override
    public double[][] getDoubleValuesMV() {
      return null;
    }

    @Override
    public java.math.BigDecimal[][] getBigDecimalValuesMV() {
      return null;
    }

    @Override
    public String[][] getStringValuesMV() {
      return null;
    }

    @Override
    public byte[][][] getBytesValuesMV() {
      return null;
    }

    @Override
    public int[] getNumMVEntries() {
      return null;
    }
  }
}
