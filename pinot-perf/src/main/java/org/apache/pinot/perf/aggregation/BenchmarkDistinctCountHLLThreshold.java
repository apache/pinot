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
package org.apache.pinot.perf.aggregation;

import it.unimi.dsi.fastutil.ints.IntSet;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.DistinctCountSmartHLLAggregationFunction;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
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
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.roaringbitmap.RoaringBitmap;


/**
 * Benchmark for DistinctCountSmartHLLAggregationFunction with dictionary-encoded columns.
 *
 * <p>Tests the performance impact of adaptive RoaringBitmap → HLL conversion strategy
 * across different cardinality ratios and data scales.
 *
 */
@Fork(0)
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class BenchmarkDistinctCountHLLThreshold {
  private static final ExpressionContext EXPR = ExpressionContext.forIdentifier("col");
  private static final int BATCH_SIZE = DocIdSetPlanNode.MAX_DOC_PER_CALL;

  @Param({"true", "false"})
  private boolean _useRoaringBitmap;

  @Param({"25000000"})
  private int _recordCount;

  @Param({"10", "50", "100"})
  private int _cardinalityRatioPercent;

  private DistinctCountSmartHLLAggregationFunction _aggregationFunction;
  private TestDictionary _dictionary;
  private int _numBatches;
  private int[][] _batchedDictIds;

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(BenchmarkDistinctCountHLLThreshold.class.getSimpleName())
        .build();
    new Runner(opt).run();
  }

  @Setup(Level.Trial)
  public void setUp() {
    int cardinality = (_recordCount * _cardinalityRatioPercent) / 100;
    int dictThreshold = _useRoaringBitmap ? (cardinality + 1) : 100_000;

    _aggregationFunction = new TestDistinctCountSmartHLLAggregationFunction(dictThreshold);
    _dictionary = new TestDictionary(_recordCount);
    _numBatches = (_recordCount + BATCH_SIZE - 1) / BATCH_SIZE;
    _batchedDictIds = generateAllBatches(_recordCount, cardinality, BATCH_SIZE);
  }

  @Benchmark
  public void benchmarkAggregate(Blackhole bh) {
    AggregationResultHolder resultHolder = _aggregationFunction.createAggregationResultHolder();

    for (int i = 0; i < _numBatches; i++) {
      int[] dictIds = _batchedDictIds[i];
      Map<ExpressionContext, BlockValSet> blockValSetMap =
          Collections.singletonMap(EXPR, new TestBlockValSet(_dictionary, dictIds));
      _aggregationFunction.aggregate(dictIds.length, resultHolder, blockValSetMap);
    }

    bh.consume(_aggregationFunction.extractAggregationResult(resultHolder));
  }

  private int[][] generateAllBatches(int totalRecords, int cardinality, int batchSize) {
    int numBatches = (totalRecords + batchSize - 1) / batchSize;
    int[][] batches = new int[numBatches][];
    Random random = new Random(42);

    int recordsGenerated = 0;
    for (int batchIdx = 0; batchIdx < numBatches; batchIdx++) {
      int currentBatchSize = Math.min(batchSize, totalRecords - recordsGenerated);
      int[] dictIds = new int[currentBatchSize];

      for (int i = 0; i < currentBatchSize; i++) {
        if (random.nextDouble() < 0.7) {
          dictIds[i] = (recordsGenerated + i) % cardinality;
        } else {
          dictIds[i] = random.nextInt(cardinality);
        }
      }

      batches[batchIdx] = dictIds;
      recordsGenerated += currentBatchSize;
    }

    return batches;
  }

  private static class TestDistinctCountSmartHLLAggregationFunction extends DistinctCountSmartHLLAggregationFunction {
    public TestDistinctCountSmartHLLAggregationFunction(int dictThreshold) {
      super(java.util.Arrays.asList(
          EXPR,
          ExpressionContext.forLiteral(DataType.STRING, String.format("dictThreshold=%d", dictThreshold))
      ));
    }
  }

  private static class TestDictionary implements Dictionary {
    private final int _length;

    public TestDictionary(int length) {
      _length = length;
    }

    @Override
    public boolean isSorted() {
      return true;
    }

    @Override
    public DataType getValueType() {
      return DataType.STRING;
    }

    @Override
    public int length() {
      return _length;
    }

    @Override
    public int indexOf(String stringValue) {
      return Integer.parseInt(stringValue.substring(6));
    }

    @Override
    public int insertionIndexOf(String stringValue) {
      return indexOf(stringValue);
    }

    @Override
    public IntSet getDictIdsInRange(String lower, String upper, boolean includeLower, boolean includeUpper) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int compare(int dictId1, int dictId2) {
      return Integer.compare(dictId1, dictId2);
    }

    @Override
    public Comparable getMinVal() {
      return "value_0";
    }

    @Override
    public Comparable getMaxVal() {
      return "value_" + (_length - 1);
    }

    @Override
    public Object getSortedValues() {
      String[] values = new String[_length];
      for (int i = 0; i < _length; i++) {
        values[i] = "value_" + i;
      }
      return values;
    }

    @Override
    public Object get(int dictId) {
      return dictId;
    }

    @Override
    public int getIntValue(int dictId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getLongValue(int dictId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public float getFloatValue(int dictId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public double getDoubleValue(int dictId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public BigDecimal getBigDecimalValue(int dictId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getStringValue(int dictId) {
      return String.valueOf(dictId);
    }

    @Override
    public void close() {
    }
  }

  private static class TestBlockValSet implements BlockValSet {
    private final Dictionary _dictionary;
    private final int[] _dictIds;

    public TestBlockValSet(Dictionary dictionary, int[] dictIds) {
      _dictionary = dictionary;
      _dictIds = dictIds;
    }

    @Override
    public RoaringBitmap getNullBitmap() {
      return null;
    }

    @Override
    public DataType getValueType() {
      return DataType.STRING;
    }

    @Override
    public boolean isSingleValue() {
      return true;
    }

    @Override
    public Dictionary getDictionary() {
      return _dictionary;
    }

    @Override
    public int[] getDictionaryIdsSV() {
      return _dictIds;
    }

    @Override
    public int[] getIntValuesSV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long[] getLongValuesSV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public float[] getFloatValuesSV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public double[] getDoubleValuesSV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public BigDecimal[] getBigDecimalValuesSV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String[] getStringValuesSV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[][] getBytesValuesSV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int[][] getDictionaryIdsMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int[][] getIntValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long[][] getLongValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public float[][] getFloatValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public double[][] getDoubleValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String[][] getStringValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[][][] getBytesValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int[] getNumMVEntries() {
      throw new UnsupportedOperationException();
    }
  }
}
