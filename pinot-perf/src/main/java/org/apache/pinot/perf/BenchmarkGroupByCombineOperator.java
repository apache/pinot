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

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.IntermediateRecord;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.combine.BaseCombineOperator;
import org.apache.pinot.core.operator.combine.GroupByCombineOperator;
import org.apache.pinot.core.operator.combine.NonblockingGroupByCombineOperator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;


/**
 * Benchmarks end-to-end server-side group-by combine operators on synthetic segment-level result blocks.
 *
 * <p>The benchmark materializes fresh intermediate records per invocation so that repeated runs do not reuse mutable
 * records from prior combines.
 */
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-server", "-Xms8G", "-Xmx8G", "-XX:MaxDirectMemorySize=8G"})
public class BenchmarkGroupByCombineOperator {
  private static final String QUERY =
      "SELECT d1, d2, SUM(m1), MAX(m2) FROM testTable GROUP BY d1, d2 ORDER BY SUM(m1) DESC LIMIT 500";
  private static final int NUM_SEGMENTS = 32;
  private static final int CARDINALITY_D1 = 4096;
  private static final int CARDINALITY_D2 = 2048;
  private static final long QUERY_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(10);

  @Param({
      GroupByCombineOperator.ALGORITHM,
      NonblockingGroupByCombineOperator.ALGORITHM
  })
  private String _algorithm;

  @Param({"10000", "100000", "500000"})
  private int _numRecordsPerSegment;

  @Param({"8"})
  private int _numThreads;

  private final String[] _d1Values = new String[CARDINALITY_D1];
  private final Integer[] _d2Values = new Integer[CARDINALITY_D2];

  private DataSchema _dataSchema;
  private SegmentData[] _segmentData;
  private Constructor<IntermediateRecord> _intermediateRecordConstructor;
  private ExecutorService _executorService;

  private List<Operator> _operators;
  private QueryContext _queryContext;

  @Setup(Level.Trial)
  public void setup()
      throws NoSuchMethodException {
    for (int i = 0; i < CARDINALITY_D1; i++) {
      _d1Values[i] = "d1_" + i;
    }
    for (int i = 0; i < CARDINALITY_D2; i++) {
      _d2Values[i] = i;
    }

    _dataSchema = new DataSchema(new String[]{"d1", "d2", "sum(m1)", "max(m2)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT,
            DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE});
    _segmentData = new SegmentData[NUM_SEGMENTS];
    Random random = new Random(8675309L);
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      _segmentData[i] = new SegmentData(_numRecordsPerSegment);
      for (int j = 0; j < _numRecordsPerSegment; j++) {
        _segmentData[i]._d1Ids[j] = random.nextInt(CARDINALITY_D1);
        _segmentData[i]._d2Ids[j] = random.nextInt(CARDINALITY_D2);
        _segmentData[i]._sumValues[j] = random.nextInt(1000);
        _segmentData[i]._maxValues[j] = random.nextInt(1000);
      }
    }

    _intermediateRecordConstructor =
        IntermediateRecord.class.getDeclaredConstructor(Key.class, Record.class, Comparable[].class);
    _intermediateRecordConstructor.setAccessible(true);
    _executorService = Executors.newFixedThreadPool(_numThreads);
  }

  @Setup(Level.Invocation)
  public void setupInvocation()
      throws Exception {
    _queryContext = QueryContextConverterUtils.getQueryContext(QUERY);
    _queryContext.setEndTimeMs(System.currentTimeMillis() + QUERY_TIMEOUT_MS);
    _queryContext.setMaxExecutionThreads(_numThreads);

    _operators = new ArrayList<>(NUM_SEGMENTS);
    for (SegmentData segmentData : _segmentData) {
      _operators.add(new StaticGroupByOperator(
          new GroupByResultsBlock(_dataSchema, createIntermediateRecords(segmentData), _queryContext)));
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    _executorService.shutdownNow();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void combineGroupBy(Blackhole blackhole) {
    BaseCombineOperator<?> combineOperator = createCombineOperator();
    BaseResultsBlock resultsBlock = (BaseResultsBlock) combineOperator.nextBlock();
    if (!(resultsBlock instanceof GroupByResultsBlock)) {
      throw new IllegalStateException("Expected GroupByResultsBlock but got " + resultsBlock.getClass().getName()
          + " with errors: " + resultsBlock.getErrorMessages());
    }
    GroupByResultsBlock groupByResultsBlock = (GroupByResultsBlock) resultsBlock;
    blackhole.consume(groupByResultsBlock.getNumRows());
    blackhole.consume(groupByResultsBlock.getResizeTimeMs());
    blackhole.consume(groupByResultsBlock.getNumResizes());
  }

  private BaseCombineOperator<?> createCombineOperator() {
    switch (_algorithm) {
      case NonblockingGroupByCombineOperator.ALGORITHM:
        return new NonblockingGroupByCombineOperator(_operators, _queryContext, _executorService);
      default:
        return new GroupByCombineOperator(_operators, _queryContext, _executorService);
    }
  }

  private List<IntermediateRecord> createIntermediateRecords(SegmentData segmentData)
      throws Exception {
    List<IntermediateRecord> intermediateRecords = new ArrayList<>(_numRecordsPerSegment);
    for (int i = 0; i < _numRecordsPerSegment; i++) {
      Object d1 = _d1Values[segmentData._d1Ids[i]];
      Object d2 = _d2Values[segmentData._d2Ids[i]];
      Object[] keyValues = new Object[]{d1, d2};
      intermediateRecords.add(_intermediateRecordConstructor.newInstance(new Key(keyValues),
          new Record(new Object[]{d1, d2, segmentData._sumValues[i], segmentData._maxValues[i]}), null));
    }
    return intermediateRecords;
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder options = new OptionsBuilder().include(BenchmarkGroupByCombineOperator.class.getSimpleName())
        .warmupIterations(1).warmupTime(TimeValue.seconds(5)).measurementIterations(3)
        .measurementTime(TimeValue.seconds(10)).forks(1);
    new Runner(options.build()).run();
  }

  private static final class SegmentData {
    private final int[] _d1Ids;
    private final int[] _d2Ids;
    private final double[] _sumValues;
    private final double[] _maxValues;

    private SegmentData(int numRecords) {
      _d1Ids = new int[numRecords];
      _d2Ids = new int[numRecords];
      _sumValues = new double[numRecords];
      _maxValues = new double[numRecords];
    }
  }

  private static final class StaticGroupByOperator extends BaseOperator<GroupByResultsBlock> {
    private static final String EXPLAIN_NAME = "STATIC_GROUP_BY";

    private final GroupByResultsBlock _resultsBlock;

    private StaticGroupByOperator(GroupByResultsBlock resultsBlock) {
      _resultsBlock = resultsBlock;
    }

    @Override
    protected GroupByResultsBlock getNextBlock() {
      return _resultsBlock;
    }

    @Override
    public List<Operator> getChildOperators() {
      return List.of();
    }

    @Override
    public String toExplainString() {
      return EXPLAIN_NAME;
    }

    @Override
    public ExecutionStatistics getExecutionStatistics() {
      return new ExecutionStatistics(0, 0, 0, 0);
    }
  }
}
