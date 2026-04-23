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
import org.apache.pinot.core.data.table.IndexedTable;
import org.apache.pinot.core.data.table.IntermediateRecord;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.operator.AcquireReleaseColumnsSegmentOperator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.combine.BaseCombineOperator;
import org.apache.pinot.core.operator.combine.GroupByCombineOperator;
import org.apache.pinot.core.operator.combine.NonblockingGroupByCombineOperator;
import org.apache.pinot.core.operator.combine.PartitionedGroupByCombineOperator;
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
 * <p>Group key cardinality: {@code CARDINALITY_D1 × CARDINALITY_D2 = 5000 × 2000 = 10M} unique groups.
 *
 * <p>Approximate unique groups in the combined result by (numSegments, numRecordsPerSegment):
 * <ul>
 *   <li>(10,  100k) ≈  1 M unique groups (10M total input records per combine)</li>
 *   <li>(100, 100k) ≈  6 M unique groups (10M total input records per combine)</li>
 *   <li>(10,  1M)   ≈  6 M unique groups (10M total input records per combine)</li>
 *   <li>(100, 1M)   ≈ 10 M unique groups (100M total input records per combine)</li>
 * </ul>
 *
 * <p>Segment data is stored in a pool of {@value #SEGMENT_DATA_POOL_SIZE} unique {@link SegmentData} objects and
 * cycled across all {@code numSegments} operators. Records are created lazily inside each operator's
 * {@code getNextBlock()} call so that memory usage is proportional to concurrent thread count rather than
 * total segment count — enabling large-scale tests (100 segments × 1M records) within an 8 GB heap.
 */
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-server", "-Xms16G", "-Xmx16G", "-XX:MaxDirectMemorySize=8G"})
public class BenchmarkGroupByCombineOperator {
  private static final String QUERY =
      "SELECT d1, d2, SUM(m1), MAX(m2) FROM testTable GROUP BY d1, d2 ORDER BY SUM(m1) DESC LIMIT 500";
  // 5000 × 2000 = 10M unique group keys
  private static final int CARDINALITY_D1 = 5000;
  private static final int CARDINALITY_D2 = 2000;
  // Number of unique SegmentData objects in the pool; large numSegments cycle through this pool.
  private static final int SEGMENT_DATA_POOL_SIZE = 10;
  private static final long QUERY_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(10);

  // NON-BLOCKING-NO-STEAL: variant of NON-BLOCKING that always creates a fresh local table
  // instead of stealing the shared table on first use, to quantify the steal optimization's benefit.
  private static final String NON_BLOCKING_NO_STEAL = "NON-BLOCKING-NO-STEAL";

  @Param({
      GroupByCombineOperator.ALGORITHM,
      NonblockingGroupByCombineOperator.ALGORITHM,
      NON_BLOCKING_NO_STEAL,
      PartitionedGroupByCombineOperator.ALGORITHM
  })
  private String _algorithm;

  // numSegments: number of segment operators presented to the combine.
  // With SEGMENT_DATA_POOL_SIZE=10, numSegments > 10 cycles through the pool (simulates key overlap).
  @Param({"10", "100"})
  private int _numSegments;

  // numRecordsPerSegment: approximate number of unique groups per segment block.
  // With cardinality=10M, 100k records/seg ≈ 100k unique groups, 1M records/seg ≈ 950k unique groups.
  @Param({"100000", "1000000"})
  private int _numRecordsPerSegment;

  @Param({"8"})
  private int _numThreads;

  // numPartitions: only used by PARTITIONED-NON-BLOCKING; ignored by other algorithms.
  @Param({"8", "32", "128"})
  private int _numPartitions;

  // Pre-created value pools; d2Values is Integer[] so boxes are allocated once at setup time, not per-record.
  private final String[] _d1Values = new String[CARDINALITY_D1];
  private final Integer[] _d2Values = new Integer[CARDINALITY_D2];

  private DataSchema _dataSchema;
  // Pool of unique segment datasets; cycled across all _numSegments operators.
  private SegmentData[] _segmentDataPool;
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

    _segmentDataPool = new SegmentData[SEGMENT_DATA_POOL_SIZE];
    Random random = new Random(8675309L);
    for (int i = 0; i < SEGMENT_DATA_POOL_SIZE; i++) {
      _segmentDataPool[i] = new SegmentData(_numRecordsPerSegment);
      for (int j = 0; j < _numRecordsPerSegment; j++) {
        _segmentDataPool[i]._d1Ids[j] = random.nextInt(CARDINALITY_D1);
        _segmentDataPool[i]._d2Ids[j] = random.nextInt(CARDINALITY_D2);
        _segmentDataPool[i]._sumValues[j] = random.nextInt(1000);
        _segmentDataPool[i]._maxValues[j] = random.nextInt(1000);
      }
    }

    _intermediateRecordConstructor =
        IntermediateRecord.class.getDeclaredConstructor(Key.class, Record.class, Comparable[].class);
    _intermediateRecordConstructor.setAccessible(true);
    _executorService = Executors.newFixedThreadPool(_numThreads);
  }

  @Setup(Level.Invocation)
  public void setupInvocation() {
    _queryContext = QueryContextConverterUtils.getQueryContext(QUERY);
    _queryContext.setEndTimeMs(System.currentTimeMillis() + QUERY_TIMEOUT_MS);
    _queryContext.setMaxExecutionThreads(_numThreads);

    _operators = new ArrayList<>(_numSegments);
    for (int i = 0; i < _numSegments; i++) {
      _operators.add(new LazyGroupByOperator(_segmentDataPool[i % SEGMENT_DATA_POOL_SIZE]));
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
      case NON_BLOCKING_NO_STEAL:
        return new NoStealNonblockingGroupByCombineOperator(_operators, _queryContext, _executorService);
      case PartitionedGroupByCombineOperator.ALGORITHM:
        return new PartitionedGroupByCombineOperator(_operators, _queryContext, _executorService, _numPartitions);
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
        .warmupIterations(1).warmupTime(TimeValue.seconds(10)).measurementIterations(3)
        .measurementTime(TimeValue.seconds(15)).forks(1);
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

  /**
   * Segment operator that creates fresh IntermediateRecords on each {@link #getNextBlock()} call.
   * This avoids accumulation bugs from mutable Record objects being reused across invocations, and keeps
   * peak memory proportional to concurrent thread count rather than total segment count.
   */
  private final class LazyGroupByOperator extends BaseOperator<GroupByResultsBlock> {
    private static final String EXPLAIN_NAME = "LAZY_GROUP_BY";

    private final SegmentData _segmentData;

    private LazyGroupByOperator(SegmentData segmentData) {
      _segmentData = segmentData;
    }

    @Override
    protected GroupByResultsBlock getNextBlock() {
      try {
        return new GroupByResultsBlock(_dataSchema, createIntermediateRecords(_segmentData), _queryContext);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
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

  /**
   * Variant of {@link NonblockingGroupByCombineOperator} that always creates a fresh local table instead of first
   * trying to steal the shared slot. Used to quantify the steal-before-create optimization's benefit.
   */
  private static final class NoStealNonblockingGroupByCombineOperator extends GroupByCombineOperator {
    NoStealNonblockingGroupByCombineOperator(List<Operator> operators, QueryContext queryContext,
        ExecutorService executorService) {
      super(operators, queryContext, executorService);
    }

    @Override
    protected void processSegments() {
      int operatorId;
      IndexedTable indexedTable = null;
      while (_processingException.get() == null && (operatorId = _nextOperatorId.getAndIncrement()) < _numOperators) {
        Operator operator = _operators.get(operatorId);
        try {
          if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
            ((AcquireReleaseColumnsSegmentOperator) operator).acquire();
          }
          GroupByResultsBlock resultsBlock = (GroupByResultsBlock) operator.nextBlock();
          // Always create a fresh local table (no steal from shared slot)
          if (indexedTable == null) {
            indexedTable = createIndexedTable(resultsBlock, 1);
          }
          mergeGroupByResultsBlock(indexedTable, resultsBlock, EXPLAIN_NAME);
        } catch (RuntimeException e) {
          throw wrapOperatorException(operator, e);
        } finally {
          if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
            ((AcquireReleaseColumnsSegmentOperator) operator).release();
          }
        }
      }

      boolean setGroupByResult = false;
      while (indexedTable != null && !setGroupByResult) {
        IndexedTable indexedTableToMerge;
        synchronized (this) {
          if (!hasSharedTable()) {
            depositSharedTable(indexedTable);
            setGroupByResult = true;
            continue;
          } else {
            indexedTableToMerge = stealSharedTable();
          }
        }
        if (indexedTable.size() > indexedTableToMerge.size()) {
          indexedTable.merge(indexedTableToMerge);
          indexedTable.absorbTrimStats(indexedTableToMerge);
        } else {
          indexedTableToMerge.merge(indexedTable);
          indexedTableToMerge.absorbTrimStats(indexedTable);
          indexedTable = indexedTableToMerge;
        }
      }
    }
  }
}
