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
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.table.ConcurrentIndexedTable;
import org.apache.pinot.core.data.table.IndexedTable;
import org.apache.pinot.core.data.table.IntermediateRecord;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.data.table.SortedRecordTable;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.query.utils.OrderByComparatorFactory;
import org.apache.pinot.core.util.GroupByUtils;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants.Server;
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


@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-server", "-Xmx8G", "-XX:MaxDirectMemorySize=16G"})
public class BenchmarkPairwiseCombineOrderByGroupBy {

  public static final String QUERY = "SELECT sum(m1), max(m2) FROM testTable GROUP BY d1, d2 ORDER BY d1, d2";
  @Param({"20", "50"})
  private int _numSegments;
  @Param({"1000", "10000"})
  private int _numRecordsPerSegment;
  @Param({"500", "1000"})
  private int _limit;
  @Param({"4", "8", "16"})
  private int _numThreads;
  @Param({QUERY})
  private String _query;

  private static final int CARDINALITY_D1 = 2000;
  private static final int CARDINALITY_D2 = 2000;
  private static final Random RANDOM = new Random(43);

  private QueryContext _queryContext;
  private DataSchema _dataSchema;

  private List<String> _d1;
  private List<Integer> _d2;

  private AtomicReference<SortedRecordTable> _waitingTable;
  private AtomicReference<SortedRecordTable> _satisfiedTable;
  private Comparator<Record> _recordKeyComparator;

  private List<List<IntermediateRecord>> _segmentIntermediateRecords;

  private ExecutorService _executorService;

  @Setup(Level.Trial)
  public void setup() {

    // create data
    Set<String> d1 = new HashSet<>(CARDINALITY_D1);
    while (d1.size() < CARDINALITY_D1) {
      d1.add(RandomStringUtils.randomAlphabetic(3));
    }
    _d1 = new ArrayList<>(CARDINALITY_D1);
    _d1.addAll(d1);

    _d2 = new ArrayList<>(CARDINALITY_D2);
    for (int i = 0; i < CARDINALITY_D2; i++) {
      _d2.add(i);
    }

    _queryContext = QueryContextConverterUtils.getQueryContext(_query + " LIMIT " + _limit);

    _dataSchema = new DataSchema(new String[]{"d1", "d2", "sum(m1)", "max(m2)"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.DOUBLE,
        DataSchema.ColumnDataType.DOUBLE
    });

    _executorService = Executors.newFixedThreadPool(_numThreads);
    _recordKeyComparator = OrderByComparatorFactory.getRecordKeyComparator(_queryContext.getOrderByExpressions(),
        _queryContext.getGroupByExpressions(), _queryContext.isNullHandlingEnabled());
  }

  @TearDown(Level.Trial)
  public void destroy() {
    _executorService.shutdown();
  }

  @Setup(Level.Invocation)
  public void setupInvocation()
      throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    _segmentIntermediateRecords = new ArrayList<>(_numSegments);
    for (int i = 0; i < _numSegments; i++) {
      List<IntermediateRecord> intermediateRecords = new ArrayList<>(_numRecordsPerSegment);
      for (int j = 0; j < _numRecordsPerSegment; j++) {
        intermediateRecords.add(getIntermediateRecord());
      }
      intermediateRecords.sort(Comparator.comparing((IntermediateRecord r) -> r._key));
      _segmentIntermediateRecords.add(intermediateRecords);
    }

    _waitingTable = new AtomicReference<>();
    _satisfiedTable = new AtomicReference<>();
  }

  private IntermediateRecord getIntermediateRecord()
      throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
    Object[] key = new Object[]{
        _d1.get(RANDOM.nextInt(_d1.size())), _d2.get(RANDOM.nextInt(_d2.size()))
    };
    Object[] record = new Object[]{
        key[0], key[1], (double) RANDOM.nextInt(
        1000), (double) RANDOM.nextInt(1000)
    };

    Constructor<IntermediateRecord> constructor =
        IntermediateRecord.class.getDeclaredConstructor(Key.class, Record.class, Comparable[].class);
    constructor.setAccessible(true);

    return constructor.newInstance(new Key(key), new Record(record), null);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void concurrentIndexedTableForCombineGroupBy(Blackhole blackhole)
      throws InterruptedException, ExecutionException, TimeoutException {
    AtomicInteger nextSegmentId1 = new AtomicInteger(0);
    int trimSize = GroupByUtils.getTableCapacity(_queryContext.getLimit());

    // make 1 concurrent table
    IndexedTable concurrentIndexedTable =
        new ConcurrentIndexedTable(_dataSchema, false, _queryContext, trimSize, trimSize,
            Server.DEFAULT_QUERY_EXECUTOR_GROUPBY_TRIM_THRESHOLD,
            Server.DEFAULT_QUERY_EXECUTOR_MIN_INITIAL_INDEXED_TABLE_CAPACITY, _executorService);

    List<Callable<Void>> innerSegmentCallables = new ArrayList<>(_numSegments);

    // NUM_THREADS parallel threads putting 10k records into the table
    for (int i = 0; i < _numThreads; i++) {
      Callable<Void> callable = () -> {
        int segmentId;
        while ((segmentId = nextSegmentId1.getAndIncrement()) < _numSegments) {
          List<IntermediateRecord> records = _segmentIntermediateRecords.get(segmentId);
          for (int r = 0; r < _numRecordsPerSegment; r++) {
            IntermediateRecord record = records.get(r);
            concurrentIndexedTable.upsert(record._key, record._record);
          }
        }
        return null;
      };
      innerSegmentCallables.add(callable);
    }

    List<Future<Void>> futures = _executorService.invokeAll(innerSegmentCallables);
    for (Future<Void> future : futures) {
      future.get(30, TimeUnit.SECONDS);
    }

    concurrentIndexedTable.finish(false);
    blackhole.consume(concurrentIndexedTable);
  }

  // prev approach
  // ----------------
  // curr approach

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void sortedPairwiseCombineGroupBy(Blackhole blackhole)
      throws InterruptedException, ExecutionException, TimeoutException {
    final AtomicInteger nextSegmentId = new AtomicInteger(0);

    List<Callable<Void>> innerSegmentCallables = new ArrayList<>(_numSegments);
    for (int i = 0; i < _numThreads; i++) {

      Callable<Void> callable = () -> {
        processSortedGroupByCombine(nextSegmentId);
        return null;
      };
      innerSegmentCallables.add(callable);
    }

    List<Future<Void>> futures = _executorService.invokeAll(innerSegmentCallables);
    for (Future<Void> future : futures) {
      future.get(30, TimeUnit.SECONDS);
    }

    SortedRecordTable table = _waitingTable.get();
    table.finish(true);
    blackhole.consume(table);
  }

  // multi-threaded approach
  // ---
  // single threaded approach

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void sequentialCombineGroupBy(Blackhole blackhole) {
    SortedRecordTable waitingTable = null;
    for (int segmentId = 0; segmentId < _numSegments; segmentId++) {
      if (waitingTable == null) {
        waitingTable = getAndPopulateSortedRecordTable(segmentId);
        continue;
      }
      GroupByResultsBlock resultsBlock = new GroupByResultsBlock(_dataSchema,
          _segmentIntermediateRecords.get(segmentId), _queryContext);
      waitingTable = mergeBlocks(waitingTable, resultsBlock);
      Tracing.ThreadAccountantOps.sampleAndCheckInterruption();
    }

    waitingTable.finish(true);
    blackhole.consume(waitingTable);
  }

  public void processSortedGroupByCombine(AtomicInteger nextSegmentId) {
    int segmentId;
    while ((segmentId = nextSegmentId.getAndIncrement()) < _numSegments) {
      int finalSegmentId = segmentId;
      SortedRecordTable waitingTable = _waitingTable.getAndUpdate(v -> v == null
          ? getAndPopulateSortedRecordTable(finalSegmentId)
          : null);
      if (waitingTable == null) {
        continue;
      }

      GroupByResultsBlock resultsBlock = new GroupByResultsBlock(_dataSchema,
          _segmentIntermediateRecords.get(finalSegmentId), _queryContext);
      SortedRecordTable table = mergeBlocks(waitingTable, resultsBlock);
      Tracing.ThreadAccountantOps.sampleAndCheckInterruption();

      while (true) {
        if (_satisfiedTable.get() != null) {
          return;
        }
        SortedRecordTable finalTable = table;
        waitingTable = _waitingTable.getAndUpdate(v -> v == null ? finalTable : null);
        if (waitingTable == null) {
          break;
        }
        // if found waiting block, merge and loop
        table = mergeBlocks(table, waitingTable);
        Tracing.ThreadAccountantOps.sampleAndCheckInterruption();
      }
    }
  }

  public SortedRecordTable getAndPopulateSortedRecordTable(int segmentId) {
    SortedRecordTable table =
        new SortedRecordTable(_dataSchema, _queryContext, _queryContext.getLimit(), _executorService,
            _recordKeyComparator);
    int mergedKeys = 0;
    for (IntermediateRecord intermediateRecord : _segmentIntermediateRecords.get(segmentId)) {
      if (!table.upsert(intermediateRecord._record)) {
        break;
      }
      Tracing.ThreadAccountantOps.sampleAndCheckInterruptionPeriodically(mergedKeys++);
    }
    return table;
  }

  private SortedRecordTable mergeBlocks(SortedRecordTable block1, GroupByResultsBlock block2) {
    return block1.mergeSortedGroupByResultBlock(block2);
  }

  private SortedRecordTable mergeBlocks(SortedRecordTable block1, SortedRecordTable block2) {
    return block1.mergeSortedRecordTable(block2);
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt =
        new OptionsBuilder().include(BenchmarkPairwiseCombineOrderByGroupBy.class.getSimpleName())
            .warmupTime(TimeValue.seconds(1))
            .warmupIterations(3)
            .measurementTime(TimeValue.seconds(10))
            .measurementIterations(3)
            .forks(1);

    new Runner(opt.build()).run();
  }
}
