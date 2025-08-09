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

import com.google.common.base.Preconditions;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import javax.ws.rs.NotSupportedException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.table.ConcurrentIndexedTable;
import org.apache.pinot.core.data.table.IndexedTable;
import org.apache.pinot.core.data.table.IntermediateRecord;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.data.table.PartitionedIndexedTable;
import org.apache.pinot.core.data.table.RadixPartitionedHashMap;
import org.apache.pinot.core.data.table.RadixPartitionedIntermediateRecords;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.data.table.SimpleIndexedTable;
import org.apache.pinot.core.data.table.TwoLevelHashMapIndexedTable;
import org.apache.pinot.core.data.table.TwoLevelLinearProbingRecordHashMap;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.util.GroupByUtils;
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
public class BenchmarkPartitionedCombineGroupBy {

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt =
        new OptionsBuilder().include(BenchmarkPartitionedCombineGroupBy.class.getSimpleName())
            .warmupTime(TimeValue.seconds(1))
            .warmupIterations(3)
            .measurementTime(TimeValue.seconds(3))
            .measurementIterations(3)
            .forks(1);

    new Runner(opt.build()).run();
  }

  @Param({"20", "50"})
//  @Param({"200"})
  private int _numSegments;
  @Param({"1000", "10000"})
//  @Param({"10000"})
  private int _numRecordsPerSegment;
  @Param({"500", "1000000000"})
//  @Param({"1000000000"})
  private int _limit;

  //  @Param({"3", "4"})
  private int _numRadixBit = 4;
  private int _numPartitions = 16;
  private int _numThreads = 8;

  private static final int CARDINALITY_D1 = 2000;
  private static final int CARDINALITY_D2 = 2000;
  private static final Random RANDOM = new Random(43);

  private QueryContext _queryContext;
  private DataSchema _dataSchema;

  private List<String> _d1;
  private List<Integer> _d2;

  private List<List<IntermediateRecord>> _segmentIntermediateRecords;
  private CompletableFuture<RadixPartitionedIntermediateRecords>[] _partitionedRecords;
  private TwoLevelHashMapIndexedTable[] _mergedTables;
  private SimpleIndexedTable[] _mergedSimpleTables;

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

    _queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT sum(m1), max(m2) FROM testTable GROUP BY d1, d2 ORDER BY sum(m1)" + " LIMIT " + _limit);
    _dataSchema = new DataSchema(new String[]{"d1", "d2", "sum(m1)", "max(m2)"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.DOUBLE,
        DataSchema.ColumnDataType.DOUBLE
    });

    System.out.println("Num Threads: " + _numThreads + ", Num Partitions: " + _numPartitions);

    _executorService = Executors.newFixedThreadPool(_numThreads);
  }

  @TearDown(Level.Trial)
  public void destroy() {
    _executorService.shutdown();
  }

  @Setup(Level.Invocation)
  public void setupInvocation()
      throws InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException {
    _segmentIntermediateRecords = new ArrayList<>(_numSegments);
    for (int i = 0; i < _numSegments; i++) {
      List<IntermediateRecord> intermediateRecords = new ArrayList<>(_numRecordsPerSegment);
      for (int j = 0; j < _numRecordsPerSegment; j++) {
        intermediateRecords.add(getIntermediateRecord());
      }
      _segmentIntermediateRecords.add(intermediateRecords);
    }

    _partitionedRecords = new CompletableFuture[_numSegments];
    for (int i = 0; i < _numSegments; i++) {
      _partitionedRecords[i] = new CompletableFuture<>();
    }

    _mergedTables = new TwoLevelHashMapIndexedTable[_numPartitions];
    _mergedSimpleTables = new SimpleIndexedTable[_numPartitions];
  }

  private IntermediateRecord getIntermediateRecord()
      throws InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException {
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

//  @Benchmark
//  @BenchmarkMode(Mode.AverageTime)
//  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void partitionedCombineGroupBy(Blackhole blackhole)
      throws InterruptedException, ExecutionException, TimeoutException {
    final AtomicInteger nextSegmentId = new AtomicInteger(0);
    final AtomicInteger nextPartitionId = new AtomicInteger(0);

    _partitionedRecords = new CompletableFuture[_numSegments];
    for (int i = 0; i < _numSegments; i++) {
      _partitionedRecords[i] = new CompletableFuture<>();
    }
    _mergedTables = new TwoLevelHashMapIndexedTable[_numPartitions];

    List<Callable<Void>> innerSegmentCallables = new ArrayList<>(_numSegments);
    for (int i = 0; i < _numThreads; i++) {

      Callable<Void> callable = () -> {
        processPartitionedGroupBy(nextSegmentId, nextPartitionId);
        return null;
      };
      innerSegmentCallables.add(callable);
    }

    List<Future<Void>> futures = _executorService.invokeAll(innerSegmentCallables);
    for (Future<Void> future : futures) {
      future.get(30, TimeUnit.SECONDS);
    }

    // stitch
    TwoLevelLinearProbingRecordHashMap[] lookupMaps = new TwoLevelLinearProbingRecordHashMap[_numPartitions];
    int idx = 0;
    int size = 0;
    for (TwoLevelHashMapIndexedTable table : _mergedTables) {
      lookupMaps[idx++] = table.getLookupMap();
      size += table.getLookupMap().size();
    }
    RadixPartitionedHashMap map = new RadixPartitionedHashMap(lookupMaps,
        _numRadixBit);
    int trimSize = GroupByUtils.getTableCapacity(_queryContext.getLimit());
    IndexedTable table = new PartitionedIndexedTable(_dataSchema, false, _queryContext, trimSize, trimSize,
        Server.DEFAULT_QUERY_EXECUTOR_GROUPBY_TRIM_THRESHOLD, map, _executorService);

    table.finish(false);
    blackhole.consume(table);
  }

  public void processPartitionedGroupBy(AtomicInteger nextSegmentId, AtomicInteger nextPartitionId)
      throws InterruptedException, ExecutionException {

    // phase1
    int segmentId;
    while ((segmentId = nextSegmentId.getAndIncrement()) < _numSegments) {
      _partitionedRecords[segmentId].complete(new RadixPartitionedIntermediateRecords(_numRadixBit, segmentId,
          _segmentIntermediateRecords.get(segmentId)));
    }

    // phase 2
    int partitionId;
    while ((partitionId = nextPartitionId.getAndIncrement()) < _numPartitions) {
      int trimSize = GroupByUtils.getTableCapacity(_queryContext.getLimit());
      TwoLevelHashMapIndexedTable table =
          new TwoLevelHashMapIndexedTable(_dataSchema, false, _queryContext, trimSize, trimSize,
              Server.DEFAULT_QUERY_EXECUTOR_GROUPBY_TRIM_THRESHOLD,
              _executorService);

      CompletableFuture[] buf = new CompletableFuture[_numSegments];
      for (int sid = 0; sid < _numSegments; sid++) {
        buf[sid] = _partitionedRecords[sid];
      }
      int completed = 0;
      while (completed < _numSegments) {
        CompletableFuture fut =
            CompletableFuture.anyOf(buf);
        RadixPartitionedIntermediateRecords partition = (RadixPartitionedIntermediateRecords) fut.get();
        List<IntermediateRecord> records = partition.getPartition(partitionId);
        for (IntermediateRecord record : records) {
          table.upsert(record);
        }
        int processedSegmentId = partition.getSegmentId();
        buf[processedSegmentId] = buf[processedSegmentId].newIncompleteFuture();
        completed++;
      }

      table.finish(false);
      _mergedTables[partitionId] = table;
    }
  }

  // curr approach
  // ---
  // original hashtable appraoch

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void partitionedCombineGroupByJavaHashTable(Blackhole blackhole)
      throws InterruptedException, ExecutionException, TimeoutException {
    final AtomicInteger nextSegmentId = new AtomicInteger(0);
    final AtomicInteger nextPartitionId = new AtomicInteger(0);

    _partitionedRecords = new CompletableFuture[_numSegments];
    for (int i = 0; i < _numSegments; i++) {
      _partitionedRecords[i] = new CompletableFuture<>();
    }
    _mergedSimpleTables = new SimpleIndexedTable[_numPartitions];

    List<Callable<Void>> innerSegmentCallables = new ArrayList<>(_numSegments);
    for (int i = 0; i < _numThreads; i++) {

      Callable<Void> callable = () -> {
        processPartitionedGroupByJavaHashTable(nextSegmentId, nextPartitionId);
        return null;
      };
      innerSegmentCallables.add(callable);
    }

    List<Future<Void>> futures = _executorService.invokeAll(innerSegmentCallables);
    for (Future<Void> future : futures) {
      future.get(30, TimeUnit.SECONDS);
    }

    // stitch
    HashMap[] lookupMaps = new HashMap[_numPartitions];
    int idx = 0;
    int size = 0;
    for (SimpleIndexedTable table : _mergedSimpleTables) {
      lookupMaps[idx++] = (HashMap) table.getLookupMap();
      size += table.getLookupMap().size();
    }
    OldRadixPartitionedHashMap map = new OldRadixPartitionedHashMap(lookupMaps,
        _numRadixBit);
    int trimSize = GroupByUtils.getTableCapacity(_queryContext.getLimit());
    IndexedTable table = new OldPartitionedIndexedTable(_dataSchema, false, _queryContext, trimSize, trimSize,
        Server.DEFAULT_QUERY_EXECUTOR_GROUPBY_TRIM_THRESHOLD, map, _executorService);

    table.finish(false);
    blackhole.consume(table);
  }

  public void processPartitionedGroupByJavaHashTable(AtomicInteger nextSegmentId, AtomicInteger nextPartitionId)
      throws InterruptedException, ExecutionException {

    // phase1
    int segmentId;
    while ((segmentId = nextSegmentId.getAndIncrement()) < _numSegments) {
      Preconditions.checkState(!_partitionedRecords[segmentId].isDone());
      _partitionedRecords[segmentId].complete(new RadixPartitionedIntermediateRecords(_numRadixBit, segmentId,
          _segmentIntermediateRecords.get(segmentId)));
    }

    // phase 2
    int partitionId;
    while ((partitionId = nextPartitionId.getAndIncrement()) < _numPartitions) {
      int trimSize = GroupByUtils.getTableCapacity(_queryContext.getLimit());
      SimpleIndexedTable table = new SimpleIndexedTable(_dataSchema, false, _queryContext, trimSize, trimSize,
          Server.DEFAULT_QUERY_EXECUTOR_GROUPBY_TRIM_THRESHOLD,
          Server.DEFAULT_QUERY_EXECUTOR_MIN_INITIAL_INDEXED_TABLE_CAPACITY, _executorService);

      CompletableFuture[] buf = new CompletableFuture[_numSegments];
      for (int sid = 0; sid < _numSegments; sid++) {
        buf[sid] = _partitionedRecords[sid];
      }
      int completed = 0;
      while (completed < _numSegments) {
        CompletableFuture fut =
            CompletableFuture.anyOf(buf);
        RadixPartitionedIntermediateRecords partition = (RadixPartitionedIntermediateRecords) fut.get();
        List<IntermediateRecord> records = partition.getPartition(partitionId);
        for (IntermediateRecord record : records) {
          table.upsert(record._key, record._record);
        }
        int processedSegmentId = partition.getSegmentId();
        buf[processedSegmentId] = buf[processedSegmentId].newIncompleteFuture();
        completed++;
      }

      table.finish(false);
      _mergedSimpleTables[partitionId] = table;
    }
  }

  /**
   * Radix partitioned hashtable that provides a single view for multiple hashtable that could be indexed
   */
  public class OldRadixPartitionedHashMap implements Map<Key, Record> {
    private final int _numRadixBits;
    private final int _numPartitions;
    private final int _mask;
    private final HashMap[] _maps;
    private int _size;
    private int _segmentId = -1;

    public OldRadixPartitionedHashMap(int numRadixBits, int initialCapacity, int segmentId) {
      int partitionInitialCapacity = initialCapacity >> numRadixBits;
      _numRadixBits = numRadixBits;
      _numPartitions = 1 << numRadixBits;
      _mask = _numPartitions - 1;
      _segmentId = segmentId;
      _maps = new HashMap[_numPartitions];
      _size = 0;
      for (int i = 0; i < _numPartitions; i++) {
        _maps[i] = new HashMap<>();
      }
    }

    public OldRadixPartitionedHashMap(HashMap[] maps, int numRadixBits) {
      _numRadixBits = numRadixBits;
      _numPartitions = 1 << numRadixBits;
      assert (maps.length == _numPartitions);
      _mask = _numPartitions - 1;
      _maps = maps;
      _size = 0;
      for (HashMap<Key, Record> map : maps) {
        _size += map.size();
      }
    }

    public int getSegmentId() {
      return _segmentId;
    }

    public HashMap<Key, Record> getPartition(int i) {
      return _maps[i];
    }

    public int getNumPartitions() {
      return _numPartitions;
    }

    public int partition(Key key) {
      return key.hashCode() & _mask;
    }

    public int partitionFromHashCode(int hash) {
      return hash & _mask;
    }

    @Override
    public int size() {
      return _size;
    }

    @Override
    public boolean isEmpty() {
      return _size == 0;
    }

    @Override
    public boolean containsKey(Object o) {
      HashMap<Key, Record> map = _maps[partition((Key) o)];
      return map.get((Key) o) != null;
    }

    @Override
    public boolean containsValue(Object o) {
      throw new NotSupportedException("partitioned map does not support lookup by value");
    }

    @Override
    public Record get(Object o) {
      HashMap<Key, Record> map = _maps[partition((Key) o)];
      return map.get((Key) o);
    }

    public Record putIntoPartition(Key k, Record v, int partition) {
      HashMap<Key, Record> map = _maps[partition];
      Record prev = map.put((Key) k, (Record) v);
      if (prev == null) {
        _size++;
      }
      return prev;
    }

    @Nullable
    @Override
    public Record put(Key k, Record v) {
      HashMap<Key, Record> map = _maps[partition(k)];
      Record prev = map.put(k, v);
      if (prev == null) {
        _size++;
      }
      return prev;
    }

    @Override
    public Record remove(Object o) {
//      throw new NotSupportedException("don't remove");
      HashMap<Key, Record> map = _maps[partition((Key) o)];
      Record prev = map.remove(o);
      if (prev != null) {
        _size--;
      }
      return prev;
    }

    @Override
    public void putAll(Map<? extends Key, ? extends Record> map) {
      throw new NotSupportedException("partitioned map does not support removing by value");
    }

    @Override
    public void clear() {
      for (HashMap map : _maps) {
        map.clear();
      }
      _size = 0;
    }

    @Override
    public Set<Key> keySet() {
      Set<Key> set = new HashSet<>();
      for (int i = 0; i < _maps.length; i++) {
        set.addAll(_maps[i].keySet());
      }
      return set;
    }

    @Override
    public Collection<Record> values() {
      List<Record> list = new ArrayList<>();
      for (int i = 0; i < _maps.length; i++) {
        list.addAll(_maps[i].values());
      }
      return list;
    }

    @Override
    public Set<Entry<Key, Record>> entrySet() {
      Set<Entry<Key, Record>> set = new HashSet<>();
      for (int i = 0; i < _maps.length; i++) {
        set.addAll(_maps[i].entrySet());
      }
      return set;
    }
  }

  public class OldPartitionedIndexedTable extends IndexedTable {
    public OldPartitionedIndexedTable(DataSchema dataSchema, boolean hasFinalInput, QueryContext queryContext,
        int resultSize, int trimSize, int trimThreshold, OldRadixPartitionedHashMap map,
        ExecutorService executorService) {
      super(dataSchema, hasFinalInput, queryContext, resultSize, trimSize, trimThreshold, map,
          executorService);
    }

    public Map<Key, Record> getPartition(int i) {
      OldRadixPartitionedHashMap map = (OldRadixPartitionedHashMap) _lookupMap;
      return map.getPartition(i);
    }

    @Override
    public boolean upsert(Key key, Record record) {
      throw new NotSupportedException("should not finish on PartitionedIndexedTable");
    }
  }
}
