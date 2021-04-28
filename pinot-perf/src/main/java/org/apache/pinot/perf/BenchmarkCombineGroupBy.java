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

import com.google.common.base.Joiner;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.table.*;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByTrimmingService;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.util.GroupByUtils;
import org.apache.pinot.spi.utils.Pair;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;


@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-server", "-Xmx8G", "-XX:MaxDirectMemorySize=32G"})
public class BenchmarkCombineGroupBy {
  private static final int BENCHMARK_SIZE = 1000_000;
  private static final int CARDINALITY_D1 = 50000;
  private static final int CARDINALITY_D2 = 50000;
  private static final int TRIM_SIZE = 5000;
  private static final Random RANDOM = new Random();

  private QueryContext _queryContext;
  private AggregationFunction[] _aggregationFunctions;
  private DataSchema _dataSchema;
  private TableResizer _TableResizer;
  private Map<Key, Record> map1;
  private Map<Key, Record> map2;
  private Map<Key, Record> map3;
  private Map<Key, Record> map4;
  private TableResizer.IntermediateRecord[] recordArray;

  private List<String> _d1;
  private List<Integer> _d2;



  @Setup
  public void setup() throws InterruptedException, ExecutionException, TimeoutException {
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

    _queryContext = QueryContextConverterUtils
        .getQueryContextFromSQL("SELECT sum(m1), max(m2) FROM testTable GROUP BY d1, d2 ORDER BY d1");
    _aggregationFunctions = _queryContext.getAggregationFunctions();
    assert _aggregationFunctions != null;
    _dataSchema = new DataSchema(new String[]{"d1", "d2", "sum(m1)", "max(m2)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE});

    _TableResizer = new TableResizer(_dataSchema, _queryContext);

    map1 = new HashMap<>();
    map2 = new HashMap<>();
    map3 = new HashMap<>();
    map4 = new HashMap<>();

    for (int i = 0; i < BENCHMARK_SIZE; ++i) {
      Record record = getRecord();
      Key key = getKey(record);
      map1.put(key, record);
      map2.put(key, record);
      map3.put(key, record);
      map4.put(key, record);
    }
    recordArray = _TableResizer.convertToArray(map1, TRIM_SIZE, _TableResizer.getComparator());
  }

  @TearDown
  public void destroy() {
    map1.clear();
    map2.clear();
    map3.clear();
    map4.clear();
  }

  private Record getRecord() {
    Object[] columns =
        new Object[]{_d1.get(RANDOM.nextInt(_d1.size())), _d2.get(RANDOM.nextInt(_d2.size())), (double) RANDOM
            .nextInt(10000), (double) RANDOM.nextInt(10000)};

    return new Record(columns);
  }

  private Key getKey(Record record) {
    Key key = new Key(Arrays.copyOfRange(record.getValues(), 0, 2));
    return key;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public int TopKSort()
      throws InterruptedException, ExecutionException, TimeoutException {

    return _TableResizer.sortRecordsMapTopK(map1, TRIM_SIZE).size();
  }
//
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public int PQSort()
      throws InterruptedException, TimeoutException, ExecutionException {
    return _TableResizer.sortRecordsMap(map3, TRIM_SIZE).size();
  }

    @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public int TokTrimArray()
      throws InterruptedException, TimeoutException, ExecutionException {
    return _TableResizer.convertToArray(map2, TRIM_SIZE, _TableResizer.getComparator()).length;
  }

//  @Benchmark
//  @BenchmarkMode(Mode.AverageTime)
//  @OutputTimeUnit(TimeUnit.MICROSECONDS)
//  public int TopKSelect()
//          throws InterruptedException, ExecutionException, TimeoutException {
//
//     _TableResizer.quickSortSmallestK(recordArray, 0, recordArray.length - 1, TRIM_SIZE, _TableResizer.getComparator());
//     return recordArray.length;
//  }

//  @Benchmark
//  @BenchmarkMode(Mode.AverageTime)
//  @OutputTimeUnit(TimeUnit.MICROSECONDS)
//  public int PQTrim()
//          throws InterruptedException, TimeoutException, ExecutionException {
//    return _TableResizer.convertToIntermediateRecordsPQ(map4, TRIM_SIZE, _TableResizer.getComparator().reversed()).size();
//  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt =
        new OptionsBuilder().include(BenchmarkCombineGroupBy.class.getSimpleName()).warmupTime(TimeValue.seconds(10))
            .warmupIterations(1).measurementTime(TimeValue.seconds(30)).measurementIterations(3).forks(1);
    opt = opt.addProfiler(StackProfiler.class,
            "excludePackages=true;excludePackageNames=sun.,java.net.,io.netty.,org.apache.zookeeper.,org.eclipse.jetty.;lines=5;period=1;top=20");
    new Runner(opt.build()).run();
  }
}
