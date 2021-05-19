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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.data.table.TableResizer;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.profile.JavaFlightRecorderProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;


@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-server", "-Xmx4G", "-XX:MaxGCPauseMillis=200"})
public class BenchmarkGroupBySort {
  private static final int BENCHMARK_SIZE = 130_000;
  private static final int CARDINALITY_D1 = 5000;
  private static final int CARDINALITY_D2 = 5000;
  private static final int TRIM_SIZE = 5000;
  private static final Random RANDOM = new Random();

  private TableResizer _TableResizer;
  private Map<Key, Record> map1;
  private Map<Key, Record> map2;
  private Map<Key, Record> map3;
  private Map<Key, Record> map4;

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

    QueryContext _queryContext = QueryContextConverterUtils
            .getQueryContextFromSQL("SELECT sum(m1), max(m2) FROM testTable GROUP BY d1, d2 ORDER BY d1");
    AggregationFunction[] _aggregationFunctions = _queryContext.getAggregationFunctions();
    assert _aggregationFunctions != null;
    DataSchema _dataSchema = new DataSchema(new String[]{"d1", "d2", "sum(m1)", "max(m2)"},
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
    return new Key(Arrays.copyOfRange(record.getValues(), 0, 2));
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int TopKTrim()
      throws InterruptedException {
    return _TableResizer.resizeRecordsMapTopK(map1, TRIM_SIZE).size();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int PQTrim()
      throws InterruptedException {
    return _TableResizer.resizeRecordsMap(map2, TRIM_SIZE).size();
  }

    @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int TopKSort()
      throws InterruptedException {
    return _TableResizer.sortRecordsMapTopK(map3, TRIM_SIZE).size();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int PQSort()
          throws InterruptedException {
    return _TableResizer.sortRecordsMap(map4, TRIM_SIZE).size();
  }



  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt =
        new OptionsBuilder().include(BenchmarkGroupBySort.class.getSimpleName()).warmupTime(TimeValue.seconds(10))
            .warmupIterations(1).measurementTime(TimeValue.seconds(30)).measurementIterations(3).forks(1);
    opt = opt.addProfiler(JavaFlightRecorderProfiler.class);
    new Runner(opt.build()).run();
  }
}
