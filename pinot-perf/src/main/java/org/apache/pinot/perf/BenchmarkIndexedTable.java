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

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.table.ConcurrentIndexedTable;
import org.apache.pinot.core.data.table.IndexedTable;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.data.table.SimpleIndexedTable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;


@State(Scope.Benchmark)
public class BenchmarkIndexedTable {

  private int CAPACITY = 800;
  private int NUM_RECORDS = 1000;
  private Random _random = new Random();

  private DataSchema _dataSchema;
  private List<AggregationInfo> _aggregationInfos;
  private List<SelectionSort> _orderBy;

  private List<String> _d1;
  private List<Integer> _d2;

  private ExecutorService _executorService;

  @Setup(Level.Invocation)
  public void setUpInvocation() {

    // create data
    int cardinalityD1 = 100;
    _d1 = new ArrayList<>(cardinalityD1);
    for (int i = 0; i < cardinalityD1; i++) {
      _d1.add(RandomStringUtils.randomAlphabetic(3));
    }

    int cardinalityD2 = 100;
    _d2 = new ArrayList<>(cardinalityD2);
    for (int i = 0; i < cardinalityD2; i++) {
      _d2.add(i);
    }
  }

  @Setup
  public void setup() {
    _dataSchema = new DataSchema(new String[]{"d1", "d2", "sum(m1)", "max(m2)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT,
            DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE});

    AggregationInfo agg1 = new AggregationInfo();
    Map<String, String> params1 = new HashMap<>();
    params1.put("column", "m1");
    agg1.setAggregationParams(params1);
    agg1.setAggregationType("sum");
    AggregationInfo agg2 = new AggregationInfo();
    Map<String, String> params2 = new HashMap<>();
    params2.put("column", "m2");
    agg2.setAggregationParams(params2);
    agg2.setAggregationType("max");
    _aggregationInfos = Lists.newArrayList(agg1, agg2);

    SelectionSort orderBy = new SelectionSort();
    orderBy.setColumn("sum(m1)");
    orderBy.setIsAsc(true);
    _orderBy = Lists.newArrayList(orderBy);

    _executorService = Executors.newFixedThreadPool(10);
  }

  @TearDown
  public void destroy() {
    _executorService.shutdown();
  }

  private Record getNewRecord() {
    Object[] keys = new Object[]{_d1.get(_random.nextInt(_d1.size())), _d2.get(_random.nextInt(_d2.size()))};
    Object[] values = new Object[]{(double) _random.nextInt(1000), (double) _random.nextInt(1000)};
    return new Record(new Key(keys), values);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void concurrentIndexedTable() throws InterruptedException, ExecutionException, TimeoutException {

    int numSegments = 10;

    // make 1 concurrent table
    IndexedTable concurrentIndexedTable = new ConcurrentIndexedTable();
    concurrentIndexedTable.init(_dataSchema, _aggregationInfos, _orderBy, CAPACITY);

    List<Callable<Void>> innerSegmentCallables = new ArrayList<>(numSegments);

    // 10 parallel threads putting 10k records into the table

    for (int i = 0; i < numSegments; i++) {

      Callable<Void> callable = () -> {
        for (int r = 0; r < NUM_RECORDS; r++) {
          concurrentIndexedTable.upsert(getNewRecord());
        }
        return null;
      };
      innerSegmentCallables.add(callable);
    }

    List<Future<Void>> futures = _executorService.invokeAll(innerSegmentCallables);
    for (Future<Void> future : futures) {
      future.get(10, TimeUnit.SECONDS);
    }

    concurrentIndexedTable.finish();
  }


  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void simpleIndexedTable() throws InterruptedException, TimeoutException, ExecutionException {

    int numSegments = 10;

    List<IndexedTable> simpleIndexedTables = new ArrayList<>(numSegments);
    List<Callable<Void>> innerSegmentCallables = new ArrayList<>(numSegments);

    for (int i = 0; i < numSegments; i++) {

      // make 10 indexed tables
      IndexedTable simpleIndexedTable = new SimpleIndexedTable();
      simpleIndexedTable.init(_dataSchema, _aggregationInfos, _orderBy, CAPACITY);
      simpleIndexedTables.add(simpleIndexedTable);

      // put 10k records in each indexed table, in parallel
      Callable<Void> callable = () -> {
        for (int r = 0; r < NUM_RECORDS; r++) {
          simpleIndexedTable.upsert(getNewRecord());
        }
        simpleIndexedTable.finish();
        return null;
      };
      innerSegmentCallables.add(callable);
    }

    List<Future<Void>> futures = _executorService.invokeAll(innerSegmentCallables);
    for (Future<Void> future : futures) {
      future.get(10, TimeUnit.SECONDS);
    }

    // merge all indexed tables into 1
    IndexedTable mergedTable = null;
    for (IndexedTable indexedTable : simpleIndexedTables) {
      if (mergedTable == null) {
        mergedTable = indexedTable;
      } else {
        mergedTable.merge(indexedTable);
      }
    }
    mergedTable.finish();
  }


  /*@Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void simpleUpsert() {

  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void concurrentUpsert() {

  }*/

  public static void main(String[] args) throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkIndexedTable.class.getSimpleName())
        .warmupTime(TimeValue.seconds(10))
        .warmupIterations(1)
        .measurementTime(TimeValue.seconds(30))
        .measurementIterations(3)
        .forks(1);

    new Runner(opt.build()).run();
  }
}
