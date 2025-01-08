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
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.table.ConcurrentIndexedTable;
import org.apache.pinot.core.data.table.IndexedTable;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.util.GroupByUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
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
@Fork(value = 1, jvmArgs = {"-server", "-Xmx8G", "-XX:MaxDirectMemorySize=16G"})
public class BenchmarkCombineGroupBy {
  private static final int NUM_SEGMENTS = 4;
  private static final int NUM_RECORDS_PER_SEGMENT = 100_000;
  private static final int CARDINALITY_D1 = 500;
  private static final int CARDINALITY_D2 = 500;
  private static final Random RANDOM = new Random();

  private QueryContext _queryContext;
  private DataSchema _dataSchema;

  private List<String> _d1;
  private List<Integer> _d2;

  private ExecutorService _executorService;

  @Setup
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
        "SELECT sum(m1), max(m2) FROM testTable GROUP BY d1, d2 ORDER BY sum(m1) LIMIT 500");
    _dataSchema = new DataSchema(new String[]{"d1", "d2", "sum(m1)", "max(m2)"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.DOUBLE,
        DataSchema.ColumnDataType.DOUBLE
    });

    _executorService = Executors.newFixedThreadPool(10);
  }

  @TearDown
  public void destroy() {
    _executorService.shutdown();
  }

  private Record getRecord() {
    Object[] columns = new Object[]{
        _d1.get(RANDOM.nextInt(_d1.size())), _d2.get(RANDOM.nextInt(_d2.size())), (double) RANDOM.nextInt(
        1000), (double) RANDOM.nextInt(1000)
    };
    return new Record(columns);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void concurrentIndexedTableForCombineGroupBy()
      throws InterruptedException, ExecutionException, TimeoutException {
    int trimSize = GroupByUtils.getTableCapacity(_queryContext.getLimit());

    // make 1 concurrent table
    IndexedTable concurrentIndexedTable =
        new ConcurrentIndexedTable(_dataSchema, false, _queryContext, trimSize, trimSize,
            InstancePlanMakerImplV2.DEFAULT_GROUPBY_TRIM_THRESHOLD,
            InstancePlanMakerImplV2.DEFAULT_MIN_INITIAL_INDEXED_TABLE_CAPACITY);

    List<Callable<Void>> innerSegmentCallables = new ArrayList<>(NUM_SEGMENTS);

    // 10 parallel threads putting 10k records into the table

    for (int i = 0; i < NUM_SEGMENTS; i++) {

      Callable<Void> callable = () -> {

        for (int r = 0; r < NUM_RECORDS_PER_SEGMENT; r++) {
          concurrentIndexedTable.upsert(getRecord());
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
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt =
        new OptionsBuilder().include(BenchmarkCombineGroupBy.class.getSimpleName()).warmupTime(TimeValue.seconds(10))
            .warmupIterations(1).measurementTime(TimeValue.seconds(30)).measurementIterations(3).forks(1);

    new Runner(opt.build()).run();
  }
}
