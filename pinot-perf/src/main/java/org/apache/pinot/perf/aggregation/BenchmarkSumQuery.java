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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
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


@Fork(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 10, time = 1)
@State(Scope.Benchmark)
public class BenchmarkSumQuery extends AbstractAggregationQueryBenchmark {

  @Param({"false", "true"})
  public boolean _nullHandling;

  @Param({"1", "2", "4", "8", "16", "32", "64", "128"})
  protected int _nullPeriod;

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(BenchmarkSumQuery.class.getSimpleName())
        .build();

    new Runner(opt).run();
  }

  @Override
  protected Schema createSchema() {
    return new Schema.SchemaBuilder()
        .setSchemaName("benchmark")
        .addMetricField("col", FieldSpec.DataType.INT)
        .build();
  }

  @Override
  protected TableConfig createTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("benchmark")
        .setNullHandlingEnabled(true)
        .build();
  }

  @Override
  protected List<List<Object[][]>> createSegmentsPerServer() {
    Random valueRandom = new Random(420);
    List<List<Object[][]>> segmentsPerServer = new ArrayList<>();
    segmentsPerServer.add(new ArrayList<>());
    segmentsPerServer.add(new ArrayList<>());

    // 2 servers
    for (int server = 0; server < 2; server++) {
      List<Object[][]> segments = segmentsPerServer.get(server);
      // 3 segments per server
      for (int seg = 0; seg < 3; seg++) {
        // 10000 single column rows per segment
        Object[][] segment = new Object[10000][1];
        for (int row = 0; row < 10000; row++) {
          segment[row][0] = (row % _nullPeriod) == 0 ? null : valueRandom.nextInt();
        }
        segments.add(segment);
      }
    }

    return segmentsPerServer;
  }

  @Setup(Level.Trial)
  public void setup() throws IOException {
    init(_nullHandling);
  }

  @Benchmark
  public void test(Blackhole bh) {
    executeQuery("SELECT SUM(col) FROM mytable", bh);
  }
}
