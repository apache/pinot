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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowArrowFileWriter;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileWriter;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.AsyncProfiler;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1, warmups = 0, jvmArgsAppend = {"-XX:+UnlockDiagnosticVMOptions", "-XX:+DebugNonSafepoints", "-Darrow"
    + ".enable_unsafe_memory_access=true"})
@Warmup(iterations = 0, time = 5)
@Measurement(iterations = 1, time = 5)
public class GenericRowFileWriterBenchmark {

  private GenericRowArrowFileWriter _arrowWriter;
  private GenericRowFileWriter _standardWriter;
  private List<GenericRow> _sampleData;
  private File _tempDirArrow;
  private File _tempDirStandard;

  @Setup
  public void setup() throws IOException {
    _tempDirArrow = Files.createTempDirectory("arrow-benchmark").toFile();
    _tempDirStandard = Files.createTempDirectory("standard-benchmark").toFile();
    Schema pinotSchema = createSampleSchema();
    Set<String> sortColumns = new HashSet<>(Arrays.asList("id", "timestamp"));

    _arrowWriter = new GenericRowArrowFileWriter(_tempDirArrow.getAbsolutePath(), pinotSchema, 100000, 2 * 1024 * 1024 * 1024L,
        sortColumns, GenericRowArrowFileWriter.ArrowCompressionType.NONE, null);

    File offsetFile = new File(_tempDirStandard, "offset.dat");
    File dataFile = new File(_tempDirStandard, "data.dat");
    _standardWriter =
        new GenericRowFileWriter(offsetFile, dataFile, new ArrayList<>(pinotSchema.getAllFieldSpecs()), true);

    _sampleData = generateSampleData(pinotSchema, 10000);
  }

  @TearDown
  public void tearDown() throws IOException {
    _arrowWriter.close();
    _standardWriter.close();
    deleteDirectory(_tempDirArrow);
    deleteDirectory(_tempDirStandard);
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  public void benchmarkArrowGenericRowToArrowConversion(Blackhole blackhole) throws IOException {
    for (GenericRow row : _sampleData) {
      _arrowWriter.write(row);
    }
    blackhole.consume(_arrowWriter);
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  public void benchmarkStandardGenericRowConversion(Blackhole blackhole) throws IOException {
    for (GenericRow row : _sampleData) {
      _standardWriter.write(row);
    }
    blackhole.consume(_standardWriter);
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(GenericRowFileWriterBenchmark.class.getSimpleName())
        .addProfiler(AsyncProfiler.class, "event=wall;output=flamegraph")
        .build();

    Collection<RunResult> results = new Runner(opt).run();
  }

  private Schema createSampleSchema() {
    Schema schema = new Schema();
    schema.addField(new MetricFieldSpec("id", FieldSpec.DataType.INT, 0));
    schema.addField(new DimensionFieldSpec("timestamp", FieldSpec.DataType.LONG, true));
    schema.addField(new DimensionFieldSpec("name", FieldSpec.DataType.STRING, true));
    schema.addField(new DimensionFieldSpec("score", FieldSpec.DataType.DOUBLE, true));
    schema.addField(new DimensionFieldSpec("tags", FieldSpec.DataType.STRING, false));
    return schema;
  }

  private List<GenericRow> generateSampleData(Schema schema, int numRows) {
    List<GenericRow> data = new ArrayList<>(numRows);
    Random random = new Random(42);

    for (int i = 0; i < numRows; i++) {
      GenericRow row = new GenericRow();
      row.putValue("id", random.nextInt(1000000));
      row.putValue("timestamp", System.currentTimeMillis());
      row.putValue("name", "User" + random.nextInt(1000));
      row.putValue("score", random.nextDouble() * 100);
      row.putValue("tags", new String[]{"tag" + random.nextInt(10), "tag" + random.nextInt(10)});
      data.add(row);
    }

    return data;
  }

  private void deleteDirectory(File dir) {
    File[] files = dir.listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.isDirectory()) {
          deleteDirectory(file);
        } else {
          file.delete();
        }
      }
    }
    dir.delete();
  }
}
