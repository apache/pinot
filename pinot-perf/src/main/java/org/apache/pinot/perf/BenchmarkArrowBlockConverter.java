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
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.runtime.blocks.ArrowBlock;
import org.apache.pinot.query.runtime.blocks.ArrowBlockConverter;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/**
 * Benchmarks {@link ArrowBlockConverter#toArrowBlock} for the fixed-width numeric column writers (INT, LONG,
 * FLOAT, DOUBLE), which write the Arrow value buffer directly in one branch-free sweep and set validity in
 * bulk rather than going through the per-element {@code FixedWidthVector.set} API.
 *
 * <p>The direct value-buffer sweep only autovectorizes when Arrow's per-access bounds check is disabled, so
 * run this twice to see the effect:
 * <ul>
 *   <li>default (bounds checking on)</li>
 *   <li>{@code -Darrow.enable_unsafe_memory_access=true} (bounds checking off — the production posture this
 *       optimization targets)</li>
 * </ul>
 */
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsPrepend = {
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED"
})
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkArrowBlockConverter {

  @Param({"10000", "100000"})
  int _rows;

  @Param({"0", "10"})
  int _nullPercent;

  private DataBlock _dataBlock;
  private DataSchema _schema;
  private BufferAllocator _allocator;

  @Setup(Level.Trial)
  public void setup() {
    _schema = new DataSchema(new String[]{"i", "l", "f", "d"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.FLOAT, ColumnDataType.DOUBLE});
    Random random = new Random(42);
    List<Object[]> rows = new ArrayList<>(_rows);
    for (int row = 0; row < _rows; row++) {
      if (random.nextInt(100) < _nullPercent) {
        rows.add(new Object[]{null, null, null, null});
      } else {
        rows.add(new Object[]{random.nextInt(), random.nextLong(), random.nextFloat(), random.nextDouble()});
      }
    }
    // Serialize to the columnar DataBlock once, up front, so the benchmark measures only fromDataBlock (the
    // vector-writing path the direct-buffer writers changed) and not the row->columnar serialization that
    // toArrowBlock would also pay on every call.
    _dataBlock = new RowHeapDataBlock(rows, _schema).asSerialized().getDataBlock();
    _allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    _allocator.close();
  }

  @Benchmark
  public int convert() {
    ArrowBlock block = ArrowBlockConverter.fromDataBlock(_dataBlock, _schema, _allocator);
    // Read the row count before releasing (the off-heap buffers are freed by release()), and return it so the
    // conversion is not dead-code-eliminated.
    int numRows = block.getNumRows();
    block.release();
    return numRows;
  }

  public static void main(String[] args)
      throws RunnerException {
    new Runner(new OptionsBuilder()
        .include(BenchmarkArrowBlockConverter.class.getSimpleName())
        .build()).run();
  }
}
