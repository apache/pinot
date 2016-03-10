/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.perf;

import com.linkedin.pinot.core.io.writer.impl.FixedBitSingleValueMultiColWriter;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.File;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@State(Scope.Benchmark)
public class BenchmarkFileWrite {
  File file;

  PinotDataBuffer byteBuffer;

  public static final int ROWS = 2500000;
  public static final int COLUMN_SIZE_IN_BITS = 7;

  @Setup
  public void loadData() {
    byteBuffer = PinotDataBuffer.allocateDirect(ROWS * COLUMN_SIZE_IN_BITS / 8 + 1);
  }

  @Benchmark
  @BenchmarkMode({Mode.SampleTime})
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void writeSVs() throws Exception {
    FixedBitSingleValueMultiColWriter writer =
        new FixedBitSingleValueMultiColWriter(byteBuffer, ROWS, 1, new int[]{ COLUMN_SIZE_IN_BITS });
    for(int i = 0; i < ROWS; ++i) {
      writer.setInt(i, 0, i);
    }
  }

  public static void main(String[] args) throws Exception {
    Options opt = new OptionsBuilder()
        .include(BenchmarkFileWrite.class.getSimpleName())
        .forks(1)
        .build();

    new Runner(opt).run();
  }
}
