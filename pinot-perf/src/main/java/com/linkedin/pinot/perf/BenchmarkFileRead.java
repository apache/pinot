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

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.io.reader.impl.v1.FixedBitSingleValueReader;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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
public class BenchmarkFileRead {
  File file;

  RandomAccessFile raf;

  ByteBuffer byteBuffer;

  int length;

  @Setup
  public void loadData() {
    try {
      file = new File("/Users/jfim/index_dir/sTest_OFFLINE/sTest_0_0/daysSinceEpoch.sv.unsorted.fwd");
      raf = new RandomAccessFile(file, "rw");
      length = (int) file.length();
      byteBuffer = ByteBuffer.allocate(length);
      raf.getChannel().read(byteBuffer);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /*@Benchmark
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void test() {
    byteBuffer.rewind();
    byte[] rawData = new byte[length];

    byteBuffer.rewind();
    for (int i = 0; i < length; i++) {
      rawData[i] = byteBuffer.get();
    }
  }*/

  @Benchmark
  @BenchmarkMode({Mode.SampleTime})
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void readSVs() throws IOException {
    int rows = 25000000;
    int columnSizeInBits = 3;
    boolean isMMap = true;
    boolean hasNulls = false;
    PinotDataBuffer dataBuffer = PinotDataBuffer.fromFile(file, ReadMode.mmap, FileChannel.MapMode.READ_ONLY, "benchmark");
    FixedBitSingleValueReader reader =
        new FixedBitSingleValueReader(dataBuffer, rows, columnSizeInBits, hasNulls);
    int[] result2 = new int[rows];
    for (int i = 0; i < rows; i++) {
      result2[i] = reader.getInt(i);
    }
  }

/*  @Benchmark
  @BenchmarkMode({Mode.SampleTime})
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void readUnpacks() {
    int rows = 25000000;
    int columnSizeInBits = 3;
    boolean isMMap = false;
    boolean hasNulls = false;

    int output[] = new int[rows];
    final int outputBytes = MathUtils.lcm(32, columnSizeInBits) / columnSizeInBits;
    final int inputBytes = MathUtils.lcm(32, columnSizeInBits) / 32;
    int destPos = 0;
    int inPos = 0;
    byteBuffer.rewind();
    int[] input = new int[length / 4];
    byteBuffer.asIntBuffer().get(input);
    for (int i = 0; i < (length / 4) / inputBytes; i++) {
      BitPacking.fastunpack(input, inPos, output, destPos, columnSizeInBits);
      destPos += outputBytes;
      inPos += inputBytes;
    }
  }*/

  public static void main(String[] args) throws Exception {
    Options opt = new OptionsBuilder()
        .include(BenchmarkFileRead.class.getSimpleName())
        .forks(1)
        // .addProfiler(StackProfiler.class)
        .build();

    new Runner(opt).run();
  }
}
