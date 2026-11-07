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

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.segment.spi.memory.PagedPinotOutputStream;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/// Compares ways to write a primitive array to the [PagedPinotOutputStream] that data blocks and data tables use:
/// - `perValue`: one writeInt/writeLong/writeFloat/writeDouble call per value.
/// - `bulk`: the bulk write, which puts the values straight into the pages.
///
/// Each operation writes `size` values into a new stream with small heap pages.
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(3)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Benchmark)
public class BenchmarkPagedPinotOutputStream {
  @Param({"INT", "LONG", "FLOAT", "DOUBLE"})
  private String _type;

  @Param({"1", "4", "16", "64", "1024", "65536", "1048576"})
  private int _size;

  private int[] _ints;
  private long[] _longs;
  private float[] _floats;
  private double[] _doubles;

  @Setup
  public void setUp() {
    Random random = new Random(42);
    _ints = random.ints(_size).toArray();
    _longs = random.longs(_size).toArray();
    _floats = new float[_size];
    for (int i = 0; i < _size; i++) {
      _floats[i] = random.nextFloat();
    }
    _doubles = random.doubles(_size).toArray();
  }

  @Benchmark
  public PagedPinotOutputStream perValue()
      throws IOException {
    PagedPinotOutputStream output = PagedPinotOutputStream.createHeap();
    switch (_type) {
      case "INT":
        for (int value : _ints) {
          output.writeInt(value);
        }
        break;
      case "LONG":
        for (long value : _longs) {
          output.writeLong(value);
        }
        break;
      case "FLOAT":
        for (float value : _floats) {
          output.writeFloat(value);
        }
        break;
      default:
        for (double value : _doubles) {
          output.writeDouble(value);
        }
        break;
    }
    return output;
  }

  @Benchmark
  public PagedPinotOutputStream bulk()
      throws IOException {
    PagedPinotOutputStream output = PagedPinotOutputStream.createHeap();
    switch (_type) {
      case "INT":
        output.writeInts(_ints, 0, _size);
        break;
      case "LONG":
        output.writeLongs(_longs, 0, _size);
        break;
      case "FLOAT":
        output.writeFloats(_floats, 0, _size);
        break;
      default:
        output.writeDoubles(_doubles, 0, _size);
        break;
    }
    return output;
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder().include(BenchmarkPagedPinotOutputStream.class.getSimpleName()).build()).run();
  }
}
