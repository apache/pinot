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

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.segment.spi.memory.PagedPinotOutputStream;
import org.apache.pinot.segment.spi.memory.PrimitiveArrayOutput;
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


/// Compares ways to write a primitive array to the outputs data blocks and data tables use:
/// - `*PerValue`: one writeInt/writeLong/writeDouble call per value.
/// - `*Chunked`: [PrimitiveArrayOutput] through a plain [DataOutput], which encodes small chunks into a scratch array.
/// - `pagedBulk`: the [PagedPinotOutputStream] override, which puts the values straight into the pages.
///
/// Each operation writes `size` values into a new output: [PagedPinotOutputStream] with small heap pages (as data
/// blocks use), or a [DataOutputStream] over a presized [ByteArrayOutputStream] (as data tables use).
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(3)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Benchmark)
public class BenchmarkPrimitiveArrayOutput {
  @Param({"INT", "LONG", "DOUBLE"})
  private String _type;

  @Param({"65536", "1048576"})
  private int _size;

  private int[] _ints;
  private long[] _longs;
  private double[] _doubles;
  private int _numBytes;

  @Setup
  public void setUp() {
    Random random = new Random(42);
    _ints = random.ints(_size).toArray();
    _longs = random.longs(_size).toArray();
    _doubles = random.doubles(_size).toArray();
    _numBytes = _size * ("INT".equals(_type) ? Integer.BYTES : Long.BYTES);
  }

  @Benchmark
  public PagedPinotOutputStream pagedPerValue()
      throws IOException {
    PagedPinotOutputStream output = PagedPinotOutputStream.createHeap();
    writePerValue(output);
    return output;
  }

  @Benchmark
  public PagedPinotOutputStream pagedChunked()
      throws IOException {
    PagedPinotOutputStream output = PagedPinotOutputStream.createHeap();
    // A DataOutputStream wrapper is not a PinotOutputStream, so PrimitiveArrayOutput uses the chunked encoding.
    writeDispatched(new DataOutputStream(output));
    return output;
  }

  @Benchmark
  public PagedPinotOutputStream pagedBulk()
      throws IOException {
    PagedPinotOutputStream output = PagedPinotOutputStream.createHeap();
    writeDispatched(output);
    return output;
  }

  @Benchmark
  public ByteArrayOutputStream dataOutputStreamPerValue()
      throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream(_numBytes);
    writePerValue(new DataOutputStream(bytes));
    return bytes;
  }

  @Benchmark
  public ByteArrayOutputStream dataOutputStreamChunked()
      throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream(_numBytes);
    writeDispatched(new DataOutputStream(bytes));
    return bytes;
  }

  private void writePerValue(DataOutput output)
      throws IOException {
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
      default:
        for (double value : _doubles) {
          output.writeDouble(value);
        }
        break;
    }
  }

  private void writeDispatched(DataOutput output)
      throws IOException {
    switch (_type) {
      case "INT":
        PrimitiveArrayOutput.writeInts(output, _ints, 0, _size);
        break;
      case "LONG":
        PrimitiveArrayOutput.writeLongs(output, _longs, 0, _size);
        break;
      default:
        PrimitiveArrayOutput.writeDoubles(output, _doubles, 0, _size);
        break;
    }
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder().include(BenchmarkPrimitiveArrayOutput.class.getSimpleName()).build()).run();
  }
}
