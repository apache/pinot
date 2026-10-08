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

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.core.common.ObjectSerDeUtils;
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


/// Compares reading a serialized primitive array list one value at a time (`perValue`, the loop the list SerDes used
/// before) with the bulk read of the ObjectSerDeUtils list SerDes (`bulk`), from a heap or a direct buffer.
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(3)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Benchmark)
public class BenchmarkPrimitiveListDeserialize {
  @Param({"INT", "LONG", "DOUBLE"})
  private String _type;

  @Param({"65536", "1048576"})
  private int _size;

  @Param({"HEAP", "DIRECT"})
  private String _buffer;

  private ByteBuffer _serialized;

  @Setup
  public void setUp() {
    Random random = new Random(42);
    byte[] bytes;
    switch (_type) {
      case "INT":
        bytes = ObjectSerDeUtils.INT_ARRAY_LIST_SER_DE.serialize(new IntArrayList(random.ints(_size).toArray()));
        break;
      case "LONG":
        bytes = ObjectSerDeUtils.LONG_ARRAY_LIST_SER_DE.serialize(new LongArrayList(random.longs(_size).toArray()));
        break;
      default:
        bytes = ObjectSerDeUtils.DOUBLE_ARRAY_LIST_SER_DE.serialize(
            new DoubleArrayList(random.doubles(_size).toArray()));
        break;
    }
    _serialized = "HEAP".equals(_buffer) ? ByteBuffer.allocate(bytes.length) : ByteBuffer.allocateDirect(bytes.length);
    _serialized.put(bytes).flip();
  }

  @Benchmark
  public Object perValue() {
    ByteBuffer buffer = _serialized.duplicate();
    int numValues = buffer.getInt();
    switch (_type) {
      case "INT": {
        IntArrayList list = new IntArrayList(numValues);
        for (int i = 0; i < numValues; i++) {
          list.add(buffer.getInt());
        }
        return list;
      }
      case "LONG": {
        LongArrayList list = new LongArrayList(numValues);
        for (int i = 0; i < numValues; i++) {
          list.add(buffer.getLong());
        }
        return list;
      }
      default: {
        DoubleArrayList list = new DoubleArrayList(numValues);
        for (int i = 0; i < numValues; i++) {
          list.add(buffer.getDouble());
        }
        return list;
      }
    }
  }

  @Benchmark
  public Object bulk() {
    ByteBuffer buffer = _serialized.duplicate();
    switch (_type) {
      case "INT":
        return ObjectSerDeUtils.INT_ARRAY_LIST_SER_DE.deserialize(buffer);
      case "LONG":
        return ObjectSerDeUtils.LONG_ARRAY_LIST_SER_DE.deserialize(buffer);
      default:
        return ObjectSerDeUtils.DOUBLE_ARRAY_LIST_SER_DE.deserialize(buffer);
    }
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder().include(BenchmarkPrimitiveListDeserialize.class.getSimpleName()).build()).run();
  }
}
