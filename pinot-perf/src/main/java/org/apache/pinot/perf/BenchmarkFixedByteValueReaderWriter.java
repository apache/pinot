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
import java.nio.ByteOrder;
import java.util.SplittableRandom;
import org.apache.pinot.segment.local.io.util.FixedByteValueReaderWriter;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;


@State(Scope.Benchmark)
public class BenchmarkFixedByteValueReaderWriter {

  @Param({"8", "32", "1024", "65536"})
  int _length;

  @Param("4096")
  int _values;

  @Param({"true", "false"})
  boolean _nativeOrder;

  @Param("42")
  int _paddingByte;

  private PinotDataBuffer _dataBuffer;
  private FixedByteValueReaderWriter _writer;
  byte[] _buffer;

  @Setup(Level.Trial)
  public void setup() {
    _dataBuffer = PinotDataBuffer.allocateDirect(_values * _length,
        _nativeOrder ? ByteOrder.nativeOrder() : ByteOrder.BIG_ENDIAN, "");
    _writer = new FixedByteValueReaderWriter(_dataBuffer);
    SplittableRandom random = new SplittableRandom(42);
    byte[] bytes = new byte[_length];
    for (int i = 0; i < _values; i++) {
      int index = random.nextInt(bytes.length);
      bytes[index] = (byte) _paddingByte;
      _writer.writeBytes(i, _length, bytes);
      bytes[index] = 0;
    }
    _buffer = bytes;
  }

  @TearDown(Level.Trial)
  public void cleanup()
      throws IOException {
    _dataBuffer.close();
  }

  @Benchmark
  public void readStrings(Blackhole bh) {
    for (int i = 0; i < _values; i++) {
      bh.consume(_writer.getUnpaddedString(i, _length, (byte) _paddingByte, _buffer));
    }
  }
}
