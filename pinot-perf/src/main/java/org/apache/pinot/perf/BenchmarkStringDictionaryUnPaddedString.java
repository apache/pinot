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
import java.io.FileOutputStream;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.local.io.util.FixedByteValueReaderWriter;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 10)
@Measurement(iterations = 5, time = 20)
@Fork(1)
@State(Scope.Benchmark)
public class BenchmarkStringDictionaryUnPaddedString {
  private static final Random RANDOM = new Random();
  private static final int MAX_DOC_PER_CALL = DocIdSetPlanNode.MAX_DOC_PER_CALL;
  private static final int BYTES_PER_VALUE = 36; // UUID string length
  private static final String TABLE_NAME = "tbl1";
  private static final String filePath = "/home/user/benchmarks/uuid.bin";
  private static final int[] dictIds = new int[MAX_DOC_PER_CALL];

  private PinotDataBuffer _dataBuffer;

  @Setup
  public void setUp()
      throws Exception {
    _dataBuffer = PinotDataBuffer.mapFile(new File(filePath), true, 0, 36000000, ByteOrder.BIG_ENDIAN, null);
    for (int i = 0; i < dictIds.length; i++) {
      dictIds[i] = RANDOM.nextInt(1_000_000);
    }
  }

  @TearDown
  public void tearDown()
      throws Exception {
    _dataBuffer.close();
  }

  public static void createUUIDFile() {
    final int bytesPerValue = 36;
    final int numUUIDs = 1_000_000;
    byte[] values = new byte[numUUIDs * bytesPerValue];
    for (int i = 0; i < numUUIDs; i++) {
      String uuid = UUID.randomUUID().toString();
      byte[] uuidBytes = uuid.getBytes(StandardCharsets.UTF_8);
      System.arraycopy(uuidBytes, 0, values, i * bytesPerValue, uuidBytes.length);
    }
    // Write values to a file: /home/user/benchmarks/uuid.bin. Don't use PinotDataBuffer.
    String filePath = "/home/user/benchmarks/uuid.bin";
    try (FileOutputStream fileOutputStream = new FileOutputStream(filePath)) {
      fileOutputStream.write(values);
    } catch (Exception e) {
      throw new RuntimeException("Failed to write UUIDs to file", e);
    }
  }

  @Benchmark
  public void benchmarkStringDictionaryUnPaddedString() {
    FixedByteValueReaderWriter reader = new FixedByteValueReaderWriter(_dataBuffer);
    byte[] uuidBytes = new byte[BYTES_PER_VALUE];
    String[] result = new String[MAX_DOC_PER_CALL];
    for (int index = 0; index < dictIds.length; index++) {
      int dictId = dictIds[index];
      reader.readUnpaddedBytes(dictId, BYTES_PER_VALUE, uuidBytes);
      result[index] = new String(uuidBytes, StandardCharsets.UTF_8);
      // result[index] = reader.getPaddedString(dictId, BYTES_PER_VALUE, uuidBytes);
    }
  }

  public static void main(String[] args)
      throws Exception {
    // createUUIDFile();
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkStringDictionaryUnPaddedString.class.getSimpleName());
    new Runner(opt.build()).run();
  }
}
