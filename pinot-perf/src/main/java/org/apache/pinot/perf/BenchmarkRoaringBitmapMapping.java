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

import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.segment.spi.memory.PinotByteBuffer;
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
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringBitmapWriter;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Test optimal settings for transforming bitmap via mapping.
 *  Depends on following files:
 *  - docMapping.buffer (json flattened doc ids -> doc ids mapping)
 *  - test.bitmap (serialized mutable roaring bitmap)
 *  that have to be generated (copied from pinot instance) before benchmark run.
 *  */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Benchmark)
public class BenchmarkRoaringBitmapMapping {

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder()
        .shouldDoGC(true)
        .addProfiler(GCProfiler.class)
        //.addProfiler(JavaFlightRecorderProfiler.class)
        .include(BenchmarkRoaringBitmapMapping.class.getSimpleName());
    new Runner(opt.build()).run();
  }

  PinotDataBuffer _bitmapBuffer;
  ImmutableRoaringBitmap _docIds;
  PinotDataBuffer _docIdMapping;

  private int getDocId(int flattenedDocId) {
    return _docIdMapping.getInt((long) flattenedDocId << 2);
  }

  @Setup
  public void setUp()
      throws IOException {
    String fileName = "test.bitmap";

    _bitmapBuffer = getPinotDataBuffer(fileName);
    _docIds = new ImmutableRoaringBitmap(
        _bitmapBuffer.toDirectByteBuffer(0, (int) _bitmapBuffer.size()));
    _docIdMapping = getPinotDataBuffer("docMapping.buffer");
  }

  private static PinotDataBuffer getPinotDataBuffer(String fileName)
      throws IOException {
    URL bitmapUrl = Resources.getResource(fileName);
    File file = new File(bitmapUrl.getFile());
    if (!file.exists()) {
      throw new RuntimeException("File test.bitmap doesn't exist!");
    }
    return PinotByteBuffer.mapReadOnlyBigEndianFile(file);
  }

  @TearDown
  public void tearDown()
      throws IOException {
    if (_bitmapBuffer != null) {
      try {
        _bitmapBuffer.close();
      } catch (Exception e) {
        // Ignore
      }
    }

    if (_docIdMapping != null) {
      try {
        _docIdMapping.close();
      } catch (Exception e) {
        // Ignore
      }
    }
  }

  @Benchmark
  public MutableRoaringBitmap mapWithDefaults() {
    RoaringBitmapWriter<MutableRoaringBitmap> writer = RoaringBitmapWriter.bufferWriter()
        .get();
    return map(writer);
  }

  @Benchmark
  public MutableRoaringBitmap mapWithInitCapacity() {
    RoaringBitmapWriter<MutableRoaringBitmap> writer = RoaringBitmapWriter.bufferWriter()
        .initialCapacity((_docIds.getCardinality() >>> 16) + 1)
        .get();
    return map(writer);
  }

  @Benchmark
  public MutableRoaringBitmap mapWithMaxInitCapacity() {
    RoaringBitmapWriter<MutableRoaringBitmap> writer = RoaringBitmapWriter.bufferWriter()
        .initialCapacity(65534)
        .get();
    return map(writer);
  }

  @Benchmark
  public MutableRoaringBitmap mapWithRunCompressDisabled() {
    RoaringBitmapWriter<MutableRoaringBitmap> writer = RoaringBitmapWriter.bufferWriter()
        .runCompress(false)
        .get();
    return map(writer);
  }

  @Benchmark
  public MutableRoaringBitmap mapWithPartialRadixSort() {
    RoaringBitmapWriter<MutableRoaringBitmap> writer = RoaringBitmapWriter.bufferWriter()
        .doPartialRadixSort()
        .get();

    int[] buffer = new int[1024];

    IntConsumer consumer = new IntConsumer() {
      int _idx = 0;

      @Override
      public void accept(int value) {
        buffer[_idx++] = getDocId(value);
        if (_idx == 1024) {
          writer.addMany(buffer);
          _idx = 0;
        }
      }
    };
    _docIds.forEach(consumer);

    // ignore small leftover

    return writer.get();
  }

  @Benchmark
  public MutableRoaringBitmap mapWithPartialRadixSortPrealloc() {
    RoaringBitmapWriter<MutableRoaringBitmap> writer = RoaringBitmapWriter.bufferWriter()
        .get();

    final int[] buffer = new int[10 * 1024];
    final int bufLen = buffer.length;

    IntConsumer consumer = new IntConsumer() {
      int _idx = 0;
      final int[] _low = new int[257];
      final int[] _high = new int[257];
      int[] _copy = new int[buffer.length];

      @Override
      public void accept(int value) {
        buffer[_idx++] = getDocId(value);
        if (_idx == bufLen) {
          partialRadixSort(buffer, _low, _high, _copy);
          writer.addMany(buffer);
          _idx = 0;
        }
      }
    };
    _docIds.forEach(consumer);

    // ignore small leftover

    return writer.get();
  }

  @Benchmark
  public MutableRoaringBitmap mapWithOptimisedForRunsAppender() {
    RoaringBitmapWriter<MutableRoaringBitmap> writer = RoaringBitmapWriter.bufferWriter()
        .optimiseForRuns()
        .get();
    return map(writer);
  }

  @Benchmark
  public MutableRoaringBitmap mapWithOptimisedForArraysAppender() {
    RoaringBitmapWriter<MutableRoaringBitmap> writer = RoaringBitmapWriter.bufferWriter()
        .optimiseForArrays()
        .get();
    return map(writer);
  }

  @Benchmark
  public MutableRoaringBitmap mapSimple() {
    MutableRoaringBitmap target = new MutableRoaringBitmap();
    _docIds.forEach((IntConsumer) flattenedDocId -> target.add(getDocId(flattenedDocId)));
    return target;
  }

  @Benchmark
  public RoaringBitmap mapRoaringSimple() {
    RoaringBitmap target = new RoaringBitmap();
    _docIds.forEach((IntConsumer) flattenedDocId -> target.add(getDocId(flattenedDocId)));
    return target;
  }

  @Benchmark
  public RoaringBitmap mapRoaringAppender() {
    RoaringBitmapWriter<RoaringBitmap> writer = RoaringBitmapWriter.writer()
        .get();
    _docIds.forEach((IntConsumer) flattenedDocId -> writer.add(getDocId(flattenedDocId)));
    RoaringBitmap result = writer.get();
    return result;
  }

  @Benchmark
  public RoaringBitmap mapRoaringAppenderConstantMem() {
    RoaringBitmapWriter<RoaringBitmap> writer = RoaringBitmapWriter.writer()
        .constantMemory()
        .get();
    _docIds.forEach((IntConsumer) flattenedDocId -> writer.add(getDocId(flattenedDocId)));
    return writer.get();
  }

  @Benchmark
  public long iterateMapping() {
    long result = 0;
    for (int i = 0, n = (int) _docIdMapping.size() / 8; i < n; i++) {
      result += _docIdMapping.getLong(i);
    }
    return result;
  }

  private MutableRoaringBitmap map(RoaringBitmapWriter<MutableRoaringBitmap> writer) {
    _docIds.forEach((IntConsumer) flattenedDocId -> writer.add(getDocId(flattenedDocId)));
    return writer.get();
  }

  // same as partialRadixSort in RB, but with arrays pre-allocated
  private static void partialRadixSort(int[] data, int[] low, int[] high, int[] copy) {
    Arrays.fill(low, 0);
    Arrays.fill(high, 0);
    for (int value : data) {
      ++low[((value >>> 16) & 0xFF) + 1];
      ++high[(value >>> 24) + 1];
    }
    // avoid passes over the data if it's not required
    boolean sortLow = low[1] < data.length;
    boolean sortHigh = high[1] < data.length;
    if (!sortLow && !sortHigh) {
      return;
    }
    Arrays.fill(copy, 0);
    if (sortLow) {
      for (int i = 1; i < low.length; i++) {
        low[i] += low[i - 1];
      }
      for (int value : data) {
        copy[low[(value >>> 16) & 0xFF]++] = value;
      }
    }
    if (sortHigh) {
      for (int i = 1; i < high.length; i++) {
        high[i] += high[i - 1];
      }
      if (sortLow) {
        for (int value : copy) {
          data[high[value >>> 24]++] = value;
        }
      } else {
        for (int value : data) {
          copy[high[value >>> 24]++] = value;
        }
        System.arraycopy(copy, 0, data, 0, data.length);
      }
    } else {
      System.arraycopy(copy, 0, data, 0, data.length);
    }
  }
}
