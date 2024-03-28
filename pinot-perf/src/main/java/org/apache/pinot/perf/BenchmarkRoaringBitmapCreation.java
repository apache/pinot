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
import java.lang.ref.SoftReference;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.segment.local.segment.creator.impl.inv.BitmapInvertedIndexWriter;
import org.apache.pinot.segment.spi.memory.PinotByteBuffer;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/**
 * Benchmark created to test the impact of removing the SoftReference array cache for ImmutableRoaringBitmap
 */
@State(Scope.Benchmark)
@Fork(1)
public class BenchmarkRoaringBitmapCreation {

  private static final int NUM_DOCS = 1_000_000;
  private static final int CARDINALITY = 100_000;
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), "bitmap_creation_benchmark_" + System.currentTimeMillis());

  @Param({"100", "10000", "99999"}) // higher this is, lesser the cache access
  public int _dictIdsToRead;

  private int _numBitmaps;
  private BitmapInvertedIndexWriter _bitmapInvertedIndexWriter;
  private SoftReference<SoftReference<ImmutableRoaringBitmap>[]> _bitmapsArrayReference = null;
  private SoftReference<SoftReference<Pair<Integer, Integer>>[]> _offsetLengthPairsArrayReference = null;
  private PinotDataBuffer _offsetLengthBuffer;
  private PinotDataBuffer _bitmapBuffer;
  private int _firstOffset;

  @Setup
  public void setup()
      throws IllegalAccessException, InstantiationException, IOException {
    _numBitmaps = CARDINALITY;

    File bufferDir = new File(TEMP_DIR, "cardinality_" + CARDINALITY);
    FileUtils.forceMkdir(bufferDir);
    File bufferFile = new File(bufferDir, "buffer");
    _bitmapInvertedIndexWriter = new BitmapInvertedIndexWriter(bufferFile, _numBitmaps);
    // Insert between 10-1000 values per bitmap
    for (int i = 0; i < _numBitmaps; i++) {
      int size = 10 + RandomUtils.nextInt(0, 990);
      int[] data = new int[size];
      for (int j = 0; j < size; j++) {
        // docIds will repeat across bitmaps, but doesn't matter for purpose of this benchmark
        data[j] = RandomUtils
            .nextInt(0, NUM_DOCS);
      }
      RoaringBitmap bitmap = RoaringBitmap.bitmapOf(data);
      _bitmapInvertedIndexWriter.add(bitmap);
    }

    PinotDataBuffer dataBuffer = PinotByteBuffer.mapReadOnlyBigEndianFile(bufferFile);
    long offsetBufferEndOffset = (long) (_numBitmaps + 1) * Integer.BYTES;
    _offsetLengthBuffer = dataBuffer.view(0, offsetBufferEndOffset, ByteOrder.BIG_ENDIAN);
    _bitmapBuffer = dataBuffer.view(offsetBufferEndOffset, dataBuffer.size());
    _firstOffset = _offsetLengthBuffer.getInt(0);
  }

  @TearDown
  public void teardown()
      throws IOException {
    _bitmapInvertedIndexWriter.close();
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public boolean cacheReferences() {
    int dictId = RandomUtils.nextInt(0, _dictIdsToRead);
    ImmutableRoaringBitmap roaringBitmapFromCache = getRoaringBitmapFromCache(dictId);
    return roaringBitmapFromCache.isEmpty();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public boolean alwaysBuild() {
    int dictId = RandomUtils.nextInt(0, _dictIdsToRead);
    ImmutableRoaringBitmap immutableRoaringBitmap = buildRoaringBitmap(dictId);
    return immutableRoaringBitmap.isEmpty();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public boolean alwaysBuildCachedOffsetAndLength() {
    int dictId = RandomUtils.nextInt(0, _dictIdsToRead);
    ImmutableRoaringBitmap immutableRoaringBitmap = buildRoaringBitmapUsingOffsetPairFromCache(dictId);
    return immutableRoaringBitmap.isEmpty();
  }

  /**
   * Code as of before this commit, using an array of SoftReference for the ImmutableRoaringBitmap
   */
  private ImmutableRoaringBitmap getRoaringBitmapFromCache(int dictId) {
    SoftReference<ImmutableRoaringBitmap>[] bitmapArrayReference =
        (_bitmapsArrayReference != null) ? _bitmapsArrayReference.get() : null;
    if (bitmapArrayReference != null) {
      SoftReference<ImmutableRoaringBitmap> bitmapReference = bitmapArrayReference[dictId];
      ImmutableRoaringBitmap bitmap = (bitmapReference != null) ? bitmapReference.get() : null;
      if (bitmap != null) {
        return bitmap;
      }
    } else {
      bitmapArrayReference = new SoftReference[_numBitmaps];
      _bitmapsArrayReference = new SoftReference<>(bitmapArrayReference);
    }
    synchronized (this) {
      SoftReference<ImmutableRoaringBitmap> bitmapReference = bitmapArrayReference[dictId];
      ImmutableRoaringBitmap bitmap = (bitmapReference != null) ? bitmapReference.get() : null;
      if (bitmap == null) {
        bitmap = buildRoaringBitmap(dictId);
        bitmapArrayReference[dictId] = new SoftReference<>(bitmap);
      }
      return bitmap;
    }
  }

  private ImmutableRoaringBitmap buildRoaringBitmap(int dictId) {
    Pair<Integer, Integer> offsetLengthPair = buildOffsetLengthPair(dictId);
    return buildRoaringBitmap(offsetLengthPair);
  }

  private Pair<Integer, Integer> buildOffsetLengthPair(int dictId) {
    int offset = _offsetLengthBuffer.getInt(dictId * Integer.BYTES);
    int length = _offsetLengthBuffer.getInt((dictId + 1) * Integer.BYTES) - offset;
    return Pair.of(offset, length);
  }

  private ImmutableRoaringBitmap buildRoaringBitmap(Pair<Integer, Integer> offsetLengthPair) {
    return new ImmutableRoaringBitmap(
        _bitmapBuffer.toDirectByteBuffer(offsetLengthPair.getLeft() - _firstOffset, offsetLengthPair.getRight()));
  }

  private ImmutableRoaringBitmap buildRoaringBitmapUsingOffsetPairFromCache(int dictId) {
    return buildRoaringBitmap(getOffsetLengthPairFromCache(dictId));
  }

  private Pair<Integer, Integer> getOffsetLengthPairFromCache(int dictId) {

    SoftReference<Pair<Integer, Integer>>[] offsetLengthPairArrayReference =
        (_offsetLengthPairsArrayReference != null) ? _offsetLengthPairsArrayReference.get() : null;
    if (offsetLengthPairArrayReference != null) {
      SoftReference<Pair<Integer, Integer>> offsetLengthPairReference = offsetLengthPairArrayReference[dictId];
      Pair<Integer, Integer> offsetLengthPair =
          (offsetLengthPairReference != null) ? offsetLengthPairReference.get() : null;
      if (offsetLengthPair != null) {
        return offsetLengthPair;
      }
    } else {
      offsetLengthPairArrayReference = new SoftReference[_numBitmaps];
      _offsetLengthPairsArrayReference = new SoftReference<>(offsetLengthPairArrayReference);
    }
    synchronized (this) {
      SoftReference<Pair<Integer, Integer>> offsetLengthPairReference = offsetLengthPairArrayReference[dictId];
      Pair<Integer, Integer> offsetLengthPair =
          (offsetLengthPairReference != null) ? offsetLengthPairReference.get() : null;
      if (offsetLengthPair == null) {
        offsetLengthPair = buildOffsetLengthPair(dictId);
        offsetLengthPairArrayReference[dictId] = new SoftReference<>(offsetLengthPair);
      }
      return offsetLengthPair;
    }
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkRoaringBitmapCreation.class.getSimpleName())
        .warmupTime(TimeValue.seconds(10)).warmupIterations(1).measurementTime(TimeValue.seconds(60))
        .measurementIterations(1).forks(1).addProfiler(GCProfiler.class);
    new Runner(opt.build()).run();
  }
}
