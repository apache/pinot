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

import com.google.common.base.Preconditions;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.google.common.primitives.Longs;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.pinot.segment.local.segment.index.readers.bloom.BaseGuavaBloomFilterReader;
import org.apache.pinot.segment.local.segment.index.readers.bloom.GuavaBloomFilterReaderUtils;
import org.apache.pinot.segment.local.segment.index.readers.bloom.OffHeapGuavaBloomFilterReader;
import org.apache.pinot.segment.local.segment.index.readers.bloom.OnHeapGuavaBloomFilterReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.JavaFlightRecorderProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 500, time = 10, timeUnit = TimeUnit.MICROSECONDS)
@Measurement(iterations = 5, time = 1)
@State(Scope.Benchmark)
public class BenchmarkBloomFilter {

  @Param(value = {OFF_HEAP})
  private String _reader;

  @Param(value = {"10000"})
  private int _cardinality;
  @Param(value = {"100", "1000"})
  private int _maxSizeInBytes;

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkBloomFilter.class.getSimpleName())
        .addProfiler(JavaFlightRecorderProfiler.class);
    new Runner(opt.build()).run();
  }

  private BaseGuavaBloomFilterReader _actualReader;
  private Supplier<String> _valueSupplier;
  private String _value;
  private long _valueLong1;
  private long _valueLong2;

  public static final String ON_HEAP = "onHeap";
  public static final String OFF_HEAP = "offHeap";

  private BaseGuavaBloomFilterReader loadReader(BloomFilter<CharSequence> bloomFilter)
      throws IOException {

    File file = Files.createTempFile("test", ".bloom").toFile();
    file.deleteOnExit();
    try (FileOutputStream fos = new FileOutputStream(file)) {
      bloomFilter.writeTo(fos);
    }

    PinotDataBuffer pinotDataBuffer = PinotDataBuffer.loadBigEndianFile(file);

    switch (_reader) {
      case ON_HEAP:
        return new OnHeapGuavaBloomFilterReader(pinotDataBuffer);
      case OFF_HEAP:
        return new OffHeapGuavaBloomFilterReader(pinotDataBuffer);
      default:
        throw new IllegalArgumentException("Value " + _reader + " not recognized");
    }
  }

  @Setup
  public void setUp()
      throws IOException {
    long seed = 0;
    Random r = new Random(seed);
    List<String> words =
        IntStream.generate(r::nextInt).limit(_cardinality).mapToObj(Integer::toString).collect(Collectors.toList());

    double fpp = Math.max(0.01d, GuavaBloomFilterReaderUtils.computeFPP(_maxSizeInBytes, _cardinality));
    BloomFilter<CharSequence> bloomFilter = BloomFilter.create(
        Funnels.stringFunnel(StandardCharsets.UTF_8), _cardinality, fpp);
    words.forEach(bloomFilter::put);

    _actualReader = loadReader(bloomFilter);

    _valueSupplier = () -> words.get(r.nextInt(_cardinality));
  }

  @Setup(Level.Iteration)
  public void updateIndex() {
    _value = _valueSupplier.get();

    byte[] hash = GuavaBloomFilterReaderUtils.hash(_value);
    _valueLong1 = Longs.fromBytes(hash[7], hash[6], hash[5], hash[4], hash[3], hash[2], hash[1], hash[0]);
    _valueLong2 = Longs.fromBytes(hash[15], hash[14], hash[13], hash[12], hash[11], hash[10], hash[9], hash[8]);
  }

  @Benchmark
  public boolean mightContainString() {
    boolean result = _actualReader.mightContain(_value);
    Preconditions.checkArgument(result);

    return result;
  }

  @Benchmark
  public boolean mightContainLongs() {
    boolean result = _actualReader.mightContain(_valueLong1, _valueLong2);
    Preconditions.checkArgument(result);

    return result;
  }
}
