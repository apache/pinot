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

import java.util.Comparator;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.segment.spi.index.creator.JsonIndexCreator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.roaringbitmap.Container;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringBitmapWriter;


// simple test checking if  delaying key concatenation and checking map with stringbuilder is any faster than always
// concatenating
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(0)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 1)
@State(Scope.Benchmark)
public class BenchmarkJsonKeyMap {

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkJsonKeyMap.class.getSimpleName());
    new Runner(opt.build()).run();
  }

  private final Map<String, RoaringBitmapWriter<RoaringBitmap>> _csPostingListMap = new TreeMap<>(
      (Comparator<CharSequence>) (o1, o2) -> CharSequence.compare(o1, o2));

  private final TreeMap<String, RoaringBitmapWriter<RoaringBitmap>> _strPostingListMap = new TreeMap<>();
  private int _nextFlattenedDocId;
  final RoaringBitmapWriter.Wizard<Container, RoaringBitmap> _bitmapWriterWizard = RoaringBitmapWriter.writer();
  final String[] _keys = new String[1000];
  final String[] _values = new String[100];
  final int _iterations = 1000_000;

  @Benchmark
  public Map withConcat() {
    final Random rnd = new Random(0L);
    for (int i = 0; i < _iterations; i++) {
      String key = _keys[rnd.nextInt(_keys.length)];
      String value = _values[rnd.nextInt(_values.length)];
      addToStrPostingList(key);
      String keyAndValue = key + JsonIndexCreator.KEY_VALUE_SEPARATOR + value;
      addToStrPostingList(keyAndValue);
      _nextFlattenedDocId++;
    }
    _strPostingListMap.clear();
    return _strPostingListMap;
  }

  @Benchmark
  public Map withConcatOnBuffer() {
    final Random rnd = new Random(0L);
    StringBuilder buffer = new StringBuilder();

    for (int i = 0; i < _iterations; i++) {
      String key = _keys[rnd.nextInt(_keys.length)];
      String value = _values[rnd.nextInt(_values.length)];
      addToStrPostingList(key);
      buffer.setLength(0);
      String keyAndValue = buffer.append(key).append(JsonIndexCreator.KEY_VALUE_SEPARATOR).append(value).toString();
      addToStrPostingList(keyAndValue);
      _nextFlattenedDocId++;
    }
    _csPostingListMap.clear();
    return _csPostingListMap;
  }

  @Benchmark
  public Map withBuffer() {
    final Random rnd = new Random(0L);
    StringBuilder buffer = new StringBuilder();

    for (int i = 0; i < _iterations; i++) {
      String key = _keys[rnd.nextInt(_keys.length)];
      String value = _values[rnd.nextInt(_values.length)];
      addToCsPostingList(key);
      buffer.setLength(0);
      buffer.append(key).append(JsonIndexCreator.KEY_VALUE_SEPARATOR).append(value);
      addToCsPostingList(buffer);
      _nextFlattenedDocId++;
    }
    _csPostingListMap.clear();
    return _csPostingListMap;
  }

  @Setup
  public void setUp()
      throws Exception {
    final Random rnd = new Random(0L);
    StringBuilder sb = new StringBuilder(30);

    for (int i = 0; i < _keys.length; i++) {
      int len = rnd.nextInt(20) + 10;

      sb.setLength(0);
      for (int j = 0; j < len; j++) {
        sb.append((char) (rnd.nextInt('Z' - 'A') + 'A'));
      }

      _keys[i] = sb.toString();
    }

    for (int i = 0; i < _values.length; i++) {
      int len = rnd.nextInt(5) + 10;

      sb.setLength(0);
      for (int j = 0; j < len; j++) {
        sb.append((char) (rnd.nextInt('Z' - 'A') + 'A'));
      }

      _values[i] = sb.toString();
    }

    _strPostingListMap.clear();
    _csPostingListMap.clear();
    _nextFlattenedDocId = 0;
  }

  void addToStrPostingList(String value) {
    RoaringBitmapWriter<RoaringBitmap> bitmapWriter = _strPostingListMap.get(value);
    if (bitmapWriter == null) {
      bitmapWriter = _bitmapWriterWizard.get();
      _strPostingListMap.put(value, bitmapWriter);
    }
    bitmapWriter.add(_nextFlattenedDocId);
  }

  void addToCsPostingList(CharSequence value) {
    RoaringBitmapWriter<RoaringBitmap> bitmapWriter = _csPostingListMap.get(value);
    if (bitmapWriter == null) {
      bitmapWriter = _bitmapWriterWizard.get();
      _csPostingListMap.put(value.toString(), bitmapWriter);
    }
    bitmapWriter.add(_nextFlattenedDocId);
  }
}
