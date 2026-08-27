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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.utils.VariantUtils;
import org.apache.pinot.spi.utils.VariantEnvelope;
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
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/// Measures the cross-row behavior of the cursor navigation memo: rows sharing one metadata dictionary (the
/// common segment case), rows with rotating metadata (worst case), and misses classified against the dictionary
/// versus absent from one object. Complements [BenchmarkVariantGet], which measures single-lookup scaling.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@State(Scope.Benchmark)
public class BenchmarkVariantGetMemo {
  private static final int NUM_ROWS = 1024;

  private byte[][] _narrow5;
  private byte[][] _small4;
  private byte[][] _heterogeneous;
  private byte[][] _smallObjectLargeDictionary;
  private byte[][] _wide100;
  private byte[][] _wide1000;
  private byte[][] _wide100Rotating;
  private byte[][] _nested;

  private VariantUtils.VariantPath _narrowPath;
  private VariantUtils.VariantPath _widePathMid;
  private VariantUtils.VariantPath _wide1000PathMid;
  private VariantUtils.VariantPath _missDictPath;
  private VariantUtils.VariantPath _missObjectPath;
  private VariantUtils.VariantPath _nestedPath;

  private VariantUtils.VariantPath _small4Path;

  private VariantUtils.ReusableResult _result;

  private static String jsonRow(List<String> keys, long seed) {
    StringBuilder sb = new StringBuilder("{");
    // One nested sibling object holds "shadow" so the key is in the metadata dictionary but absent at top level.
    sb.append("\"nestedSibling\":{\"shadow\":").append(seed).append("},");
    for (int i = 0; i < keys.size(); i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append('"').append(keys.get(i)).append("\":").append(seed + i);
    }
    return sb.append('}').toString();
  }

  private static byte[][] buildRows(int numKeys, boolean rotate) {
    List<String> keys = new ArrayList<>(numKeys);
    for (int i = 0; i < numKeys; i++) {
      keys.add("field_" + String.format("%05d", i));
    }
    Random random = new Random(42);
    byte[][] rows = new byte[NUM_ROWS][];
    for (int r = 0; r < NUM_ROWS; r++) {
      List<String> order = new ArrayList<>(keys);
      if (rotate) {
        Collections.shuffle(order, random);
      }
      rows[r] = VariantUtils.parseJsonToVariant(jsonRow(order, r));
    }
    return rows;
  }

  private static void assertSharedMetadata(byte[][] rows, String label) {
    int len0 = VariantEnvelope.validateAndGetMetadataLength(rows[0]);
    for (int r = 1; r < rows.length; r++) {
      int len = VariantEnvelope.validateAndGetMetadataLength(rows[r]);
      if (len != len0 || !Arrays.equals(rows[0], VariantEnvelope.HEADER_SIZE, VariantEnvelope.HEADER_SIZE + len0,
          rows[r], VariantEnvelope.HEADER_SIZE, VariantEnvelope.HEADER_SIZE + len)) {
        throw new IllegalStateException(label + ": rows do not share metadata; benchmark premise broken at row " + r);
      }
    }
  }

  @Setup
  public void setUp() {
    _narrow5 = buildRows(5, false);
    _wide100 = buildRows(100, false);
    _wide1000 = buildRows(1000, false);
    _wide100Rotating = buildRows(100, true);
    assertSharedMetadata(_narrow5, "narrow5");
    assertSharedMetadata(_wide100, "wide100");
    assertSharedMetadata(_wide1000, "wide1000");

    _nested = new byte[NUM_ROWS][];
    for (int r = 0; r < NUM_ROWS; r++) {
      _nested[r] = VariantUtils.parseJsonToVariant(
          "{\"a\":{\"b\":{\"c\":" + r + ",\"pad\":1}},\"x\":2,\"y\":3}");
    }
    assertSharedMetadata(_nested, "nested");

    // At or below the small-object threshold: the memo is bypassed entirely.
    _small4 = new byte[NUM_ROWS][];
    for (int r = 0; r < NUM_ROWS; r++) {
      _small4[r] = VariantUtils.parseJsonToVariant("{\"a\":" + r + ",\"b\":1,\"c\":2,\"d\":3}");
    }
    // Heterogeneous per-row dictionaries with the probed key absent: the stability gate must keep the pre-memo
    // miss cost instead of scanning every row's dictionary.
    _heterogeneous = new byte[NUM_ROWS][];
    for (int r = 0; r < NUM_ROWS; r++) {
      StringBuilder sb = new StringBuilder("{\"nested\":{");
      for (int k = 0; k < 100; k++) {
        sb.append(k > 0 ? "," : "").append("\"row").append(r).append("key").append(k).append("\":").append(k);
      }
      sb.append("},\"e0\":0,\"e1\":1,\"e2\":2,\"e3\":3,\"e4\":4,\"e5\":5,\"e6\":6}");
      _heterogeneous[r] = VariantUtils.parseJsonToVariant(sb.toString());
    }
    // Small probed object over a much larger shared dictionary, probed key absent from the dictionary.
    _smallObjectLargeDictionary = new byte[NUM_ROWS][];
    for (int r = 0; r < NUM_ROWS; r++) {
      StringBuilder sb = new StringBuilder("{\"nested\":{");
      for (int k = 0; k < 400; k++) {
        sb.append(k > 0 ? "," : "").append("\"sharedKey").append(k).append("\":").append(r);
      }
      sb.append("},\"e0\":0,\"e1\":1,\"e2\":2,\"e3\":3,\"e4\":4,\"e5\":5,\"e6\":6}");
      _smallObjectLargeDictionary[r] = VariantUtils.parseJsonToVariant(sb.toString());
    }
    assertSharedMetadata(_smallObjectLargeDictionary, "smallObjectLargeDictionary");

    _narrowPath = VariantUtils.compilePath("$.field_00002");
    _small4Path = VariantUtils.compilePath("$.c");
    _widePathMid = VariantUtils.compilePath("$.field_00066");
    _wide1000PathMid = VariantUtils.compilePath("$.field_00666");
    _missDictPath = VariantUtils.compilePath("$.absent_everywhere");
    _missObjectPath = VariantUtils.compilePath("$.shadow");
    _nestedPath = VariantUtils.compilePath("$.a.b.c");
    _result = new VariantUtils.ReusableResult();
  }

  private void run(Blackhole bh, byte[][] rows, VariantUtils.VariantPath path) {
    for (byte[] row : rows) {
      if (VariantUtils.extractInto(row, path, VariantUtils.ResultType.LONG, _result)) {
        bh.consume(_result.getLongValue());
      } else {
        bh.consume(false);
      }
    }
  }

  @Benchmark
  public void narrow5Hit(Blackhole bh) {
    run(bh, _narrow5, _narrowPath);
  }

  @Benchmark
  public void wide100Hit(Blackhole bh) {
    run(bh, _wide100, _widePathMid);
  }

  @Benchmark
  public void wide1000Hit(Blackhole bh) {
    run(bh, _wide1000, _wide1000PathMid);
  }

  @Benchmark
  public void wide100MissFromDictionary(Blackhole bh) {
    run(bh, _wide100, _missDictPath);
  }

  @Benchmark
  public void wide100MissFromObject(Blackhole bh) {
    run(bh, _wide100, _missObjectPath);
  }

  @Benchmark
  public void nestedHit(Blackhole bh) {
    run(bh, _nested, _nestedPath);
  }

  @Benchmark
  public void wide100RotatingMetadataHit(Blackhole bh) {
    run(bh, _wide100Rotating, _widePathMid);
  }

  @Benchmark
  public void small4Hit(Blackhole bh) {
    run(bh, _small4, _small4Path);
  }

  @Benchmark
  public void heterogeneousMetadataMiss(Blackhole bh) {
    run(bh, _heterogeneous, _missDictPath);
  }

  @Benchmark
  public void smallObjectLargeDictionaryMiss(Blackhole bh) {
    run(bh, _smallObjectLargeDictionary, _missDictPath);
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder()
        .include(BenchmarkVariantGetMemo.class.getSimpleName())
        .addProfiler(GCProfiler.class)
        .build()).run();
  }
}
