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

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;
import org.apache.parquet.variant.Variant;
import org.apache.parquet.variant.VariantBuilder;
import org.apache.pinot.common.utils.VariantUtils;
import org.apache.pinot.common.utils.VariantUtils.ResultType;
import org.apache.pinot.common.utils.VariantUtils.ReusableResult;
import org.apache.pinot.common.utils.VariantUtils.VariantPath;
import org.apache.pinot.spi.utils.VariantEnvelope;
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
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/// Measures primitive extraction directly from DECIMAL4/8/16 encodings. Run with the GC profiler to verify that
/// converting a reusable cursor into INT, LONG, FLOAT, or DOUBLE does not allocate per row.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Thread)
public class BenchmarkVariantDecimalGet {

  @Param({"DECIMAL4", "DECIMAL8", "DECIMAL16"})
  private String _encoding;

  private byte[] _floatingEnvelope;
  private byte[] _integralEnvelope;
  private VariantPath _path;
  private ReusableResult _result;

  @Setup
  public void setUp() {
    BigDecimal floatingValue;
    BigDecimal integralValue;
    switch (_encoding) {
      case "DECIMAL4":
        floatingValue = new BigDecimal("-0.029322138");
        integralValue = new BigDecimal("123.00");
        break;
      case "DECIMAL8":
        floatingValue = new BigDecimal("-0.0293221387768523759");
        integralValue = new BigDecimal("123.0000000000");
        break;
      case "DECIMAL16":
        floatingValue = new BigDecimal("-2276255542851026358.8820475617538817020");
        integralValue = new BigDecimal("123.0000000000000000000");
        break;
      default:
        throw new IllegalStateException("Unhandled decimal encoding: " + _encoding);
    }
    _floatingEnvelope = encode(floatingValue);
    _integralEnvelope = encode(integralValue);
    _path = VariantUtils.compilePath("$");
    _result = new ReusableResult();
  }

  private byte[] encode(BigDecimal value) {
    VariantBuilder builder = new VariantBuilder();
    builder.appendDecimal(value);
    Variant variant = builder.build();
    if (!variant.getType().name().equals(_encoding)) {
      throw new IllegalStateException("Expected " + _encoding + " but encoded " + variant.getType());
    }
    return VariantEnvelope.encode(variant.getMetadataBuffer(), variant.getValueBuffer());
  }

  @Benchmark
  public int extractInt() {
    VariantUtils.extractInto(_integralEnvelope, _path, ResultType.INT, _result);
    return _result.getIntValue();
  }

  @Benchmark
  public long extractLong() {
    VariantUtils.extractInto(_integralEnvelope, _path, ResultType.LONG, _result);
    return _result.getLongValue();
  }

  @Benchmark
  public float extractFloat() {
    VariantUtils.extractInto(_floatingEnvelope, _path, ResultType.FLOAT, _result);
    return _result.getFloatValue();
  }

  @Benchmark
  public double extractDouble() {
    VariantUtils.extractInto(_floatingEnvelope, _path, ResultType.DOUBLE, _result);
    return _result.getDoubleValue();
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder()
        .include(BenchmarkVariantDecimalGet.class.getSimpleName())
        .addProfiler(GCProfiler.class)
        .build()).run();
  }
}
