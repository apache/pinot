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

import java.util.concurrent.TimeUnit;
import org.apache.parquet.variant.Variant;
import org.apache.parquet.variant.VariantBuilder;
import org.apache.parquet.variant.VariantObjectBuilder;
import org.apache.pinot.common.utils.VariantUtils;
import org.apache.pinot.common.utils.VariantUtils.ResultType;
import org.apache.pinot.common.utils.VariantUtils.ReusableResult;
import org.apache.pinot.common.utils.VariantUtils.VariantPath;
import org.apache.pinot.spi.utils.VariantEnvelope;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/// Measures Pinot's reusable zero-copy Variant cursor against parquet-java object navigation.
///
/// The 31/32-field cases pin the shared linear-to-binary lookup threshold. First, middle, last, and missing fields
/// expose position-dependent scans; nested values include a second object lookup. Run with `-prof gc` to compare
/// allocation rates in addition to latency.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Thread)
public class BenchmarkVariantGet {

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder options = new OptionsBuilder().parent(new CommandLineOptions(args))
        .include(BenchmarkVariantGet.class.getSimpleName());
    new Runner(options.build()).run();
  }

  @Param({"8", "31", "32", "100"})
  private int _numFields;

  @Param({"first", "middle", "last", "missing"})
  private String _targetPosition;

  @Param({"flat", "nested"})
  private String _valueShape;

  private byte[] _envelope;
  private Variant _parquetVariant;
  private String _targetKey;
  private VariantPath _path;
  private ReusableResult _result;
  private boolean _nested;

  @Setup(Level.Trial)
  public void setUp() {
    _nested = "nested".equals(_valueShape);
    VariantBuilder builder = new VariantBuilder();
    VariantObjectBuilder objectBuilder = builder.startObject();
    for (int i = _numFields - 1; i >= 0; i--) {
      objectBuilder.appendKey(field(i));
      if (_nested) {
        VariantObjectBuilder nestedBuilder = objectBuilder.startObject();
        nestedBuilder.appendKey("value");
        nestedBuilder.appendInt(i);
        objectBuilder.endObject();
      } else {
        objectBuilder.appendInt(i);
      }
    }
    builder.endObject();
    _parquetVariant = builder.build();
    _envelope = VariantEnvelope.encode(_parquetVariant.getMetadataBuffer(), _parquetVariant.getValueBuffer());

    switch (_targetPosition) {
      case "first":
        _targetKey = field(0);
        break;
      case "middle":
        _targetKey = field(_numFields / 2);
        break;
      case "last":
        _targetKey = field(_numFields - 1);
        break;
      case "missing":
        _targetKey = "missing";
        break;
      default:
        throw new IllegalStateException("Unhandled target position: " + _targetPosition);
    }
    _path = VariantUtils.compilePath("$." + _targetKey + (_nested ? ".value" : ""));
    _result = new ReusableResult();
  }

  @Benchmark
  public int pinotReusableCursor() {
    return VariantUtils.extractInto(_envelope, _path, ResultType.INT, _result) ? _result.getIntValue() : -1;
  }

  @Benchmark
  public int parquetJava() {
    Variant value = _parquetVariant.getFieldByKey(_targetKey);
    if (value == null) {
      return -1;
    }
    if (_nested) {
      value = value.getFieldByKey("value");
    }
    return value != null ? value.getInt() : -1;
  }

  private static String field(int index) {
    return String.format("field%03d", index);
  }
}
