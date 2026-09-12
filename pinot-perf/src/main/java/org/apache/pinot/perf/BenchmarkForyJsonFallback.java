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
import org.apache.pinot.common.function.ForyJsonPathExtractor;
import org.apache.pinot.common.function.SimpleJsonPath;
import org.apache.pinot.common.function.scalar.JsonFunctions;
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


/// Compares normal, Fast, and Fory production scalar functions on values that require a reference-parser fallback
/// and on documents beyond Fory's former depth-20 limit. Run with multiple JMH threads to expose parser-pool
/// contention as well as per-row exception/fallback costs.
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@State(Scope.Thread)
public class BenchmarkForyJsonFallback {
  private static final String DEFAULT_VALUE = "DEFAULT";

  @Param({"object", "array", "deepSelected", "deepUnrelated"})
  private String _scenario;

  private String _json;
  private String _path;

  @Setup
  public void setUp() {
    if (!ForyJsonPathExtractor.isAvailable()) {
      throw new IllegalStateException("Fory JSON is unavailable; refusing to publish fallback results as Fory");
    }
    switch (_scenario) {
      case "object":
        _json = "{\"v\":{\"n\":1}}";
        _path = "$.v";
        break;
      case "array":
        _json = "{\"v\":[1,2,3]}";
        _path = "$.v";
        break;
      case "deepSelected":
        _json = "{\"a\":".repeat(25) + "\"value\"" + "}".repeat(25);
        _path = "$." + "a.".repeat(24) + "a";
        break;
      case "deepUnrelated":
        _json = "{\"v\":\"value\",\"deep\":" + "{\"a\":".repeat(25) + "1" + "}".repeat(25) + "}";
        _path = "$.v";
        break;
      default:
        throw new IllegalArgumentException("Unsupported scenario: " + _scenario);
    }

    String expected = JsonFunctions.jsonPathString(_json, _path, DEFAULT_VALUE);
    if (!expected.equals(JsonFunctions.jsonPathStringFast(_json, _path, DEFAULT_VALUE))
        || !expected.equals(JsonFunctions.jsonPathStringFory(_json, _path, DEFAULT_VALUE))) {
      throw new IllegalStateException("JSON functions disagree for scenario: " + _scenario);
    }
    Object directResult = ForyJsonPathExtractor.extract(_json, SimpleJsonPath.compile(_path));
    boolean expectedFallback = _scenario.equals("object") || _scenario.equals("array");
    if (ForyJsonPathExtractor.isFallbackRequired(directResult) != expectedFallback
        || !expectedFallback && !expected.equals(directResult)) {
      throw new IllegalStateException("Fory did not directly exercise the expected path for scenario: " + _scenario);
    }
  }

  @Benchmark
  public String jsonPathStringJayway() {
    return JsonFunctions.jsonPathString(_json, _path, DEFAULT_VALUE);
  }

  @Benchmark
  public String jsonPathStringFast() {
    return JsonFunctions.jsonPathStringFast(_json, _path, DEFAULT_VALUE);
  }

  @Benchmark
  public String jsonPathStringFory() {
    return JsonFunctions.jsonPathStringFory(_json, _path, DEFAULT_VALUE);
  }
}
