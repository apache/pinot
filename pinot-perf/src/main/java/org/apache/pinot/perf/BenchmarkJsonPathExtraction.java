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

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.Predicate;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.function.FastJsonPathExtractor;
import org.apache.pinot.common.function.SimpleJsonPath;
import org.apache.pinot.common.function.scalar.JsonFunctions;
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
import org.openjdk.jmh.runner.options.OptionsBuilder;


/// Compares JsonPath extraction through Jayway (today's implementation, which builds a full Jackson DOM of the
/// document and then walks to the field) against {@link FastJsonPathExtractor}.
/// <p>
/// The {@code fieldPosition} parameter puts the extracted field either near the start or at the very end of a
/// ~700 byte nested event payload, because that is what decides whether early exit can pay off.
/// <p>
/// The single-column benchmarks are also the per-row cost of {@code jsonExtractScalar}: that transform function
/// does exactly {@code parseContext.parse(row).read(jsonPath)} per row, so measuring the extraction in isolation
/// measures it without the surrounding {@code ValueBlock} scaffolding.
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@State(Scope.Benchmark)
public class BenchmarkJsonPathExtraction {
  private static final Predicate[] NO_PREDICATES = new Predicate[0];

  /// Exactly the context {@code JsonExtractScalarTransformFunction} and {@code JsonFunctions} use.
  private static final ParseContext PARSE_CONTEXT = JsonPath.using(
      new Configuration.ConfigurationBuilder().jsonProvider(new JacksonJsonProvider())
          .mappingProvider(new JacksonMappingProvider()).options(Option.SUPPRESS_EXCEPTIONS).build());

  private static final String JSON = "{"
      + "\"ts\":1719878400123,"
      + "\"user\":{\"id\":\"u-19283\",\"country\":\"US\",\"tier\":\"gold\",\"age\":41},"
      + "\"event\":{\"name\":\"checkout\",\"cart\":[{\"sku\":\"A1\",\"qty\":2,\"price\":19.99},"
      + "{\"sku\":\"B7\",\"qty\":1,\"price\":149.5},{\"sku\":\"C3\",\"qty\":5,\"price\":3.25}],"
      + "\"total\":352.73,\"currency\":\"USD\"},"
      + "\"device\":{\"os\":\"iOS\",\"version\":\"17.4.1\",\"model\":\"iPhone15,3\",\"screen\":{\"w\":1179,"
      + "\"h\":2556}},"
      + "\"geo\":{\"lat\":37.7749,\"lon\":-122.4194,\"city\":\"San Francisco\",\"region\":\"CA\"},"
      + "\"tags\":[\"mobile\",\"ios\",\"returning\",\"promo-eligible\",\"newsletter\"],"
      + "\"session\":{\"id\":\"s-aaaabbbbccccdddd\",\"start\":1719878300000,\"pages\":14,\"referrer\":"
      + "\"https://example.com/landing?utm_source=x&utm_medium=y\"},"
      + "\"trailer\":{\"country\":\"DE\",\"note\":\"last field in the document\"}"
      + "}";

  /// Four derived columns, spread through the document, as an ingestion {@code transformConfigs} would pull.
  private static final String[] FOUR_PATHS = {"$.user.country", "$.event.currency", "$.device.os", "$.geo.city"};

  @Param({"early", "late"})
  private String _fieldPosition;

  private String _path;
  private SimpleJsonPath _simplePath;
  private SimpleJsonPath[] _simpleFourPaths;
  private Object[] _fourResults;

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt =
        new OptionsBuilder().include(BenchmarkJsonPathExtraction.class.getSimpleName()).shouldDoGC(true);
    new Runner(opt.build()).run();
  }

  @Setup(Level.Trial)
  public void setUp() {
    _path = "early".equals(_fieldPosition) ? "$.user.country" : "$.trailer.country";
    _simplePath = SimpleJsonPath.compile(_path);
    _simpleFourPaths = new SimpleJsonPath[FOUR_PATHS.length];
    for (int i = 0; i < FOUR_PATHS.length; i++) {
      _simpleFourPaths[i] = SimpleJsonPath.compile(FOUR_PATHS[i]);
    }
    _fourResults = new Object[FOUR_PATHS.length];
  }

  @Benchmark
  public Object jaywayOneColumn() {
    return PARSE_CONTEXT.parse(JSON).read(_path, NO_PREDICATES);
  }

  @Benchmark
  public Object fastOneColumnFullScan() {
    return FastJsonPathExtractor.extract(JSON, _simplePath, false, false);
  }

  @Benchmark
  public Object fastOneColumnEarlyExit() {
    return FastJsonPathExtractor.extract(JSON, _simplePath, false, true);
  }

  /// The existing Jayway scalar function (applicability check + String coercion).
  @Benchmark
  public String jsonPathStringJayway() {
    return JsonFunctions.jsonPathString(JSON, _path, "");
  }

  /// The opt-in fast scalar function, full scan (exact parity).
  @Benchmark
  public String jsonPathStringFast() {
    return JsonFunctions.jsonPathStringFast(JSON, _path, "");
  }

  /// The opt-in fast scalar function, early exit / first match.
  @Benchmark
  public String jsonPathStringFirstMatch() {
    return JsonFunctions.jsonPathStringFirstMatch(JSON, _path, "");
  }

  @Benchmark
  public Object jaywayFourColumns() {
    Object last = null;
    for (String path : FOUR_PATHS) {
      last = PARSE_CONTEXT.parse(JSON).read(path, NO_PREDICATES);
    }
    return last;
  }

  @Benchmark
  public Object fastFourColumnsSeparatePasses() {
    Object last = null;
    for (SimpleJsonPath path : _simpleFourPaths) {
      last = FastJsonPathExtractor.extract(JSON, path, false, false);
    }
    return last;
  }

  @Benchmark
  public Object[] fastFourColumnsSinglePass() {
    FastJsonPathExtractor.extract(JSON, _simpleFourPaths, _fourResults, false, false);
    return _fourResults;
  }
}
