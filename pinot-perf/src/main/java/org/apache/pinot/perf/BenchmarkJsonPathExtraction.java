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
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.Predicate;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import java.util.concurrent.TimeUnit;
import org.apache.fory.json.ForyJson;
import org.apache.pinot.common.function.FastJsonPathExtractor;
import org.apache.pinot.common.function.ForyJsonPathExtractor;
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
/// document and then walks to the field), Fory's streaming and dynamic-tree parsers, and [FastJsonPathExtractor].
///
/// The `fieldPosition` parameter puts the extracted field either near the start or at the very end of a
/// nested event payload, because that is what decides whether early exit can pay off. `documentBytes` pads the
/// payload immediately before the late field so parser scaling is visible without changing the addressed values.
///
/// The scalar-function methods measure the production `jsonPath*` ingestion wrappers. The lower-level single-column
/// methods isolate parser/traversal strategies; [BenchmarkJsonExtractScalarQuery] is the authoritative query-transform
/// measurement.
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@State(Scope.Thread)
public class BenchmarkJsonPathExtraction {
  private static final Predicate[] NO_PREDICATES = new Predicate[0];
  private static final ForyJson FORY_JSON = ForyJson.builder().build();
  private static final String STRING_DEFAULT = "DEFAULT";
  private static final long LONG_DEFAULT = -1L;
  private static final double DOUBLE_DEFAULT = -1.25d;

  /// Exactly the context `JsonExtractScalarTransformFunction` and `JsonFunctions` use.
  private static final ParseContext PARSE_CONTEXT = JsonPath.using(
      new Configuration.ConfigurationBuilder().jsonProvider(new JacksonJsonProvider())
          .mappingProvider(new JacksonMappingProvider()).options(Option.SUPPRESS_EXCEPTIONS).build());

  private static final String BASE_JSON = "{"
      + "\"ts\":1719878400123,"
      + "\"user\":{\"id\":\"u-19283\",\"country\":\"US\",\"score\":17.25,\"age\":41},"
      + "\"event\":{\"name\":\"checkout\",\"cart\":[{\"sku\":\"A1\",\"qty\":2,\"price\":19.99},"
      + "{\"sku\":\"B7\",\"qty\":1,\"price\":149.5},{\"sku\":\"C3\",\"qty\":5,\"price\":3.25}],"
      + "\"total\":352.73,\"currency\":\"USD\"},"
      + "\"device\":{\"os\":\"iOS\",\"version\":\"17.4.1\",\"model\":\"iPhone15,3\",\"screen\":{\"w\":1179,"
      + "\"h\":2556}},"
      + "\"geo\":{\"lat\":37.7749,\"lon\":-122.4194,\"city\":\"San Francisco\",\"region\":\"CA\"},"
      + "\"tags\":[\"mobile\",\"ios\",\"returning\",\"promo-eligible\",\"newsletter\"],"
      + "\"session\":{\"id\":\"s-aaaabbbbccccdddd\",\"start\":1719878300000,\"pages\":14,\"referrer\":"
      + "\"https://example.com/landing?utm_source=x&utm_medium=y\"},"
      + "\"trailer\":{\"country\":\"DE\",\"double\":19.25,\"long\":19}"
      + "}";

  /// Four derived columns, spread through the document, as an ingestion `transformConfigs` would pull.
  private static final String[] FOUR_PATHS = {"$.user.country", "$.event.currency", "$.device.os", "$.geo.city"};
  private static final String COMPLEX_PATH = "$.event.cart[*].sku";

  @Param({"early", "late"})
  private String _fieldPosition;

  @Param({"700", "8192", "65536"})
  private int _documentBytes;

  /// Use `-p _valueCase=missing` to measure the explicit-default path without doubling the default benchmark suite.
  @Param({"hit"})
  private String _valueCase;

  private String _json;
  private String _path;
  private String _longPath;
  private String _doublePath;
  private SimpleJsonPath _simplePath;
  private SimpleJsonPath _simpleLongPath;
  private SimpleJsonPath _simpleDoublePath;
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
    if (!ForyJsonPathExtractor.isAvailable()) {
      throw new IllegalStateException("Fory JSON is unavailable; refusing to publish fallback results as Fory");
    }
    _json = buildJson(_documentBytes);
    boolean missing;
    if ("hit".equals(_valueCase)) {
      missing = false;
    } else if ("missing".equals(_valueCase)) {
      missing = true;
    } else {
      throw new IllegalArgumentException("Unsupported value case: " + _valueCase);
    }
    if (missing) {
      _path = "$.missing";
      _longPath = _path;
      _doublePath = _path;
    } else if ("early".equals(_fieldPosition)) {
      _path = "$.user.country";
      _longPath = "$.user.age";
      _doublePath = "$.user.score";
    } else {
      _path = "$.trailer.country";
      _longPath = "$.trailer.long";
      _doublePath = "$.trailer.double";
    }
    _simplePath = SimpleJsonPath.compile(_path);
    _simpleLongPath = SimpleJsonPath.compile(_longPath);
    _simpleDoublePath = SimpleJsonPath.compile(_doublePath);
    _simpleFourPaths = new SimpleJsonPath[FOUR_PATHS.length];
    for (int i = 0; i < FOUR_PATHS.length; i++) {
      _simpleFourPaths[i] = SimpleJsonPath.compile(FOUR_PATHS[i]);
    }
    _fourResults = new Object[FOUR_PATHS.length];

    String expectedString = missing ? STRING_DEFAULT : "early".equals(_fieldPosition) ? "US" : "DE";
    long expectedLong = missing ? LONG_DEFAULT : "early".equals(_fieldPosition) ? 41L : 19L;
    double expectedDouble = missing ? DOUBLE_DEFAULT : "early".equals(_fieldPosition) ? 17.25d : 19.25d;
    verifyScalarFunctions(expectedString, expectedLong, expectedDouble);
    for (String path : FOUR_PATHS) {
      String jayway = JsonFunctions.jsonPathString(_json, path, "");
      String fory = JsonFunctions.jsonPathStringFory(_json, path, "");
      if (!jayway.equals(fory)) {
        throw new IllegalStateException("Fory result does not match Jayway for " + path);
      }
    }
    String expectedCart = "[\"A1\",\"B7\",\"C3\"]";
    if (!expectedCart.equals(JsonFunctions.jsonPathStringFory(_json, COMPLEX_PATH, ""))) {
      throw new IllegalStateException("Complex-path fallback does not match the expected cart");
    }
  }

  private void verifyScalarFunctions(String expectedString, long expectedLong, double expectedDouble) {
    Object extractedString = ForyJsonPathExtractor.extract(_json, _simplePath);
    Object extractedLong = ForyJsonPathExtractor.extract(_json, _simpleLongPath);
    Object extractedDouble = ForyJsonPathExtractor.extract(_json, _simpleDoublePath);
    if ("missing".equals(_valueCase)) {
      if (extractedString != null || extractedLong != null || extractedDouble != null) {
        throw new IllegalStateException("Fory returned a value for a missing benchmark path");
      }
    } else if (!expectedString.equals(extractedString) || ((Number) extractedLong).longValue() != expectedLong
        || Double.compare(((Number) extractedDouble).doubleValue(), expectedDouble) != 0) {
      throw new IllegalStateException("Direct Fory extraction produced an unexpected benchmark result");
    }

    String jaywayString = JsonFunctions.jsonPathString(_json, _path, STRING_DEFAULT);
    String fastString = JsonFunctions.jsonPathStringFast(_json, _path, STRING_DEFAULT);
    String foryString = JsonFunctions.jsonPathStringFory(_json, _path, STRING_DEFAULT);
    if (!expectedString.equals(jaywayString) || !jaywayString.equals(fastString) || !jaywayString.equals(foryString)) {
      throw new IllegalStateException("JSON string extractors disagree for " + _path);
    }

    long jaywayLong = JsonFunctions.jsonPathLong(_json, _longPath, LONG_DEFAULT);
    long fastLong = JsonFunctions.jsonPathLongFast(_json, _longPath, LONG_DEFAULT);
    long foryLong = JsonFunctions.jsonPathLongFory(_json, _longPath, LONG_DEFAULT);
    if (jaywayLong != expectedLong || fastLong != jaywayLong || foryLong != jaywayLong) {
      throw new IllegalStateException("JSON long extractors disagree for " + _longPath);
    }

    double jaywayDouble = JsonFunctions.jsonPathDouble(_json, _doublePath, DOUBLE_DEFAULT);
    double fastDouble = JsonFunctions.jsonPathDoubleFast(_json, _doublePath, DOUBLE_DEFAULT);
    double foryDouble = JsonFunctions.jsonPathDoubleFory(_json, _doublePath, DOUBLE_DEFAULT);
    if (Double.compare(jaywayDouble, expectedDouble) != 0 || Double.compare(fastDouble, jaywayDouble) != 0
        || Double.compare(foryDouble, jaywayDouble) != 0) {
      throw new IllegalStateException("JSON double extractors disagree for " + _doublePath);
    }
  }

  private static String buildJson(int targetBytes) {
    if (BASE_JSON.length() >= targetBytes) {
      return BASE_JSON;
    }
    String marker = "\"trailer\":";
    int markerOffset = BASE_JSON.indexOf(marker);
    String paddingPrefix = "\"padding\":\"";
    String paddingSuffix = "\",";
    if (BASE_JSON.length() + paddingPrefix.length() + paddingSuffix.length() >= targetBytes) {
      return BASE_JSON;
    }
    int paddingLength = targetBytes - BASE_JSON.length() - paddingPrefix.length() - paddingSuffix.length();
    return BASE_JSON.substring(0, markerOffset) + paddingPrefix + "x".repeat(paddingLength) + paddingSuffix
        + BASE_JSON.substring(markerOffset);
  }

  @Benchmark
  public Object jaywayOneColumn() {
    return PARSE_CONTEXT.parse(_json).read(_path, NO_PREDICATES);
  }

  /// Fory dynamic-tree parsing plus Jayway traversal, retained as a non-production comparison baseline.
  @Benchmark
  public Object foryOneColumn() {
    Object root = FORY_JSON.fromJson(_json, Object.class);
    return PARSE_CONTEXT.parse(root).read(_path, NO_PREDICATES);
  }

  @Benchmark
  public Object fastOneColumnFullScan() {
    return FastJsonPathExtractor.extract(_json, _simplePath, false, false);
  }

  @Benchmark
  public Object fastOneColumnEarlyExit() {
    return FastJsonPathExtractor.extract(_json, _simplePath, false, true);
  }

  /// The existing Jayway scalar function (applicability check + String coercion).
  @Benchmark
  public String jsonPathStringJayway() {
    return JsonFunctions.jsonPathString(_json, _path, STRING_DEFAULT);
  }

  /// The production Fory scalar function (applicability check + String coercion).
  @Benchmark
  public String jsonPathStringFory() {
    return JsonFunctions.jsonPathStringFory(_json, _path, STRING_DEFAULT);
  }

  /// The opt-in fast scalar function, full scan with fallback for unsupported or failed extraction.
  @Benchmark
  public String jsonPathStringFast() {
    return JsonFunctions.jsonPathStringFast(_json, _path, STRING_DEFAULT);
  }

  /// The opt-in fast scalar function, early exit / first match.
  @Benchmark
  public String jsonPathStringFirstMatch() {
    return JsonFunctions.jsonPathStringFirstMatch(_json, _path, STRING_DEFAULT);
  }

  /// The existing Jayway scalar function (applicability check + long coercion).
  @Benchmark
  public long jsonPathLongJayway() {
    return JsonFunctions.jsonPathLong(_json, _longPath, LONG_DEFAULT);
  }

  /// The opt-in fast scalar function, full scan with fallback for unsupported or failed extraction.
  @Benchmark
  public long jsonPathLongFast() {
    return JsonFunctions.jsonPathLongFast(_json, _longPath, LONG_DEFAULT);
  }

  /// The production Fory scalar function (applicability check + long coercion).
  @Benchmark
  public long jsonPathLongFory() {
    return JsonFunctions.jsonPathLongFory(_json, _longPath, LONG_DEFAULT);
  }

  /// The existing Jayway scalar function (applicability check + double coercion).
  @Benchmark
  public double jsonPathDoubleJayway() {
    return JsonFunctions.jsonPathDouble(_json, _doublePath, DOUBLE_DEFAULT);
  }

  /// The opt-in fast scalar function, full scan with fallback for unsupported or failed extraction.
  @Benchmark
  public double jsonPathDoubleFast() {
    return JsonFunctions.jsonPathDoubleFast(_json, _doublePath, DOUBLE_DEFAULT);
  }

  /// The production Fory scalar function (applicability check + double coercion).
  @Benchmark
  public double jsonPathDoubleFory() {
    return JsonFunctions.jsonPathDoubleFory(_json, _doublePath, DOUBLE_DEFAULT);
  }

  @Benchmark
  public String jsonPathStringJaywayComplex() {
    return JsonFunctions.jsonPathString(_json, COMPLEX_PATH, "");
  }

  @Benchmark
  public String jsonPathStringFastComplex() {
    return JsonFunctions.jsonPathStringFast(_json, COMPLEX_PATH, "");
  }

  @Benchmark
  public String jsonPathStringForyComplex() {
    return JsonFunctions.jsonPathStringFory(_json, COMPLEX_PATH, "");
  }

  @Benchmark
  public Object jaywayFourColumns() {
    Object last = null;
    for (String path : FOUR_PATHS) {
      last = PARSE_CONTEXT.parse(_json).read(path, NO_PREDICATES);
    }
    return last;
  }

  @Benchmark
  public Object fastFourColumnsSeparatePasses() {
    Object last = null;
    for (SimpleJsonPath path : _simpleFourPaths) {
      last = FastJsonPathExtractor.extract(_json, path, false, false);
    }
    return last;
  }

  @Benchmark
  public Object[] fastFourColumnsSinglePass() {
    FastJsonPathExtractor.extract(_json, _simpleFourPaths, _fourResults, false, false);
    return _fourResults;
  }

  /// Models four independent scalar expressions: each call streams over the document again.
  @Benchmark
  public String foryFourColumnsSeparateParses() {
    String last = null;
    for (String path : FOUR_PATHS) {
      last = JsonFunctions.jsonPathStringFory(_json, path, "");
    }
    return last;
  }

  /// Upper bound for a future parse-sharing optimization; Pinot does not currently expose this execution shape.
  @Benchmark
  public Object foryFourColumnsSingleParse() {
    Object root = FORY_JSON.fromJson(_json, Object.class);
    DocumentContext context = PARSE_CONTEXT.parse(root);
    Object last = null;
    for (String path : FOUR_PATHS) {
      last = context.read(path, NO_PREDICATES);
    }
    return last;
  }
}
