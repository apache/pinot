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
package org.apache.pinot.common.function;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.Predicate;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.StringJoiner;
import org.apache.pinot.common.function.scalar.JsonFunctions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/// Differential test for {@link FastJsonPathExtractor} and the opt-in fast scalar overloads, with
/// Jayway as the oracle.
/// <p>
/// Every case runs the same {json, path} pair through the fast extractor and through the exact Jayway
/// configuration it replaces, and asserts the two produce the same value - or the same exception type. The
/// oracle is the real production Jayway config ({@link #PLAIN_CONTEXT} / {@link #BIG_DECIMAL_CONTEXT}) and the
/// existing Jayway scalar overloads, so it cannot drift away from what Pinot actually does today.
public class FastJsonPathExtractorTest {
  private static final Predicate[] NO_PREDICATES = new Predicate[0];

  /// Rejects the two inputs early exit is allowed to disagree on: duplicate keys and a malformed / trailing tail.
  private static final JsonFactory STRICT_FACTORY =
      JsonFactory.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build();

  /// Mirrors {@code JsonExtractScalarTransformFunction.JSON_PARSER_CONTEXT}.
  private static final ParseContext PLAIN_CONTEXT = JsonPath.using(
      new Configuration.ConfigurationBuilder().jsonProvider(new JacksonJsonProvider())
          .mappingProvider(new JacksonMappingProvider()).options(Option.SUPPRESS_EXCEPTIONS).build());

  /// Mirrors {@code JsonExtractScalarTransformFunction.JSON_PARSER_CONTEXT_WITH_BIG_DECIMAL}, the one production
  /// context that is not reachable through {@link JsonFunctions}.
  private static final ParseContext BIG_DECIMAL_CONTEXT = JsonPath.using(
      new Configuration.ConfigurationBuilder().jsonProvider(new JacksonJsonProvider(
              new ObjectMapper().configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)))
          .mappingProvider(new JacksonMappingProvider()).options(Option.SUPPRESS_EXCEPTIONS).build());

  static {
    /// JsonFunctions registers Pinot's JsonPath cache in its static initializer, and Jayway rejects that
    /// registration once its own cache has been read. This test reads through Jayway directly, so load
    /// JsonFunctions first or its class initialization fails.
    JsonFunctions.jsonPathExists("{}", "$.x");
  }

  /// The value a call produced, or the type of the exception it threw.
  private static final class Outcome {
    final Object _value;
    final Class<?> _thrown;

    private Outcome(Object value, Class<?> thrown) {
      _value = value;
      _thrown = thrown;
    }

    static Outcome of(ThrowingSupplier supplier) {
      try {
        return new Outcome(supplier.get(), null);
      } catch (Exception e) {
        return new Outcome(null, e.getClass());
      }
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Outcome)) {
        return false;
      }
      Outcome other = (Outcome) o;
      return _thrown == other._thrown && Objects.deepEquals(_value, other._value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_thrown, _value);
    }

    @Override
    public String toString() {
      if (_thrown != null) {
        return "threw " + _thrown.getSimpleName();
      }
      return _value == null ? "null" : _value.getClass().getSimpleName() + "(" + _value + ")";
    }
  }

  @FunctionalInterface
  private interface ThrowingSupplier {
    Object get()
        throws Exception;
  }

  /// Each typed extractor that has an opt-in fast overload, paired with its existing Jayway overload. The
  /// two must return the same thing for every input; that is what makes the fast overload a safe drop-in.
  private enum Fn {
    STRING((o, p) -> JsonFunctions.jsonPathString(o, p, "DEFAULT"),
        (o, p) -> JsonFunctions.jsonPathStringFast(o, p, "DEFAULT")),
    LONG((o, p) -> JsonFunctions.jsonPathLong(o, p, -7L),
        (o, p) -> JsonFunctions.jsonPathLongFast(o, p, -7L)),
    DOUBLE((o, p) -> JsonFunctions.jsonPathDouble(o, p, -7.5d),
        (o, p) -> JsonFunctions.jsonPathDoubleFast(o, p, -7.5d));

    private final Invocation _jayway;
    private final Invocation _fast;

    Fn(Invocation jayway, Invocation fast) {
      _jayway = jayway;
      _fast = fast;
    }

    Outcome jayway(String json, String path) {
      return Outcome.of(() -> _jayway.invoke(json, path));
    }

    Outcome fast(String json, String path) {
      return Outcome.of(() -> _fast.invoke(json, path));
    }
  }

  @FunctionalInterface
  private interface Invocation {
    Object invoke(Object json, String path)
        throws Exception;
  }

  /// Asserts full parity with Jayway at two levels: the extractor itself against the exact Jayway config it
  /// replaces (value level, covering raw values and containers), and every opt-in fast scalar overload
  /// against its existing Jayway overload (function level, covering the coercion and fallback wiring).
  private static void assertParity(String json, String path) {
    SimpleJsonPath simpleJsonPath = SimpleJsonPath.compile(path);
    if (simpleJsonPath != null && FastJsonPathExtractor.canExtract(json)) {
      Outcome jayway = Outcome.of(() -> PLAIN_CONTEXT.parse(json).read(path, NO_PREDICATES));
      Outcome fast = Outcome.of(() -> FastJsonPathExtractor.extract(json, simpleJsonPath, false, false));
      assertEquals(fast, jayway,
          String.format("extractor mismatch for json=<%s> path=<%s>: fast=%s jayway=%s", json, path, fast,
              jayway));
    }
    for (Fn fn : Fn.values()) {
      Outcome jayway = fn.jayway(json, path);
      Outcome fast = fn.fast(json, path);
      assertEquals(fast, jayway,
          String.format("%s mismatch for json=<%s> path=<%s>: fast=%s jayway=%s", fn, json, path, fast,
              jayway));
    }
  }

  @DataProvider(name = "parityCases")
  public Object[][] parityCases() {
    return new Object[][]{
        /// Scalar leaves, all JSON types.
        {"{\"a\":\"s\"}", "$.a"},
        {"{\"a\":1}", "$.a"},
        {"{\"a\":true}", "$.a"},
        {"{\"a\":false}", "$.a"},
        {"{\"a\":null}", "$.a"},

        /// Missing paths, at every depth and shape.
        {"{\"a\":1}", "$.b"},
        {"{\"a\":1}", "$.b.c"},
        {"{\"a\":1}", "$.a.b.c.d"},
        {"{\"a\":{\"b\":{}}}", "$.a.b.c"},
        {"{\" a\":1}", "$.a"},

        /// Type mismatches: descend into a scalar / array, index a non-array, out of range.
        {"{\"a\":5}", "$.a.b"},
        {"{\"a\":\"xy\"}", "$.a[0]"},
        {"{\"a\":[1,2]}", "$.a.b"},
        {"{\"a\":{\"x\":1}}", "$.a[0]"},
        {"{\"a\":[10,20]}", "$.a[5]"},
        {"{\"a\":null}", "$.a.b"},
        {"{\"a\":null}", "$.a[0]"},
        {"[1,2,3]", "$.a"},
        {"{\"a\":1}", "$[0]"},

        /// Roots.
        {"[10,20,30]", "$[0]"},
        {"[10,20,30]", "$[5]"},
        {"{\"a\":1}", "$['a']"},
        {"{\"a\":1}", "$[\"a\"]"},

        /// Nested arrays and arrays of objects.
        {"{\"a\":[{\"b\":7}]}", "$.a[0].b"},
        {"{\"a\":[{\"b\":7}]}", "$.a[1].b"},
        {"{\"a\":[[1,2]]}", "$.a[0][1]"},
        {"{\"a\":[[1,2]]}", "$.a[0][9]"},
        {"{\"a\":[10,20]}", "$.a[01]"},
        {"{\"a\":[10,20]}", "$.a[ 1 ]"},
        {"[[{\"x\":[5]}]]", "$[0][0].x[0]"},

        /// Container leaves.
        {"{\"a\":{\"x\":1,\"y\":[1,2]}}", "$.a"},
        {"{\"a\":[1,\"two\",null,{\"b\":3}]}", "$.a"},
        {"{\"a\":[]}", "$.a"},
        {"{\"a\":{}}", "$.a"},
        {"{\"a\":{\"b\":{\"c\":{\"d\":[1,{\"e\":2}]}}}}", "$.a.b.c"},

        /// Deep nesting.
        {"{\"a\":{\"b\":{\"c\":{\"d\":{\"e\":{\"f\":42}}}}}}", "$.a.b.c.d.e.f"},

        /// Dotted / quoted / empty keys.
        {"{\"a.b\":1}", "$['a.b']"},
        {"{\"a\":{\"b\":1}}", "$['a.b']"},
        {"{\"a\":{\"b\":1}}", "$.a['b']"},
        {"{\"a\":{\"b\":1}}", "$['a'].b"},
        {"{\"\":1}", "$['']"},
        {"{\"a b\":1}", "$['a b']"},
        {"{\"a\\\"b\":1}", "$['a\"b']"},
        {"{\"a'b\":1}", "$[\"a'b\"]"},
        {"{\"a-b\":1}", "$.a-b"},
        {"{\"a_b\":1}", "$.a_b"},
        {"{\"0\":1}", "$['0']"},

        /// Duplicate keys: last occurrence wins, at every depth, and a later one discards the earlier subtree.
        {"{\"a\":1,\"a\":2}", "$.a"},
        {"{\"a\":{\"x\":1},\"a\":{\"x\":2}}", "$.a.x"},
        {"{\"a\":{\"x\":1},\"a\":{\"y\":2}}", "$.a.x"},
        {"{\"a\":{\"x\":1},\"a\":5}", "$.a.x"},
        {"{\"a\":5,\"a\":{\"x\":1}}", "$.a.x"},
        {"{\"a\":{\"x\":1,\"x\":2}}", "$.a.x"},
        {"{\"a\":[1],\"a\":[2]}", "$.a[0]"},
        {"{\"a\":1,\"a\":null}", "$.a"},
        {"{\"a\":null,\"a\":1}", "$.a"},

        /// Numbers: every boundary the untyped Jackson deserializer switches on.
        {"{\"a\":2147483647}", "$.a"},
        {"{\"a\":2147483648}", "$.a"},
        {"{\"a\":-2147483648}", "$.a"},
        {"{\"a\":-2147483649}", "$.a"},
        {"{\"a\":9223372036854775807}", "$.a"},
        {"{\"a\":9223372036854775808}", "$.a"},
        {"{\"a\":-9223372036854775809}", "$.a"},
        {"{\"a\":1.5}", "$.a"},
        {"{\"a\":1.0}", "$.a"},
        {"{\"a\":-0.0}", "$.a"},
        {"{\"a\":0.1}", "$.a"},
        {"{\"a\":1E2}", "$.a"},
        {"{\"a\":1e400}", "$.a"},
        {"{\"a\":-1e400}", "$.a"},
        {"{\"a\":1e-400}", "$.a"},
        {"{\"a\":123456789012345678901234567890.123}", "$.a"},
        {"{\"a\":[1,2147483648,1.5]}", "$.a"},

        /// Unicode escapes, surrogate pairs, control characters, escaped keys.
        {"{\"a\":\"caf\\u00e9\"}", "$.a"},
        {"{\"a\":\"\\ud83d\\ude00\"}", "$.a"},
        {"{\"a\":\"\\u0000\"}", "$.a"},
        {"{\"a\":\"line\\nbreak\\ttab\\\\slash\\\"quote\"}", "$.a"},
        {"{\"\\u0061\":1}", "$.a"},
        {"{\"a\":\"\\/\"}", "$.a"},

        /// Invalid JSON: before, at and after the addressed field; and structurally broken.
        {"{\"a\":1,\"b\":}", "$.a"},
        {"{\"b\":,\"a\":1}", "$.a"},
        {"{\"a\":1,\"b\":[1,", "$.a"},
        {"{\"a\":1,", "$.a"},
        {"{\"a\":1,}", "$.a"},
        {"{\"a\":01}", "$.a"},
        {"{\"a\":NaN}", "$.a"},
        {"{'a':1}", "$.a"},
        {"{a:1}", "$.a"},
        {"{\"a\":1,\"b\":\"\\uZZZZ\"}", "$.a"},
        {"[1,2", "$[0]"},
        {"{", "$.a"},

        /// Content after the root value is ignored, exactly as ObjectMapper.readValue ignores it.
        {"{\"a\":1} xyz", "$.a"},
        {"{\"a\":1}{\"a\":2}", "$.a"},
        {"[1,2] xyz", "$[0]"},

        /// Non-JSON, empty, whitespace, BOM, bare scalars: all must take the Jayway path.
        {"hello world", "$.a"},
        {"", "$.a"},
        {"   ", "$.a"},
        {"﻿{\"a\":1}", "$.a"},
        {"5", "$.a"},
        {"\"str\"", "$.a"},
        {"true", "$.a"},
        {"null", "$.a"},
        {"nullx", "$.a"},
        {"  {\"a\":1}  ", "$.a"},

        /// Paths the fast path must decline, so these assert the fallback is wired correctly.
        {"{\"a\":{\"b\":1}}", "$..b"},
        {"{\"a\":{\"b\":1}}", "$.*"},
        {"{\"a\":[1,2,3]}", "$.a[*]"},
        {"{\"a\":[1,2,3]}", "$.a[-1]"},
        {"{\"a\":[1,2,3]}", "$.a[0:2]"},
        {"{\"a\":[1,2,3]}", "$.a.length()"},
        {"{\"a\":[{\"b\":1},{\"b\":2}]}", "$.a[?(@.b==2)]"},
        {"{\"a\":1,\"b\":2}", "$['a','b']"},
        {"{\"a\":1}", "$"},
        {"{\"a\":1}", "a"},

        /// String coercions that only bite the typed wrappers.
        {"{\"a\":\"123\"}", "$.a"},
        {"{\"a\":\"12.5\"}", "$.a"},
        {"{\"a\":\"not a number\"}", "$.a"},
        {"{\"a\":{\"b\":1}}", "$.a"},
    };
  }

  @Test(dataProvider = "parityCases")
  public void testParityWithJayway(String json, String path) {
    assertParity(json, path);
  }

  /// The {@code USE_BIG_DECIMAL_FOR_FLOATS} context used by {@code jsonExtractScalar} for STRING and BIG_DECIMAL
  /// results is not reachable through {@link JsonFunctions}, so it gets its own oracle.
  @Test(dataProvider = "parityCases")
  public void testBigDecimalParityWithJayway(String json, String path) {
    SimpleJsonPath simpleJsonPath = SimpleJsonPath.compile(path);
    if (simpleJsonPath == null || !FastJsonPathExtractor.canExtract(json)) {
      return;
    }
    Outcome jayway = Outcome.of(() -> BIG_DECIMAL_CONTEXT.parse(json).read(path, NO_PREDICATES));
    Outcome fast = Outcome.of(() -> FastJsonPathExtractor.extract(json, simpleJsonPath, true, false));
    assertEquals(fast, jayway,
        String.format("big-decimal mismatch for json=<%s> path=<%s>: fast=%s jayway=%s", json, path, fast,
            jayway));
  }

  /// The {@code byte[]} overloads back {@code jsonExtractScalar} over a BYTES column, which reaches
  /// {@code parseContext.parseUtf8} rather than {@code parse}. They get their own oracle.
  @Test(dataProvider = "parityCases")
  public void testBytesParityWithJayway(String json, String path) {
    SimpleJsonPath simpleJsonPath = SimpleJsonPath.compile(path);
    byte[] jsonBytes = json.getBytes(StandardCharsets.UTF_8);
    assertEquals(FastJsonPathExtractor.canExtract(jsonBytes), FastJsonPathExtractor.canExtract(json),
        "canExtract disagrees between String and byte[] for json=<" + json + ">");
    if (simpleJsonPath == null || !FastJsonPathExtractor.canExtract(jsonBytes)) {
      return;
    }
    Outcome jayway = Outcome.of(() -> PLAIN_CONTEXT.parseUtf8(jsonBytes).read(path, NO_PREDICATES));
    Outcome fast = Outcome.of(() -> FastJsonPathExtractor.extract(jsonBytes, simpleJsonPath, false, false));
    assertEquals(fast, jayway,
        String.format("bytes mismatch for json=<%s> path=<%s>: fast=%s jayway=%s", json, path, fast, jayway));
  }

  /// Multibyte UTF-8 in keys and values, which only the {@code byte[]} path can get wrong.
  @Test
  public void testBytesParityWithMultibyteContent() {
    for (String[] jsonAndPath : new String[][]{
        {"{\"a\":\"café\"}", "$.a"}, {"{\"café\":1}", "$.café"}, {"{\"café\":1}", "$['café']"},
        {"{\"a\":\"日本語\"}", "$.a"}, {"{\"日本\":{\"語\":7}}", "$.日本.語"}, {"{\"a\":\"😀\"}", "$.a"},
        {"{\"a\":\"😀\",\"b\":2}", "$.b"}
    }) {
      testBytesParityWithJayway(jsonAndPath[0], jsonAndPath[1]);
      assertParity(jsonAndPath[0], jsonAndPath[1]);
    }
  }

  /// The opt-in fast overloads are new SQL-visible signatures, so resolve and invoke them through the
  /// {@link FunctionRegistry} the query/ingestion path uses. This is what a Java-level call cannot prove: that
  /// the four-argument overloads register under their own arity and that a {@code boolean} literal converts.
  @Test
  public void testFastFunctionsResolveThroughFunctionRegistry()
      throws Exception {
    String json = "{\"user\":{\"country\":\"US\",\"age\":41,\"score\":9.5}}";
    assertEquals(invoke("jsonPathStringFast", json, "$.user.country", "DEFAULT"), "US");
    assertEquals(invoke("jsonPathStringFirstMatch", json, "$.user.country", "DEFAULT"), "US");
    assertEquals(invoke("jsonPathStringFast", json, "$.missing", "DEFAULT"), "DEFAULT");
    assertEquals(invoke("jsonPathLongFirstMatch", json, "$.user.age", -7L), 41L);
    assertEquals(invoke("jsonPathDoubleFast", json, "$.user.score", -7.5d), 9.5d);
    /// A complex path must still resolve through the function by falling back to Jayway, i.e. produce exactly
    /// what the existing Jayway function produces (here the deep-scan list, stringified).
    assertEquals(invoke("jsonPathStringFast", json, "$..country", "DEFAULT"),
        JsonFunctions.jsonPathString(json, "$..country", "DEFAULT"));
  }

  /// Function-level coverage of the {@code FirstMatch} (early-exit) functions: they equal {@code Fast}/Jayway on
  /// clean data, and diverge to the first occurrence on the documented duplicate-key case.
  @Test
  public void testFirstMatchFunctions() {
    String clean = "{\"a\":{\"b\":\"x\"},\"n\":{\"m\":7}}";
    assertEquals(JsonFunctions.jsonPathStringFirstMatch(clean, "$.a.b", "d"),
        JsonFunctions.jsonPathStringFast(clean, "$.a.b", "d"));
    assertEquals(JsonFunctions.jsonPathLongFirstMatch(clean, "$.n.m", -1L),
        JsonFunctions.jsonPathLongFast(clean, "$.n.m", -1L));

    String duplicateKeys = "{\"a\":1,\"a\":2}";
    assertEquals(JsonFunctions.jsonPathLong(duplicateKeys, "$.a", -1L), 2L, "Jayway takes the last occurrence");
    assertEquals(JsonFunctions.jsonPathLongFast(duplicateKeys, "$.a", -1L), 2L, "Fast matches Jayway");
    assertEquals(JsonFunctions.jsonPathLongFirstMatch(duplicateKeys, "$.a", -1L), 1L, "FirstMatch takes the first");
  }

  private static Object invoke(String name, Object... arguments)
      throws Exception {
    FunctionInfo functionInfo = FunctionRegistry.getFunctionInfo(name, arguments.length);
    assertNotNull(functionInfo, name + "/" + arguments.length + " is not registered");
    FunctionInvoker invoker = new FunctionInvoker(functionInfo);
    Object[] copy = arguments.clone();
    invoker.convertTypes(copy);
    return invoker.invoke(copy);
  }

  /// Invalid UTF-8 must raise the same exception type Jayway raises.
  @Test
  public void testBytesParityWithInvalidUtf8() {
    byte[] invalid = {'{', '"', 'a', '"', ':', '"', (byte) 0xC3, '"', '}'};
    SimpleJsonPath path = SimpleJsonPath.compile("$.a");
    assertNotNull(path);
    assertTrue(FastJsonPathExtractor.canExtract(invalid));
    Outcome jayway = Outcome.of(() -> PLAIN_CONTEXT.parseUtf8(invalid).read("$.a", NO_PREDICATES));
    Outcome fast = Outcome.of(() -> FastJsonPathExtractor.extract(invalid, path, false, false));
    assertEquals(fast, jayway);
  }

  /// The {@code long} live-path bitmask special-cases a full 64-path batch; anything more must be rejected.
  @Test
  public void testMaxPathsBoundary() {
    String json = "{\"k0\":0,\"k63\":63,\"k64\":64}";
    SimpleJsonPath[] paths = new SimpleJsonPath[FastJsonPathExtractor.MAX_PATHS];
    for (int i = 0; i < paths.length; i++) {
      paths[i] = SimpleJsonPath.compile("$.k" + i);
    }
    Object[] out = new Object[paths.length];
    FastJsonPathExtractor.extract(json, paths, out, false, false);
    assertEquals(out[0], 0);
    assertEquals(out[63], 63);
    assertNull(out[1]);

    SimpleJsonPath[] tooMany = new SimpleJsonPath[FastJsonPathExtractor.MAX_PATHS + 1];
    Arrays.fill(tooMany, paths[0]);
    try {
      FastJsonPathExtractor.extract(json, tooMany, new Object[tooMany.length], false, false);
      fail("must reject more paths than the bitmask can hold");
    } catch (IllegalArgumentException e) {
      /// Expected.
    }
  }

  /// Pins the one documented divergence from Jayway in full-scan mode. Jayway materializes the whole document, so
  /// a value it can lex but not materialize makes it reject the document even when the path never addresses that
  /// value; the fast extractor skips such subtrees and returns the requested value. Matching Jayway here
  /// would mean materializing every string in every document, which is exactly the cost this class exists to
  /// avoid. Asserted explicitly so it can never widen unnoticed.
  @Test
  public void testUnmaterializableValueOutsideThePathDivergesFromJayway() {
    SimpleJsonPath path = SimpleJsonPath.compile("$.a");
    assertNotNull(path);

    /// (1) float literal whose exponent overflows an int, only unrepresentable under useBigDecimal.
    String hostileFloat = "{\"other\":1e999999999999,\"a\":1}";
    Outcome jaywayBigDecimal = Outcome.of(() -> BIG_DECIMAL_CONTEXT.parse(hostileFloat).read("$.a", NO_PREDICATES));
    assertNotNull(jaywayBigDecimal._thrown, "Jayway must still reject the hostile float under useBigDecimal");
    assertEquals(FastJsonPathExtractor.extract(hostileFloat, path, true, false), 1,
        "fast skips the unaddressed subtree and returns the addressed value");
    /// The plain (double) context represents it as Infinity, so there is no divergence there.
    assertParity(hostileFloat, "$.a");
    /// When the hostile literal IS the addressed value, both raise - parity holds.
    String addressedHostileFloat = "{\"a\":1e999999999999}";
    try {
      FastJsonPathExtractor.extract(addressedHostileFloat, path, true, false);
      fail("addressing the hostile float must still raise under useBigDecimal");
    } catch (Exception e) {
      /// Expected, same as Jayway.
    }

    /// (2) string longer than StreamReadConstraints.maxStringLength (20,000,000), outside the path.
    String hostileString = "{\"a\":\"ERROR\",\"other\":\"" + "x".repeat(20_000_001) + "\"}";
    Outcome jaywayLongString = Outcome.of(() -> PLAIN_CONTEXT.parse(hostileString).read("$.a", NO_PREDICATES));
    assertNotNull(jaywayLongString._thrown, "Jayway must still reject the over-long unaddressed string");
    assertEquals(FastJsonPathExtractor.extract(hostileString, path, false, false), "ERROR",
        "fast never materializes the unaddressed string, so it does not hit the limit");
  }

  /// The {@code useBigDecimal} context backs {@code jsonExtractScalar(..., 'STRING')} and {@code 'BIG_DECIMAL'} -
  /// the most common result types - but is unreachable through {@link JsonFunctions}, so the other fuzzers never
  /// touch it. Fuzz it directly against its own Jayway oracle.
  @Test
  public void testBigDecimalFuzzAgainstJayway() {
    Random random = new Random(4242L);
    for (int i = 0; i < 20_000; i++) {
      String json = randomJson(random, 0);
      String path = randomPath(random);
      SimpleJsonPath simpleJsonPath = SimpleJsonPath.compile(path);
      if (simpleJsonPath == null || !FastJsonPathExtractor.canExtract(json)) {
        continue;
      }
      Outcome jayway = Outcome.of(() -> BIG_DECIMAL_CONTEXT.parse(json).read(path, NO_PREDICATES));
      Outcome fast = Outcome.of(() -> FastJsonPathExtractor.extract(json, simpleJsonPath, true, false));
      assertEquals(fast, jayway,
          String.format("big-decimal fuzz mismatch for json=<%s> path=<%s>", json, path));
    }
  }

  @Test
  public void testCompileAcceptsSimpleLinearPaths() {
    for (String path : new String[]{
        "$.a", "$.a.b", "$['a']", "$[\"a\"]", "$[0]", "$.a[0]", "$.a[0].b", "$['a.b']", "$.a['b'][2]", "$['']",
        "$.a-b", "$.a_b", "$.abc123", "$[2147483647]",
        /// Jayway reads a leading zero as a plain decimal index, so this is index 1, not a rejected literal.
        "$[01]"
    }) {
      assertNotNull(SimpleJsonPath.compile(path), path);
    }
  }

  @Test
  public void testCompileRejectsEverythingElse() {
    for (String path : new String[]{
        "$", "$..a", "$.*", "$..*", "$.a[*]", "$.a[-1]", "$.a[0:2]", "$.a[:2]", "$.a[1,2]", "$['a','b']",
        "$.a[?(@.b==2)]", "$.a.length()", "$.a.", "$..", "a", "a.b", "", "$.", "$[", "$[']", "$['a", "$['a'",
        "$['a\\'b']", "$['a']extra", "$.a b", "$[2147483648]", "$[ 1 ]", "$.@", "$.a$b"
    }) {
      assertNull(SimpleJsonPath.compile(path), path);
    }
  }

  /// The two behavior changes early exit buys, asserted explicitly so they can never regress silently.
  @Test
  public void testEarlyExitDivergences() {
    String duplicateKeys = "{\"a\":1,\"a\":2}";
    String malformedTail = "{\"a\":1,\"b\":}";
    SimpleJsonPath path = SimpleJsonPath.compile("$.a");
    assertNotNull(path);

    assertEquals(FastJsonPathExtractor.extract(duplicateKeys, path, false, false), 2,
        "full pass takes the last key");
    try {
      FastJsonPathExtractor.extract(malformedTail, path, false, false);
      fail("full pass must reject a document malformed inside its root value");
    } catch (Exception e) {
      /// Expected: same as Jayway.
    }

    assertEquals(FastJsonPathExtractor.extract(duplicateKeys, path, false, true), 1,
        "early exit takes the first key");
    assertEquals(FastJsonPathExtractor.extract(malformedTail, path, false, true), 1,
        "early exit never reads the malformed tail");
  }

  /// A JSON document that parses cleanly with duplicate-key detection on, and has nothing after its root value, is
  /// exactly a document for which early exit provably cannot change the answer. Those must match a full pass.
  private static boolean isStrictAndSingleRooted(String json) {
    try (JsonParser parser = STRICT_FACTORY.createParser(json)) {
      while (parser.nextToken() != null) {
        /// Reading every token also rejects trailing content, which a full pass would ignore but never see.
      }
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private static void assertEarlyExitParity(String json, String path) {
    if (!isStrictAndSingleRooted(json)) {
      return;
    }
    SimpleJsonPath simpleJsonPath = SimpleJsonPath.compile(path);
    if (simpleJsonPath == null || !FastJsonPathExtractor.canExtract(json)) {
      return;
    }
    Outcome fullPass = Outcome.of(() -> FastJsonPathExtractor.extract(json, simpleJsonPath, false, false));
    Outcome earlyExit = Outcome.of(() -> FastJsonPathExtractor.extract(json, simpleJsonPath, false, true));
    assertEquals(earlyExit, fullPass, String.format("early-exit mismatch for json=<%s> path=<%s>", json, path));
  }

  /// Everything but a duplicate key or a malformed tail must be identical whether or not early exit is on.
  @Test(dataProvider = "parityCases")
  public void testEarlyExitParityExceptKnownDivergences(String json, String path) {
    assertEarlyExitParity(json, path);
  }

  @Test
  public void testEarlyExitFuzzParityExceptKnownDivergences() {
    Random random = new Random(556677L);
    for (int i = 0; i < 20_000; i++) {
      assertEarlyExitParity(randomJson(random, 0), randomPath(random));
    }
  }

  @Test
  public void testMultiPathMatchesSinglePath() {
    String json = "{\"user\":{\"id\":7,\"country\":\"US\"},\"tags\":[\"a\",\"b\"],\"total\":1.5,\"nil\":null}";
    String[] paths = {"$.user.country", "$.user.id", "$.tags[1]", "$.total", "$.nil", "$.missing", "$.user",
        "$.user.missing"};
    SimpleJsonPath[] compiled = new SimpleJsonPath[paths.length];
    for (int i = 0; i < paths.length; i++) {
      compiled[i] = SimpleJsonPath.compile(paths[i]);
      assertNotNull(compiled[i], paths[i]);
    }
    Object[] batch = new Object[paths.length];
    FastJsonPathExtractor.extract(json, compiled, batch, false, false);
    for (int i = 0; i < paths.length; i++) {
      assertEquals(batch[i], FastJsonPathExtractor.extract(json, compiled[i], false, false), paths[i]);
      assertEquals(batch[i], PLAIN_CONTEXT.parse(json).read(paths[i], NO_PREDICATES), paths[i]);
    }
  }

  /// A path that is a strict prefix of another must not consume the value the longer path needs.
  @Test
  public void testMultiPathWithOverlappingPrefixes() {
    String json = "{\"a\":{\"b\":{\"c\":1}}}";
    SimpleJsonPath[] compiled = {
        SimpleJsonPath.compile("$.a"), SimpleJsonPath.compile("$.a.b"), SimpleJsonPath.compile("$.a.b.c"),
        SimpleJsonPath.compile("$.a.b.missing")
    };
    Object[] batch = new Object[compiled.length];
    FastJsonPathExtractor.extract(json, compiled, batch, false, false);
    assertEquals(batch[0].toString(), "{b={c=1}}");
    assertEquals(batch[1].toString(), "{c=1}");
    assertEquals(batch[2], 1);
    assertNull(batch[3]);
  }

  @Test
  public void testFuzzAgainstJayway() {
    Random random = new Random(20260709L);
    for (int i = 0; i < 20_000; i++) {
      String json = randomJson(random, 0);
      String path = randomPath(random);
      assertParity(json, path);
    }
  }

  /// Same corpus, but the paths are generated to actually hit something more often than not.
  @Test
  public void testFuzzWithDuplicateKeysAndDeepNesting() {
    Random random = new Random(981723L);
    for (int i = 0; i < 20_000; i++) {
      StringJoiner object = new StringJoiner(",", "{", "}");
      int numFields = 1 + random.nextInt(4);
      for (int f = 0; f < numFields; f++) {
        /// A small key alphabet makes duplicate keys frequent, which is the case early exit gets wrong.
        object.add("\"" + KEYS[random.nextInt(3)] + "\":" + randomJson(random, 2));
      }
      assertParity(object.toString(), randomPath(random));
    }
  }

  private static final String[] KEYS = {"a", "b", "c", "a-b", "a.b", ""};
  private static final String[] SCALARS = {
      "1", "0", "-1", "2147483648", "9223372036854775808", "1.5", "-0.0", "1e400", "1E2",
      "123456789012345678901234567890.123", "true", "false", "null", "\"\"", "\"s\"", "\"caf\\u00e9\"",
      "\"\\ud83d\\ude00\"", "\"123\""
  };

  private static String randomJson(Random random, int depth) {
    int choice = depth >= 3 ? 2 : random.nextInt(5);
    if (choice == 0) {
      StringJoiner object = new StringJoiner(",", "{", "}");
      int numFields = random.nextInt(4);
      for (int i = 0; i < numFields; i++) {
        object.add("\"" + KEYS[random.nextInt(KEYS.length)] + "\":" + randomJson(random, depth + 1));
      }
      return object.toString();
    }
    if (choice == 1) {
      StringJoiner array = new StringJoiner(",", "[", "]");
      int numElements = random.nextInt(4);
      for (int i = 0; i < numElements; i++) {
        array.add(randomJson(random, depth + 1));
      }
      return array.toString();
    }
    return SCALARS[random.nextInt(SCALARS.length)];
  }

  private static String randomPath(Random random) {
    StringBuilder path = new StringBuilder("$");
    int numSegments = 1 + random.nextInt(4);
    for (int i = 0; i < numSegments; i++) {
      switch (random.nextInt(4)) {
        case 0:
          path.append('.').append(KEYS[random.nextInt(3)]);
          break;
        case 1:
          path.append("['").append(KEYS[random.nextInt(KEYS.length)]).append("']");
          break;
        case 2:
          path.append('[').append(random.nextInt(4)).append(']');
          break;
        default:
          path.append("[\"").append(KEYS[random.nextInt(KEYS.length)]).append("\"]");
          break;
      }
    }
    return path.toString();
  }

  /// The fuzz corpora must actually reach the fast path, otherwise the test proves nothing.
  @Test
  public void testFuzzCorpusExercisesTheFastPath() {
    Random random = new Random(20260709L);
    List<String> handled = new ArrayList<>();
    for (int i = 0; i < 20_000; i++) {
      String json = randomJson(random, 0);
      String path = randomPath(random);
      if (SimpleJsonPath.compile(path) != null && FastJsonPathExtractor.canExtract(json)) {
        handled.add(path);
      }
    }
    assertTrue(handled.size() > 5_000, "fuzz corpus only reached the fast path " + handled.size() + " times");
  }
}
