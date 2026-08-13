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

import com.fasterxml.jackson.core.StreamReadConstraints;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.function.scalar.JsonFunctions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Differential and registry coverage for the opt-in Fory-backed JSON path scalar functions.
public class ForyJsonPathFunctionsTest {

  @DataProvider(name = "stringCases")
  public Object[][] stringCases() {
    return new Object[][]{
        {"{\"user\":{\"country\":\"US\"}}", "$.user.country"},
        {"{\"items\":[{\"sku\":\"A1\"},{\"sku\":\"B7\"}]}", "$.items[1].sku"},
        {"{\"v\":2147483648}", "$.v"},
        {"{\"v\":9223372036854775808}", "$.v"},
        {"{\"v\":1.2345678901234567}", "$.v"},
        {"{\"v\":123456789012345678901234567890.123}", "$.v"},
        {"{\"v\":true}", "$.v"},
        {"{\"v\":\"café 😀\"}", "$.v"},
        {"{\"v\":[1,2,3]}", "$.v"},
        {"{\"v\":{\"nested\":1}}", "$.v"},
        {"{\"v\":1,\"v\":2}", "$.v"}
    };
  }

  @Test(dataProvider = "stringCases")
  public void testStringParity(String json, String path) {
    assertEquals(JsonFunctions.jsonPathStringFory(json, path, "DEFAULT"),
        JsonFunctions.jsonPathString(json, path, "DEFAULT"));
  }

  @Test
  public void testTypedFunctionsAndDefaults() {
    String json = "{\"n\":9223372036854775806,\"d\":19.75,\"numeric\":\"41\",\"nil\":null}";
    assertEquals(JsonFunctions.jsonPathLongFory(json, "$.n", -1L),
        JsonFunctions.jsonPathLong(json, "$.n", -1L));
    assertEquals(JsonFunctions.jsonPathLongFory(json, "$.numeric", -1L), 41L);
    assertEquals(JsonFunctions.jsonPathDoubleFory(json, "$.d", -1d),
        JsonFunctions.jsonPathDouble(json, "$.d", -1d));
    assertEquals(JsonFunctions.jsonPathStringFory(json, "$.missing", "DEFAULT"), "DEFAULT");
    assertEquals(JsonFunctions.jsonPathStringFory(json, "$.nil", "DEFAULT"), "DEFAULT");
    assertEquals(JsonFunctions.jsonPathLongFory("plain text", "$.n", -7L), -7L);
    assertEquals(JsonFunctions.jsonPathDoubleFory("{broken", "$.d", -7.5d), -7.5d);
    assertEquals(JsonFunctions.jsonPathStringFory(null, "$.v", "DEFAULT"), "DEFAULT");
  }

  @Test
  public void testStreamingParserIsAvailable() {
    assertTrue(ForyJsonPathExtractor.isAvailable());
    SimpleJsonPath path = SimpleJsonPath.compile("$.nested.value");
    assertNotNull(path);
    assertEquals(ForyJsonPathExtractor.extract("{\"n\":7,\"nested\":{\"value\":8}}", path), 8L);
  }

  @Test
  public void testJaywayFallbacks() {
    String complex = "{\"left\":{\"country\":\"US\"},\"right\":{\"country\":\"DE\"}}";
    assertEquals(JsonFunctions.jsonPathStringFory(complex, "$..country", "DEFAULT"),
        JsonFunctions.jsonPathString(complex, "$..country", "DEFAULT"));

    String trailingContent = "{\"v\":7} {\"ignored\":true}";
    assertEquals(JsonFunctions.jsonPathStringFory(trailingContent, "$.v", "DEFAULT"),
        JsonFunctions.jsonPathString(trailingContent, "$.v", "DEFAULT"));

    String malformedAfterMatch = "{\"v\":7,\"broken\":[}";
    assertEquals(JsonFunctions.jsonPathStringFory(malformedAfterMatch, "$.v", "DEFAULT"),
        JsonFunctions.jsonPathString(malformedAfterMatch, "$.v", "DEFAULT"));

    Map<String, Object> parsed = Map.of("v", Map.of("n", 42));
    assertEquals(JsonFunctions.jsonPathLongFory(parsed, "$.v.n", -1L),
        JsonFunctions.jsonPathLong(parsed, "$.v.n", -1L));

    StringBuilder deepJson = new StringBuilder();
    StringBuilder deepPath = new StringBuilder("$");
    for (int i = 0; i < 25; i++) {
      deepJson.append("{\"a\":");
      deepPath.append(".a");
    }
    deepJson.append("\"value\"");
    for (int i = 0; i < 25; i++) {
      deepJson.append('}');
    }
    assertEquals(JsonFunctions.jsonPathStringFory(deepJson.toString(), deepPath.toString(), "DEFAULT"), "value");
  }

  @Test
  public void testJacksonConstraintFallbacks() {
    StreamReadConstraints constraints = StreamReadConstraints.defaults();

    String oversizedNumber = "1".repeat(constraints.getMaxNumberLength() + 1);
    String numberDocument = "{\"oversized\":" + oversizedNumber + ",\"v\":7}";
    assertEquals(JsonFunctions.jsonPathLongFory(numberDocument, "$.v", -1),
        JsonFunctions.jsonPathLong(numberDocument, "$.v", -1));

    String oversizedName = "n".repeat(constraints.getMaxNameLength() + 1);
    String nameDocument = "{\"" + oversizedName + "\":1,\"v\":7}";
    assertEquals(JsonFunctions.jsonPathLongFory(nameDocument, "$.v", -1),
        JsonFunctions.jsonPathLong(nameDocument, "$.v", -1));
  }

  @Test
  public void testStreamingExtractorNavigatesSimplePaths() {
    assertEquals(extract("[{\"v\":1},{\"v\":2}]", "$[1].v"), 2L);
    assertEquals(extract("{\"items\":[0,{\"detail\":{\"value\":3}}]}", "$.items[1].detail.value"), 3L);
    assertEquals(extract("{\"a-b\":{\"café\":4}}", "$['a-b'].café"), 4L);
  }

  @Test
  public void testStreamingExtractorUsesLastDuplicate() {
    assertEquals(extract("{\"v\":1,\"v\":2}", "$.v"), 2L);
    assertEquals(extract("{\"a\":{\"v\":1},\"a\":{\"v\":2}}", "$.a.v"), 2L);
    assertNull(extract("{\"a\":{\"v\":1},\"a\":7}", "$.a.v"));
    assertNull(extract("{\"v\":1,\"v\":null}", "$.v"));
  }

  @Test
  public void testStreamingExtractorReturnsNullForUnresolvedPaths() {
    assertNull(extract("{\"a\":{\"v\":1}}", "$.missing"));
    assertNull(extract("{\"a\":{\"v\":null}}", "$.a.v"));
    assertNull(extract("{\"a\":7}", "$.a.v"));
    assertNull(extract("{\"a\":[]}", "$.a[1]"));
  }

  @Test
  public void testStreamingExtractorSignalsContainerFallbackWithoutExceptions() {
    assertTrue(ForyJsonPathExtractor.isFallbackRequired(extract("{\"v\":{\"n\":1}}", "$.v")));
    assertTrue(ForyJsonPathExtractor.isFallbackRequired(extract("{\"v\":[1,2]}", "$.v")));
    assertEquals(extract("{\"v\":{},\"v\":2}", "$.v"), 2L);
    assertTrue(ForyJsonPathExtractor.isFallbackRequired(extract("{\"v\":2,\"v\":{}}", "$.v")));
    assertNull(extract("{\"v\":{},\"v\":null}", "$.v"));

    SimpleJsonPath path = SimpleJsonPath.compile("$.v");
    assertNotNull(path);
    assertThrows(RuntimeException.class,
        () -> ForyJsonPathExtractor.extract("{\"v\":{},\"broken\":[}", path));
    assertEquals(ForyJsonPathExtractor.extract("{\"v\":3}", path), 3L);
  }

  @Test
  public void testStreamingExtractorUsesJacksonNestingLimit() {
    int depth = 25;
    StringBuilder selectedJson = new StringBuilder();
    StringBuilder selectedPath = new StringBuilder("$");
    for (int i = 0; i < depth; i++) {
      selectedJson.append("{\"a\":");
      selectedPath.append(".a");
    }
    selectedJson.append('7');
    selectedJson.append("}".repeat(depth));
    assertEquals(extract(selectedJson.toString(), selectedPath.toString()), 7L);

    String unrelated = "{\"selected\":1,\"deep\":" + "{\"a\":".repeat(depth) + "7" + "}".repeat(depth)
        + "}";
    assertEquals(extract(unrelated, "$.selected"), 1L);

    int maximumDepth = StreamReadConstraints.defaults().getMaxNestingDepth();
    String maximumJson = "{\"a\":".repeat(maximumDepth) + "7" + "}".repeat(maximumDepth);
    assertEquals(extract(maximumJson, "$." + "a.".repeat(maximumDepth - 1) + "a"), 7L);

    String oversizedJson = "{\"a\":".repeat(maximumDepth + 1) + "7" + "}".repeat(maximumDepth + 1);
    SimpleJsonPath oversizedPath = SimpleJsonPath.compile("$." + "a.".repeat(maximumDepth) + "a");
    assertNotNull(oversizedPath);
    assertThrows(RuntimeException.class, () -> ForyJsonPathExtractor.extract(oversizedJson, oversizedPath));
  }

  @Test
  public void testStreamingExtractorRejectsMalformedAndTrailingContent() {
    SimpleJsonPath path = SimpleJsonPath.compile("$.v");
    assertNotNull(path);
    assertThrows(RuntimeException.class, () -> ForyJsonPathExtractor.extract("{\"v\":1,\"broken\":[}", path));
    assertEquals(ForyJsonPathExtractor.extract("{\"v\":2}", path), 2L);
    assertThrows(RuntimeException.class, () -> ForyJsonPathExtractor.extract("{\"v\":1} {\"ignored\":2}", path));
    assertEquals(ForyJsonPathExtractor.extract("{\"v\":3}", path), 3L);
  }

  @Test
  public void testStreamingExtractorJacksonConstraintBoundaries() {
    StreamReadConstraints constraints = StreamReadConstraints.defaults();
    SimpleJsonPath valuePath = SimpleJsonPath.compile("$.v");
    assertNotNull(valuePath);

    String maximumNumber = "1".repeat(constraints.getMaxNumberLength());
    assertEquals(ForyJsonPathExtractor.extract("{\"v\":" + maximumNumber + "}", valuePath).toString(),
        maximumNumber);
    String oversizedNumber = maximumNumber + '1';
    assertThrows(RuntimeException.class,
        () -> ForyJsonPathExtractor.extract("{\"v\":" + oversizedNumber + "}", valuePath));
    assertThrows(RuntimeException.class,
        () -> ForyJsonPathExtractor.extract("{\"oversized\":" + oversizedNumber + ",\"v\":7}", valuePath));

    String maximumName = "n".repeat(constraints.getMaxNameLength());
    assertEquals(ForyJsonPathExtractor.extract("{\"" + maximumName + "\":1,\"v\":7}", valuePath), 7L);
    String oversizedName = maximumName + 'n';
    assertThrows(RuntimeException.class,
        () -> ForyJsonPathExtractor.extract("{\"" + oversizedName + "\":1,\"v\":7}", valuePath));
  }

  @Test
  public void testStreamingExtractorDoesNotLeakContextAcrossThreads()
      throws Exception {
    int numThreads = 8;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch ready = new CountDownLatch(numThreads);
    CountDownLatch start = new CountDownLatch(1);
    try {
      List<Future<?>> futures = new ArrayList<>(numThreads);
      for (int worker = 0; worker < numThreads; worker++) {
        int expected = worker;
        futures.add(executor.submit(() -> {
          SimpleJsonPath workerPath = SimpleJsonPath.compile("$.worker");
          SimpleJsonPath nestedPath = SimpleJsonPath.compile("$.nested.worker");
          assertNotNull(workerPath);
          assertNotNull(nestedPath);
          String json = "{\"worker\":" + expected + ",\"nested\":{\"worker\":" + (expected + 100) + "}}";
          ready.countDown();
          assertTrue(start.await(10, TimeUnit.SECONDS));
          for (int iteration = 0; iteration < 100; iteration++) {
            if ((iteration & 1) == 0) {
              assertEquals(ForyJsonPathExtractor.extract(json, workerPath), (long) expected);
            } else {
              assertEquals(ForyJsonPathExtractor.extract(json, nestedPath), (long) expected + 100);
            }
          }
          return null;
        }));
      }
      assertTrue(ready.await(10, TimeUnit.SECONDS));
      start.countDown();
      for (Future<?> future : futures) {
        future.get(30, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdownNow();
      executor.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testFunctionsResolveThroughRegistry()
      throws Exception {
    String json = "{\"user\":{\"country\":\"US\",\"age\":41,\"score\":9.5}}";
    assertEquals(invoke("jsonPathStringFory", json, "$.user.country", "DEFAULT"), "US");
    assertEquals(invoke("jsonPathLongFory", json, "$.user.age", -7L), 41L);
    assertEquals(invoke("jsonPathDoubleFory", json, "$.user.score", -7.5d), 9.5d);
  }

  private static Object invoke(String name, Object... arguments)
      throws Exception {
    FunctionInfo functionInfo =
        FunctionRegistry.lookupFunctionInfo(FunctionRegistry.canonicalize(name), arguments.length);
    assertNotNull(functionInfo, name + "/" + arguments.length + " is not registered");
    FunctionInvoker invoker = new FunctionInvoker(functionInfo);
    Object[] copy = arguments.clone();
    invoker.convertTypes(copy);
    return invoker.invoke(copy);
  }

  private static Object extract(String json, String jsonPath) {
    SimpleJsonPath path = SimpleJsonPath.compile(jsonPath);
    assertNotNull(path);
    return ForyJsonPathExtractor.extract(json, path);
  }
}
