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
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.pinot.common.function.scalar.JsonFunctions;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Verifies that an optional Fory runtime linkage failure does not break the JSON functions.
public class ForyJsonLinkageFallbackTest {
  private static final String FALLBACK_ARGUMENT = "verifyFallback";
  private static final String TOKEN_LIMIT_ARGUMENT = "verifyTokenLimit";

  /// Child-process entry point used by the missing-Fory fallback tests.
  public static void main(String[] arguments) {
    if (arguments.length != 1) {
      throw new IllegalArgumentException("Expected one child-process verification argument");
    }
    if (TOKEN_LIMIT_ARGUMENT.equals(arguments[0])) {
      verifyConfiguredTokenLimit();
      return;
    }
    if (!FALLBACK_ARGUMENT.equals(arguments[0])) {
      throw new IllegalArgumentException("Unknown child-process verification argument: " + arguments[0]);
    }
    String actual = JsonFunctions.jsonPathStringFory("{\"v\":7}", "$.v", "DEFAULT");
    if (!"7".equals(actual)) {
      throw new AssertionError("Expected Jayway fallback result 7, got: " + actual);
    }
    long longValue = JsonFunctions.jsonPathLongFory("{\"v\":7}", "$.v", -1L);
    if (longValue != 7L) {
      throw new AssertionError("Expected Jayway fallback long 7, got: " + longValue);
    }
    double doubleValue = JsonFunctions.jsonPathDoubleFory("{\"v\":7.5}", "$.v", -1d);
    if (doubleValue != 7.5d) {
      throw new AssertionError("Expected Jayway fallback double 7.5, got: " + doubleValue);
    }
  }

  @Test
  public void testMissingForyCoreFallsBack()
      throws Exception {
    runChild(false, true, FALLBACK_ARGUMENT);
  }

  @Test
  public void testMissingForyJsonFallsBack()
      throws Exception {
    runChild(true, false, FALLBACK_ARGUMENT);
  }

  @Test
  public void testConfiguredJacksonTokenLimit()
      throws Exception {
    runChild(false, false, TOKEN_LIMIT_ARGUMENT);
  }

  private static void verifyConfiguredTokenLimit() {
    StreamReadConstraints constraints = StreamReadConstraints.builder().maxTokenCount(3).build();
    StreamReadConstraints.overrideDefaultStreamReadConstraints(constraints);
    SimpleJsonPath path = SimpleJsonPath.compile("$.v");
    if (path == null) {
      throw new AssertionError("Expected a simple JSON path");
    }
    try {
      ForyJsonPathExtractor.extract("{\"v\":7}", path);
      throw new AssertionError("Expected Fory to enforce Jackson's configured token limit");
    } catch (IllegalArgumentException expected) {
      // Expected: START_OBJECT, FIELD_NAME, VALUE_NUMBER_INT, END_OBJECT exceeds the configured limit of three.
    }
  }

  private static void runChild(boolean removeForyJson, boolean removeForyCore, String childArgument)
      throws Exception {
    String separator = System.getProperty("path.separator");
    StringJoiner childClassPath = new StringJoiner(separator);
    boolean foundForyCore = false;
    boolean foundForyJson = false;
    for (String entry : System.getProperty("java.class.path").split(Pattern.quote(separator))) {
      String fileName = new File(entry).getName();
      if (fileName.startsWith("fory-core-")) {
        foundForyCore = true;
        if (!removeForyCore) {
          childClassPath.add(entry);
        }
      } else if (fileName.startsWith("fory-json-")) {
        foundForyJson = true;
        if (!removeForyJson) {
          childClassPath.add(entry);
        }
      } else {
        childClassPath.add(entry);
      }
    }
    assertTrue(foundForyCore, "Test classpath does not contain fory-core");
    assertTrue(foundForyJson, "Test classpath does not contain fory-json");

    String javaExecutable = new File(new File(System.getProperty("java.home"), "bin"), "java").getPath();
    ProcessBuilder processBuilder = new ProcessBuilder(javaExecutable, "-cp", childClassPath.toString(),
        ForyJsonLinkageFallbackTest.class.getName(), childArgument).redirectErrorStream(true);
    processBuilder.environment().remove("JAVA_TOOL_OPTIONS");
    processBuilder.environment().remove("JDK_JAVA_OPTIONS");

    Process process = processBuilder.start();
    boolean exited = process.waitFor(30, TimeUnit.SECONDS);
    if (!exited) {
      process.destroyForcibly();
      process.waitFor(30, TimeUnit.SECONDS);
    }
    String output;
    try (InputStream input = process.getInputStream()) {
      output = new String(input.readAllBytes(), StandardCharsets.UTF_8);
    }
    assertTrue(exited, "Fallback child process timed out: " + output);
    assertEquals(process.exitValue(), 0, output);
  }
}
