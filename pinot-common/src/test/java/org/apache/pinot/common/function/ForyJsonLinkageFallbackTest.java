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
  private static final String CHILD_ARGUMENT = "verifyFallback";

  /// Child-process entry point used by [#testMissingForyCoreFallsBack()].
  public static void main(String[] arguments) {
    if (arguments.length != 1 || !CHILD_ARGUMENT.equals(arguments[0])) {
      throw new IllegalArgumentException("Expected the fallback verification argument");
    }
    String actual = JsonFunctions.jsonPathStringFory("{\"v\":7}", "$.v", "DEFAULT");
    if (!"7".equals(actual)) {
      throw new AssertionError("Expected Jayway fallback result 7, got: " + actual);
    }
  }

  @Test
  public void testMissingForyCoreFallsBack()
      throws Exception {
    String separator = System.getProperty("path.separator");
    StringJoiner childClassPath = new StringJoiner(separator);
    boolean foundForyCore = false;
    for (String entry : System.getProperty("java.class.path").split(Pattern.quote(separator))) {
      if (new File(entry).getName().startsWith("fory-core-")) {
        foundForyCore = true;
      } else {
        childClassPath.add(entry);
      }
    }
    assertTrue(foundForyCore, "Test classpath does not contain fory-core");

    String javaExecutable = new File(new File(System.getProperty("java.home"), "bin"), "java").getPath();
    ProcessBuilder processBuilder = new ProcessBuilder(javaExecutable, "-cp", childClassPath.toString(),
        ForyJsonLinkageFallbackTest.class.getName(), CHILD_ARGUMENT).redirectErrorStream(true);
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
