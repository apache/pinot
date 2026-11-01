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
package org.apache.pinot.integration.tests;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Verifies the custom suite stops after a shared fixture cleanup failure.
public class CustomSuiteHealthCheckTest {
  @Test
  public void testCleanupFailureWithContinuePolicy()
      throws Exception {
    runInSeparateJvm("continue");
  }

  @Test
  public void testCleanupFailureWithDefaultPolicy()
      throws Exception {
    runInSeparateJvm("default");
  }

  /// Runs nested TestNG outside the outer TestNG engine, so nested listeners cannot corrupt its state.
  private static void runInSeparateJvm(String policy)
      throws Exception {
    String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
    String classPath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    Path output = Files.createTempFile("custom-suite-health-", ".log");
    Process process = null;
    try {
      process = new ProcessBuilder(java, "-cp", classPath, CustomSuiteHealthProbe.class.getName(), policy)
          .redirectErrorStream(true).redirectOutput(output.toFile()).start();
      boolean completed = process.waitFor(120, TimeUnit.SECONDS);
      if (!completed) {
        process.destroyForcibly();
        process.waitFor(10, TimeUnit.SECONDS);
      }
      String childOutput = Files.readString(output);
      assertTrue(completed, "Health-check subprocess timed out: " + childOutput);
      assertEquals(process.exitValue(), 0, "Health-check subprocess failed: " + childOutput);
    } finally {
      if (process != null && process.isAlive()) {
        process.destroyForcibly();
      }
      Files.deleteIfExists(output);
    }
  }
}
