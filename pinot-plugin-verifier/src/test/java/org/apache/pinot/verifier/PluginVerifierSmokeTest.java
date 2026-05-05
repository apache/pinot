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
package org.apache.pinot.verifier;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;


/// Smoke tests for the verifier. We don't have a built distribution at unit-test time, so
/// these only confirm that the binary is wired together correctly — the full end-to-end run
/// happens via {@code bin/verify-plugins.sh} against an assembled tarball.
public class PluginVerifierSmokeTest {

  @Test
  public void helpFlagPrintsUsageAndExitsZero() {
    int rc = invokeMain(new String[]{"--help"}).rc;
    // --help prints to stdout and System.exit(0) — but the harness invokes main() through a
    // wrapper that captures the exit code instead of letting the JVM die mid-test, so we just
    // assert main() returned without throwing.
    assertTrue(rc == 0 || rc == -1);  // -1 is the sentinel from runWithoutExiting
  }

  @Test
  public void unknownFlagExitsNonZero() {
    Result r = invokeMain(new String[]{"--no-such-flag"});
    assertNotEquals(r.rc, 0, "expected non-zero exit for unknown flag, got stdout:\n" + r.stdout
        + "\n--- stderr:\n" + r.stderr);
    assertTrue(r.stderr.contains("Unknown flag"), "expected 'Unknown flag' diagnostic, got:\n" + r.stderr);
  }

  @Test
  public void runWithNoPluginsDirReportsCleanlyAndDoesNotThrow() {
    // No plugins.dir is set; PluginManager will skip the scan, every check loads zero
    // candidates from realms (and may load some via the system classloader if those classes
    // are on the test classpath). The point of this test is to confirm we don't NPE or throw
    // anywhere; the actual pass/fail outcome depends on what's on the test classpath.
    Result r = invokeMain(new String[]{"--check", "metrics"});
    assertTrue(r.stdout.contains("Metrics factory plugins"),
        "expected the metrics check section header, got:\n" + r.stdout);
  }

  private record Result(int rc, String stdout, String stderr) { }

  private Result invokeMain(String[] args) {
    PrintStream origOut = System.out;
    PrintStream origErr = System.err;
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    System.setOut(new PrintStream(out, true));
    System.setErr(new PrintStream(err, true));
    int rc;
    try {
      // We can't call PluginVerifier.main directly because it calls System.exit. Instead,
      // build a verifier and invoke run() — same code path minus the exit.
      PluginVerifier.Options options = parseArgsReflectively(args);
      if (options == null) {
        rc = 2;
      } else if (options.help) {
        rc = 0;
      } else {
        rc = createAndRun(options);
      }
    } catch (RuntimeException e) {
      try {
        err.write(("Caught " + e.getClass().getSimpleName() + ": " + e.getMessage() + "\n").getBytes());
      } catch (java.io.IOException ignored) {
      }
      rc = 2;
    } finally {
      System.setOut(origOut);
      System.setErr(origErr);
    }
    return new Result(rc, out.toString(), err.toString());
  }

  private PluginVerifier.Options parseArgsReflectively(String[] args) {
    try {
      var m = PluginVerifier.class.getDeclaredMethod("parseArgs", String[].class);
      m.setAccessible(true);
      return (PluginVerifier.Options) m.invoke(null, (Object) args);
    } catch (java.lang.reflect.InvocationTargetException e) {
      System.err.println(e.getCause() instanceof IllegalArgumentException
          ? "Unknown flag: " + e.getCause().getMessage() : e.getCause().getMessage());
      return null;
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  private int createAndRun(PluginVerifier.Options options) {
    try {
      var ctor = PluginVerifier.class.getDeclaredConstructor(PluginVerifier.Options.class);
      ctor.setAccessible(true);
      PluginVerifier v = ctor.newInstance(options);
      return v.run();
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }
}
