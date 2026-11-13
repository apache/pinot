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
package org.apache.pinot.compat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Verifies that verifier server logs survive abrupt JVM termination. Each invocation owns its child and log directory.
public class ServerLoggingTest {
  @Test(timeOut = 30_000)
  public void testFinalMessagesSurviveAbruptExit()
      throws Exception {
    Path processDir = Files.createTempDirectory("compat server logging ");
    Process process = null;
    try {
      Path logConfig = processDir.resolve("server log4j2.xml");
      Files.copy(Path.of(System.getProperty("basedir", "."), "..", "compatibility-verifier", "server-log4j2.xml"),
          logConfig);
      Path consoleLog = processDir.resolve("console.log");
      ProcessBuilder builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
          "-cp", System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
          LogProbe.class.getName()).redirectErrorStream(true).redirectOutput(consoleLog.toFile());
      builder.environment().put("LOG4J_CONFIGURATION_FILE", logConfig.toString());
      builder.environment().put("LOG_ROOT", processDir.toString());
      builder.environment().put("PINOT_COMPONENT", "server");
      process = builder.start();
      assertTrue(process.waitFor(20, TimeUnit.SECONDS), "Log probe did not exit");
      assertEquals(process.exitValue(), 0, Files.readString(consoleLog));
      Path serverLog = processDir.resolve("pinot-server.log");
      assertTrue(Files.exists(serverLog), Files.readString(consoleLog));
      String messages = Files.readString(serverLog);
      assertTrue(messages.contains("SERVER_FINAL_MARKER"), messages);
      assertTrue(messages.contains("HELIX_FINAL_MARKER"), messages);
    } finally {
      if (process != null && process.isAlive()) {
        process.destroyForcibly();
        process.waitFor(5, TimeUnit.SECONDS);
      }
      FileUtils.deleteDirectory(processDir.toFile());
    }
  }

  /// Emits final diagnostic messages on the main thread and exits without running logging shutdown hooks.
  public static final class LogProbe {
    private LogProbe() {
    }

    public static void main(String[] args) {
      LogManager.getLogger("org.apache.pinot.server.starter.helix.BaseServerStarter").info("SERVER_FINAL_MARKER");
      LogManager.getLogger("org.apache.helix.participant.HelixStateMachineEngine").info("HELIX_FINAL_MARKER");
      Runtime.getRuntime().halt(0);
    }
  }
}
