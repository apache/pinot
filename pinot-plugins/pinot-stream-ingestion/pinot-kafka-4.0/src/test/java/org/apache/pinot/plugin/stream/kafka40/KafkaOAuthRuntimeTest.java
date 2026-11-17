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
package org.apache.pinot.plugin.stream.kafka40;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.security.oauthbearer.BrokerJwtValidator;
import org.apache.kafka.common.security.oauthbearer.DefaultJwtValidator;
import org.apache.kafka.common.utils.Utils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Verifies OAuth initialization without Kafka server dependencies supplied only by the test kit.
public class KafkaOAuthRuntimeTest {
  @Test
  public void testOAuthValidatorsOnRuntimeClasspath()
      throws Exception {
    String runtimeClasspath = Files.readString(Path.of("target", "runtime-classpath.txt")).trim();
    String testClasses = Path.of(RuntimeCheck.class.getProtectionDomain().getCodeSource().getLocation().toURI())
        .toString();
    Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "-cp", testClasses + File.pathSeparator + runtimeClasspath, RuntimeCheck.class.getName())
        .redirectErrorStream(true).start();
    try {
      assertTrue(process.waitFor(30, TimeUnit.SECONDS), "OAuth runtime initialization did not finish");
      assertEquals(process.exitValue(), 0,
          new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
    } finally {
      process.destroyForcibly();
    }
  }

  /// Runs in a separate JVM so test-kit dependencies cannot conceal missing runtime libraries.
  public static class RuntimeCheck {
    private RuntimeCheck() {
    }

    public static void main(String[] args) {
      Utils.newInstance(DefaultJwtValidator.class);
      Utils.newInstance(BrokerJwtValidator.class);
    }
  }
}
