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
package org.apache.pinot.query.mailbox;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/**
 * Tests disabled startup in a fresh JVM without inherited module opens or Arrow allocator initialization.
 */
public class MailboxArrowDisabledStartupTest {
  @Test
  public void testDisabledStartupWithoutArrowJvmOptions()
      throws Exception {
    String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    Path output = Files.createTempFile("pinot-arrow-disabled-startup-", ".log");
    ProcessBuilder builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "-Xlog:class+init=info", "-cp", classpath, DisabledStartupProbe.class.getName())
        .redirectErrorStream(true).redirectOutput(output.toFile());
    for (String key : List.of("JAVA_TOOL_OPTIONS", "_JAVA_OPTIONS", "JDK_JAVA_OPTIONS")) {
      builder.environment().remove(key);
    }
    Process process = null;
    try {
      process = builder.start();
      boolean exited = process.waitFor(60, TimeUnit.SECONDS);
      String log = Files.readString(output);
      assertTrue(exited, "Disabled startup JVM timed out:\n" + log);
      assertEquals(process.exitValue(), 0, log);
      assertTrue(log.contains("Initializing 'org/apache/pinot/query/runtime/memory/ArrowBuffers'"), log);
      assertFalse(log.contains("Initializing 'org/apache/arrow/memory/"),
          "Disabled startup initialized Arrow allocator machinery:\n" + log);
    } finally {
      try {
        if (process != null && process.isAlive()) {
          process.destroyForcibly();
          assertTrue(process.waitFor(10, TimeUnit.SECONDS), "Disabled startup JVM did not terminate");
        }
      } finally {
        Files.deleteIfExists(output);
      }
    }
  }

  /** Single-threaded subprocess entry point exercising real broker/server services without any module-opening flags. */
  public static final class DisabledStartupProbe {
    private DisabledStartupProbe() {
    }

    public static void main(String[] args)
        throws Exception {
      assertFalse(Object.class.getModule().isOpen("java.nio", DisabledStartupProbe.class.getModule()));
      for (InstanceType instanceType : List.of(InstanceType.BROKER, InstanceType.SERVER)) {
        verifyStartup(instanceType, new PinotConfiguration());
        verifyStartup(instanceType, new PinotConfiguration(
            Map.of(CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_USE_ARROW, "false")));
      }
    }

    private static void verifyStartup(InstanceType instanceType, PinotConfiguration configuration)
        throws Exception {
      MailboxService mailbox = new MailboxService("localhost", 0, instanceType, configuration);
      try {
        assertFalse(mailbox.isArrowEnabled());
        assertNull(FieldUtils.readField(mailbox.getArrowBuffers(), "_root", true));
        mailbox.start();
        WorkerMetadata worker = new WorkerMetadata(0, Map.of(), Map.of());
        OpChainExecutionContext context = OpChainExecutionContext.fromQueryContext(mailbox, Map.of(),
            new StageMetadata(0, List.of(worker), Map.of()), worker, null, false, false,
            QueryExecutionContext.forMseTest());
        try {
          assertFalse(context.isArrowEnabled());
          expectThrows(IllegalStateException.class, context::getOrCreateArrowContext);
        } finally {
          context.closeArrowResources();
        }
        assertNull(FieldUtils.readField(mailbox.getArrowBuffers(), "_root", true));
      } finally {
        mailbox.shutdown();
      }
      assertNull(FieldUtils.readField(mailbox.getArrowBuffers(), "_root", true));
    }
  }
}
