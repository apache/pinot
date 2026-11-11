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
package org.apache.pinot.tools.admin.command;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Verifies the heap limits used by actual child JVMs. Tests execute sequentially because constructing their commands
/// temporarily changes the dedicated quickstart heap system properties, restoring them before launching a child.
public class QuickstartServerHeapTest {
  @DataProvider
  public Object[][] heapSizes() {
    return new Object[][]{
        {null, null, "1g", "1g", 1024L * 1024 * 1024},
        {null, "512m", "512m", "512m", 512L * 1024 * 1024},
        {"64m", "192m", "64m", "192m", 192L * 1024 * 1024}
    };
  }

  @Test(dataProvider = "heapSizes", timeOut = 30_000)
  public void testChildHeap(String initialHeap, String maxHeap, String expectedInitialHeap, String expectedMaxHeap,
      long expectedMaxBytes)
      throws Exception {
    Path processDir = Files.createTempDirectory("quickstart-server-heap-test");
    Process process = null;
    try {
      Files.createDirectory(processDir.resolve("tmp"));
      Path reportFile = processDir.resolve("heap.properties");
      List<String> command = buildProbeCommand(processDir, reportFile, initialHeap, maxHeap);
      Path logFile = processDir.resolve("probe.log");
      process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(logFile.toFile()).start();
      assertTrue(process.waitFor(15, TimeUnit.SECONDS), "Heap probe did not exit");
      assertEquals(process.exitValue(), 0, Files.readString(logFile));
      Properties report = new Properties();
      try (InputStream input = Files.newInputStream(reportFile)) {
        report.load(input);
      }
      List<String> arguments = List.of(report.getProperty("arguments").split("\n"));
      assertEquals(arguments.stream().filter(argument -> argument.startsWith("-Xms")).toList(),
          List.of("-Xms" + expectedInitialHeap));
      assertEquals(arguments.stream().filter(argument -> argument.startsWith("-Xmx")).toList(),
          List.of("-Xmx" + expectedMaxHeap));
      long maxBytes = Long.parseLong(report.getProperty("maxMemory"));
      assertTrue(maxBytes <= expectedMaxBytes, "Child exceeded its configured heap limit: " + maxBytes);
      // Some collectors exclude a survivor region from Runtime.maxMemory().
      assertTrue(maxBytes >= expectedMaxBytes * 3 / 4, "Child did not apply the requested heap limit: " + maxBytes);
    } finally {
      if (process != null && process.isAlive()) {
        process.destroyForcibly();
        process.waitFor(10, TimeUnit.SECONDS);
      }
      FileUtils.deleteDirectory(processDir.toFile());
    }
  }

  private static List<String> buildProbeCommand(Path processDir, Path reportFile, String initialHeap, String maxHeap) {
    String previousInitialHeap = System.getProperty(QuickstartServerProcess.INITIAL_HEAP_PROPERTY);
    String previousMaxHeap = System.getProperty(QuickstartServerProcess.MAX_HEAP_PROPERTY);
    List<String> command;
    try {
      setProperty(QuickstartServerProcess.INITIAL_HEAP_PROPERTY, initialHeap);
      setProperty(QuickstartServerProcess.MAX_HEAP_PROPERTY, maxHeap);
      command = new ArrayList<>(QuickstartServerProcess.buildCommand(processDir.resolve("server.properties").toFile(),
          processDir.toFile()));
    } finally {
      setProperty(QuickstartServerProcess.INITIAL_HEAP_PROPERTY, previousInitialHeap);
      setProperty(QuickstartServerProcess.MAX_HEAP_PROPERTY, previousMaxHeap);
    }
    command.set(command.indexOf("-cp") + 1,
        System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")));
    int mainIndex = command.indexOf(QuickstartServerProcess.class.getName());
    command.subList(mainIndex, command.size()).clear();
    command.addAll(List.of(HeapProbe.class.getName(), reportFile.toString()));
    return command;
  }

  private static void setProperty(String name, String value) {
    if (value == null) {
      System.clearProperty(name);
    } else {
      System.setProperty(name, value);
    }
  }

  /// Reports this JVM's heap settings and exits. The probe only runs on its main thread.
  public static final class HeapProbe {
    private HeapProbe() {
    }

    public static void main(String[] args)
        throws Exception {
      Properties report = new Properties();
      report.setProperty("arguments", String.join("\n", ManagementFactory.getRuntimeMXBean().getInputArguments()));
      report.setProperty("maxMemory", Long.toString(Runtime.getRuntime().maxMemory()));
      try (OutputStream output = Files.newOutputStream(Path.of(args[0]))) {
        report.store(output, null);
      }
    }
  }
}
