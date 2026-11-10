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

import com.sun.net.httpserver.HttpServer;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.tools.utils.PinotConfigUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Exercises child JVM readiness and cleanup without starting a Pinot cluster. Test methods run sequentially because
/// they share the per-method temporary directory; the HTTP child handles requests on its own executor.
public class QuickstartServerProcessTest {
  private Path _processDir;
  private Path _pidFile;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _processDir = Files.createTempDirectory("quickstart-server-process-test");
    _pidFile = _processDir.resolve("child.pid");
    Files.createDirectory(_processDir.resolve("tmp"));
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown()
      throws Exception {
    // Also clean up a child if an assertion fails before the helper can stop it.
    if (Files.exists(_pidFile)) {
      ProcessHandle process = ProcessHandle.of(readPid()).orElse(null);
      if (process != null && process.isAlive()) {
        process.destroyForcibly();
        process.onExit().get(10, TimeUnit.SECONDS);
      }
    }
    FileUtils.deleteDirectory(_processDir.toFile());
  }

  @Test(timeOut = 30_000)
  public void testReadyChildAndRepeatedStop()
      throws Exception {
    int port = availablePort();
    QuickstartServerProcess server = QuickstartServerProcess.start(childCommand("ready", port), _processDir.toFile(),
        port, Duration.ofSeconds(15));
    try {
      assertNotEquals(readPid(), ProcessHandle.current().pid());
      assertEquals(server.getProcess().pid(), readPid());
      assertTrue(server.getProcess().isAlive());
      assertTrue(Files.exists(_processDir.resolve("health-checked")));
      List<String> arguments = Files.readAllLines(_processDir.resolve("jvm-arguments"));
      assertEquals(arguments.stream().filter(argument -> argument.startsWith("-Xmx")).toList(), List.of("-Xmx1g"));
      assertFalse(arguments.stream().anyMatch(argument -> argument.startsWith("-agentlib:")
          || argument.startsWith("-javaagent:")));
    } finally {
      server.stop();
    }
    server.stop();
    assertFalse(server.getProcess().isAlive());
  }

  @Test(timeOut = 30_000)
  public void testEarlyExitIsReported()
      throws Exception {
    int port = availablePort();
    IOException failure = expectThrows(IOException.class,
        () -> QuickstartServerProcess.start(childCommand("exit", port), _processDir.toFile(), port,
            Duration.ofSeconds(15)));
    assertTrue(failure.getMessage().contains("23"), failure.getMessage());
    assertTrue(failure.getMessage().contains(_processDir.toString()), failure.getMessage());
    assertFalse(isChildAlive());
  }

  @Test(timeOut = 30_000)
  public void testReadinessTimeoutStopsChild()
      throws Exception {
    int port = availablePort();
    expectThrows(IOException.class,
        () -> QuickstartServerProcess.start(childCommand("unready", port), _processDir.toFile(), port,
            Duration.ofSeconds(5)));
    assertTrue(Files.exists(_processDir.resolve("health-checked")), "The child must run but remain unready");
    assertFalse(isChildAlive(), "A failed startup must not leave its child running");
  }

  @Test(timeOut = 30_000)
  public void testExistingHealthyListenerDoesNotMakeChildReady()
      throws Exception {
    HttpServer existingServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    existingServer.createContext("/health/readiness", exchange -> {
      byte[] body = "OK".getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(200, body.length);
      exchange.getResponseBody().write(body);
      exchange.close();
    });
    existingServer.start();
    try {
      int port = existingServer.getAddress().getPort();
      // A marker from a previous run must not be accepted either.
      Files.writeString(_processDir.resolve(QuickstartServerProcess.STARTED_FILE_NAME), "stale");
      IOException failure = expectThrows(IOException.class,
          () -> QuickstartServerProcess.start(childCommand("no-marker", port), _processDir.toFile(), port,
              Duration.ofSeconds(5)));
      assertTrue(failure.getMessage().contains("Timed out"), failure.getMessage());
      assertFalse(isChildAlive(), "A child that did not start its own server must be stopped");
    } finally {
      existingServer.stop(0);
    }
  }

  @Test(timeOut = 30_000)
  public void testParentPipeClosureStopsChild()
      throws Exception {
    int port = availablePort();
    QuickstartServerProcess server = QuickstartServerProcess.start(childCommand("watch-parent", port),
        _processDir.toFile(), port, Duration.ofSeconds(15));
    try {
      server.getProcess().getOutputStream().close();
      assertTrue(server.getProcess().waitFor(10, TimeUnit.SECONDS), "The child must exit when its parent disappears");
      assertEquals(server.getProcess().exitValue(), 0);
    } finally {
      server.stop();
    }
  }

  @Test
  public void testConfigRoundTrip()
      throws Exception {
    File configFile = _processDir.resolve("server.properties").toFile();
    String directory = _processDir.resolve("data with spaces=and-unicode-\u00e9").toString();
    Map<String, Object> properties = Map.of("pinot.server.instance.dataDir", directory,
        "pinot.server.adminapi.port", 7500, "pinot.server.instance.enableThreadCpuTimeMeasurement", true,
        "test.list", List.of("first", "second"), "test.literal", "comma,value=with:punctuation");
    QuickstartServerProcess.writeConfig(properties, configFile);
    PinotConfiguration loaded = new PinotConfiguration(PinotConfigUtils.readConfigFromFile(configFile.toString()));
    assertEquals(loaded.getProperty("pinot.server.instance.dataDir"), directory);
    assertEquals(loaded.getProperty("pinot.server.adminapi.port", 0), 7500);
    assertTrue(loaded.getProperty("pinot.server.instance.enableThreadCpuTimeMeasurement", false));
    assertEquals(loaded.getCommaSeparatedList("test.list", List.of()), List.of("first", "second"));
    assertEquals(loaded.getProperty("test.literal"), properties.get("test.literal"));
  }

  private List<String> childCommand(String mode, int port) {
    List<String> command = new ArrayList<>(QuickstartServerProcess.buildCommand(
        _processDir.resolve("server.properties").toFile(), _processDir.toFile()));
    int mainIndex = command.indexOf(QuickstartServerProcess.class.getName());
    command.subList(mainIndex, command.size()).clear();
    command.addAll(List.of(TestServer.class.getName(), mode, Integer.toString(port), _pidFile.toString()));
    return command;
  }

  private static int availablePort()
      throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private long readPid()
      throws Exception {
    return Long.parseLong(Files.readString(_pidFile));
  }

  private boolean isChildAlive()
      throws Exception {
    return ProcessHandle.of(readPid()).map(ProcessHandle::isAlive).orElse(false);
  }

  /// Minimal child process used by these tests. Its mode and paths are immutable after startup; HTTP requests only
  /// write an idempotent readiness marker.
  public static final class TestServer {
    private TestServer() {
    }

    public static void main(String[] args)
        throws Exception {
      String mode = args[0];
      if (mode.equals("watch-parent")) {
        QuickstartServerProcess.watchParent();
      }
      Path pidFile = Path.of(args[2]);
      Files.writeString(pidFile, Long.toString(ProcessHandle.current().pid()));
      Files.write(pidFile.resolveSibling("jvm-arguments"), ManagementFactory.getRuntimeMXBean().getInputArguments());
      if (mode.equals("exit")) {
        System.exit(23);
      }
      if (mode.equals("no-marker")) {
        new CountDownLatch(1).await();
      }
      HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", Integer.parseInt(args[1])), 0);
      server.createContext("/health/readiness", exchange -> {
        Files.writeString(pidFile.resolveSibling("health-checked"), "checked");
        byte[] body = "OK".getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(mode.equals("unready") ? 503 : 200, body.length);
        exchange.getResponseBody().write(body);
        exchange.close();
      });
      server.start();
      Files.writeString(pidFile.resolveSibling(QuickstartServerProcess.STARTED_FILE_NAME), "started");
    }
  }
}
