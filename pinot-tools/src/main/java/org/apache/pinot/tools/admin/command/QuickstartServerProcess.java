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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.convert.LegacyListDelimiterHandler;
import org.apache.commons.configuration2.io.FileHandler;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.tools.admin.PinotAdministrator;


/// Owns one quickstart server JVM, its configuration, and its startup log. Startup waits for the server's readiness
/// endpoint. The child exits when its parent's stdin pipe closes, including when the parent is killed abruptly.
/// [#stop] is synchronized and can be called repeatedly or concurrently with waiting for readiness.
final class QuickstartServerProcess {
  static final String STARTED_FILE_NAME = "server.started";
  private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(2);
  private static final long SHUTDOWN_TIMEOUT_SECONDS = 30;
  private static final long FORCED_SHUTDOWN_TIMEOUT_SECONDS = 10;

  private final Process _process;
  private final File _logFile;
  private final File _startedFile;
  private final int _port;

  private QuickstartServerProcess(Process process, File logFile, File startedFile, int port) {
    _process = process;
    _logFile = logFile;
    _startedFile = startedFile;
    _port = port;
  }

  static QuickstartServerProcess launch(Map<String, Object> effectiveConfig, File processDir)
      throws Exception {
    Files.createDirectories(processDir.toPath());
    Files.createDirectories(new File(processDir, "tmp").toPath());
    File configFile = new File(processDir, "server.properties");
    writeConfig(effectiveConfig, configFile);
    int port = Integer.parseInt(effectiveConfig.get(CommonConstants.Server.CONFIG_OF_ADMIN_API_PORT).toString());
    return launch(buildCommand(configFile, processDir), processDir, port);
  }

  static void writeConfig(Map<String, Object> effectiveConfig, File configFile)
      throws Exception {
    PropertiesConfiguration configuration = new PropertiesConfiguration();
    configuration.setListDelimiterHandler(new LegacyListDelimiterHandler(','));
    configuration.getLayout().setForceSingleLine(true);
    effectiveConfig.forEach(configuration::setProperty);
    new FileHandler(configuration).save(configFile);
  }

  static List<String> buildCommand(File configFile, File processDir) {
    List<String> command = new ArrayList<>();
    File javaExecutable = new File(System.getProperty("java.home"), "bin/java");
    command.add(javaExecutable.getAbsolutePath());
    command.add("-Xms128m");
    command.add("-Xmx1g");
    Iterator<String> parentArguments = ManagementFactory.getRuntimeMXBean().getInputArguments().iterator();
    while (parentArguments.hasNext()) {
      String argument = parentArguments.next();
      for (String option : List.of("--add-opens", "--add-exports", "--add-modules", "--enable-native-access")) {
        if (argument.startsWith(option + "=")) {
          command.add(argument);
          break;
        }
        if (argument.equals(option) && parentArguments.hasNext()) {
          command.add(argument);
          command.add(parentArguments.next());
          break;
        }
      }
    }
    for (String name : System.getProperties().stringPropertyNames()) {
      if (name.startsWith("plugins.") || name.startsWith("log4j.") || name.startsWith("log4j2.")
          || name.startsWith("io.netty.") || name.startsWith("io.grpc.netty.")) {
        command.add("-D" + name + "=" + System.getProperty(name));
      }
    }
    command.add("-Djava.io.tmpdir=" + new File(processDir, "tmp").getAbsolutePath());
    command.add("-Dpinot.admin.system.exit=true");
    command.add("-cp");
    command.add(System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")));
    command.add(QuickstartServerProcess.class.getName());
    command.add(configFile.getAbsolutePath());
    command.add(new File(processDir, STARTED_FILE_NAME).getAbsolutePath());
    return command;
  }

  static QuickstartServerProcess start(List<String> command, File processDir, int port, Duration timeout)
      throws Exception {
    QuickstartServerProcess serverProcess = launch(command, processDir, port);
    serverProcess.awaitReady(timeout);
    return serverProcess;
  }

  private static QuickstartServerProcess launch(List<String> command, File processDir, int port)
      throws IOException {
    Files.createDirectories(processDir.toPath());
    Files.createDirectories(new File(processDir, "tmp").toPath());
    File logDir = new File(processDir, "logs");
    Files.createDirectories(logDir.toPath());
    File logFile = new File(processDir, "server.log");
    File startedFile = new File(processDir, STARTED_FILE_NAME);
    Files.deleteIfExists(startedFile.toPath());
    ProcessBuilder processBuilder = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(logFile);
    processBuilder.environment().put("LOG_ROOT", logDir.getAbsolutePath());
    processBuilder.environment().put("PINOT_COMPONENT", "server");
    Process process = processBuilder.start();
    return new QuickstartServerProcess(process, logFile, startedFile, port);
  }

  void awaitReady()
      throws Exception {
    awaitReady(STARTUP_TIMEOUT);
  }

  private void awaitReady(Duration timeout)
      throws Exception {
    try {
      waitForReadiness(timeout);
    } catch (Exception e) {
      try {
        stop();
      } catch (Exception stopException) {
        e.addSuppressed(stopException);
      }
      addLogTail(e);
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw e;
    }
  }

  private void waitForReadiness(Duration timeout)
      throws IOException, InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    URI healthUri = URI.create("http://127.0.0.1:" + _port + "/health/readiness");
    while (System.nanoTime() < deadline) {
      if (!_process.isAlive()) {
        throw new IOException("Quickstart server exited with code " + _process.exitValue() + "; see logs in "
            + _logFile.getParentFile());
      }
      // An existing service on the same port must not make a child that has not finished starting look ready.
      if (!_startedFile.isFile()) {
        Thread.sleep(200);
        continue;
      }
      HttpURLConnection connection = (HttpURLConnection) healthUri.toURL().openConnection();
      connection.setConnectTimeout(1000);
      connection.setReadTimeout(1000);
      try {
        if (connection.getResponseCode() == HttpURLConnection.HTTP_OK) {
          try (InputStream response = connection.getInputStream()) {
            if ("OK".equals(new String(response.readNBytes(3), StandardCharsets.UTF_8).trim()) && _process.isAlive()) {
              return;
            }
          }
        }
      } catch (IOException e) {
        // The admin listener may not be bound yet; retry until readiness or the startup deadline.
      } finally {
        connection.disconnect();
      }
      Thread.sleep(200);
    }
    throw new IOException("Timed out waiting for quickstart server readiness at " + healthUri + "; see logs in "
        + _logFile.getParentFile());
  }

  private void addLogTail(Exception failure) {
    // Keep useful diagnostics in the exception even if the quickstart's shutdown hook removes its working directory.
    addLogTail(failure, _logFile);
    File runtimeLog = new File(_logFile.getParentFile(), "logs/pinot-server.log");
    if (runtimeLog.isFile()) {
      addLogTail(failure, runtimeLog);
    }
  }

  private static void addLogTail(Exception failure, File logFile) {
    try (RandomAccessFile log = new RandomAccessFile(logFile, "r")) {
      int length = (int) Math.min(log.length(), 8192);
      if (length > 0) {
        byte[] bytes = new byte[length];
        log.seek(log.length() - length);
        log.readFully(bytes);
        failure.addSuppressed(new IOException("Quickstart server log tail from " + logFile + ":\n"
            + new String(bytes, StandardCharsets.UTF_8)));
      }
    } catch (IOException e) {
      failure.addSuppressed(e);
    }
  }

  Process getProcess() {
    return _process;
  }

  synchronized void stop()
      throws IOException, InterruptedException {
    if (!_process.isAlive()) {
      return;
    }
    _process.destroy();
    try {
      if (!_process.waitFor(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        _process.destroyForcibly();
        if (!_process.waitFor(FORCED_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
          throw new IOException("Quickstart server did not exit after forced shutdown; see " + _logFile);
        }
      }
    } catch (InterruptedException e) {
      _process.destroyForcibly();
      try {
        _process.waitFor(FORCED_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      } catch (InterruptedException interruptedAgain) {
        e.addSuppressed(interruptedAgain);
      } finally {
        Thread.currentThread().interrupt();
      }
      throw e;
    }
  }

  static void watchParent() {
    Thread.ofPlatform().daemon().name("quickstart-parent-watch").start(() -> {
      try {
        while (System.in.read() != -1) {
          // The parent keeps this pipe open for the lifetime of the server.
        }
        System.exit(0);
      } catch (IOException e) {
        System.exit(1);
      }
    });
  }

  public static void main(String[] args) {
    watchParent();
    PinotAdministrator.main(new String[]{"StartServer", "-configFile", args[0]});
    try {
      Files.createFile(new File(args[1]).toPath());
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(1);
    }
  }
}
