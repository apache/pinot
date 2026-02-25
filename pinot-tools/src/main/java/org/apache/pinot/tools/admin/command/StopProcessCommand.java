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
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.tools.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


/**
 * Class for command to stop a process.
 * Relies on pid written by process, and works on local-host only.
 *
 *
 */
@CommandLine.Command(name = "StopProcess", mixinStandardHelpOptions = true)
public class StopProcessCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(StopProcessCommand.class);
  private static final String QUICKSTART_KAFKA_CONTAINER_PREFIX = "pinot-qs-kafka-";
  private static final int PORT_RELEASE_TIMEOUT_SECONDS = 30;
  private static final long PORT_RELEASE_POLL_INTERVAL_MS = 200L;
  private static final int PROCESS_TIMEOUT_SECONDS = 60;

  @CommandLine.Option(names = {"-controller"}, required = false, description = "Stop the PinotController process.")
  private boolean _controller = false;

  @CommandLine.Option(names = {"-server"}, required = false, description = "Stop the PinotServer process.")
  private boolean _server = false;

  @CommandLine.Option(names = {"-broker"}, required = false, description = "Stop the PinotBroker process.")
  private boolean _broker = false;

  @CommandLine.Option(names = {"-zooKeeper"}, required = false, description = "Stop the ZooKeeper process.")
  private boolean _zooKeeper = false;

  @CommandLine.Option(names = {"-kafka"}, required = false, description = "Stop the Kafka process.")
  private boolean _kafka = false;

  public StopProcessCommand() {
  }

  public StopProcessCommand(boolean noCleapUpRequired) {
    super(false);
  }

  @Override
  public String description() {
    return "Stop the specified processes.";
  }

  public StopProcessCommand stopController() {
    _controller = true;
    return this;
  }

  public StopProcessCommand stopBroker() {
    _broker = true;
    return this;
  }

  public StopProcessCommand stopServer() {
    _server = true;
    return this;
  }

  public StopProcessCommand stopZookeeper() {
    _zooKeeper = true;
    return this;
  }

  public StopProcessCommand stopKafka() {
    _kafka = true;
    return this;
  }

  @Override
  public boolean execute()
      throws Exception {
    LOGGER.info("Executing command: {}", toString());

    Map<String, String> processes = new LinkedHashMap<>();
    String prefix = System.getProperty("java.io.tmpdir") + File.separator;
    File tempDir = new File(System.getProperty("java.io.tmpdir"));

    if (_kafka) {
      stopManagedQuickstartKafkaContainers();
      String kafkaPidFile = prefix + ".kafka.pid";
      if (new File(kafkaPidFile).exists()) {
        processes.put("Kafka", kafkaPidFile);
      }
    }

    if (_server) {
      File[] serverFiles = tempDir.listFiles(new FilenameFilter() {

        @Override
        public boolean accept(File dir, String name) {
          if (StringUtils.containsIgnoreCase(name, "pinotAdminServer")) {
            return true;
          }
          return false;
        }
      });

      for (File serverFile : serverFiles) {
        processes.put(serverFile.getName(), serverFile.getAbsolutePath());
      }
    }

    if (_broker) {
      File[] serverFiles = tempDir.listFiles(new FilenameFilter() {

        @Override
        public boolean accept(File dir, String name) {
          if (StringUtils.containsIgnoreCase(name, "pinotAdminBroker")) {
            return true;
          }
          return false;
        }
      });

      for (File serverFile : serverFiles) {
        processes.put(serverFile.getName(), serverFile.getAbsolutePath());
      }
    }

    if (_controller) {
      File[] serverFiles = tempDir.listFiles(new FilenameFilter() {

        @Override
        public boolean accept(File dir, String name) {
          if (StringUtils.containsIgnoreCase(name, "pinotAdminController")) {
            return true;
          }
          return false;
        }
      });

      for (File serverFile : serverFiles) {
        processes.put(serverFile.getName(), serverFile.getAbsolutePath());
      }
    }

    if (_zooKeeper) {
      processes.put("Zookeeper", prefix + ".zooKeeper.pid");
    }

    boolean ret = true;
    for (Map.Entry<String, String> entry : processes.entrySet()) {
      try {
        stopProcess(entry.getValue());
      } catch (Exception e) {
        System.out.println("Failed to stop process: " + entry.getKey() + ": " + e);
        ret = false;
      }
    }

    return ret;
  }

  @Override
  public String getName() {
    return "StopProcess";
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder("StopProcess");
    if (_controller) {
      stringBuilder.append(" -controller");
    }

    if (_server) {
      stringBuilder.append(" -server");
    }

    if (_broker) {
      stringBuilder.append(" -broker");
    }

    if (_zooKeeper) {
      stringBuilder.append(" -zooKeeper");
    }

    if (_kafka) {
      stringBuilder.append(" -kafka");
    }

    return stringBuilder.toString();
  }

  @Override
  public void cleanup() {
  }

  private boolean stopProcess(String fileName)
      throws IOException {
    File file = new File(fileName);
    FileReader reader = new FileReader(file);
    int pid = reader.read();

    Runtime.getRuntime().exec("kill " + pid);
    reader.close();

    file.delete();
    return true;
  }

  private void stopManagedQuickstartKafkaContainers() {
    try {
      List<String> containers = runProcess(List.of("docker", "ps", "-a",
          "--filter", "name=" + QUICKSTART_KAFKA_CONTAINER_PREFIX,
          "--format", "{{.Names}}"));
      Set<Integer> publishedPorts = new HashSet<>();
      for (String container : containers) {
        if (!container.isBlank()) {
          publishedPorts.addAll(getPublishedPorts(container));
          LOGGER.info("Stopping managed quickstart Kafka container: {}", container);
          runProcess(List.of("docker", "rm", "-f", container));
        }
      }
      waitForPortsReleased(publishedPorts);
    } catch (Exception e) {
      LOGGER.warn("Failed to stop managed quickstart Kafka containers", e);
    }
  }

  private static Set<Integer> getPublishedPorts(String containerName)
      throws Exception {
    Set<Integer> ports = new HashSet<>();
    for (String line : runProcess(List.of("docker", "port", containerName))) {
      int lastColon = line.lastIndexOf(':');
      if (lastColon < 0 || lastColon == line.length() - 1) {
        continue;
      }
      String portToken = line.substring(lastColon + 1).trim();
      if (portToken.isEmpty()) {
        continue;
      }
      try {
        ports.add(Integer.parseInt(portToken));
      } catch (NumberFormatException ignored) {
        // Ignore malformed output lines and continue parsing remaining mappings.
      }
    }
    return ports;
  }

  private static void waitForPortsReleased(Set<Integer> ports)
      throws Exception {
    if (ports.isEmpty()) {
      return;
    }
    long deadlineMs = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(PORT_RELEASE_TIMEOUT_SECONDS);
    while (System.currentTimeMillis() < deadlineMs) {
      boolean allReleased = true;
      for (int port : ports) {
        if (!NetUtils.available(port)) {
          allReleased = false;
          break;
        }
      }
      if (allReleased) {
        return;
      }
      Thread.sleep(PORT_RELEASE_POLL_INTERVAL_MS);
    }
    throw new IllegalStateException("Kafka ports are still in use after stop timeout: " + ports);
  }

  private static List<String> runProcess(List<String> command)
      throws Exception {
    Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
    if (!process.waitFor(PROCESS_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
      process.destroyForcibly();
      process.waitFor(PROCESS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      String timeoutOutput = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
      throw new IllegalStateException("Command timed out after " + PROCESS_TIMEOUT_SECONDS + "s: "
          + String.join(" ", command) + (timeoutOutput.isEmpty() ? "" : "\n" + timeoutOutput));
    }
    String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    int code = process.exitValue();
    if (code != 0) {
      throw new IllegalStateException("Command failed (" + code + "): " + String.join(" ", command)
          + (output.isBlank() ? "" : "\n" + output.trim()));
    }
    return output.lines().collect(Collectors.toList());
  }
}
