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
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.tools.Command;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class for command to stop a process.
 * Relies on pid written by process, and works on local-host only.
 *
 *
 */
public class StopProcessCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(StopProcessCommand.class);

  @Option(name = "-controller", required = false, usage = "Stop the PinotController process.")
  private boolean _controller = false;

  @Option(name = "-server", required = false, usage = "Stop the PinotServer process.")
  private boolean _server = false;

  @Option(name = "-broker", required = false, usage = "Stop the PinotBroker process.")
  private boolean _broker = false;

  @Option(name = "-zooKeeper", required = false, usage = "Stop the ZooKeeper process.")
  private boolean _zooKeeper = false;

  @Option(name = "-kafka", required = false, usage = "Stop the Kafka process.")
  private boolean _kafka = false;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"}, usage = "Stop the PinotServer.")
  private boolean _help = false;

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

  @Override
  public boolean execute()
      throws Exception {
    LOGGER.info("Executing command: " + toString());

    Map<String, String> processes = new HashMap<String, String>();
    String prefix = System.getProperty("java.io.tmpdir") + File.separator;
    File tempDir = new File(System.getProperty("java.io.tmpdir"));

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

    if (_kafka) {
      processes.put("Kafka", prefix + ".kafka.pid");
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
  public boolean getHelp() {
    return _help;
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
}
