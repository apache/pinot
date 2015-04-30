/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.tools.admin.command;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.kohsuke.args4j.Option;

/**
 * Class for command to stop a process.
 * Relies on pid written by process, and works on local-host only.
 *
 * @author Mayank Shrivastava <mshrivastava@linkedin.com>
 *
 */
public class StopProcessCommand extends AbstractBaseCommand implements Command {
  @Option(name="-controller", required=false, usage="Stop the PinotController process.")
  private boolean _controller = false;

  @Option(name="-server", required=false, usage="Stop the PinotServer process.")
  private boolean _server = false;

  @Option(name="-broker", required=false, usage="Stop the PinotBroker process.")
  private boolean _broker = false;

  @Option(name="-zooKeeper", required=false, usage="Stop the ZooKeeper process.")
  private boolean _zooKeeper = false;

  @Option(name="-help", required=false, usage="Stop the PinotServer.")
  private boolean _help = false;

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
  public boolean execute() throws Exception {
    Map<String, String> processes = new HashMap<String, String>();

    if (_controller) {
      processes.put("PinotController", "/tmp/.pinotAdminController.pid");
    }

    if (_server) {
      processes.put("PinotServer", "/tmp/.pinotAdminServer.pid");
    }

    if (_broker) {
      processes.put("PinotBroker", "/tmp/.pinotAdminBroker.pid");
    }

    if (_zooKeeper) {
      processes.put("Zookeeper", "/tmp/.zooKeeper.pid");
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

    return stringBuilder.toString();
  }

  @Override
  public void cleanup() {

  }

  private boolean stopProcess(String fileName) throws IOException {
    File file = new File(fileName);
    FileReader reader = new FileReader(file);
    int pid = reader.read();

    Runtime.getRuntime().exec("kill " + pid);
    reader.close();

    file.delete();
    return true;
  }
}
