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
package org.apache.pinot.tools;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.QuickstartRunner;


public class EmptyQuickstart extends QuickStartBase {
  @Override
  public List<String> types() {
    return Arrays.asList("EMPTY", "DEFAULT");
  }

  public String getAuthToken() {
    return null;
  }

  public Map<String, Object> getConfigOverrides() {
    return null;
  }

  public void execute()
      throws Exception {
    File quickstartTmpDir = new File(_dataDir.getAbsolutePath());
    File dataDir = new File(quickstartTmpDir, "rawdata");
    if (!dataDir.mkdirs()) {
      printStatus(Quickstart.Color.YELLOW, "***** Bootstrapping data from existing directory *****");
    } else {
      printStatus(Quickstart.Color.YELLOW, "***** Creating new data directory for fresh installation *****");
    }

    QuickstartRunner runner =
        new QuickstartRunner(new ArrayList<>(), 1, 1, 1, 0,
            dataDir, true, getAuthToken(), getConfigOverrides(), _zkExternalAddress, false);

    if (_zkExternalAddress != null) {
      printStatus(Quickstart.Color.CYAN, "***** Starting controller, broker and server *****");
    } else {
      printStatus(Quickstart.Color.CYAN, "***** Starting Zookeeper, controller, broker and server *****");
    }

    runner.startAll();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        printStatus(Quickstart.Color.GREEN, "***** Shutting down empty quick start *****");
        runner.stop();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }));

    waitForBootstrapToComplete(runner);

    printStatus(Quickstart.Color.YELLOW, "***** Empty quickstart setup complete *****");
    printStatus(Quickstart.Color.GREEN,
        "You can always go to http://localhost:9000 to play around in the query console");
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "EMPTY"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }
}
