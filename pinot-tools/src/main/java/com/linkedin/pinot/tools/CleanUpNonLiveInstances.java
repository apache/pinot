/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.tools;

import java.util.HashSet;
import java.util.Set;
import org.apache.helix.manager.zk.ZkClient;
import org.kohsuke.args4j.Option;


public class CleanUpNonLiveInstances extends AbstractBaseCommand implements Command {
  @Option(name = "-zkAddress", required = true, metaVar = "<string>", usage = "Address of the Zookeeper (host:port)")
  private String _zkAddress;

  @Option(name = "-clusterName", required = true, metaVar = "<string>", usage = "Pinot cluster name")
  private String _clusterName;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h",
      "--help"}, usage = "Print this message.")
  private boolean _help = false;

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return getClass().getSimpleName();
  }

  @Override
  public String description() {
    return "Clean up instances that are not in LIVEINSTANCES from INSTANCES and CONFIGS/PARTICIPANT. Use with caution!";
  }

  @Override
  public boolean execute() throws Exception {
    ZkClient zkClient = new ZkClient(_zkAddress + "/" + _clusterName);
    Set<String> liveInstances = new HashSet<>(zkClient.getChildren("/LIVEINSTANCES"));
    for (String instance : zkClient.getChildren("/INSTANCES")) {
      if (!liveInstances.contains(instance)) {
        System.out.println("Removing instance: " + instance + " from /INSTANCES");
        zkClient.deleteRecursively("/INSTANCES/" + instance);
      }
    }
    for (String instance : zkClient.getChildren("/CONFIGS/PARTICIPANT")) {
      if (!liveInstances.contains(instance)) {
        System.out.println("Removing instance: " + instance + " from /CONFIGS/PARTICIPANT");
        zkClient.delete("/CONFIGS/PARTICIPANT/" + instance);
      }
    }
    return true;
  }

  public static void main(String[] args) throws Exception {
    CleanUpNonLiveInstances cleanUpNonLiveInstances = new CleanUpNonLiveInstances();
    cleanUpNonLiveInstances._zkAddress = "localhost:2191";
    cleanUpNonLiveInstances._clusterName = "pinot";
    cleanUpNonLiveInstances.execute();
  }
}
