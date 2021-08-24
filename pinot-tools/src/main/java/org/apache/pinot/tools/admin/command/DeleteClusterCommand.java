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

import org.apache.helix.manager.zk.ZkClient;
import org.apache.pinot.tools.Command;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DeleteClusterCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(DeleteClusterCommand.class);

  @Option(name = "-clusterName", required = true, metaVar = "<String>", usage = "Pinot cluster name.")
  private String _clusterName = "PinotCluster";

  @Option(name = "-zkAddress", required = false, metaVar = "<http>", usage = "Http address of Zookeeper.")
  private String _zkAddress = DEFAULT_ZK_ADDRESS;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"},
      usage = "Print this message.")
  private boolean _help = false;

  @Override
  public String toString() {
    return ("DeleteCluster -clusterName " + _clusterName + " -zkAddress " + _zkAddress);
  }

  @Override
  public String description() {
    return "Remove the Pinot Cluster from Helix.";
  }

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return "DeleteCluster";
  }

  @Override
  public void cleanup() {

  }

  public DeleteClusterCommand setClusterName(String clusterName) {
    _clusterName = clusterName;
    return this;
  }

  public DeleteClusterCommand setZkAddress(String zkAddress) {
    _zkAddress = zkAddress;
    return this;
  }

  @Override
  public boolean execute()
      throws Exception {
    LOGGER.info("Connecting to Zookeeper at address: {}", _zkAddress);
    ZkClient zkClient = new ZkClient(_zkAddress, 5000);
    String helixClusterName = "/" + _clusterName;

    LOGGER.info("Executing command: " + toString());
    if (!zkClient.exists(helixClusterName)) {
      LOGGER.error("Cluster {} does not exist.", _clusterName);
      return false;
    }

    zkClient.deleteRecursive(helixClusterName);
    return true;
  }
}
