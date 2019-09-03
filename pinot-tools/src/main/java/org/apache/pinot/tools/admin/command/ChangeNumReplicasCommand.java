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

import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.PinotNumReplicaChanger;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ChangeNumReplicasCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(StartBrokerCommand.class);

  @Option(name = "-zkAddress", required = false, metaVar = "<http>", usage = "HTTP address of Zookeeper.")
  private String _zkAddress = DEFAULT_ZK_ADDRESS;

  @Option(name = "-clusterName", required = false, metaVar = "<String>", usage = "Pinot cluster name.")
  private String _clusterName = "PinotCluster";

  @Option(name = "-tableName", required = true, metaVar = "<String>", usage = "Table name to rebalance (e.g. myTable_OFFLINE)")
  private String _tableName;

  @Option(name = "-exec", required = false, metaVar = "<boolean>", usage = "Execute command (Run the replica changer)")
  private boolean _exec;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help = false;

  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return "ChangeNumReplicas";
  }

  @Override
  public boolean execute()
      throws Exception {
    boolean _dryRun = !_exec;
    PinotNumReplicaChanger replicaChanger = new PinotNumReplicaChanger(_zkAddress, _clusterName, _dryRun);
    replicaChanger.changeNumReplicas(_tableName);
    if (_dryRun) {
      LOGGER.info("That was a dryrun");
      LOGGER.info("Use the -exec option to actually execute the command");
    }
    return true;
  }

  @Override
  public String description() {
    return "Re-writes idealState to reflect the value of numReplicas in table config";
  }
}
