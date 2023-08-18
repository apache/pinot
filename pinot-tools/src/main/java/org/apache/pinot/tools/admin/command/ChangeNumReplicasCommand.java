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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


@CommandLine.Command(name = "ChangeNumReplicas", description = "Re-writes idealState to reflect the value of "
                                                               + "numReplicas in table config",
    mixinStandardHelpOptions = true)
public class ChangeNumReplicasCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(StartBrokerCommand.class);

  @CommandLine.Option(names = {"-zkAddress"}, required = false, description = "HTTP address of Zookeeper.")
  private String _zkAddress = DEFAULT_ZK_ADDRESS;

  @CommandLine.Option(names = {"-clusterName"}, required = false, description = "Pinot cluster name.")
  private String _clusterName = "PinotCluster";

  @CommandLine.Option(names = {"-tableName"}, required = true,
      description = "Table name to rebalance (e.g. myTable_OFFLINE)")
  private String _tableName;

  @CommandLine.Option(names = {"-exec"}, required = false, description = "Execute command (Run the replica changer)")
  private boolean _exec;

  @Override
  public String getName() {
    return "ChangeNumReplicas";
  }

  @Override
  public boolean execute()
      throws Exception {
    boolean dryRun = !_exec;
    PinotNumReplicaChanger replicaChanger = new PinotNumReplicaChanger(_zkAddress, _clusterName, dryRun);
    replicaChanger.changeNumReplicas(_tableName);
    if (dryRun) {
      LOGGER.info("That was a dryrun");
      LOGGER.info("Use the -exec option to actually execute the command");
    }
    return true;
  }
}
